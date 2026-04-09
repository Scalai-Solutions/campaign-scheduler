'use strict';

const axios = require('axios');
const logger = require('../utils/logger');

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://localhost:3004';
const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://localhost:3000';
const INTERNAL_HEADER = { 'x-internal-service': 'campaign-scheduler' };

// Cache for AI-selected relevant fields per agent (same pattern as MCP server)
const _fieldSelectionCache = {};

function withTimeout(promise, ms) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Timeout after ${ms}ms`)), ms);
    })
  ]).finally(() => clearTimeout(timer));
}

/**
 * Get AI-selected relevant fields for a HubSpot object type, based on the agent's prompt.
 * Results are cached per agentId + objectType.
 */
async function getRelevantFields(subaccountId, agentId, objectType) {
  const cacheKey = `${agentId}:${objectType}`;
  if (_fieldSelectionCache[cacheKey]) return _fieldSelectionCache[cacheKey];

  try {
    // Fetch agent prompt via internal endpoint (no auth required)
    const { data: agentData } = await axios.get(
      `${DATABASE_SERVER_URL}/internal/agents/${subaccountId}/${agentId}/prompt`,
      { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' } }
    );
    const prompt = agentData?.data?.prompt || agentData?.prompt;
    if (!prompt) return null;

    const { data } = await axios.post(
      `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/select-relevant-fields`,
      { agentPrompt: prompt, objectType },
      { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' } }
    );
    if (data.success && data.data?.fields?.length > 0) {
      _fieldSelectionCache[cacheKey] = data.data.fields;
      logger.info('Prefetch field selection resolved', { objectType, agentId, fieldCount: data.data.fields.length });
      return data.data.fields;
    }
  } catch (err) {
    logger.warn('Prefetch field selection failed, using defaults', { objectType, agentId, error: err.message });
  }
  return null;
}

async function prefetchCallerContext(subaccountId, phoneNumber, agentId) {
  const params = new URLSearchParams();
  if (phoneNumber) params.append('phone_number', phoneNumber);
  if (subaccountId) params.append('subaccountId', subaccountId);
  if (agentId) params.append('agent_id', agentId);
  params.append('history_limit', '3');

  const { data } = await axios.get(
    `${CONNECTOR_SERVER_URL}/api/call-insights/context?${params}`,
    { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' } }
  );

  if (!data.success) return null;

  const d = data.data || data;
  return {
    currentTime: d.currentTime || data.currentTime,
    calendarInfo: d.calendarInfo,
    callerName: d.callerName || data.callerName,
    callCount: d.callHistory?.count || data.callCount || 0,
    chatCount: d.chatHistory?.count || data.chatCount || 0,
    history: d.callHistory?.calls || data.history || [],
    chatHistory: d.chatHistory?.chats || data.chatHistory || [],
    appointmentCount: d.appointments?.count || data.appointmentCount || 0,
    appointments: d.appointments?.appointments || data.appointments || []
  };
}

const CONTACT_DEFAULT_FIELDS = ['firstname', 'lastname', 'email', 'company', 'phone', 'associatedcompanyid'];

async function prefetchHubSpotContact(subaccountId, phoneNumber, contactFields) {
  if (!phoneNumber) return null;

  const baseFields = ['firstname', 'lastname', 'associatedcompanyid', 'company'];
  const fields = contactFields
    ? [...new Set([...baseFields, ...contactFields])]
    : CONTACT_DEFAULT_FIELDS;

  const { data } = await axios.post(
    `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/identify-contact`,
    { phone: phoneNumber, properties: fields },
    { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' } }
  );

  if (!data.success) return null;
  return data.data || data;
}

async function prefetchHubSpotCompany(subaccountId, contactData, phoneNumber, companyFields) {
  const props = contactData?.properties || {};
  const headers = { ...INTERNAL_HEADER, 'Content-Type': 'application/json' };
  const searchUrl = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/search-company`;
  const properties = companyFields || undefined;

  const assocId = props.associatedcompanyid;
  if (assocId) {
    try {
      const { data } = await axios.post(searchUrl,
        { searchField: 'hs_object_id', searchValue: String(assocId), limit: 1, properties },
        { headers });
      const companies = data.data?.companies || data.companies || [];
      if (companies.length) return companies[0];
    } catch (_) { /* fall through */ }
  }

  if (props.company) {
    try {
      const { data } = await axios.post(searchUrl,
        { searchField: 'name', searchValue: props.company, limit: 1, properties },
        { headers });
      const companies = data.data?.companies || data.companies || [];
      if (companies.length) return companies[0];
    } catch (_) { /* fall through */ }
  }

  const phone = phoneNumber || props.phone;
  if (phone) {
    try {
      const { data } = await axios.post(searchUrl,
        { searchField: 'phone', searchValue: phone, limit: 1, properties },
        { headers });
      const companies = data.data?.companies || data.companies || [];
      if (companies.length) return companies[0];
    } catch (_) { /* fall through */ }
  }

  return null;
}

async function prefetchAll(subaccountId, phoneNumber, agentId, opts = {}) {
  const { timeoutMs = 8000 } = opts;

  // Fetch AI-selected relevant fields for this agent (cached after first call)
  let contactFields = null;
  let companyFields = null;
  if (agentId && subaccountId) {
    try {
      const [cf, cpf] = await Promise.allSettled([
        withTimeout(getRelevantFields(subaccountId, agentId, 'contacts'), 3000),
        withTimeout(getRelevantFields(subaccountId, agentId, 'companies'), 3000)
      ]);
      contactFields = cf.status === 'fulfilled' ? cf.value : null;
      companyFields = cpf.status === 'fulfilled' ? cpf.value : null;
    } catch (_) { /* use defaults */ }
  }

  const [callerCtx, hubspotContact] = await Promise.allSettled([
    withTimeout(prefetchCallerContext(subaccountId, phoneNumber, agentId), timeoutMs),
    withTimeout(prefetchHubSpotContact(subaccountId, phoneNumber, contactFields), timeoutMs)
  ]);

  const callerContext = callerCtx.status === 'fulfilled' ? callerCtx.value : null;
  const contact = hubspotContact.status === 'fulfilled' ? hubspotContact.value : null;

  if (callerCtx.status === 'rejected') {
    logger.warn('Prefetch caller context failed', { subaccountId, error: callerCtx.reason?.message });
  }
  if (hubspotContact.status === 'rejected') {
    logger.warn('Prefetch HubSpot contact failed', { subaccountId, error: hubspotContact.reason?.message });
  }

  let company = null;
  try {
    company = await withTimeout(
      prefetchHubSpotCompany(subaccountId, contact, phoneNumber, companyFields),
      Math.max(timeoutMs - 3000, 2000)
    );
  } catch (err) {
    logger.warn('Prefetch HubSpot company failed', { subaccountId, error: err.message });
  }

  return { callerContext, contact, company };
}

/**
 * Format prefetch results as flat key-value dynamic variables.
 * Contact and company properties are flattened with prefixes instead of raw JSON dumps,
 * matching the selective field approach used by MCP tool calls.
 */
function formatAsDynamicVariables(results) {
  const vars = {};

  if (results.callerContext) {
    const ctx = results.callerContext;
    if (ctx.currentTime) vars.prefetch_current_time = ctx.currentTime;
    if (ctx.calendarInfo) vars.prefetch_calendar_info = typeof ctx.calendarInfo === 'string' ? ctx.calendarInfo : JSON.stringify(ctx.calendarInfo);
    if (ctx.callerName) vars.caller_name = ctx.callerName;
    vars.prefetch_call_count = String(ctx.callCount || 0);
    vars.prefetch_chat_count = String(ctx.chatCount || 0);
    if (ctx.history?.length) vars.prefetch_call_history = JSON.stringify(ctx.history);
    if (ctx.chatHistory?.length) vars.prefetch_chat_history = JSON.stringify(ctx.chatHistory);
    vars.prefetch_appointment_count = String(ctx.appointmentCount || 0);
    if (ctx.appointments?.length) vars.prefetch_appointments = JSON.stringify(ctx.appointments);
  }

  if (results.contact) {
    const props = results.contact.properties || {};
    if (results.contact.contactId) vars.prefetch_contact_id = String(results.contact.contactId);
    for (const [key, value] of Object.entries(props)) {
      if (value != null && value !== '') {
        vars[`prefetch_contact_${key}`] = String(value);
      }
    }
    if (!vars.caller_name) {
      const firstName = props.firstname || '';
      const lastName = props.lastname || '';
      const name = [firstName, lastName].filter(Boolean).join(' ');
      if (name) vars.caller_name = name;
    }
  }

  if (results.company) {
    const props = results.company.properties || {};
    if (results.company.companyId) vars.prefetch_company_id = String(results.company.companyId);
    for (const [key, value] of Object.entries(props)) {
      if (value != null && value !== '') {
        vars[`prefetch_company_${key}`] = String(value);
      }
    }
  }

  return vars;
}

/**
 * Prefetch caller context + HubSpot data for a batch of leads with concurrency control.
 */
async function prefetchBatch(leads, subaccountId, agentId, opts = {}) {
  const { timeoutMs = 8000 } = opts;
  const concurrency = parseInt(process.env.PREFETCH_CONCURRENCY || '5');
  const map = new Map();

  for (let i = 0; i < leads.length; i += concurrency) {
    const chunk = leads.slice(i, i + concurrency);
    const results = await Promise.allSettled(
      chunk.map(lead =>
        prefetchAll(subaccountId, lead.phone, agentId, { timeoutMs })
          .then(result => ({ leadId: lead._id.toString(), vars: formatAsDynamicVariables(result) }))
      )
    );
    for (const result of results) {
      if (result.status === 'fulfilled') {
        map.set(result.value.leadId, result.value.vars);
      }
    }
    logger.info('[Prefetch] Chunk complete', {
      chunk: Math.floor(i / concurrency) + 1,
      total: Math.ceil(leads.length / concurrency),
      resolved: results.filter(r => r.status === 'fulfilled').length,
      failed: results.filter(r => r.status === 'rejected').length
    });
  }

  return map;
}

module.exports = {
  prefetchCallerContext,
  prefetchHubSpotContact,
  prefetchHubSpotCompany,
  prefetchAll,
  formatAsDynamicVariables,
  prefetchBatch,
  getRelevantFields
};
