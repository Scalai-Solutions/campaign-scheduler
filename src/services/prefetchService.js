'use strict';

const axios = require('axios');
const logger = require('../utils/logger');

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://localhost:3004';
const INTERNAL_HEADER = { 'x-internal-service': 'campaign-scheduler' };

function withTimeout(promise, ms) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Timeout after ${ms}ms`)), ms);
    })
  ]).finally(() => clearTimeout(timer));
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
    success: true,
    currentTime: d.currentTime || data.currentTime,
    calendarInfo: d.calendarInfo,
    callerName: d.callerName || data.callerName,
    agentFilteringEnabled: d.agentFilteringEnabled || data.agentFilteringEnabled,
    callCount: d.callHistory?.count || data.callCount || 0,
    chatCount: d.chatHistory?.count || data.chatCount || 0,
    history: d.callHistory?.calls || data.history || [],
    chatHistory: d.chatHistory?.chats || data.chatHistory || [],
    appointmentCount: d.appointments?.count || data.appointmentCount || 0,
    appointments: d.appointments?.appointments || data.appointments || []
  };
}

async function prefetchHubSpotContact(subaccountId, phoneNumber) {
  if (!phoneNumber) return null;

  const { data } = await axios.post(
    `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/identify-contact`,
    { phone: phoneNumber, properties: ['firstname', 'lastname', 'email', 'company', 'phone', 'associatedcompanyid'] },
    { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' } }
  );

  if (!data.success) return null;
  return data.data || data;
}

async function prefetchHubSpotCompany(subaccountId, contactData, phoneNumber) {
  const props = contactData?.properties || {};
  const headers = { ...INTERNAL_HEADER, 'Content-Type': 'application/json' };
  const searchUrl = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/search-company`;

  const assocId = props.associatedcompanyid;
  if (assocId) {
    try {
      const { data } = await axios.post(searchUrl,
        { searchField: 'hs_object_id', searchValue: String(assocId), limit: 1 },
        { headers });
      const companies = data.data?.companies || data.companies || [];
      if (companies.length) return companies[0];
    } catch (_) { /* fall through */ }
  }

  if (props.company) {
    try {
      const { data } = await axios.post(searchUrl,
        { searchField: 'name', searchValue: props.company, limit: 1 },
        { headers });
      const companies = data.data?.companies || data.companies || [];
      if (companies.length) return companies[0];
    } catch (_) { /* fall through */ }
  }

  const phone = phoneNumber || props.phone;
  if (phone) {
    try {
      const { data } = await axios.post(searchUrl,
        { searchField: 'phone', searchValue: phone, limit: 1 },
        { headers });
      const companies = data.data?.companies || data.companies || [];
      if (companies.length) return companies[0];
    } catch (_) { /* fall through */ }
  }

  return null;
}

async function prefetchAll(subaccountId, phoneNumber, agentId, opts = {}) {
  const { timeoutMs = 8000 } = opts;

  const [callerCtx, hubspotContact] = await Promise.allSettled([
    withTimeout(prefetchCallerContext(subaccountId, phoneNumber, agentId), timeoutMs),
    withTimeout(prefetchHubSpotContact(subaccountId, phoneNumber), timeoutMs)
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
      prefetchHubSpotCompany(subaccountId, contact, phoneNumber),
      Math.max(timeoutMs - 3000, 2000)
    );
  } catch (err) {
    logger.warn('Prefetch HubSpot company failed', { subaccountId, error: err.message });
  }

  return { callerContext, contact, company };
}

function formatAsDynamicVariables(results) {
  const vars = {};

  if (results.callerContext) {
    vars.prefetched_caller_context = JSON.stringify(results.callerContext);
  }
  if (results.contact) {
    vars.prefetched_hubspot_contact = JSON.stringify(results.contact);
    const firstName = results.contact.properties?.firstname || results.contact.firstName || '';
    const lastName = results.contact.properties?.lastname || results.contact.lastName || '';
    const name = [firstName, lastName].filter(Boolean).join(' ');
    if (name) vars.caller_name = name;
  }
  if (results.company) {
    vars.prefetched_hubspot_company = JSON.stringify(results.company);
  }

  return vars;
}

/**
 * Prefetch caller context + HubSpot data for a batch of leads in parallel.
 *
 * Runs ALL leads simultaneously using Promise.allSettled (no chunking).
 * Each lead gets its own timeout via prefetchAll. A failure for any lead
 * produces {} (empty vars) so the call proceeds without prefetched data.
 *
 * @param {Array<{_id: ObjectId|string, phone: string}>} leads
 * @param {string} subaccountId
 * @param {string} agentId
 * @param {object} [opts]
 * @param {number} [opts.timeoutMs=8000]
 * @returns {Promise<Map<string, object>>} leadId → formatted dynamic variables
 */
async function prefetchBatch(leads, subaccountId, agentId, opts = {}) {
  const { timeoutMs = 8000 } = opts;

  const results = await Promise.allSettled(
    leads.map(lead =>
      prefetchAll(subaccountId, lead.phone, agentId, { timeoutMs })
        .then(result => ({ leadId: lead._id.toString(), vars: formatAsDynamicVariables(result) }))
    )
  );

  const map = new Map();
  for (const result of results) {
    if (result.status === 'fulfilled') {
      map.set(result.value.leadId, result.value.vars);
    }
    // rejected leads get no entry → callers use || {} fallback
  }

  return map;
}

module.exports = {
  prefetchCallerContext,
  prefetchHubSpotContact,
  prefetchHubSpotCompany,
  prefetchAll,
  formatAsDynamicVariables,
  prefetchBatch
};
