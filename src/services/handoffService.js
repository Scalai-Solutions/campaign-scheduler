'use strict';

const axios = require('axios');
const logger = require('../utils/logger');
const programTypeService = require('./programTypeService');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://localhost:3000';
const INTERNAL_HEADER = { 'x-internal-service': 'campaign-scheduler' };

const STANDARD_CONTEXT_KEYS = new Set([
  'phone_number', 'agent_id', 'subaccount_id', 'subaccountId', 'call_id', 'chat_id'
]);

function stringifyValue(value) {
  if (value == null) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value).trim();
}

/**
 * Extract flat candidate variables from a completed voice/chat Retell webhook payload.
 */
function extractNodeOutput(payload = {}, agentType = 'voice') {
  const isVoice = agentType === 'voice' || !!payload.call;
  const entity = isVoice ? (payload.call || payload) : (payload.chat || payload);
  const analysis = isVoice
    ? (entity.call_analysis || payload.call_analysis || {})
    : (entity.chat_analysis || payload.chat_analysis || {});

  const vars = {};

  const mergeEntry = (key, value, { prefix = 'handoff_' } = {}) => {
    const str = stringifyValue(value);
    if (!str) return;
    vars[`${prefix}${key}`] = str;
    if (!STANDARD_CONTEXT_KEYS.has(key)) {
      vars[key] = str;
    }
  };

  const collected = entity.collected_dynamic_variables || {};
  for (const [key, value] of Object.entries(collected)) {
    mergeEntry(`collected_${key}`, value);
    mergeEntry(key, value, { prefix: '' });
  }

  const custom = analysis.custom_analysis_data || {};
  for (const [key, value] of Object.entries(custom)) {
    mergeEntry(key, value);
  }

  for (const [key, value] of Object.entries(analysis)) {
    if (['custom_analysis_data', 'summary', 'call_summary', 'chat_summary'].includes(key)) continue;
    if (typeof value === 'object') continue;
    mergeEntry(key, value);
  }

  const summary = analysis.summary || analysis.call_summary || analysis.chat_summary;
  if (summary) mergeEntry('prior_summary', summary);

  if (entity.transcript) {
    mergeEntry('prior_transcript', String(entity.transcript).slice(0, 4000));
  }

  const priorDynamic = entity.retell_llm_dynamic_variables || {};
  for (const [key, value] of Object.entries(priorDynamic)) {
    if (key.startsWith('prefetch_') || key === 'caller_name') {
      mergeEntry(`prior_${key}`, value);
    }
  }

  if (entity.disconnection_reason) {
    mergeEntry('prior_disconnection_reason', entity.disconnection_reason);
  }

  const customerName =
    vars.customer_name ||
    vars.handoff_customer_name ||
    collected.customer_name ||
    collected.user_name ||
    custom.customer_name;
  if (customerName && !vars.caller_name) {
    vars.caller_name = customerName;
  }

  return vars;
}

/**
 * Ask database-server to map source node output onto target agent prompt placeholders.
 */
async function selectHandoffVariables(subaccountId, {
  sourceAgentId,
  targetAgentId,
  sourceAgentPrompt,
  targetAgentPrompt,
  availableVariables,
  sourceOutcome
}) {
  try {
    const { data } = await axios.post(
      `${DATABASE_SERVER_URL}/internal/campaign/${subaccountId}/select-handoff-variables`,
      {
        sourceAgentId,
        targetAgentId,
        sourceAgentPrompt,
        targetAgentPrompt,
        availableVariables,
        sourceOutcome
      },
      { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' }, timeout: 12000 }
    );

    if (data.success && data.data?.variables && typeof data.data.variables === 'object') {
      return data.data.variables;
    }
  } catch (err) {
    logger.warn('[Handoff] AI variable selection failed, using fallback mapping', {
      subaccountId, sourceAgentId, targetAgentId, error: err.message
    });
  }

  return fallbackHandoffMapping(targetAgentPrompt, availableVariables, sourceOutcome);
}

function extractPromptVariables(prompt = '') {
  const vars = new Set();
  for (const match of String(prompt).matchAll(/\{\{\s*([^}]+?)\s*\}\}/g)) {
    const name = match[1].trim();
    if (name) vars.add(name);
  }
  return [...vars];
}

function fallbackHandoffMapping(targetAgentPrompt, availableVariables, sourceOutcome) {
  const mapped = {};
  const placeholders = extractPromptVariables(targetAgentPrompt);

  for (const placeholder of placeholders) {
    if (STANDARD_CONTEXT_KEYS.has(placeholder)) continue;

    const candidates = [
      placeholder,
      `handoff_${placeholder}`,
      `handoff_collected_${placeholder}`,
      placeholder.replace(/^prefetch_/, 'handoff_prior_prefetch_'),
      'caller_name',
      'customer_name',
      'handoff_customer_name'
    ];

    for (const key of candidates) {
      const value = availableVariables[key];
      if (value != null && stringifyValue(value)) {
        mapped[placeholder] = stringifyValue(value);
        break;
      }
    }
  }

  if (sourceOutcome) mapped.handoff_prior_outcome = String(sourceOutcome);

  return mapped;
}

async function fetchAgentPrompt(subaccountId, agentId) {
  try {
    const { data } = await axios.get(
      `${DATABASE_SERVER_URL}/internal/agents/${subaccountId}/${agentId}/prompt`,
      { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' }, timeout: 8000 }
    );
    return data?.data?.prompt || data?.prompt || '';
  } catch (err) {
    logger.warn('[Handoff] Failed to fetch agent prompt', { subaccountId, agentId, error: err.message });
    return '';
  }
}

/**
 * Build dynamic variables to inject when the next workflow node starts.
 */
async function buildHandoffVariables({
  subaccountId,
  sourceAgentId,
  targetAgentId,
  payload,
  sourceAgentType = 'voice',
  sourceOutcome,
  priorAnalysis = null
}) {
  const availableVariables = extractNodeOutput(payload, sourceAgentType);

  if (priorAnalysis) {
    if (priorAnalysis.summary) availableVariables.handoff_prior_summary = stringifyValue(priorAnalysis.summary);
    if (priorAnalysis.customData && typeof priorAnalysis.customData === 'object') {
      for (const [key, value] of Object.entries(priorAnalysis.customData)) {
        availableVariables[`handoff_${key}`] = stringifyValue(value);
        if (!availableVariables[key]) availableVariables[key] = stringifyValue(value);
      }
    }
  }

  if (sourceOutcome) availableVariables.handoff_prior_outcome = String(sourceOutcome);
  if (sourceAgentId) availableVariables.handoff_prior_agent_id = String(sourceAgentId);

  const programType =
    (sourceAgentId && await programTypeService.resolveProgramType(subaccountId, sourceAgentId)) ||
    (targetAgentId && await programTypeService.resolveProgramType(subaccountId, targetAgentId));

  if (programType) {
    availableVariables.program_type = programType;
    availableVariables.handoff_program_type = programType;
  }

  const [sourcePrompt, targetPrompt] = await Promise.all([
    fetchAgentPrompt(subaccountId, sourceAgentId),
    fetchAgentPrompt(subaccountId, targetAgentId)
  ]);

  const selected = await selectHandoffVariables(subaccountId, {
    sourceAgentId,
    targetAgentId,
    sourceAgentPrompt: sourcePrompt,
    targetAgentPrompt: targetPrompt,
    availableVariables,
    sourceOutcome
  });

  logger.info('[Handoff] Built workflow handoff variables', {
    subaccountId,
    sourceAgentId,
    targetAgentId,
    sourceOutcome,
    availableCount: Object.keys(availableVariables).length,
    selectedCount: Object.keys(selected).length,
    selectedKeys: Object.keys(selected)
  });

  return {
    ...programTypeService.buildProgramTypeVariables(programType),
    ...selected
  };
}

module.exports = {
  extractNodeOutput,
  extractPromptVariables,
  fallbackHandoffMapping,
  buildHandoffVariables
};
