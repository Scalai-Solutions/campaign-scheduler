'use strict';

const axios = require('axios');
const logger = require('../utils/logger');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://localhost:3000';
const INTERNAL_HEADER = { 'x-internal-service': 'campaign-scheduler' };

const CACHE_TTL_MS = parseInt(process.env.PROGRAM_TYPE_CACHE_TTL_MS || String(10 * 60 * 1000), 10);
const PROGRAM_TYPE_KEYS = [
  'program_type',
  'contact_program',
  'campaign_program',
  'handoff_program_type',
  'handoff_collected_program_type',
  'collected_program_type',
  'prior_program_type',
  'handoff_prior_program_type'
];

const _cache = new Map();

function normalizeProgramType(value) {
  if (value == null) return null;
  const raw = String(value).trim().toUpperCase();
  if (!raw) return null;
  if (raw === 'FPE' || raw.includes('FPE')) return 'FPE';
  if (raw === 'FUNDAE' || raw.includes('FUNDAE')) return 'FUNDAE';
  return null;
}

/**
 * Derive FPE/FUNDAE program type from agent/campaign name text.
 */
function deriveProgramTypeFromAgentIdentity(agentName = '', agentId = '') {
  const nameUpper = String(agentName).toUpperCase();
  const idUpper = String(agentId).toUpperCase();
  const haystack = `${nameUpper} ${idUpper}`;

  const hasFpe = /\bFPE\b/.test(haystack);
  const hasFundae = /\bFUNDAE\b/.test(haystack);

  if (hasFpe && !hasFundae) return 'FPE';
  if (hasFundae && !hasFpe) return 'FUNDAE';

  if (hasFpe && hasFundae) {
    if (nameUpper.includes('FUNDAE')) return 'FUNDAE';
    if (nameUpper.includes('FPE')) return 'FPE';
  }

  return null;
}

function extractProgramTypeFromObject(obj = {}) {
  if (!obj || typeof obj !== 'object') return null;

  for (const key of PROGRAM_TYPE_KEYS) {
    const normalized = normalizeProgramType(obj[key]);
    if (normalized) return normalized;
  }

  for (const [key, value] of Object.entries(obj)) {
    if (!/program|fundae|fpe/i.test(key)) continue;
    const normalized = normalizeProgramType(value);
    if (normalized) return normalized;
  }

  if (obj.hubspot && typeof obj.hubspot === 'object') {
    const fromHubspot = extractProgramTypeFromObject(obj.hubspot);
    if (fromHubspot) return fromHubspot;
  }

  if (obj.properties && typeof obj.properties === 'object') {
    const fromProps = extractProgramTypeFromObject(obj.properties);
    if (fromProps) return fromProps;
  }

  return null;
}

/**
 * Resolve program_type from call/handoff variables with priority:
 * collected during call > post-call analysis > prior dispatch vars > lead/campaign > agent default.
 */
function resolveProgramTypeFromVariables(availableVariables = {}, {
  leadAttrs = null,
  campaignName = null,
  agentDefault = null
} = {}) {
  const fromVars = extractProgramTypeFromObject(availableVariables);
  if (fromVars) return fromVars;

  const fromLead = extractProgramTypeFromObject(leadAttrs || {});
  if (fromLead) return fromLead;

  if (campaignName) {
    const fromCampaign = deriveProgramTypeFromAgentIdentity(campaignName, '');
    if (fromCampaign) return fromCampaign;
  }

  return agentDefault || null;
}

async function fetchAgentName(subaccountId, agentId) {
  if (!subaccountId || !agentId) return null;

  const { data } = await axios.get(
    `${DATABASE_SERVER_URL}/internal/agents/${subaccountId}/${agentId}/prompt`,
    { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' }, timeout: 8000 }
  );

  return data?.data?.agentName || data?.agentName || null;
}

async function resolveProgramTypeFromAgent(subaccountId, agentId) {
  if (!agentId) return null;

  const cacheKey = `${subaccountId || 'unknown'}:${agentId}`;
  const cached = _cache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.programType;
  }

  let agentName = '';
  try {
    agentName = await fetchAgentName(subaccountId, agentId);
  } catch (err) {
    logger.warn('[ProgramType] Failed to fetch agent name', {
      subaccountId, agentId, error: err.message
    });
  }

  const programType = deriveProgramTypeFromAgentIdentity(agentName, agentId);

  _cache.set(cacheKey, {
    programType,
    expiresAt: Date.now() + CACHE_TTL_MS
  });

  return programType;
}

function buildProgramTypeVariables(programType) {
  if (!programType) return {};
  return { program_type: programType };
}

async function buildProgramTypeVarsForAgent(subaccountId, agentId) {
  const programType = await resolveProgramTypeFromAgent(subaccountId, agentId);
  return buildProgramTypeVariables(programType);
}

async function buildProgramTypeVarsForDispatch({
  subaccountId,
  agentId,
  lead = null,
  campaignName = null
}) {
  const agentDefault = await resolveProgramTypeFromAgent(subaccountId, agentId);
  const programType = resolveProgramTypeFromVariables({}, {
    leadAttrs: lead?.attrs,
    campaignName,
    agentDefault
  });
  return buildProgramTypeVariables(programType);
}

async function resolveProgramTypeForHandoff({
  subaccountId,
  sourceAgentId,
  targetAgentId,
  availableVariables = {},
  leadAttrs = null,
  campaignName = null
}) {
  const sourceDefault = sourceAgentId
    ? await resolveProgramTypeFromAgent(subaccountId, sourceAgentId)
    : null;
  const targetDefault = targetAgentId
    ? await resolveProgramTypeFromAgent(subaccountId, targetAgentId)
    : null;

  return resolveProgramTypeFromVariables(availableVariables, {
    leadAttrs,
    campaignName,
    agentDefault: sourceDefault || targetDefault
  });
}

module.exports = {
  PROGRAM_TYPE_KEYS,
  normalizeProgramType,
  deriveProgramTypeFromAgentIdentity,
  extractProgramTypeFromObject,
  resolveProgramTypeFromVariables,
  resolveProgramTypeFromAgent,
  resolveProgramTypeForHandoff,
  buildProgramTypeVariables,
  buildProgramTypeVarsForAgent,
  buildProgramTypeVarsForDispatch
};
