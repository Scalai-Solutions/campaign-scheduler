'use strict';

const axios = require('axios');
const logger = require('../utils/logger');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://localhost:3000';
const INTERNAL_HEADER = { 'x-internal-service': 'campaign-scheduler' };

const CACHE_TTL_MS = parseInt(process.env.PROGRAM_TYPE_CACHE_TTL_MS || String(10 * 60 * 1000), 10);
const _cache = new Map();

/**
 * Derive FPE/FUNDAE program type from agent name or id.
 * Returns 'FPE', 'FUNDAE', or null when indeterminate.
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

async function fetchAgentName(subaccountId, agentId) {
  if (!subaccountId || !agentId) return null;

  const { data } = await axios.get(
    `${DATABASE_SERVER_URL}/internal/agents/${subaccountId}/${agentId}/prompt`,
    { headers: { ...INTERNAL_HEADER, 'Content-Type': 'application/json' }, timeout: 8000 }
  );

  return data?.data?.agentName || data?.agentName || null;
}

async function resolveProgramType(subaccountId, agentId) {
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
  const programType = await resolveProgramType(subaccountId, agentId);
  return buildProgramTypeVariables(programType);
}

module.exports = {
  deriveProgramTypeFromAgentIdentity,
  resolveProgramType,
  buildProgramTypeVariables,
  buildProgramTypeVarsForAgent
};
