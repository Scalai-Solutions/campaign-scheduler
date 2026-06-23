/**
 * WhatsApp warmup gate client (campaign-scheduler side).
 *
 * Proactive campaign first-messages are the main outbound ban trigger on unofficial WAHA numbers,
 * so before each send we reserve a slot against the agent-number's ramping daily cap. The cap logic
 * lives in ONE place — connector-server's whatsappWarmupService — and we call it over the existing
 * in-network channel (CONNECTOR_SERVER_URL + x-internal-service header) rather than duplicating it.
 *
 * Opt-in via WA_WARMUP_GATE_ENABLED=true (default OFF → zero change to the current send path).
 * Fails OPEN on any error/timeout: a gate hiccup must never halt live campaigns — connector-server's
 * own per-send gate is the hard backstop for connector-initiated sends.
 */
const axios = require('axios');
const logger = require('../utils/logger');

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://connector-server:3004';
const ENABLED = process.env.WA_WARMUP_GATE_ENABLED === 'true';
const TIMEOUT_MS = parseInt(process.env.WA_WARMUP_GATE_TIMEOUT_MS || '6000', 10);

/**
 * Atomically check + reserve one proactive send slot for {tenantId, agentId}.
 * @returns {Promise<{allowed:boolean, gated:boolean, reason?:string, dailyLimit?:number, sentToday?:number, warmupDay?:number}>}
 */
async function reserve(tenantId, agentId) {
  if (!ENABLED) return { allowed: true, gated: false, disabled: true };
  if (!tenantId || !agentId) return { allowed: true, gated: false, reason: 'missing_ids' };
  try {
    const { data } = await axios.post(
      `${CONNECTOR_SERVER_URL}/api/internal/whatsapp/${tenantId}/warmup-reserve`,
      { agentId: String(agentId) },
      { headers: { 'x-internal-service': 'campaign-scheduler', 'Content-Type': 'application/json' }, timeout: TIMEOUT_MS }
    );
    return data || { allowed: true, gated: false, reason: 'empty_response' };
  } catch (err) {
    logger.warn('[WarmupGate] reserve failed — failing open', { tenantId, agentId, error: err.message });
    return { allowed: true, gated: false, reason: 'gate_error', error: err.message };
  }
}

module.exports = { reserve, ENABLED };
