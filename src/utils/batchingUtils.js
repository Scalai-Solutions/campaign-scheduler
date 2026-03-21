const crypto = require('crypto');

/**
 * Compute a hash of dispatch configuration (retry policy, timeout, rate limits, agent config)
 * Used to ensure only compatible leads are batched together
 */
function computeDispatchConfigHash(node, agentConfig) {
    const config = {
        maxRetries: node.dispatchPolicy?.maxRetries || 3,
        timeoutMs: node.dispatchPolicy?.timeoutMs || 600000,  // 10 min default
        rateLimitPerSecond: node.dispatchPolicy?.rateLimitPerSecond || 0, // 0 = unlimited
        priorityLevel: node.dispatchPolicy?.priorityLevel || 'normal',
        customCallbackUrl: node.dispatchPolicy?.customCallbackUrl || null,
        agentVersion: agentConfig?.version || '1.0',
        agentModel: agentConfig?.model || 'default',
        retellConfigHash: agentConfig?.configHash || 'unknown'
    };

    // Create stable JSON (sorted keys for deterministic hashing)
    const stableJson = JSON.stringify(config, Object.keys(config).sort());
    
    // Hash and truncate to 8 chars
    return crypto
        .createHash('sha256')
        .update(stableJson)
        .digest('hex')
        .slice(0, 8);
}

/**
 * Compute batch compatibility key for grouping leads
 * All leads with the same key CAN be dispatched together
 * Combines: tenant, campaign, version, node, agent, dispatch config, time bucket
 * 
 * @param {object} intent - Object with tenantId, campaignId, campaignVersion, nextNodeId, nextNodeAgentId, nextNodeAgentType
 * @param {string} dispatchConfigHash - Hash of dispatch policy
 * @param {number} resolvedDelayMs - Optional delay in milliseconds (for time bucketing)
 */
function computeBatchCompatibilityKey(intent, dispatchConfigHash, resolvedDelayMs = 0) {
    // Time bucket: 250ms granularity for immediate dispatch, separate buckets for delayed
    const now = Date.now();
    const startTimeMs = resolvedDelayMs > 0 ? now + resolvedDelayMs : now;
    const timeBucket = Math.floor(startTimeMs / 250);

    return [
        intent.tenantId,
        intent.campaignId,
        intent.campaignVersion,
        intent.nextNodeId,
        intent.nextNodeAgentId,
        intent.nextNodeAgentType,
        dispatchConfigHash,  // Dispatch policy must match exactly
        timeBucket           // Time window (250ms bucket)
    ].filter(v => v != null).join('|');
}

/**
 * Compute idempotency key for a NextStepIntent
 * Based on: step execution + outcome (unique per decision point)
 */
function computeIntentDedupeKey(stepExecutionId, outcome) {
    return `step:${stepExecutionId}:outcome:${outcome}`;
}

/**
 * Compute batch deduplication key
 * Based on: sorted array of intent IDs (so same set of intents always get same key)
 */
function computeBatchDedupeKey(intentIds) {
    const sorted = intentIds
        .map(id => id.toString())
        .sort()
        .join('|');

    return crypto
        .createHash('sha256')
        .update(sorted)
        .digest('hex')
        .slice(0, 8);
}

/**
 * Determine call outcome from Retell webhook payload
 * Prioritizes: unanswered > successful > unsuccessful
 */
function determineOutcome(payload) {
    const reason = payload.call?.disconnection_reason;
    const analysis = payload.call_analysis || {};

    // Tier 1: Check for unanswered (no connection)
    const unansweredReasons = [
        'dial_busy',
        'dial_failed',
        'dial_no_answer',
        'voicemail'
    ];
    if (unansweredReasons.includes(reason)) {
        return 'not_answered';
    }

    // Tier 2: Check for successful completion
    const isSuccessful =
        analysis.call_successful === true ||
        analysis.call_successful === 'true' ||
        analysis.custom_analysis_data?.call_successful === true ||
        analysis.call_completion_rating === 'Complete';

    // Tier 3: Default to unsuccessful
    return isSuccessful ? 'successful' : 'unsuccessful';
}

/**
 * Extract call analysis from webhook payload
 */
function extractAnalysis(payload) {
    return {
        summary: payload.call_analysis?.summary,
        sentiment: payload.call_analysis?.sentiment,
        customData: payload.call_analysis?.custom_analysis_data,
        duration: payload.call?.duration_ms,
        recordingUrl: payload.call?.recording_url
    };
}

module.exports = {
    computeDispatchConfigHash,
    computeBatchCompatibilityKey,
    computeIntentDedupeKey,
    computeBatchDedupeKey,
    determineOutcome,
    extractAnalysis
};
