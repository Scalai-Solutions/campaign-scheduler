const { Worker } = require('bullmq');
const axios = require('axios');
const { connection, BULL_PREFIX, QUEUE_NAMES } = require('../queues');
const logger = require('../utils/logger');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://database-server:3000';
const RETRYABLE_STATUS_CODES = new Set([408, 429, 500, 502, 503, 504]);
const RETRYABLE_ERROR_CODES = new Set(['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED']);

function parseRetryAfter(value) {
    if (!value) return null;
    const seconds = Number(value);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
    const date = Date.parse(value);
    if (!Number.isNaN(date)) return Math.max(0, date - Date.now());
    return null;
}

function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(operation, retry = {}) {
    const maxRetries = Math.max(0, Math.min(10, Number(retry.maxRetries ?? 3)));
    const baseDelayMs = Math.max(100, Math.min(60000, Number(retry.baseDelayMs ?? 1000)));

    let attempt = 0;
    while (true) {
        try {
            return await operation();
        } catch (error) {
            const status = error.response?.status;
            const retryAfterMs = parseRetryAfter(error.response?.headers?.['retry-after']);
            const retryable = RETRYABLE_STATUS_CODES.has(status) || RETRYABLE_ERROR_CODES.has(error.code);
            if (!retryable || attempt >= maxRetries) throw error;

            const backoff = Math.min(30000, retryAfterMs ?? baseDelayMs * (2 ** attempt));
            attempt += 1;
            await delay(backoff);
        }
    }
}

async function loadConnectorConfig(tenantId, customConnectorId) {
    const { data } = await axios.get(
        `${DATABASE_SERVER_URL}/internal/custom-connectors/${tenantId}/${customConnectorId}`,
        { headers: { 'x-internal-service': 'campaign-scheduler' } }
    );
    if (!data?.success) throw new Error(data?.message || 'Failed to load custom endpoint connector');
    return data.data?.config || {};
}

const worker = new Worker(QUEUE_NAMES.customEndpointOutcome, async (job) => {
    const payload = job.data || {};
    const config = await loadConnectorConfig(payload.tenantId, payload.customConnectorId);
    const endpoint = config.outcomeEndpoint || {};

    if (!endpoint.enabled || !endpoint.url) {
        logger.debug('[CustomEndpointOutcome] Outcome endpoint disabled', {
            tenantId: payload.tenantId,
            customConnectorId: payload.customConnectorId
        });
        return;
    }

    const method = String(endpoint.method || 'POST').toUpperCase();
    const body = {
        lead_id: payload.leadId,
        phone: payload.phone,
        outcome: payload.outcome,
        campaign_id: payload.campaignId,
        node_id: payload.nodeId,
        completed_at: payload.completedAt,
        properties: payload.properties || {}
    };

    await withRetry(() => axios({
        method,
        url: endpoint.url,
        headers: endpoint.headers || {},
        timeout: Number(endpoint.timeoutMs || 8000),
        data: method === 'GET' ? undefined : body,
        params: method === 'GET' ? body : undefined,
        maxContentLength: 1024 * 1024,
        validateStatus: (status) => status >= 200 && status < 300
    }), endpoint.retry || {});

    logger.info('[CustomEndpointOutcome] Outcome callback sent', {
        tenantId: payload.tenantId,
        campaignId: payload.campaignId,
        leadId: payload.leadId,
        outcome: payload.outcome,
        customConnectorId: payload.customConnectorId
    });
}, {
    connection,
    prefix: BULL_PREFIX,
    concurrency: parseInt(process.env.WORKER_CONCURRENCY_CUSTOM_ENDPOINT_OUTCOME || '5', 10)
});

worker.on('failed', (job, error) => {
    logger.warn('[CustomEndpointOutcome] Job failed', {
        jobId: job?.id,
        tenantId: job?.data?.tenantId,
        leadId: job?.data?.leadId,
        outcome: job?.data?.outcome,
        error: error?.message
    });
});

module.exports = worker;