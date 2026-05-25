const logger = require('../utils/logger');
const { connection } = require('../queues');

/**
 * WahaClient
 *
 * Thin wrapper around the connector-server's WhatsApp send endpoint.
 * The campaign-scheduler never talks to WAHA directly; all WhatsApp i/o
 * goes through the connector-server which manages the WAHA session.
 *
 * Environment:
 *   CONNECTOR_SERVER_URL       – base URL of the connector server
 *   WAHA_SEND_DELAY_MS         – optional extra delay between batch sends
 *   WAHA_MIN_INTERVAL_MS       – global minimum interval between sends
 *   WAHA_REQUEST_TIMEOUT_MS    – per-request timeout
 *   WAHA_SEND_MAX_RETRIES      – retry attempts for transient failures
 *   WAHA_RETRY_BASE_DELAY_MS   – base backoff between retries
 */

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://localhost:3004';
const WAHA_DEFAULT_SEND_DELAY_MS = parseInt(
    process.env.WAHA_SEND_DELAY_MS || '2000', 10
);
const WAHA_MIN_INTERVAL_MS = parseInt(
    process.env.WAHA_MIN_INTERVAL_MS || String(WAHA_DEFAULT_SEND_DELAY_MS), 10
);
const WAHA_REQUEST_TIMEOUT_MS = parseInt(
    process.env.WAHA_REQUEST_TIMEOUT_MS || '20000', 10
);
const WAHA_SEND_MAX_RETRIES = Math.max(
    1,
    parseInt(process.env.WAHA_SEND_MAX_RETRIES || '4', 10)
);
const WAHA_RETRY_BASE_DELAY_MS = Math.max(
    250,
    parseInt(process.env.WAHA_RETRY_BASE_DELAY_MS || '2000', 10)
);
const WAHA_GLOBAL_LOCK_TTL_MS = Math.max(
    WAHA_REQUEST_TIMEOUT_MS * WAHA_SEND_MAX_RETRIES + 5000,
    60000
);

const WAHA_GLOBAL_LOCK_KEY = '{bull}:waha:send:lock';
const WAHA_GLOBAL_LAST_SENT_KEY = '{bull}:waha:send:last-at';

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableHttpStatus(status) {
    return status === 408 || status === 429 || status >= 500;
}

function isRetryableWahaError(error) {
    const message = String(error?.message || error || '').toLowerCase();
    if (!message) return false;

    if (/aborterror|timed out|timeout|econnrefused|econnreset|enotfound|socket hang up|network|fetch failed|service unavailable|bad gateway|gateway timeout/.test(message)) {
        return true;
    }

    const statusMatch = message.match(/http (\d{3})/i);
    if (statusMatch) {
        return isRetryableHttpStatus(parseInt(statusMatch[1], 10));
    }

    return false;
}

class WahaClient {
    constructor(connectorUrl = CONNECTOR_SERVER_URL) {
        this.connectorUrl = connectorUrl.replace(/\/$/, '');
        this.minIntervalMs = WAHA_MIN_INTERVAL_MS;
        this._lastSentAt = 0;
        this._sendChain = Promise.resolve();
    }

    isRetryableError(error) {
        return isRetryableWahaError(error);
    }

    async _acquireGlobalSendLock() {
        const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const deadline = Date.now() + WAHA_GLOBAL_LOCK_TTL_MS;

        while (Date.now() < deadline) {
            const acquired = await connection.set(
                WAHA_GLOBAL_LOCK_KEY,
                token,
                'PX',
                WAHA_GLOBAL_LOCK_TTL_MS,
                'NX'
            );
            if (acquired === 'OK') {
                return token;
            }
            await sleep(100);
        }

        throw new Error('Timed out waiting for global WAHA send lock');
    }

    async _releaseGlobalSendLock(token) {
        const script = `
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            end
            return 0
        `;
        try {
            await connection.eval(script, 1, WAHA_GLOBAL_LOCK_KEY, token);
        } catch (error) {
            logger.warn('[WahaClient] Failed to release global WAHA send lock', {
                error: error.message
            });
        }
    }

    async _waitForGlobalSendInterval() {
        while (true) {
            const now = Date.now();
            const lastSentRaw = await connection.get(WAHA_GLOBAL_LAST_SENT_KEY);
            const lastSentAt = parseInt(lastSentRaw || '0', 10);
            const waitMs = Math.max(0, this.minIntervalMs - (now - lastSentAt));

            if (waitMs <= 0) {
                await connection.set(WAHA_GLOBAL_LAST_SENT_KEY, String(now), 'PX', 86400000);
                return;
            }

            await sleep(Math.min(waitMs, 250));
        }
    }

    async _sendWithGlobalRateLimit(sendFn) {
        const previous = this._sendChain;

        let releaseCurrent;
        this._sendChain = new Promise((resolve) => {
            releaseCurrent = resolve;
        });

        await previous;

        let lockToken = null;
        try {
            lockToken = await this._acquireGlobalSendLock();
            await this._waitForGlobalSendInterval();

            const result = await sendFn();
            this._lastSentAt = Date.now();
            await connection.set(WAHA_GLOBAL_LAST_SENT_KEY, String(this._lastSentAt), 'PX', 86400000);
            return result;
        } finally {
            if (lockToken) {
                await this._releaseGlobalSendLock(lockToken);
            }
            releaseCurrent();
        }
    }

    /**
     * Verify connector-server can reach a WORKING WAHA session before dispatching
     * a large batch. Throws on failure so BullMQ can retry the job later.
     */
    async ensureSessionReady({ subaccountId }) {
        if (!subaccountId) throw new Error('subaccountId is required');

        const url = `${this.connectorUrl}/api/whatsapp/${encodeURIComponent(subaccountId)}/session`;

        const response = await this._sendWithRetries(async () => {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), WAHA_REQUEST_TIMEOUT_MS);

            try {
                return await fetch(url, {
                    method: 'GET',
                    signal: controller.signal
                });
            } finally {
                clearTimeout(timeoutId);
            }
        }, { operation: 'session readiness check', subaccountId });

        if (!response.ok) {
            const errorText = await response.text().catch(() => '');
            throw new Error(`WAHA session check failed HTTP ${response.status}: ${errorText}`);
        }

        const body = await response.json().catch(() => ({}));
        if (!body.ready) {
            throw new Error(body.error || `WAHA session is not ready (${body.sessionStatus || 'unknown'})`);
        }

        return body;
    }

    async _sendWithRetries(sendFn, context = {}) {
        let lastError = null;

        for (let attempt = 1; attempt <= WAHA_SEND_MAX_RETRIES; attempt++) {
            try {
                const response = await sendFn();
                if (response.ok || !isRetryableHttpStatus(response.status)) {
                    return response;
                }

                const errorText = await response.text().catch(() => '');
                lastError = new Error(`WAHA send failed HTTP ${response.status}: ${errorText}`);
            } catch (error) {
                lastError = error.name === 'AbortError'
                    ? new Error(`WAHA request timed out after ${WAHA_REQUEST_TIMEOUT_MS}ms`)
                    : error;
            }

            const isLastAttempt = attempt === WAHA_SEND_MAX_RETRIES;
            if (isLastAttempt || !isRetryableWahaError(lastError)) {
                throw lastError;
            }

            const delayMs = WAHA_RETRY_BASE_DELAY_MS * attempt;
            logger.warn('[WahaClient] Transient WAHA failure, retrying', {
                ...context,
                attempt,
                maxAttempts: WAHA_SEND_MAX_RETRIES,
                delayMs,
                error: lastError.message
            });
            await sleep(delayMs);
        }

        throw lastError;
    }

    /**
     * Send a single WhatsApp message via the connector server.
     */
    async sendMessage({ subaccountId, phone, message }) {
        if (!subaccountId) throw new Error('subaccountId is required');
        if (!phone)        throw new Error('phone is required');
        if (!message)      throw new Error('message is required');

        const url = `${this.connectorUrl}/api/whatsapp/${encodeURIComponent(subaccountId)}/send`;

        const response = await this._sendWithGlobalRateLimit(async () => this._sendWithRetries(async () => {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), WAHA_REQUEST_TIMEOUT_MS);

            try {
                return await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ to: phone, message }),
                    signal: controller.signal
                });
            } finally {
                clearTimeout(timeoutId);
            }
        }, { operation: 'send message', subaccountId, phone }));

        if (!response.ok) {
            const errorText = await response.text().catch(() => '');
            throw new Error(`WAHA send failed HTTP ${response.status}: ${errorText}`);
        }

        const body = await response.json().catch(() => ({}));
        if (body.success === false) {
            throw new Error(body.error || 'WAHA send failed');
        }

        return {
            messageId: body.data?.messageId || body.messageId || body.id || null,
            success: true
        };
    }

    /**
     * Send WhatsApp messages sequentially with global pacing.
     */
    async sendBatch({ subaccountId, targets, delayMs = WAHA_DEFAULT_SEND_DELAY_MS }) {
        if (!Array.isArray(targets) || targets.length === 0) return [];

        const results = [];
        for (let i = 0; i < targets.length; i++) {
            const { phone, message } = targets[i];
            try {
                const result = await this.sendMessage({ subaccountId, phone, message });
                results.push({ phone, success: true, messageId: result.messageId });
                logger.info('[WahaClient] Message sent', { subaccountId, phone, messageId: result.messageId });
            } catch (error) {
                results.push({ phone, success: false, error: error.message });
                logger.warn('[WahaClient] Message send failed', {
                    subaccountId, phone, error: error.message
                });
            }

            if (i < targets.length - 1 && delayMs > 0) {
                await sleep(delayMs);
            }
        }

        return results;
    }
}

const wahaClient = new WahaClient();
module.exports = wahaClient;
module.exports.isRetryableWahaError = isRetryableWahaError;
