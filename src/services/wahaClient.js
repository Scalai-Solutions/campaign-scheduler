const logger = require('../utils/logger');

/**
 * WahaClient
 *
 * Thin wrapper around the connector-server's WhatsApp send endpoint.
 * The campaign-scheduler never talks to WAHA directly; all WhatsApp i/o
 * goes through the connector-server which manages the WAHA session.
 *
 * Environment:
 *   CONNECTOR_SERVER_URL  – base URL of the connector server
 *                           (default: http://localhost:3004)
 */

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://localhost:3004';
// Inter-message delay for sequential sends to avoid WhatsApp rate-limiting.
// Can be overridden per-deploy without code changes.
const WAHA_DEFAULT_SEND_DELAY_MS = parseInt(
    process.env.WAHA_SEND_DELAY_MS || '2000', 10
);
// Global minimum interval between outbound sends per scheduler process.
// This protects WAHA even when multiple chat-dispatch jobs run concurrently.
const WAHA_MIN_INTERVAL_MS = parseInt(
    process.env.WAHA_MIN_INTERVAL_MS || String(WAHA_DEFAULT_SEND_DELAY_MS), 10
);
// Individual send timeout
const WAHA_REQUEST_TIMEOUT_MS = parseInt(
    process.env.WAHA_REQUEST_TIMEOUT_MS || '15000', 10
);

class WahaClient {
    constructor(connectorUrl = CONNECTOR_SERVER_URL) {
        this.connectorUrl = connectorUrl.replace(/\/$/, '');
        this.minIntervalMs = WAHA_MIN_INTERVAL_MS;
        this._lastSentAt = 0;
        this._sendChain = Promise.resolve();
    }

    async _sendWithGlobalRateLimit(sendFn) {
        const previous = this._sendChain;

        let releaseCurrent;
        this._sendChain = new Promise((resolve) => {
            releaseCurrent = resolve;
        });

        await previous;

        try {
            const elapsed = Date.now() - this._lastSentAt;
            const waitMs = Math.max(0, this.minIntervalMs - elapsed);
            if (waitMs > 0) {
                await new Promise((resolve) => setTimeout(resolve, waitMs));
            }

            const result = await sendFn();
            this._lastSentAt = Date.now();
            return result;
        } finally {
            releaseCurrent();
        }
    }

    /**
     * Send a single WhatsApp message via the connector server.
     *
     * @param {object} params
     * @param {string} params.subaccountId - Tenant's subaccount id (determines WAHA session)
     * @param {string} params.phone        - E.164 phone without leading '+', e.g. "919876543210"
     * @param {string} params.message      - Plain-text message body
     * @returns {Promise<{messageId: string|null, success: boolean}>}
     */
    async sendMessage({ subaccountId, phone, message }) {
        if (!subaccountId) throw new Error('subaccountId is required');
        if (!phone)        throw new Error('phone is required');
        if (!message)      throw new Error('message is required');

        const url = `${this.connectorUrl}/api/whatsapp/${encodeURIComponent(subaccountId)}/send`;

        try {
            const response = await this._sendWithGlobalRateLimit(async () => {
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
            });

            if (!response.ok) {
                const errorText = await response.text().catch(() => '');
                throw new Error(`WAHA send failed HTTP ${response.status}: ${errorText}`);
            }

            const body = await response.json().catch(() => ({}));
            return { messageId: body.messageId || body.id || null, success: true };
        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error(`WAHA send timed out after ${WAHA_REQUEST_TIMEOUT_MS}ms for ${phone}`);
            }
            throw error;
        }
    }

    /**
    * Send WhatsApp messages to a list of recipients sequentially.
    *
    * Global WAHA pacing is enforced in sendMessage() across all concurrent
    * jobs in this scheduler process. delayMs remains as an optional extra
    * buffer for batch callers that want slower pacing than the global limit.
     *
     * Returns a results array in the same order as `targets` — each entry has:
     *   { phone, success, messageId?, error? }
     *
     * This method never throws; failures are captured per-lead so the caller
     * can decide which leads to record as failed.
     *
     * @param {object}   params
     * @param {string}   params.subaccountId
     * @param {Array<{phone: string, message: string}>} params.targets
     * @param {number}   [params.delayMs]  - Override inter-message delay
     * @returns {Promise<Array>}
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

            // Apply delay between messages; skip after the last one
            if (i < targets.length - 1 && delayMs > 0) {
                await new Promise(resolve => setTimeout(resolve, delayMs));
            }
        }

        return results;
    }
}

const wahaClient = new WahaClient();
module.exports = wahaClient;
