const logger = require('../utils/logger');
const Retell = require('retell-sdk');

/**
 * Retell API Client
 * 
 * Handles communication with Retell's batch call API
 * - Sending batches of calls
 * - Polling for batch status
 * - Error handling and retries
 */

const RETELL_API_BASE_URL = 'https://api.retellai.com/v1';
const RETELL_API_KEY = process.env.RETELL_API_KEY;

if (!RETELL_API_KEY) {
    logger.warn('RETELL_API_KEY not set - Retell client will fail at runtime');
}

class RetellClient {
    constructor(apiKey = RETELL_API_KEY, baseUrl = RETELL_API_BASE_URL) {
        this.apiKey = apiKey;
        this.baseUrl = baseUrl;
        this.sdkClient = new Retell({ apiKey });
    }

    /**
     * Send a batch of calls to Retell
     * 
     * @param {Array|object} config - Array of tasks, or an object with tasks plus batch-level config
     * @returns {Promise<{batchCallId: string, metadata: object}>}
     */
    async sendBatchCalls(config) {
        const batchConfig = Array.isArray(config) ? { tasks: config } : (config || {});
        const tasks = [...(batchConfig.tasks || [])];

        if (!Array.isArray(tasks) || tasks.length === 0) {
            throw new Error('tasks must be a non-empty array');
        }

        const payload = {
            base_agent_id: batchConfig.baseAgentId,
            from_number: batchConfig.fromNumber,
            name: batchConfig.name,
            tasks: tasks.map(task => ({
                to_number: task.to_number || task.phone_number,
                metadata: task.metadata || {},
                ...task
            }))
        };

        try {
            const response = await this.sdkClient.batchCall.createBatchCall(payload);

            logger.info('Batch calls sent to Retell successfully', {
                batchCallId: response.batch_call_id,
                taskCount: tasks.length,
                invalidCount: 0
            });

            return {
                batchCallId: response.batch_call_id,
                invalidTasks: [],
                metadata: {
                    sentAt: new Date().toISOString(),
                    taskCount: tasks.length,
                    invalidCount: 0,
                    retellBatchId: response.batch_call_id
                }
            };
        } catch (error) {
            const invalidNumbers = this._extractInvalidNumbers(error);
            if (invalidNumbers.length > 0) {
                const invalidTasks = this._matchInvalidTasks(tasks, invalidNumbers);
                const partialBatchCallId = this._extractBatchCallId(error);

                logger.warn('Retell rejected invalid numbers; no retry performed to avoid duplicate calls', {
                    invalidNumbers,
                    matchedInvalidCount: invalidTasks.length,
                    taskCount: tasks.length,
                    partialBatchCallId: partialBatchCallId || null
                });

                const invalidError = new Error('Retell rejected one or more invalid phone numbers');
                invalidError.code = 'RETELL_INVALID_NUMBER';
                invalidError.invalidTasks = invalidTasks;
                invalidError.invalidNumbers = invalidNumbers;
                invalidError.batchCallId = partialBatchCallId || null;
                invalidError.originalError = error;
                throw invalidError;
            }

            logger.error('Failed to send batch calls to Retell', {
                error: error.message,
                taskCount: tasks.length
            });
            throw error;
        }
    }

    /**
     * Extract invalid phone number from Retell API error message.
     * Matches: "The number provided: +91636692565 is not a valid number"
     * @private
     */
    _extractInvalidNumbers(error) {
        const msg = error.message || '';
        const statusCode = error.status || error.statusCode || 0;

        if (statusCode !== 400 && !msg.startsWith('400')) return [];

        const pattern = /number provided:\s*(\+?\d+)\s*is not a valid number/gi;
        const numbers = new Set();
        let match;
        while ((match = pattern.exec(msg)) !== null) {
            if (match[1]) numbers.add(match[1]);
        }
        return [...numbers];
    }

    _matchInvalidTasks(tasks, invalidNumbers) {
        const invalidNormSet = new Set(invalidNumbers.map(n => String(n || '').replace(/\D/g, '')));
        return tasks
            .filter(task => {
                const num = (task.to_number || task.phone_number || '').replace(/\D/g, '');
                return invalidNormSet.has(num);
            })
            .map(task => ({
                ...task,
                failureReason: 'Rejected by Retell (invalid phone number)'
            }));
    }

    _extractBatchCallId(error) {
        const direct = error?.batch_call_id || error?.batchCallId || error?.response?.batch_call_id;
        if (direct) return direct;

        const msg = error?.message || '';
        const msgMatch = msg.match(/batch[_\s-]?call[_\s-]?id[:=\s]+([a-zA-Z0-9_-]+)/i);
        return msgMatch ? msgMatch[1] : null;
    }

    /**
     * Get the status of a batch call
     * 
     * @param {string} batchCallId - The batch call ID from Retell
     * @returns {Promise<{status: string, calls: Array}>}
     */
    async getBatchCallStatus(batchCallId) {
        if (!batchCallId) {
            throw new Error('batchCallId is required');
        }

        try {
            const calls = await this.sdkClient.call.list({
                filter_criteria: {
                    batch_call_id: [batchCallId]
                },
                limit: 1000,
                sort_order: 'ascending'
            });

            const normalizedCalls = calls.map((call) => ({
                status: call.status || call.call_status || 'pending',
                call_id: call.call_id || call.id,
                phone_number: call.phone_number || call.to_number,
                metadata: call.metadata || {},
                result: call.result || {},
                disconnection_reason: call.disconnection_reason,
                error: call.error || null,
                call_analysis: call.call_analysis || {},
                duration_ms: call.duration_ms,
                started_at: call.started_at || call.start_timestamp,
                ended_at: call.ended_at || call.end_timestamp
            }));

            const hasPending = normalizedCalls.some((call) => !['completed', 'succeeded', 'ended', 'failed', 'error', 'cancelled', 'not_connected'].includes(call.status));
            const hasFailures = normalizedCalls.some((call) => ['failed', 'error', 'cancelled', 'not_connected'].includes(call.status));
            const status = normalizedCalls.length === 0 ? 'pending' : hasPending ? 'pending' : hasFailures ? 'partial_failed' : 'completed';

            logger.debug('Batch call status retrieved from Retell', {
                batchCallId,
                status,
                callCount: normalizedCalls.length
            });

            return {
                status,
                calls: normalizedCalls
            };
        } catch (error) {
            logger.error('Failed to get batch call status from Retell', {
                batchCallId,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Get status of a single call
     * 
     * @param {string} callId - The call ID
     * @returns {Promise<object>}
     */
    async getCallStatus(callId) {
        if (!callId) {
            throw new Error('callId is required');
        }

        try {
            const response = await this.sdkClient.call.retrieve(callId);
            
            logger.debug('Call status retrieved from Retell', { callId });

            return {
                status: response.status || response.call_status || 'pending',
                call_id: response.call_id || response.id,
                phone_number: response.phone_number || response.to_number,
                metadata: response.metadata || {},
                result: response.result || {},
                disconnection_reason: response.disconnection_reason,
                error: response.error || null,
                call_analysis: response.call_analysis || {},
                duration_ms: response.duration_ms,
                started_at: response.started_at || response.start_timestamp,
                ended_at: response.ended_at || response.end_timestamp
            };
        } catch (error) {
            logger.error('Failed to get call status from Retell', {
                callId,
                error: error.message
            });
            throw error;
        }
    }

    /**
     * Make HTTP request to Retell API
     * 
     * @private
     * @param {string} method - HTTP method (GET, POST, etc)
     * @param {string} path - API endpoint path
     * @param {object} data - Request body (for POST/PUT)
     * @returns {Promise<object>}
     */
    async _makeRequest(method, path, data = null) {
        if (!this.apiKey) {
            throw new Error('RETELL_API_KEY not configured');
        }

        const url = `${this.baseUrl}${path}`;
        const headers = {
            'Authorization': `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json',
            'User-Agent': 'scalai-campaign-scheduler/1.0'
        };

        const options = {
            method,
            headers
        };

        if (data) {
            options.body = JSON.stringify(data);
        }

        try {
            const response = await fetch(url, options);

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Retell API error ${response.status}: ${errorText}`);
            }

            return await response.json();
        } catch (error) {
            if (error instanceof TypeError) {
                // Network error
                throw new Error(`Network error calling Retell API: ${error.message}`);
            }
            throw error;
        }
    }
}

// Create singleton instance
const retellClient = new RetellClient();

module.exports = retellClient;
