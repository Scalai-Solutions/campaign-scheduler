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
        let remainingTasks = [...(batchConfig.tasks || [])];

        if (!Array.isArray(remainingTasks) || remainingTasks.length === 0) {
            throw new Error('tasks must be a non-empty array');
        }

<<<<<<< Updated upstream
        const MAX_RETRIES = remainingTasks.length; // worst case: every number is invalid
        const invalidTasks = [];

        for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            if (remainingTasks.length === 0) {
                logger.warn('All tasks had invalid numbers, no calls dispatched', {
                    invalidCount: invalidTasks.length
                });
                return {
                    batchCallId: null,
                    invalidTasks,
                    metadata: {
                        sentAt: new Date().toISOString(),
                        taskCount: 0,
                        invalidCount: invalidTasks.length
                    }
                };
            }
=======
        const payload = {
            base_agent_id: batchConfig.baseAgentId,
            from_number: batchConfig.fromNumber,
            name: batchConfig.name,
            tasks: tasks.map(task => {
                // Sanitize dynamic variables — Retell requires ALL values to be strings
                const dynVars = task.retell_llm_dynamic_variables || {};
                const sanitizedVars = {};
                for (const [k, v] of Object.entries(dynVars)) {
                    sanitizedVars[k] = typeof v === 'string' ? v : String(v ?? '');
                }
                return {
                    to_number: task.to_number || task.phone_number,
                    retell_llm_dynamic_variables: sanitizedVars,
                    metadata: task.metadata || {}
                };
            })
        };

        // Log a sample task for debugging
        if (payload.tasks.length > 0) {
            const sample = payload.tasks[0];
            const nonStringVars = Object.entries(sample.retell_llm_dynamic_variables || {})
                .filter(([, v]) => typeof v !== 'string')
                .map(([k, v]) => ({ key: k, type: typeof v }));
            logger.info('Retell batch call sample task', {
                varCount: Object.keys(sample.retell_llm_dynamic_variables || {}).length,
                metadataKeys: Object.keys(sample.metadata || {}),
                nonStringVars: nonStringVars.length > 0 ? nonStringVars : 'none'
            });
        }

        try {
            const response = await this.sdkClient.batchCall.createBatchCall(payload);
            
            logger.info('Batch calls sent to Retell successfully', {
                batchCallId: response.batch_call_id,
                taskCount: tasks.length
            });
>>>>>>> Stashed changes

            const payload = {
                base_agent_id: batchConfig.baseAgentId,
                from_number: batchConfig.fromNumber,
                name: batchConfig.name,
                tasks: remainingTasks.map(task => ({
                    to_number: task.to_number || task.phone_number,
                    metadata: task.metadata || {},
                    ...task
                }))
            };

            try {
                const response = await this.sdkClient.batchCall.createBatchCall(payload);

                logger.info('Batch calls sent to Retell successfully', {
                    batchCallId: response.batch_call_id,
                    taskCount: remainingTasks.length,
                    invalidCount: invalidTasks.length
                });

                return {
                    batchCallId: response.batch_call_id,
                    invalidTasks,
                    metadata: {
                        sentAt: new Date().toISOString(),
                        taskCount: remainingTasks.length,
                        invalidCount: invalidTasks.length,
                        retellBatchId: response.batch_call_id
                    }
                };
            } catch (error) {
                // Check if this is an invalid phone number error (400)
                const invalidNumber = this._extractInvalidNumber(error);
                if (invalidNumber) {
                    logger.warn('Removing invalid phone number from batch and retrying', {
                        invalidNumber,
                        attempt: attempt + 1,
                        remainingTasks: remainingTasks.length - 1
                    });

                    // Find and remove the task with the invalid number
                    const invalidIdx = remainingTasks.findIndex(
                        t => (t.to_number || t.phone_number) === invalidNumber
                    );
                    if (invalidIdx !== -1) {
                        const [removed] = remainingTasks.splice(invalidIdx, 1);
                        invalidTasks.push({ ...removed, failureReason: `Invalid phone number: ${invalidNumber}` });
                    } else {
                        // Number format may differ — try normalized match
                        const normalizedInvalid = invalidNumber.replace(/\D/g, '');
                        const partialIdx = remainingTasks.findIndex(t => {
                            const num = (t.to_number || t.phone_number || '').replace(/\D/g, '');
                            return num === normalizedInvalid || num.endsWith(normalizedInvalid) || normalizedInvalid.endsWith(num);
                        });
                        if (partialIdx !== -1) {
                            const [removed] = remainingTasks.splice(partialIdx, 1);
                            invalidTasks.push({ ...removed, failureReason: `Invalid phone number: ${invalidNumber}` });
                        } else {
                            logger.error('Could not identify invalid number in task list', {
                                invalidNumber, taskNumbers: remainingTasks.map(t => t.to_number || t.phone_number)
                            });
                            throw error;
                        }
                    }
                    continue; // retry without the invalid number
                }

                // Non-retryable error
                logger.error('Failed to send batch calls to Retell', {
                    error: error.message,
                    taskCount: remainingTasks.length
                });
                throw error;
            }
        }

        throw new Error('Exceeded max retries removing invalid numbers from batch');
    }

    /**
     * Extract invalid phone number from Retell API error message.
     * Matches: "The number provided: +91636692565 is not a valid number"
     * @private
     */
    _extractInvalidNumber(error) {
        const msg = error.message || '';
        const statusCode = error.status || error.statusCode || 0;

        if (statusCode !== 400 && !msg.startsWith('400')) return null;

        const match = msg.match(/number provided:\s*(\+?\d+)\s*is not a valid number/i);
        return match ? match[1] : null;
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
