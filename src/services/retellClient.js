const logger = require('../utils/logger');

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
    }

    /**
     * Send a batch of calls to Retell
     * 
     * @param {Array} tasks - Array of task objects with phone_number, metadata, agent, etc
     * @returns {Promise<{batchCallId: string, metadata: object}>}
     */
    async sendBatchCalls(tasks) {
        if (!Array.isArray(tasks) || tasks.length === 0) {
            throw new Error('tasks must be a non-empty array');
        }

        const payload = {
            tasks: tasks.map(task => ({
                phone_number: task.phone_number,
                metadata: task.metadata || {},
                agent: task.agent || {},
                retry_config: task.retry_config || {},
                ...task // Include any other fields passed
            }))
        };

        try {
            const response = await this._makeRequest('POST', '/batch-calls', payload);
            
            logger.info('Batch calls sent to Retell successfully', {
                batchCallId: response.batch_call_id,
                taskCount: tasks.length
            });

            return {
                batchCallId: response.batch_call_id,
                metadata: {
                    sentAt: new Date().toISOString(),
                    taskCount: tasks.length,
                    retellBatchId: response.batch_call_id
                }
            };
        } catch (error) {
            logger.error('Failed to send batch calls to Retell', {
                error: error.message,
                taskCount: tasks.length
            });
            throw error;
        }
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
            const response = await this._makeRequest('GET', `/batch-calls/${batchCallId}`);
            
            // Parse response status
            const status = response.status || 'pending';
            const calls = response.calls || [];

            logger.debug('Batch call status retrieved from Retell', {
                batchCallId,
                status,
                callCount: calls.length
            });

            return {
                status,
                calls: calls.map(call => ({
                    status: call.status || call.call_status || 'pending',
                    call_id: call.call_id || call.id,
                    phone_number: call.phone_number,
                    metadata: call.metadata || {},
                    result: call.result || {},
                    disconnection_reason: call.disconnection_reason,
                    error: call.error || null,
                    call_analysis: call.call_analysis || {},
                    duration_ms: call.duration_ms,
                    started_at: call.started_at,
                    ended_at: call.ended_at
                }))
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
            const response = await this._makeRequest('GET', `/calls/${callId}`);
            
            logger.debug('Call status retrieved from Retell', { callId });

            return {
                status: response.status || response.call_status || 'pending',
                call_id: response.call_id || response.id,
                phone_number: response.phone_number,
                metadata: response.metadata || {},
                result: response.result || {},
                disconnection_reason: response.disconnection_reason,
                error: response.error || null,
                call_analysis: response.call_analysis || {},
                duration_ms: response.duration_ms,
                started_at: response.started_at,
                ended_at: response.ended_at
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
