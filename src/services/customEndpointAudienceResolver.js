const axios = require('axios');
const logger = require('../utils/logger');
const { normalizeE164Phone } = require('../utils/phoneValidation');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://database-server:3000';
const RETRYABLE_STATUS_CODES = new Set([408, 429, 500, 502, 503, 504]);
const RETRYABLE_ERROR_CODES = new Set(['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED']);

function getByPath(value, path) {
    if (!path) return value;
    return String(path).split('.').filter(Boolean).reduce((current, key) => {
        if (current === null || current === undefined) return undefined;
        return current[key];
    }, value);
}

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

function parseCsvLine(line) {
    const cells = [];
    let current = '';
    let inQuotes = false;

    for (let index = 0; index < line.length; index += 1) {
        const char = line[index];
        const next = line[index + 1];
        if (char === '"' && inQuotes && next === '"') {
            current += '"';
            index += 1;
        } else if (char === '"') {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            cells.push(current.trim());
            current = '';
        } else {
            current += char;
        }
    }

    cells.push(current.trim());
    return cells;
}

function parseCsv(text, phoneColumn) {
    const lines = String(text || '').split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    if (lines.length === 0) return [];

    const header = parseCsvLine(lines[0]);
    const phoneIndex = Math.max(0, header.findIndex((name) => name === phoneColumn));
    const rows = [];

    for (const line of lines.slice(1)) {
        const cells = parseCsvLine(line);
        const properties = {};
        header.forEach((name, index) => {
            properties[name] = cells[index] || '';
        });
        rows.push({ phone: cells[phoneIndex], properties });
    }

    return rows;
}

function parseBody(rawBody, parser = {}, contentType = '') {
    const responseType = String(parser.responseType || 'auto').toLowerCase();
    const text = typeof rawBody === 'string' ? rawBody : JSON.stringify(rawBody || '');
    const looksJson = /^\s*[\[{]/.test(text) || contentType.includes('application/json');
    const looksCsv = contentType.includes('text/csv') || text.split(/\r?\n/)[0]?.includes(',');

    if (responseType === 'json' || responseType === 'array' || (responseType === 'auto' && looksJson)) {
        const json = typeof rawBody === 'string' ? JSON.parse(rawBody) : rawBody;
        const records = parser.responsePath ? getByPath(json, parser.responsePath) : json;
        if (Array.isArray(records)) return records;
        if (Array.isArray(records?.leads)) return records.leads;
        if (Array.isArray(records?.phones)) return records.phones;
        if (Array.isArray(records?.numbers)) return records.numbers;
        return [];
    }

    if (responseType === 'csv' || (responseType === 'auto' && looksCsv)) {
        return parseCsv(text, parser.phoneColumn || 'phone');
    }

    return text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
}

function pickPhone(record, parser = {}, phoneMapping = null) {
    if (typeof record === 'string') return record;
    const preferred = parser.phoneColumn || phoneMapping;
    if (preferred) {
        const value = getByPath(record, preferred) || record.properties?.[preferred];
        if (value) return value;
    }

    return record.phone || record.mobilephone || record.mobilePhone || record.number || record.properties?.phone || null;
}

function normalizeRecords(records, parser = {}, pipelineConfig = {}) {
    const defaultRegion = parser.defaultRegion || pipelineConfig.phoneRegion || 'ES';
    const phoneMapping = pipelineConfig.phoneMapping || null;
    const leads = [];
    let invalidCount = 0;
    let skippedCount = 0;
    const seenPhones = new Set();

    for (const record of Array.isArray(records) ? records : []) {
        const phone = normalizeE164Phone(String(pickPhone(record, parser, phoneMapping) || ''), defaultRegion);
        if (!phone) {
            invalidCount += 1;
            continue;
        }
        if (seenPhones.has(phone)) {
            skippedCount += 1;
            continue;
        }
        seenPhones.add(phone);

        const attrs = typeof record === 'object' && record !== null
            ? (parser.attrsPath ? getByPath(record, parser.attrsPath) : record)
            : {};

        leads.push({
            phone,
            properties: typeof attrs === 'object' && attrs !== null ? attrs : {},
            source: {
                provider: 'custom_endpoint',
                mode: 'endpoint',
                customConnectorId: pipelineConfig.selectedSource?.customConnectorId || null
            }
        });
    }

    return { leads, invalidCount, skippedCount };
}

function sliceByCursor(leads, audienceCursor, leadsPerRun) {
    const limit = Math.max(1, Math.min(200, Number(leadsPerRun || 200)));
    const start = Math.max(0, Number(audienceCursor || 0));
    const selected = leads.slice(start, start + limit);
    const nextCursor = leads.length > 0 && start + limit < leads.length ? String(start + limit) : null;
    return { selected, nextCursor };
}

class CustomEndpointAudienceResolver {
    static async _loadConnectorConfig(subaccountId, customConnectorId) {
        const { data } = await axios.get(
            `${DATABASE_SERVER_URL}/internal/custom-connectors/${subaccountId}/${customConnectorId}`,
            { headers: { 'x-internal-service': 'campaign-scheduler' } }
        );
        if (!data?.success) throw new Error(data?.message || 'Failed to load custom endpoint connector');
        return data.data;
    }

    static async _fetchEndpoint(fetchEndpoint = {}) {
        const method = String(fetchEndpoint.method || 'POST').toUpperCase();
        const options = {
            method,
            url: fetchEndpoint.url,
            headers: fetchEndpoint.headers || {},
            timeout: Number(fetchEndpoint.timeoutMs || 8000),
            responseType: 'text',
            transformResponse: [(data) => data],
            maxContentLength: 2 * 1024 * 1024,
            validateStatus: (status) => status >= 200 && status < 300
        };

        if (method === 'GET') options.params = fetchEndpoint.payload || undefined;
        else options.data = fetchEndpoint.payload || {};

        return withRetry(() => axios(options), fetchEndpoint.retry || {});
    }

    static async resolveAudience(subaccountId, pipelineConfig = {}, options = {}) {
        const selectedSource = pipelineConfig.selectedSource || {};
        const customConnectorId = selectedSource.customConnectorId;
        if (!customConnectorId) {
            throw new Error('selectedSource.customConnectorId is required for custom_endpoint audience resolution');
        }

        const connector = await this._loadConnectorConfig(subaccountId, customConnectorId);
        const config = connector.config || {};
        const parser = config.parser || {};
        const response = await this._fetchEndpoint(config.fetchEndpoint || {});
        const records = parseBody(response.data, parser, String(response.headers?.['content-type'] || '').toLowerCase());
        const { leads, invalidCount, skippedCount } = normalizeRecords(records, parser, pipelineConfig);
        const { selected, nextCursor } = sliceByCursor(leads, options.audienceCursor, options.leadsPerRun);

        const snapshot = {
            provider: 'custom_endpoint',
            sourceId: customConnectorId,
            customConnectorId,
            fetchedCount: Array.isArray(records) ? records.length : 0,
            validCount: leads.length,
            selectedLeadsCount: selected.length,
            invalidCount,
            skippedCount,
            nextCursor,
            audienceCursor: options.audienceCursor || null,
            parser: {
                responseType: parser.responseType || 'auto',
                responsePath: parser.responsePath || null,
                phoneColumn: parser.phoneColumn || null
            }
        };

        logger.info('[CustomEndpointAudienceResolver] Resolved audience', {
            subaccountId,
            customConnectorId,
            fetchedCount: snapshot.fetchedCount,
            validCount: snapshot.validCount,
            selectedLeadsCount: snapshot.selectedLeadsCount,
            invalidCount,
            skippedCount
        });

        return { leads: selected, snapshot };
    }
}

module.exports = CustomEndpointAudienceResolver;