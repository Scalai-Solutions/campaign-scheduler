const axios = require('axios');
const logger = require('../utils/logger');
const { normalizeE164Phone } = require('../utils/phoneValidation');

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://connector-server:3004';

function buildHeaders() {
    return {
        'x-internal-service': 'campaign-scheduler',
        'Content-Type': 'application/json'
    };
}

function pickPhoneFromRecord(record, phoneMapping) {
    const properties = record?.properties || {};

    if (phoneMapping && properties[phoneMapping]) {
        return properties[phoneMapping];
    }

    return (
        properties.phone ||
        properties.mobilephone ||
        properties.hs_phone_number ||
        record?.phone ||
        null
    );
}

function normalizeLead(record, phoneMapping) {
    const rawPhone = pickPhoneFromRecord(record, phoneMapping);
    const normalizedPhone = normalizeE164Phone(rawPhone || '');

    if (!normalizedPhone) {
        return { lead: null, reason: 'invalid_phone' };
    }

    return {
        lead: {
            phone: normalizedPhone,
            sourceRecordId: record.id,
            properties: record.properties || {}
        },
        reason: null
    };
}

function buildHubspotFilterGroups(selectedSource, filters = []) {
    const normalized = filters
        .filter((filter) => filter && filter.property && filter.operator)
        .map((filter) => ({
            propertyName: filter.property,
            operator: filter.operator,
            value: filter.value
        }));

    const baseFilters = [];

    if (selectedSource?.pipelineId) {
        baseFilters.push({ propertyName: 'pipeline', operator: 'EQ', value: selectedSource.pipelineId });
    }
    if (selectedSource?.stageId) {
        baseFilters.push({ propertyName: 'dealstage', operator: 'EQ', value: selectedSource.stageId });
    }

    return [{ filters: [...baseFilters, ...normalized] }];
}

class HubspotAudienceResolver {
    static async _fetchListMembers(subaccountId, listId) {
        const url = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/lists/${listId}/contacts`;
        const { data } = await axios.get(url, { headers: buildHeaders() });
        if (!data?.success) {
            throw new Error(data?.error || 'Failed to fetch list members from HubSpot');
        }

        const records = Array.isArray(data.data?.records)
            ? data.data.records
            : (data.data?.contacts || []).map((contact) => ({
                id: contact.contactId,
                properties: {
                    firstname: contact.firstName,
                    lastname: contact.lastName,
                    email: contact.email,
                    phone: contact.phone,
                    mobilephone: contact.mobilePhone,
                    company: contact.company
                }
            }));

        return {
            records,
            sourceId: listId,
            fetchedCount: Number(data.data?.total || records.length || 0)
        };
    }

    static async _fetchPipelineRecords(subaccountId, selectedSource, filters = []) {
        const filterGroups = buildHubspotFilterGroups(selectedSource, filters);

        const url = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/objects/deals/search`;
        const { data } = await axios.post(
            url,
            {
                filterGroups,
                properties: ['dealname', 'pipeline', 'dealstage', 'phone', 'mobilephone', 'hs_phone_number'],
                limit: 200
            },
            { headers: buildHeaders() }
        );

        if (!data?.success) {
            throw new Error(data?.error || 'Failed to search HubSpot deals');
        }

        const records = Array.isArray(data.data?.results) ? data.data.results : [];
        const sourceId = `${selectedSource?.pipelineId || 'unknown'}:${selectedSource?.stageId || 'unknown'}`;

        return {
            records,
            sourceId,
            fetchedCount: Number(data.data?.total || records.length || 0)
        };
    }

    static async resolveAudience(subaccountId, pipelineConfig = {}) {
        const mode = pipelineConfig.mode;
        const provider = String(pipelineConfig.provider || '').toLowerCase();
        const selectedSource = pipelineConfig.selectedSource || {};
        const filters = Array.isArray(pipelineConfig.filters) ? pipelineConfig.filters : [];
        const phoneMapping = pipelineConfig.phoneMapping || null;

        if (provider !== 'hubspot') {
            throw new Error('Unsupported provider for resolver. Expected provider=hubspot');
        }

        let fetched;
        if (mode === 'list') {
            if (!selectedSource.listId) {
                throw new Error('selectedSource.listId is required for HubSpot list mode');
            }
            fetched = await this._fetchListMembers(subaccountId, selectedSource.listId);
        } else if (mode === 'pipeline') {
            if (!selectedSource.pipelineId || !selectedSource.stageId) {
                throw new Error('selectedSource.pipelineId and selectedSource.stageId are required for HubSpot pipeline mode');
            }
            fetched = await this._fetchPipelineRecords(subaccountId, selectedSource, filters);
        } else {
            throw new Error('Unsupported pipelineConfig.mode. Expected list or pipeline');
        }

        const leads = [];
        let invalidCount = 0;
        let skippedCount = 0;

        for (const record of fetched.records) {
            const { lead, reason } = normalizeLead(record, phoneMapping);
            if (!lead) {
                if (reason === 'invalid_phone') invalidCount += 1;
                else skippedCount += 1;
                continue;
            }
            leads.push(lead);
        }

        const deduped = [];
        const seenPhones = new Set();
        for (const lead of leads) {
            if (seenPhones.has(lead.phone)) {
                skippedCount += 1;
                continue;
            }
            seenPhones.add(lead.phone);
            deduped.push(lead);
        }

        const snapshot = {
            filtersUsed: filters,
            sourceId: fetched.sourceId,
            fetchedCount: fetched.fetchedCount,
            validCount: deduped.length,
            invalidCount,
            skippedCount
        };

        logger.info('[HubspotAudienceResolver] Resolved audience', {
            subaccountId,
            mode,
            sourceId: snapshot.sourceId,
            fetchedCount: snapshot.fetchedCount,
            validCount: snapshot.validCount,
            invalidCount: snapshot.invalidCount,
            skippedCount: snapshot.skippedCount
        });

        return {
            leads: deduped,
            snapshot
        };
    }
}

module.exports = HubspotAudienceResolver;
