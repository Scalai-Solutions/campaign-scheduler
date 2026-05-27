const axios = require('axios');
const logger = require('../utils/logger');
const { normalizeE164Phone } = require('../utils/phoneValidation');

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://connector-server:3004';
const VOONE_OUTCOME_PROPERTY = 'voone_call_outcome';
const TERMINAL_VOONE_OUTCOMES = new Set(['successful', 'unsuccessful']);
const VOONE_OUTCOME_INTENTS = new Set(['callable', 'terminal', 'successful_only', 'unsuccessful_only', 'any']);

function sanitizeCampaignIdForProperty(campaignId) {
    if (campaignId === undefined || campaignId === null) return '';
    const sanitized = String(campaignId).trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
    return sanitized.slice(0, 60);
}

function resolveOutcomePropertyFromConfig(pipelineConfig = {}) {
    if (pipelineConfig.vooneCallOutcomeProperty) {
        return pipelineConfig.vooneCallOutcomeProperty;
    }
    const safe = sanitizeCampaignIdForProperty(pipelineConfig.campaignId);
    return safe ? `${VOONE_OUTCOME_PROPERTY}_${safe}` : VOONE_OUTCOME_PROPERTY;
}

function normalizeVooneOutcomeIntent(value, fallback = 'any') {
    const normalized = String(value || '').trim().toLowerCase();
    if (VOONE_OUTCOME_INTENTS.has(normalized)) return normalized;
    return fallback;
}

function isVooneOutcomeProperty(propertyName, outcomeProperty) {
    if (!propertyName) return false;
    const name = String(propertyName);
    if (name === outcomeProperty) return true;
    if (name === VOONE_OUTCOME_PROPERTY) return true;
    return name.startsWith(`${VOONE_OUTCOME_PROPERTY}_`);
}

function partitionFilters(filters = [], outcomeProperty = VOONE_OUTCOME_PROPERTY) {
    const serverFilters = [];
    const clientFilters = [];
    for (const filter of Array.isArray(filters) ? filters : []) {
        const propertyName = filter?.property || filter?.propertyName;
        if (isVooneOutcomeProperty(propertyName, outcomeProperty)) {
            clientFilters.push(filter);
        } else {
            serverFilters.push(filter);
        }
    }
    return { serverFilters, clientFilters };
}

function toComparableNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    if (typeof value === 'number') return value;
    const str = String(value).trim();
    if (str === '') return null;
    if (/^-?\d+$/.test(str)) return Number(str);
    const epoch = Date.parse(str);
    if (!Number.isNaN(epoch)) return epoch;
    return null;
}

function evaluateRecordAgainstFilter(record, filter) {
    const propertyName = filter.property || filter.propertyName;
    if (!propertyName) return true;
    const rawValue = record?.properties?.[propertyName];
    const op = String(filter.operator || 'EQ').toUpperCase();
    const isAbsent = rawValue === undefined || rawValue === null || rawValue === '';

    if (op === 'HAS_PROPERTY') return !isAbsent;
    if (op === 'NOT_HAS_PROPERTY') return isAbsent;

    if (isAbsent) {
        if (op === 'NEQ' || op === 'NOT_IN' || op === 'NOT_CONTAINS_TOKEN') return true;
        return false;
    }

    const rawNumeric = toComparableNumber(rawValue);
    const valueNumeric = toComparableNumber(filter.value);
    const highNumeric = toComparableNumber(filter.highValue);
    const compareNumeric = (cmp) => rawNumeric !== null && valueNumeric !== null && cmp(rawNumeric, valueNumeric);

    switch (op) {
        case 'EQ': return String(rawValue) === String(filter.value);
        case 'NEQ': return String(rawValue) !== String(filter.value);
        case 'GT': return compareNumeric((a, b) => a > b);
        case 'GTE': return compareNumeric((a, b) => a >= b);
        case 'LT': return compareNumeric((a, b) => a < b);
        case 'LTE': return compareNumeric((a, b) => a <= b);
        case 'BETWEEN':
            return rawNumeric !== null && valueNumeric !== null && highNumeric !== null && rawNumeric >= valueNumeric && rawNumeric <= highNumeric;
        case 'CONTAINS_TOKEN': return String(rawValue).toLowerCase().includes(String(filter.value ?? '').toLowerCase());
        case 'NOT_CONTAINS_TOKEN': return !String(rawValue).toLowerCase().includes(String(filter.value ?? '').toLowerCase());
        case 'IN': return Array.isArray(filter.values) && filter.values.map((value) => String(value)).includes(String(rawValue));
        case 'NOT_IN': return !(Array.isArray(filter.values) && filter.values.map((value) => String(value)).includes(String(rawValue)));
        default: return true;
    }
}

function applyRecordFilters(records = [], filters = []) {
    if (!Array.isArray(filters) || filters.length === 0) {
        return { records, excludedFilterCount: 0 };
    }
    const kept = [];
    let excludedFilterCount = 0;
    for (const record of records) {
        if (filters.every((filter) => evaluateRecordAgainstFilter(record, filter))) {
            kept.push(record);
        } else {
            excludedFilterCount += 1;
        }
    }
    return { records: kept, excludedFilterCount };
}

function filterRecordsByVooneOutcomeIntent(records = [], intent = 'any', outcomeProperty = VOONE_OUTCOME_PROPERTY) {
    if (intent === 'any') {
        return { records, excludedByIntent: 0 };
    }
    const kept = [];
    let excludedByIntent = 0;
    for (const record of records) {
        const outcome = String(record?.properties?.[outcomeProperty] || '').trim().toLowerCase();
        const isTerminal = TERMINAL_VOONE_OUTCOMES.has(outcome);
        const isSuccessful = outcome === 'successful';
        const isUnsuccessful = outcome === 'unsuccessful';

        let keep = true;
        if (intent === 'callable') keep = !isTerminal;
        else if (intent === 'terminal') keep = isTerminal;
        else if (intent === 'successful_only') keep = isSuccessful;
        else if (intent === 'unsuccessful_only') keep = isUnsuccessful;

        if (keep) kept.push(record);
        else excludedByIntent += 1;
    }
    return { records: kept, excludedByIntent };
}

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

function normalizeLead(record, phoneMapping, defaultRegion) {
    const rawPhone = pickPhoneFromRecord(record, phoneMapping);
    const normalizedPhone = normalizeE164Phone(rawPhone || '', defaultRegion || undefined);

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

function hasTerminalVooneOutcome(record, outcomeProperty = VOONE_OUTCOME_PROPERTY) {
    // Terminal-outcome filtering is driven exclusively by the per-record
    // outcome property (per-campaign or global). An absent or empty value
    // means "not yet called" — treat it as not-terminal so those contacts
    // remain callable. No global HubSpot list membership is consulted: the
    // only outcome filter at runtime is the LLM-emitted intent evaluated
    // against the per-record property.
    const value = record?.properties?.[outcomeProperty];
    return TERMINAL_VOONE_OUTCOMES.has(String(value || '').trim().toLowerCase());
}

function mergePropertiesWithOutcome(properties = [], outcomeProperty = VOONE_OUTCOME_PROPERTY) {
    const extras = outcomeProperty === VOONE_OUTCOME_PROPERTY
        ? [VOONE_OUTCOME_PROPERTY]
        : [VOONE_OUTCOME_PROPERTY, outcomeProperty];
    return [...new Set([...(Array.isArray(properties) ? properties : []), ...extras])];
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

function buildGenericFilterGroups(filters = []) {
    const normalized = filters
        .filter((filter) => filter && filter.property && filter.operator)
        .map((filter) => {
            const built = {
                propertyName: filter.property,
                operator: filter.operator
            };
            if (filter.values) built.values = filter.values;
            if (filter.value !== undefined) built.value = filter.value;
            if (filter.highValue !== undefined) built.highValue = filter.highValue;
            return built;
        });
    return normalized.length ? [{ filters: normalized }] : [];
}

class HubspotAudienceResolver {
    static async _fetchListMembers(subaccountId, listId, outcomeProperty = VOONE_OUTCOME_PROPERTY) {
        const url = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/lists/${listId}/contacts`;
        const { data } = await axios.get(url, {
            headers: buildHeaders(),
            params: {
                properties: mergePropertiesWithOutcome([
                    'firstname',
                    'lastname',
                    'email',
                    'phone',
                    'mobilephone',
                    'company'
                ], outcomeProperty).join(',')
            }
        });
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
                    company: contact.company,
                    [VOONE_OUTCOME_PROPERTY]: contact[VOONE_OUTCOME_PROPERTY]
                }
            }));

        return {
            records,
            sourceId: listId,
            objectTypeId: data.data?.objectTypeId || '0-1',
            objectTypeName: data.data?.objectTypeName || 'contacts',
            fetchedCount: Number(data.data?.total || records.length || 0)
        };
    }

    static async _fetchPipelineRecords(subaccountId, selectedSource, filters = [], outcomeProperty = VOONE_OUTCOME_PROPERTY) {
        // Outcome property filters are evaluated locally so contacts missing the
        // (often brand-new per-campaign) property are not silently dropped by
        // HubSpot's server-side search.
        const { serverFilters } = partitionFilters(filters, outcomeProperty);
        const filterGroups = buildHubspotFilterGroups(selectedSource, serverFilters);

        const url = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/objects/deals/search`;
        const { data } = await axios.post(
            url,
            {
                filterGroups,
                properties: mergePropertiesWithOutcome(['dealname', 'pipeline', 'dealstage', 'phone', 'mobilephone', 'hs_phone_number'], outcomeProperty),
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

    static async _fetchObjectRecords(subaccountId, selectedSource, filters = [], phoneMapping = null, outcomeProperty = VOONE_OUTCOME_PROPERTY) {
        const objectType = selectedSource.objectType || selectedSource.objectTypeName || selectedSource.objectTypeId;
        if (!objectType) {
            throw new Error('selectedSource.objectType is required for HubSpot object mode');
        }

        const url = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${subaccountId}/objects/${objectType}/search`;
        const searchProperties = [
            selectedSource.primaryDisplayProperty,
            phoneMapping,
            'firstname',
            'lastname',
            'email',
            'phone',
            'mobilephone',
            'hs_phone_number',
            'company',
            'dealname',
            'pipeline',
            'dealstage'
        ].filter(Boolean);

        const { serverFilters } = partitionFilters(filters, outcomeProperty);
        const serverFilterGroups = buildGenericFilterGroups(serverFilters);

        const { data } = await axios.post(
            url,
            {
                query: selectedSource.query || undefined,
                filterGroups: serverFilterGroups.length ? serverFilterGroups : undefined,
                properties: mergePropertiesWithOutcome(searchProperties, outcomeProperty),
                limit: Number(selectedSource.limit || 200)
            },
            { headers: buildHeaders() }
        );

        if (!data?.success) {
            throw new Error(data?.error || `Failed to search HubSpot ${objectType} records`);
        }

        const records = Array.isArray(data.data?.results) ? data.data.results : [];
        return {
            records,
            sourceId: objectType,
            objectTypeId: selectedSource.objectTypeId || null,
            objectTypeName: objectType,
            fetchedCount: Number(data.data?.total || records.length || 0)
        };
    }

    static async resolveAudience(subaccountId, pipelineConfig = {}, options = {}) {
        const mode = pipelineConfig.mode;
        const provider = String(pipelineConfig.provider || '').toLowerCase();
        const selectedSource = pipelineConfig.selectedSource || {};
        const filters = Array.isArray(pipelineConfig.filters) ? pipelineConfig.filters : [];
        const phoneMapping = pipelineConfig.phoneMapping || null;
        const phoneRegion = pipelineConfig.phoneRegion
            ? String(pipelineConfig.phoneRegion).trim().toUpperCase()
            : 'ES';
        const outcomeProperty = resolveOutcomePropertyFromConfig(pipelineConfig);
        const isCampaignScopedOutcome = outcomeProperty !== VOONE_OUTCOME_PROPERTY;
        const vooneCallOutcomeIntent = normalizeVooneOutcomeIntent(pipelineConfig.vooneCallOutcomeIntent, 'callable');
        const { audienceCursor = null, leadsPerRun: optionsLeadsPerRun = null } = options;

        if (provider !== 'hubspot') {
            throw new Error('Unsupported provider for resolver. Expected provider=hubspot');
        }

        let fetched;
        if (mode === 'list') {
            if (!selectedSource.listId) {
                throw new Error('selectedSource.listId is required for HubSpot list mode');
            }
            fetched = await this._fetchListMembers(subaccountId, selectedSource.listId, outcomeProperty);
        } else if (mode === 'pipeline') {
            if (!selectedSource.pipelineId || !selectedSource.stageId) {
                throw new Error('selectedSource.pipelineId and selectedSource.stageId are required for HubSpot pipeline mode');
            }
            fetched = await this._fetchPipelineRecords(subaccountId, selectedSource, filters, outcomeProperty);
        } else if (mode === 'object') {
            fetched = await this._fetchObjectRecords(subaccountId, selectedSource, filters, phoneMapping, outcomeProperty);
        } else {
            throw new Error('Unsupported pipelineConfig.mode. Expected list, object, or pipeline');
        }

        // Apply LLM-emitted filters in-memory. For list mode no filters are sent
        // to HubSpot. For object/pipeline modes the non-outcome filters were
        // already pushed down, but we re-run them locally with corrected
        // absent-property semantics — cheap, and keeps behavior uniform.
        const { records: filteredByPlan, excludedFilterCount } = applyRecordFilters(fetched.records, filters);

        // Intent-based outcome filtering is the ONLY outcome filter applied at
        // audience resolution. It evaluates the per-record outcome property
        // (per-campaign or global, depending on resolveOutcomePropertyFromConfig)
        // against the LLM-emitted intent. When intent === "any" no implicit
        // exclusion is applied; the property's absent state is treated as
        // "not yet contacted" and those records flow through. The legacy
        // global voone:successful / voone:unsuccessful HubSpot lists are
        // intentionally NOT consulted — the user has explicitly required no
        // hardcoded global-list filters.
        const { records: filteredByIntent, excludedByIntent } = filterRecordsByVooneOutcomeIntent(
            filteredByPlan,
            vooneCallOutcomeIntent,
            outcomeProperty
        );

        const leads = [];
        let invalidCount = 0;
        let skippedCount = 0;
        const skippedTaggedCount = excludedByIntent;

        for (const record of filteredByIntent) {
            const { lead, reason } = normalizeLead(record, phoneMapping, phoneRegion);
            if (!lead) {
                if (reason === 'invalid_phone') invalidCount += 1;
                else skippedCount += 1;
                continue;
            }
            lead.source = {
                provider: 'hubspot',
                mode,
                recordId: record.id,
                listId: mode === 'list' ? selectedSource.listId : null,
                objectType: mode === 'object' ? selectedSource.objectType : null,
                objectTypeId: fetched.objectTypeId || null,
                objectTypeName: fetched.objectTypeName || null
            };
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

        // Cursor-based windowing for list mode — advance to next set of leads per run
        let selectedLeads = deduped;
        let nextCursor = audienceCursor;
        if (mode === 'list' && optionsLeadsPerRun != null) {
            const target = Number(optionsLeadsPerRun);
            if (audienceCursor) {
                const cursorBig = BigInt(audienceCursor);
                const afterCursor = deduped.filter(l => l.source?.recordId && BigInt(l.source.recordId) > cursorBig);
                selectedLeads = afterCursor.slice(0, target);
                if (selectedLeads.length < target) {
                    const beforeOrAt = deduped.filter(l => l.source?.recordId && BigInt(l.source.recordId) <= cursorBig);
                    selectedLeads = selectedLeads.concat(beforeOrAt.slice(0, target - selectedLeads.length));
                }
            } else {
                selectedLeads = deduped.slice(0, target);
            }
            nextCursor = selectedLeads.at(-1)?.source?.recordId ?? audienceCursor;
        }

        const snapshot = {
            filtersUsed: filters,
            sourceId: fetched.sourceId,
            objectTypeId: fetched.objectTypeId,
            objectTypeName: fetched.objectTypeName,
            fetchedCount: fetched.fetchedCount,
            validCount: deduped.length,
            invalidCount,
            skippedCount,
            skippedTaggedCount,
            excludedFilterCount,
            outcomeProperty,
            campaignScopedOutcome: isCampaignScopedOutcome,
            vooneCallOutcomeIntent,
            phoneRegion: phoneRegion || null,
            nextCursor,
            audienceCursor
        };

        logger.info('[HubspotAudienceResolver] Resolved audience', {
            subaccountId,
            mode,
            sourceId: snapshot.sourceId,
            fetchedCount: snapshot.fetchedCount,
            validCount: snapshot.validCount,
            invalidCount: snapshot.invalidCount,
            skippedCount: snapshot.skippedCount,
            skippedTaggedCount: snapshot.skippedTaggedCount,
            excludedFilterCount: snapshot.excludedFilterCount,
            outcomeProperty: snapshot.outcomeProperty,
            campaignScopedOutcome: snapshot.campaignScopedOutcome,
            vooneCallOutcomeIntent: snapshot.vooneCallOutcomeIntent,
            phoneRegion: snapshot.phoneRegion || null
        });

        return {
            leads: selectedLeads,
            snapshot
        };
    }

    static async filterTerminalOutcomeLeads(subaccountId, leads = [], pipelineConfig = {}) {
        const outcomeProperty = resolveOutcomePropertyFromConfig(pipelineConfig);
        const isCampaignScopedOutcome = outcomeProperty !== VOONE_OUTCOME_PROPERTY;
        const vooneCallOutcomeIntent = normalizeVooneOutcomeIntent(pipelineConfig.vooneCallOutcomeIntent, 'callable');

        // When the user did not opt into terminal exclusion (i.e. intent is
        // anything other than "callable"), do not drop any leads here. An
        // explicit intent like "terminal" or "successful_only" must not be
        // re-excluded at dispatch time, and the default "callable" intent means
        // contacts with terminal outcomes are skipped unless overridden.
        if (vooneCallOutcomeIntent !== 'callable') {
            return {
                allowed: leads,
                skipped: [],
                outcomeProperty,
                campaignScopedOutcome: isCampaignScopedOutcome,
                vooneCallOutcomeIntent
            };
        }

        // Dispatch-time double-check. Only the per-record outcome property is
        // consulted — the legacy global voone:successful / voone:unsuccessful
        // HubSpot lists are intentionally NOT looked up here, per the
        // explicit "no hardcoded filters" requirement.
        const skipped = [];
        const allowed = [];

        for (const lead of leads) {
            const hubspot = lead?.attrs?.hubspot || {};
            const record = {
                id: hubspot.contactId || hubspot.recordId || lead.sourceRecordId,
                properties: {
                    ...(lead.properties || {}),
                    phone: lead.phone
                }
            };

            if (hasTerminalVooneOutcome(record, outcomeProperty)) {
                skipped.push(lead);
            } else {
                allowed.push(lead);
            }
        }

        return {
            allowed,
            skipped,
            outcomeProperty,
            campaignScopedOutcome: isCampaignScopedOutcome,
            vooneCallOutcomeIntent
        };
    }
}

module.exports = HubspotAudienceResolver;
