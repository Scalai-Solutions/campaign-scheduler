const axios = require('axios');
const logger = require('../utils/logger');

const CONNECTOR_SERVER_URL = process.env.CONNECTOR_SERVER_URL || 'http://connector-server:3004';

function mapRetellOutcome(outcome) {
    if (outcome === 'successful') return 'successful';
    if (outcome === 'unsuccessful' || outcome === 'not_answered') return 'unsuccessful';
    return null;
}

function getHubspotContactId(lead) {
    const hubspot = lead?.attrs?.hubspot;
    if (!hubspot || hubspot.provider !== 'hubspot') return null;
    if (hubspot.contactId) return hubspot.contactId;
    if (hubspot.objectTypeName === 'contacts' || hubspot.objectTypeId === '0-1') {
        return hubspot.recordId || null;
    }
    return null;
}

async function writeBackRetellOutcome({ tenantId, lead, outcome, metadata = {}, payload = {} }) {
    const hubspotOutcome = mapRetellOutcome(outcome);
    if (!hubspotOutcome) {
        return { skipped: true, reason: 'unsupported_outcome' };
    }

    const contactId = getHubspotContactId(lead);
    if (!tenantId || !contactId) {
        return { skipped: true, reason: 'missing_hubspot_contact' };
    }

    try {
        const url = `${CONNECTOR_SERVER_URL}/api/internal/hubspot/${tenantId}/outcome-writeback`;
        const { data } = await axios.post(
            url,
            {
                contactId,
                outcome: hubspotOutcome,
                campaignId: metadata.campaignId,
                nodeId: metadata.nodeId,
                leadId: metadata.leadId,
                callId: payload.call?.call_id || payload.call_id || null
            },
            {
                headers: {
                    'x-internal-service': 'campaign-scheduler',
                    'Content-Type': 'application/json'
                },
                timeout: parseInt(process.env.HUBSPOT_OUTCOME_WRITEBACK_TIMEOUT_MS || '8000', 10)
            }
        );

        if (!data?.success) {
            logger.warn('[HubspotOutcomeWriteback] HubSpot outcome writeback returned failure', {
                tenantId,
                contactId,
                outcome: hubspotOutcome,
                error: data?.error,
                details: data?.data
            });
            return { success: false, error: data?.error };
        }

        logger.info('[HubspotOutcomeWriteback] HubSpot outcome written back', {
            tenantId,
            contactId,
            outcome: hubspotOutcome
        });
        return { success: true, data: data.data };
    } catch (error) {
        logger.warn('[HubspotOutcomeWriteback] Failed to write HubSpot outcome', {
            tenantId,
            contactId,
            outcome: hubspotOutcome,
            error: error.message
        });
        return { success: false, error: error.message };
    }
}

module.exports = {
    mapRetellOutcome,
    writeBackRetellOutcome
};
