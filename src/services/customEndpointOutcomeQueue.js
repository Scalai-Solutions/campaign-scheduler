const { queues } = require('../queues');
const logger = require('../utils/logger');

function buildLeadPayload(lead, outcome, context = {}) {
    const customEndpoint = lead?.attrs?.customEndpoint;
    if (!customEndpoint?.customConnectorId) return null;

    return {
        tenantId: context.tenantId || lead.tenantId,
        campaignId: context.campaignId || lead.campaignId,
        nodeId: context.nodeId || lead.currentNodeId,
        leadId: lead._id?.toString(),
        phone: lead.phone,
        outcome,
        customConnectorId: customEndpoint.customConnectorId,
        properties: customEndpoint.properties || {},
        completedAt: new Date().toISOString(),
        source: customEndpoint
    };
}

async function enqueueCustomEndpointOutcome({ lead, outcome, context = {} }) {
    const payload = buildLeadPayload(lead, outcome, context);
    if (!payload) return false;

    await queues.customEndpointOutcome.add(
        `custom-outcome-${payload.leadId}-${payload.outcome}`,
        payload,
        {
            jobId: `custom-outcome-${payload.leadId}-${payload.outcome}-${payload.nodeId || 'node'}`,
            attempts: 5,
            backoff: { type: 'exponential', delay: 1000 },
            removeOnComplete: 1000,
            removeOnFail: 5000
        }
    );
    return true;
}

async function enqueueCustomEndpointOutcomes({ leads, outcome, context = {} }) {
    const results = await Promise.allSettled(
        (Array.isArray(leads) ? leads : []).map((lead) => enqueueCustomEndpointOutcome({ lead, outcome, context }))
    );
    const enqueued = results.filter((result) => result.status === 'fulfilled' && result.value).length;
    const failed = results.filter((result) => result.status === 'rejected').length;

    if (enqueued || failed) {
        logger.info('[CustomEndpointOutcomeQueue] Enqueue batch complete', {
            outcome,
            leadCount: Array.isArray(leads) ? leads.length : 0,
            enqueued,
            failed
        });
    }
}

module.exports = {
    enqueueCustomEndpointOutcome,
    enqueueCustomEndpointOutcomes
};