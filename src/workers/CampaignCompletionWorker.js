const { Worker } = require('bullmq');
const { connection, BULL_PREFIX } = require('../queues');
const MultirunCampaign = require('../models/MultirunCampaign');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://database-server:3000';

const worker = new Worker('campaign.completion', async (job) => {
    const { tenantId, campaignId } = job.data;

    const multirunProjection = await MultirunCampaign.findOne({ tenantId, campaignId }).lean();
    if (multirunProjection?.campaignType === 'multirun' && multirunProjection?.isLive) {
        const lifecycleUrl = `${DATABASE_SERVER_URL}/internal/campaigns/${campaignId}/multirun-lifecycle`;
        const lifecycleRes = await fetch(lifecycleUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tenantId,
                status: 'saved',
                isLive: true,
                lastRunAt: multirunProjection.lastRunAt,
                nextRunAt: multirunProjection.nextRunAt
            }),
        });

        if (!lifecycleRes.ok) {
            const body = await lifecycleRes.text();
            throw new Error(`Multirun lifecycle sync failed (${lifecycleRes.status}): ${body}`);
        }

        console.log(`[CampaignCompletion] Multirun campaign ${campaignId} run completed; campaign remains live`);
        return;
    }

    console.log(`[CampaignCompletion] Marking campaign ${campaignId} as completed for tenant ${tenantId}`);

    const url = `${DATABASE_SERVER_URL}/internal/campaigns/${campaignId}/complete`;
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tenantId }),
    });

    if (!res.ok) {
        const body = await res.text();
        throw new Error(`Campaign completion request failed (${res.status}): ${body}`);
    }

    console.log(`[CampaignCompletion] Campaign ${campaignId} marked as completed`);
}, {
    connection,
    prefix: BULL_PREFIX,
    concurrency: parseInt(process.env.WORKER_CONCURRENCY_CAMPAIGN_COMPLETION || '3'),
});

worker.on('failed', (job, error) => {
    console.error('[CampaignCompletion] Job failed', {
        jobId: job?.id,
        campaignId: job?.data?.campaignId,
        error: error?.message,
    });
});

module.exports = worker;
