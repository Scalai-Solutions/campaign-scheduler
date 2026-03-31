const { Worker } = require('bullmq');
const { connection, BULL_PREFIX } = require('../queues');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://scalai-database-server:3000';

const worker = new Worker('campaign.completion', async (job) => {
    const { tenantId, campaignId } = job.data;

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
