const { Worker } = require('bullmq');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const Lead = require('../models/Lead');
const { connection, queues, BULL_PREFIX } = require('../queues');
const logger = require('../utils/logger');

/**
 * NodeCompletionWorker
 *
 * Handles the `node.complete` queue.
 *
 * Job data: { nodeRunId }
 *
 * Flow:
 *   1. Mark the CampaignNodeRun as 'completed'
 *   2. Cancel pre-created sibling node run stubs that received no leads
 *      (outcome branches that were never taken must not keep the campaign running)
 *   3. Check if the entire campaign is done (no active/dispatching node runs remain)
 *
 * NOTE: Lead-to-next-node transitions are now handled per-lead in
 * RetellEventProcessWorker immediately after each lead's webhook is processed.
 * This worker no longer moves leads — it only marks the node done and checks
 * for campaign completion.
 */
const worker = new Worker('node.complete', async (job) => {
    const { nodeRunId } = job.data;

    // 1. Mark the node run as completed
    const nodeRun = await CampaignNodeRun.findOneAndUpdate(
        { _id: nodeRunId, status: { $in: ['active', 'completed'] } },
        { $set: { status: 'completed' } },
        { new: true }
    );
    if (!nodeRun) {
        logger.warn('[NodeCompletion] NodeRun not found or already cancelled', { nodeRunId });
        return;
    }

    // 2. Cancel pre-created sibling stubs that received no leads from this node.
    //    These are outcome branches that were not taken; without cancellation they
    //    would keep activeNodeRunCount > 0 and block campaign completion.
    const staleStubResult = await CampaignNodeRun.updateMany(
        {
            campaignId: nodeRun.campaignId,
            campaignVersion: nodeRun.campaignVersion,
            parentNodeId: nodeRun.nodeId,
            status: { $in: ['waiting_delay', 'dispatching'] },
            totalLeads: { $lte: 0 }
        },
        { $set: { status: 'cancelled' } }
    );

    // 3. Check if entire campaign is done (no active/waiting node runs remain)
    const activeNodeRunCount = await CampaignNodeRun.countDocuments({
        campaignId: nodeRun.campaignId,
        status: { $in: ['waiting_delay', 'dispatching', 'active'] },
        $or: [
            { totalLeads: { $gt: 0 } },
            { status: 'active' },
            { status: 'dispatching', totalLeads: { $gt: 0 } }
        ]
    });

    if (activeNodeRunCount === 0) {
        await queues.campaignCompletion.add(
            `complete-${nodeRun.campaignId}`,
            { tenantId: nodeRun.tenantId, campaignId: nodeRun.campaignId },
            { jobId: `campaign-complete-${nodeRun.tenantId}-${nodeRun.campaignId}` }
        );
        logger.info('[NodeCompletion] Campaign fully complete', {
            campaignId: nodeRun.campaignId, tenantId: nodeRun.tenantId
        });
    }

    logger.info('[NodeCompletion] Node completion processed', {
        nodeRunId,
        nodeId: nodeRun.nodeId,
        cancelledStaleStubs: staleStubResult.modifiedCount || 0
    });

}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_NODE_COMPLETE || '5') });

worker.on('failed', (job, error) => {
    logger.error('[NodeCompletion] Job failed', {
        jobId: job?.id,
        nodeRunId: job?.data?.nodeRunId,
        error: error?.message
    });
});

module.exports = worker;
