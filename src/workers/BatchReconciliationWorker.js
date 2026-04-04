const { Worker } = require('bullmq');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const Lead = require('../models/Lead');
const { connection, queues, BULL_PREFIX } = require('../queues');
const logger = require('../utils/logger');

/**
 * BatchReconciliationWorker
 *
 * Safety-net only.
 *
 * If a Retell batch has straggler leads still marked `in_progress` after the
 * reconciliation delay, mark them `not_answered`, increment node counters, and
 * enqueue node.complete so the workflow can continue.
 */
const worker = new Worker('batch.reconcile', async (job) => {
    const { nodeRunId, batchCallId } = job.data;

    const nodeRun = await CampaignNodeRun.findById(nodeRunId);
    if (!nodeRun || nodeRun.status !== 'active') {
        return;
    }

    const stragglerCount = await Lead.countDocuments({
        campaignId: nodeRun.campaignId,
        currentNodeId: nodeRun.nodeId,
        nodeStatus: 'in_progress'
    });

    if (stragglerCount === 0) {
        logger.info('[BatchReconcile] No stragglers found', { nodeRunId, batchCallId });
        return;
    }

    await Lead.updateMany(
        {
            campaignId: nodeRun.campaignId,
            currentNodeId: nodeRun.nodeId,
            nodeStatus: 'in_progress'
        },
        {
            $set: {
                nodeStatus: 'completed',
                outcome: 'not_answered'
            }
        }
    );

    const updatedNodeRun = await CampaignNodeRun.findByIdAndUpdate(
        nodeRunId,
        {
            $inc: {
                completedLeads: stragglerCount,
                'outcomes.not_answered': stragglerCount
            }
        },
        { new: true }
    );

    logger.warn('[BatchReconcile] Marked stragglers as not_answered', {
        nodeRunId,
        batchCallId,
        stragglerCount,
        completedLeads: updatedNodeRun?.completedLeads,
        totalLeads: updatedNodeRun?.totalLeads
    });

    if (updatedNodeRun && updatedNodeRun.completedLeads >= updatedNodeRun.totalLeads) {
        await queues.nodeComplete.add(
            `complete-${nodeRunId}`,
            { nodeRunId: nodeRunId.toString() },
            { jobId: `node-complete-${nodeRunId}` }
        );
    }
}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_BATCH_RECONCILE || '3', 10) });

worker.on('failed', (job, error) => {
    logger.error('[BatchReconcile] Job failed', {
        jobId: job?.id,
        nodeRunId: job?.data?.nodeRunId,
        error: error?.message
    });
});

module.exports = worker;
