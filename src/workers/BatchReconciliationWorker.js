const { Worker } = require('bullmq');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { getOutgoingEdges, getNode, parseDelayToMs } = require('../campaignKernel');
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

    const stragglerFilter = {
        campaignId: nodeRun.campaignId,
        currentNodeId: nodeRun.nodeId,
        nodeStatus: 'in_progress'
    };

    // Fetch straggler IDs once and reuse throughout — avoids a redundant find later
    const stragglerLeadIds = await Lead.find(stragglerFilter)
        .select('_id')
        .lean()
        .then(docs => docs.map(d => d._id));

    if (stragglerLeadIds.length === 0) {
        logger.info('[BatchReconcile] No stragglers found', { nodeRunId, batchCallId });
        return;
    }

    await Lead.updateMany(
        { _id: { $in: stragglerLeadIds } },
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
                completedLeads: stragglerLeadIds.length,
                'outcomes.not_answered': stragglerLeadIds.length
            }
        },
        { new: true }
    );

    logger.warn('[BatchReconcile] Marked stragglers as not_answered', {
        nodeRunId,
        batchCallId,
        stragglerCount: stragglerLeadIds.length,
        completedLeads: updatedNodeRun?.completedLeads,
        totalLeads: updatedNodeRun?.totalLeads
    });

    // Transition straggler leads to their next node (not_answered edge).
    // Since per-lead transitions now happen in RetellEventProcessWorker, we must
    // do the same here for leads that only got resolved by the safety-net reconciler.
    try {
        const definition = await CampaignDefinition.findOne({
            tenantId: nodeRun.tenantId,
            campaignId: nodeRun.campaignId,
            version: nodeRun.campaignVersion
        });

        if (definition) {
            const outgoingEdges = getOutgoingEdges(definition.workflowJson, nodeRun.nodeId);
            const notAnsweredEdge = outgoingEdges.find(e => e.outcome === 'not_answered');

            if (notAnsweredEdge && notAnsweredEdge.toNodeId && stragglerLeadIds.length > 0) {
                await Lead.updateMany(
                    { _id: { $in: stragglerLeadIds } },
                    { $set: { currentNodeId: notAnsweredEdge.toNodeId, nodeStatus: 'pending', outcome: null } }
                );

                const nextNode = getNode(definition.workflowJson, notAnsweredEdge.toNodeId);
                const delayMs = parseDelayToMs(notAnsweredEdge.delay);
                const hasDelay = delayMs > 0;

                const nextNodeRun = await CampaignNodeRun.findOneAndUpdate(
                    {
                        campaignId: nodeRun.campaignId,
                        campaignVersion: nodeRun.campaignVersion,
                        nodeId: notAnsweredEdge.toNodeId,
                        parentNodeId: nodeRun.nodeId,
                        sourceOutcome: 'not_answered'
                    },
                    {
                        $inc: { totalLeads: stragglerLeadIds.length },
                        $set: { updatedAt: new Date() },
                        $setOnInsert: {
                            tenantId: nodeRun.tenantId,
                            campaignId: nodeRun.campaignId,
                            campaignVersion: nodeRun.campaignVersion,
                            nodeId: notAnsweredEdge.toNodeId,
                            agentId: nextNode?.agentId,
                            agentType: nextNode?.agentType || 'voice',
                            fromNumber: nextNode?.fromNumber || null,
                            parentNodeId: nodeRun.nodeId,
                            sourceOutcome: 'not_answered',
                            status: hasDelay ? 'waiting_delay' : 'dispatching',
                            completedLeads: 0,
                            outcomes: { successful: 0, unsuccessful: 0, not_answered: 0 },
                            delayExpiresAt: hasDelay ? new Date(Date.now() + delayMs) : null
                        }
                    },
                    { upsert: true, new: true }
                );

                if (!hasDelay && nextNodeRun.status === 'waiting_delay') {
                    await CampaignNodeRun.updateOne(
                        { _id: nextNodeRun._id, status: 'waiting_delay' },
                        { $set: { status: 'dispatching' } }
                    );
                }

                if (!hasDelay) {
                    await queues.campaignNodeDispatch.add(
                        `dispatch-${nextNodeRun._id}`,
                        { nodeRunId: nextNodeRun._id.toString() },
                        {
                            jobId: `node-dispatch-${nextNodeRun._id}`,
                            delay: parseInt(process.env.PER_LEAD_DISPATCH_DELAY_MS || '3000', 10)
                        }
                    );
                }

                logger.info('[BatchReconcile] Straggler leads transitioned to next node', {
                    nodeRunId,
                    stragglerCount: stragglerLeadIds.length,
                    toNodeId: notAnsweredEdge.toNodeId,
                    nextNodeRunId: nextNodeRun._id.toString()
                });
            } else {
                // No not_answered edge — terminal leads; clear their campaign state
                if (stragglerLeadIds.length > 0) {
                    await Lead.updateMany(
                        { _id: { $in: stragglerLeadIds } },
                        {
                            $unset: {
                                campaignId: '',
                                campaignVersion: '',
                                currentNodeId: '',
                                outcome: '',
                                nodeStatus: ''
                            }
                        }
                    );
                    logger.info('[BatchReconcile] Terminal straggler leads cleared', {
                        nodeRunId, stragglerCount: stragglerLeadIds.length
                    });
                }
            }
        }
    } catch (transitionError) {
        logger.error('[BatchReconcile] Error transitioning straggler leads', {
            nodeRunId,
            error: transitionError.message
        });
    }

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
