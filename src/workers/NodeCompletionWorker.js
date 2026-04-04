const { Worker } = require('bullmq');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { getNode, getOutgoingEdges, parseDelayToMs } = require('../campaignKernel');
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
 *   2. Load workflow definition → get outgoing edges from the completed node
 *   3. For each edge (outcome-specific):
 *      a. Find leads that completed this node with the matching outcome
 *      b. Move leads to the next node: Lead.updateMany → currentNodeId, nodeStatus:'pending', outcome:null
 *      c. Find or update the pre-created waiting_delay CampaignNodeRun with totalLeads
 *      d. If no delay (delayExpiresAt === null): set status → 'dispatching' and enqueue campaign.node.dispatch
 *   4. If no outgoing edges for any lead, check if campaign is fully complete
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

    // 2. Load workflow
    const definition = await CampaignDefinition.findOne({
        tenantId: nodeRun.tenantId,
        campaignId: nodeRun.campaignId,
        version: nodeRun.campaignVersion
    });
    if (!definition) {
        logger.error('[NodeCompletion] No CampaignDefinition found', { nodeRunId });
        return;
    }

    const outgoingEdges = getOutgoingEdges(definition.workflowJson, nodeRun.nodeId);

    // 3. For each outgoing edge, move leads and prepare next node runs
    let totalLeadsMoved = 0;

    for (const edge of outgoingEdges) {
        if (!edge.toNodeId) continue;

        // 3a. Find leads that completed with this edge's outcome
        const matchFilter = {
            campaignId: nodeRun.campaignId,
            currentNodeId: nodeRun.nodeId,
            nodeStatus: 'completed',
            outcome: edge.outcome
        };

        const matchingLeadCount = await Lead.countDocuments(matchFilter);
        if (matchingLeadCount === 0) continue;

        // 3b. Move leads to the next node
        await Lead.updateMany(matchFilter, {
            $set: {
                currentNodeId: edge.toNodeId,
                nodeStatus: 'pending',
                outcome: null
            }
        });

        totalLeadsMoved += matchingLeadCount;

        // 3c. Update the pre-created CampaignNodeRun stub with lead count
        const nextNode = getNode(definition.workflowJson, edge.toNodeId);
        const delayMs = parseDelayToMs(edge.delay);
        const hasDelay = delayMs > 0;

        const nextNodeRun = await CampaignNodeRun.findOneAndUpdate(
            {
                campaignId: nodeRun.campaignId,
                campaignVersion: nodeRun.campaignVersion,
                nodeId: edge.toNodeId,
                parentNodeId: nodeRun.nodeId,
                sourceOutcome: edge.outcome
            },
            {
                $set: {
                    totalLeads: matchingLeadCount,
                    ...(hasDelay ? {} : { status: 'dispatching' })
                },
                $setOnInsert: {
                    tenantId: nodeRun.tenantId,
                    campaignId: nodeRun.campaignId,
                    campaignVersion: nodeRun.campaignVersion,
                    nodeId: edge.toNodeId,
                    agentId: nextNode?.agentId,
                    agentType: nextNode?.agentType,
                    fromNumber: nextNode?.fromNumber || null,
                    status: hasDelay ? 'waiting_delay' : 'dispatching',
                    delayExpiresAt: hasDelay ? new Date(Date.now() + delayMs) : null,
                    parentNodeId: nodeRun.nodeId,
                    sourceOutcome: edge.outcome
                }
            },
            { upsert: true, new: true }
        );

        // 3d. If no delay, dispatch immediately
        if (!hasDelay) {
            await queues.campaignNodeDispatch.add(
                `dispatch-${nextNodeRun._id}`,
                { nodeRunId: nextNodeRun._id.toString() }
            );
            logger.info('[NodeCompletion] Immediate dispatch for next node', {
                nextNodeRunId: nextNodeRun._id.toString(),
                nodeId: edge.toNodeId,
                leadCount: matchingLeadCount
            });
        } else {
            logger.info('[NodeCompletion] Next node waiting for delay', {
                nextNodeRunId: nextNodeRun._id.toString(),
                nodeId: edge.toNodeId,
                leadCount: matchingLeadCount,
                delayMs
            });
        }
    }

    // Cancel pre-created sibling stubs that received no leads from this completed node.
    // These are outcome branches that were not taken and would otherwise keep the campaign in "running".
    const staleStubResult = await CampaignNodeRun.updateMany(
        {
            campaignId: nodeRun.campaignId,
            campaignVersion: nodeRun.campaignVersion,
            parentNodeId: nodeRun.nodeId,
            status: { $in: ['waiting_delay', 'dispatching'] },
            totalLeads: { $lte: 0 }
        },
        {
            $set: { status: 'cancelled' }
        }
    );

    // 4. Check if there are leads with no outgoing edge (terminal leads)
    const terminalLeadCount = await Lead.countDocuments({
        campaignId: nodeRun.campaignId,
        currentNodeId: nodeRun.nodeId,
        nodeStatus: 'completed'
    });

    if (terminalLeadCount > 0) {
        // These leads have finished the workflow — clear their campaign assignment
        await Lead.updateMany(
            {
                campaignId: nodeRun.campaignId,
                currentNodeId: nodeRun.nodeId,
                nodeStatus: 'completed'
            },
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
    }

    // Check if entire campaign is done (no active/waiting node runs remain)
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
        nodeRunId, nodeId: nodeRun.nodeId, edgesProcessed: outgoingEdges.length,
        totalLeadsMoved, terminalLeads: terminalLeadCount,
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
