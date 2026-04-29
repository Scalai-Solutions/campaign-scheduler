const { Worker } = require('bullmq');
const CampaignChatSession = require('../models/CampaignChatSession');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { getOutgoingEdges, getNode, parseDelayToMs } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX, QUEUE_NAMES } = require('../queues');
const logger = require('../utils/logger');

function getCampaignChatCacheKeys(tenantId, phone) {
    const raw = String(phone || '').trim();
    if (!raw) return [];

    const digits = raw.replace(/^\+/, '');
    return [...new Set([
        `campaign:chat:active:${tenantId}:${raw}`,
        `campaign:chat:active:${tenantId}:${digits}`,
        `campaign:chat:active:${tenantId}:+${digits}`
    ])];
}

/**
 * ChatBatchReconciliationWorker
 *
 * Safety-net for chat nodes.
 *
 * Chat outcomes take much longer than voice calls — Retell closes a chat after
 * `end_chat_after_silence_ms` (default 6 min), but the lead may never reply.
 * This worker runs after CHAT_RECONCILE_DELAY_MS (default 2h) and forces any
 * still-active CampaignChatSessions to 'timeout', marking the lead as
 * 'not_answered' so the campaign can continue.
 *
 * Queue: chat.batch.reconcile
 * Job data: { nodeRunId }
 */
const worker = new Worker(QUEUE_NAMES.chatBatchReconcile, async (job) => {
    const { nodeRunId } = job.data;

    const nodeRun = await CampaignNodeRun.findById(nodeRunId);
    if (!nodeRun || nodeRun.status !== 'active') {
        logger.info('[ChatBatchReconcile] NodeRun not active, skipping', { nodeRunId });
        return;
    }

    // Find all sessions for this node run that are still active (lead never replied / chat never closed)
    const staleSessions = await CampaignChatSession.find({
        nodeRunId,
        status: { $in: ['active', 'pending'] }
    }).select('_id leadId phone tenantId retellChatId').lean();

    if (staleSessions.length === 0) {
        logger.info('[ChatBatchReconcile] No stale sessions found', { nodeRunId });
        return;
    }

    const staleLeadIds = staleSessions.map(s => s.leadId);
    const staleSessionIds = staleSessions.map(s => s._id);

    // Mark sessions as timed out
    await CampaignChatSession.updateMany(
        { _id: { $in: staleSessionIds } },
        {
            $set: {
                status:  'timeout',
                outcome: 'not_answered',
                lastActivityAt: new Date()
            }
        }
    );

    // Mark leads as completed with not_answered outcome
    await Lead.updateMany(
        { _id: { $in: staleLeadIds } },
        {
            $set: {
                nodeStatus:   'completed',
                outcome:      'not_answered',
                chatSessionId: null
            }
        }
    );

    // Increment node counters
    const updatedNodeRun = await CampaignNodeRun.findByIdAndUpdate(
        nodeRunId,
        {
            $inc: {
                completedLeads:           staleSessions.length,
                'outcomes.not_answered':  staleSessions.length
            }
        },
        { new: true }
    );

    logger.warn('[ChatBatchReconcile] Marked stale chat sessions as not_answered', {
        nodeRunId,
        staleCount:     staleSessions.length,
        completedLeads: updatedNodeRun?.completedLeads,
        totalLeads:     updatedNodeRun?.totalLeads
    });

    // Transition stale leads to their not_answered edge (mirrors BatchReconciliationWorker)
    try {
        const definition = await CampaignDefinition.findOne({
            tenantId:  nodeRun.tenantId,
            campaignId: nodeRun.campaignId,
            version:    nodeRun.campaignVersion
        });

        if (definition) {
            const outgoingEdges = getOutgoingEdges(definition.workflowJson, nodeRun.nodeId);
            const notAnsweredEdge = outgoingEdges.find(e => e.outcome === 'not_answered');

            if (notAnsweredEdge && notAnsweredEdge.toNodeId && staleLeadIds.length > 0) {
                await Lead.updateMany(
                    { _id: { $in: staleLeadIds } },
                    {
                        $set: {
                            currentNodeId: notAnsweredEdge.toNodeId,
                            nodeStatus:    'pending',
                            outcome:       null
                        }
                    }
                );

                const nextNode = getNode(definition.workflowJson, notAnsweredEdge.toNodeId);
                const delayMs  = parseDelayToMs(notAnsweredEdge.delay);
                const hasDelay = delayMs > 0;

                const nextNodeRun = await CampaignNodeRun.findOneAndUpdate(
                    {
                        campaignId:      nodeRun.campaignId,
                        campaignVersion: nodeRun.campaignVersion,
                        nodeId:          notAnsweredEdge.toNodeId,
                        parentNodeId:    nodeRun.nodeId,
                        sourceOutcome:   'not_answered'
                    },
                    {
                        $inc: { totalLeads: staleLeadIds.length },
                        $set: { updatedAt: new Date() },
                        $setOnInsert: {
                            tenantId:        nodeRun.tenantId,
                            campaignId:      nodeRun.campaignId,
                            campaignVersion: nodeRun.campaignVersion,
                            nodeId:          notAnsweredEdge.toNodeId,
                            agentId:         nextNode?.agentId,
                            agentType:       nextNode?.agentType || 'voice',
                            fromNumber:      nextNode?.fromNumber || null,
                            parentNodeId:    nodeRun.nodeId,
                            sourceOutcome:   'not_answered',
                            status:          hasDelay ? 'waiting_delay' : 'dispatching',
                            completedLeads:  0,
                            outcomes: { successful: 0, unsuccessful: 0, not_answered: 0 },
                            delayExpiresAt:  hasDelay ? new Date(Date.now() + delayMs) : null
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
                    const nextAgentType  = nextNode?.agentType || 'voice';
                    const dispatchQueue  = nextAgentType === 'chat'
                        ? queues.chatNodeDispatch
                        : queues.campaignNodeDispatch;

                    await dispatchQueue.add(
                        `dispatch-${nextNodeRun._id}`,
                        { nodeRunId: nextNodeRun._id.toString() },
                        {
                            jobId: `node-dispatch-${nextNodeRun._id}`,
                            delay: parseInt(process.env.PER_LEAD_DISPATCH_DELAY_MS || '3000', 10)
                        }
                    );
                }

                logger.info('[ChatBatchReconcile] Stale leads transitioned to not_answered edge', {
                    nodeRunId,
                    toNodeId:     notAnsweredEdge.toNodeId,
                    leadCount:    staleLeadIds.length,
                    nextNodeRunId: nextNodeRun._id.toString()
                });
            } else {
                // Terminal node — clear leads from campaign
                await Lead.updateMany(
                    { _id: { $in: staleLeadIds } },
                    {
                        $unset: {
                            campaignId:      '',
                            campaignVersion: '',
                            currentNodeId:   '',
                            outcome:         '',
                            nodeStatus:      '',
                            chatSessionId:   ''
                        }
                    }
                );
                logger.info('[ChatBatchReconcile] Stale leads at terminal node cleared', {
                    nodeRunId, leadCount: staleLeadIds.length
                });
            }
        }
    } catch (transitionError) {
        logger.error('[ChatBatchReconcile] Error during stale lead transition', {
            nodeRunId, error: transitionError.message
        });
    }

    // Evict Redis cache entries for stale sessions
    try {
        const { connection: redisConn } = require('../queues');
        const pipeline = redisConn.pipeline();
        for (const session of staleSessions) {
            for (const cacheKey of getCampaignChatCacheKeys(session.tenantId, session.phone)) {
                pipeline.del(cacheKey);
            }
        }
        await pipeline.exec();
    } catch (cacheErr) {
        logger.warn('[ChatBatchReconcile] Failed to evict Redis cache for stale sessions', {
            nodeRunId, error: cacheErr.message
        });
    }

    // Check node completion
    if (updatedNodeRun && updatedNodeRun.completedLeads >= updatedNodeRun.totalLeads && updatedNodeRun.totalLeads > 0) {
        await queues.nodeComplete.add(
            `complete-${nodeRunId}`,
            { nodeRunId: nodeRunId.toString() },
            { jobId: `node-complete-${nodeRunId}` }
        );
    }

}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_CHAT_RECONCILE || '5') });

worker.on('failed', (job, error) => {
    logger.error('[ChatBatchReconcile] Job failed', {
        jobId:     job?.id,
        nodeRunId: job?.data?.nodeRunId,
        error:     error?.message
    });
});

module.exports = worker;
