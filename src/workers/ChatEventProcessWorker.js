const { Worker } = require('bullmq');
const CampaignChatSession = require('../models/CampaignChatSession');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { determineChatOutcome, getOutgoingEdges, getNode, parseDelayToMs } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX, QUEUE_NAMES } = require('../queues');
const logger = require('../utils/logger');

/* ---- Lightweight in-memory cache for CampaignDefinition ---- */
const DEFINITION_CACHE_TTL_MS = 30_000;
const definitionCache = new Map();

function getCachedDefinition(key) {
    const entry = definitionCache.get(key);
    if (entry && Date.now() < entry.expiry) return entry.doc;
    definitionCache.delete(key);
    return null;
}

function setCachedDefinition(key, doc) {
    definitionCache.set(key, { doc, expiry: Date.now() + DEFINITION_CACHE_TTL_MS });
    if (definitionCache.size > 500) {
        const now = Date.now();
        for (const [k, v] of definitionCache) {
            if (now >= v.expiry) definitionCache.delete(k);
        }
    }
}

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
/* ------------------------------------------------------------ */

/**
 * ChatEventProcessWorker
 *
 * Handles the `chat.events.process` queue.
 *
 * Triggered by the database-server when Retell sends a `chat_ended` or
 * `chat_analyzed` webhook for a chat agent that has campaign metadata
 * (`metadata.channel === 'whatsapp_campaign'`).
 *
 * Job data: {
 *   retellChatId,
 *   tenantId,
 *   campaignId,
 *   version (number),
 *   nodeId,
 *   nodeRunId,
 *   leadId,
 *   chatAnalysis   (Retell chat_analysis object),
 *   chatStatus,
 * }
 *
 * Per-lead flow (mirrors RetellEventProcessWorker):
 *   1. Determine outcome from chatAnalysis
 *   2. Update CampaignChatSession → completed + outcome
 *   3. Update Lead → nodeStatus:'completed', outcome
 *   4. Transition lead to next node
 *   5. Upsert next CampaignNodeRun (increment totalLeads)
 *   6. Increment current CampaignNodeRun counters
 *   7. Check for node completion
 *   8. Evict Redis cache entry
 */
const worker = new Worker(QUEUE_NAMES.chatEventsProcess, async (job) => {
    const {
        retellChatId,
        tenantId,
        campaignId,
        version: versionRaw,
        nodeId,
        nodeRunId,
        leadId,
        chatAnalysis,
    } = job.data;

    const campaignVersion = Number(versionRaw);

    if (!campaignId || !nodeId || !leadId) {
        logger.warn('[ChatEventProcess] Missing required fields in job data', {
            jobId: job.id, campaignId, nodeId, leadId
        });
        return;
    }

    // 1. Determine outcome
    const outcome = determineChatOutcome(chatAnalysis);

    // 2. Update CampaignChatSession
    let updatedSession = null;
    if (retellChatId) {
        updatedSession = await CampaignChatSession.findOneAndUpdate(
            { retellChatId, status: { $in: ['active', 'pending'] } },
            {
                $set: {
                    status:       'completed',
                    outcome,
                    chatAnalysis: chatAnalysis || null,
                    lastActivityAt: new Date()
                }
            },
            {
                new: true,
                projection: { phone: 1, tenantId: 1 }
            }
        );
    }

    // 3. Update Lead: set outcome + nodeStatus:'completed'
    const updatedLead = await Lead.findOneAndUpdate(
        { _id: leadId, campaignId, currentNodeId: nodeId, nodeStatus: 'in_progress' },
        {
            $set: {
                outcome,
                nodeStatus: 'completed',
                chatSessionId: null
            }
        },
        { new: true }
    );

    if (!updatedLead) {
        // Already processed (idempotency guard)
        logger.warn('[ChatEventProcess] Lead not found or already completed', {
            leadId, campaignId, nodeId
        });
        return;
    }

    // 4 & 5. Per-lead transition to next node
    try {
        const defCacheKey = `${tenantId}:${campaignId}:${campaignVersion}`;
        let definition = getCachedDefinition(defCacheKey);
        if (!definition) {
            definition = await CampaignDefinition.findOne(
                { tenantId, campaignId, version: campaignVersion },
                { workflowJson: 1 }
            ).lean();
            if (definition) setCachedDefinition(defCacheKey, definition);
        }

        if (definition) {
            const outgoingEdges = getOutgoingEdges(definition.workflowJson, nodeId);
            const matchingEdge = outgoingEdges.find(e => e.outcome === outcome);

            if (matchingEdge && matchingEdge.toNodeId) {
                // Move the lead to the next node immediately
                await Lead.updateOne(
                    { _id: updatedLead._id },
                    { $set: { currentNodeId: matchingEdge.toNodeId, nodeStatus: 'pending', outcome: null } }
                );

                const nextNode = getNode(definition.workflowJson, matchingEdge.toNodeId);
                const delayMs = parseDelayToMs(matchingEdge.delay);
                const hasDelay = delayMs > 0;

                // Upsert next CampaignNodeRun
                const nextNodeRun = await CampaignNodeRun.findOneAndUpdate(
                    {
                        campaignId,
                        campaignVersion,
                        nodeId:       matchingEdge.toNodeId,
                        parentNodeId: nodeId,
                        sourceOutcome: outcome
                    },
                    {
                        $inc: { totalLeads: 1 },
                        $set: { updatedAt: new Date() },
                        $setOnInsert: {
                            tenantId,
                            campaignId,
                            campaignVersion,
                            nodeId:          matchingEdge.toNodeId,
                            agentId:         nextNode?.agentId,
                            agentType:       nextNode?.agentType || 'voice',
                            fromNumber:      nextNode?.fromNumber || null,
                            parentNodeId:    nodeId,
                            sourceOutcome:   outcome,
                            status:          hasDelay ? 'waiting_delay' : 'dispatching',
                            completedLeads:  0,
                            outcomes: { successful: 0, unsuccessful: 0, not_answered: 0 },
                            delayExpiresAt:  hasDelay ? new Date(Date.now() + delayMs) : null
                        }
                    },
                    { upsert: true, new: true }
                );

                // If the node run just became dispatchable (no delay) but was previously
                // waiting (could happen if created earlier as waiting_delay)
                if (!hasDelay && nextNodeRun.status === 'waiting_delay') {
                    await CampaignNodeRun.updateOne(
                        { _id: nextNodeRun._id, status: 'waiting_delay' },
                        { $set: { status: 'dispatching' } }
                    );
                }

                // Enqueue dispatch only for immediate (zero-delay) next nodes.
                // For delayed nodes the scheduler poll loop handles dispatch.
                if (!hasDelay) {
                    // Determine which dispatch queue to use based on next node type
                    const nextAgentType = nextNode?.agentType || 'voice';
                    const dispatchQueue = nextAgentType === 'chat'
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

                logger.info('[ChatEventProcess] Lead transitioned to next node', {
                    leadId: updatedLead._id.toString(),
                    fromNodeId: nodeId,
                    toNodeId:   matchingEdge.toNodeId,
                    outcome,
                    nextNodeRunId: nextNodeRun._id.toString(),
                    hasDelay
                });
            } else {
                // Terminal lead — no matching outgoing edge
                await Lead.updateOne(
                    { _id: updatedLead._id },
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
                logger.info('[ChatEventProcess] Terminal lead cleared from campaign', {
                    leadId: updatedLead._id.toString(), nodeId, outcome
                });
            }
        } else {
            logger.warn('[ChatEventProcess] CampaignDefinition not found for per-lead transition', {
                tenantId, campaignId, version: versionRaw
            });
        }
    } catch (transitionError) {
        logger.error('[ChatEventProcess] Error during per-lead transition', {
            leadId, campaignId, nodeId, error: transitionError.message
        });
    }

    // 6. Increment CampaignNodeRun counters atomically
    const outcomeField = `outcomes.${outcome}`;
    const nodeRunFilter = nodeRunId
        ? { _id: nodeRunId }
        : { campaignId, campaignVersion, nodeId, status: 'active' };
    const updatedNodeRun = await CampaignNodeRun.findOneAndUpdate(
        nodeRunFilter,
        {
            $inc: {
                completedLeads:  1,
                [outcomeField]:  1
            }
        },
        { new: true }
    );

    if (!updatedNodeRun) {
        logger.error('[ChatEventProcess] CampaignNodeRun not found for counter update', {
            campaignId, nodeId, nodeRunId
        });
        return;
    }

    // 7. 8. Evict the Redis cache entry and check for node completion
    try {
        if (updatedSession?.phone && updatedSession?.tenantId) {
            const pipeline = connection.pipeline();
            for (const cacheKey of getCampaignChatCacheKeys(updatedSession.tenantId, updatedSession.phone)) {
                pipeline.del(cacheKey);
            }
            await pipeline.exec();
        }
    } catch (cacheErr) {
        logger.warn('[ChatEventProcess] Failed to evict Redis cache for chat session', {
            retellChatId, error: cacheErr.message
        });
    }

    logger.info('[ChatEventProcess] Node counters updated', {
        campaignId, nodeId, nodeRunId,
        completedLeads: updatedNodeRun.completedLeads,
        totalLeads:     updatedNodeRun.totalLeads,
        outcome
    });

    // Check if all leads are accounted for
    if (updatedNodeRun.completedLeads >= updatedNodeRun.totalLeads && updatedNodeRun.totalLeads > 0) {
        await queues.nodeComplete.add(
            `complete-${updatedNodeRun._id}`,
            { nodeRunId: updatedNodeRun._id.toString() },
            { jobId: `node-complete-${updatedNodeRun._id}` }
        );
        logger.info('[ChatEventProcess] Node complete enqueued', {
            nodeRunId: updatedNodeRun._id.toString()
        });
    }

}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_CHAT_EVENTS || '10') });

worker.on('failed', (job, error) => {
    logger.error('[ChatEventProcess] Job failed', {
        jobId:         job?.id,
        campaignId:    job?.data?.campaignId,
        leadId:        job?.data?.leadId,
        error:         error?.message
    });
});

module.exports = worker;
