const { Worker } = require('bullmq');
const RetellEvent = require('../models/RetellEvent');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { determineOutcome, extractAnalysis } = require('../utils/batchingUtils');
const { getOutgoingEdges, getNode, parseDelayToMs } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX, QUEUE_NAMES } = require('../queues');
const logger = require('../utils/logger');

/* ---- Lightweight in-memory cache for CampaignDefinition ---- */
const DEFINITION_CACHE_TTL_MS = 30_000; // 30 s — definitions don't change mid-run
const definitionCache = new Map();       // key → { doc, expiry }

function getCachedDefinition(key) {
    const entry = definitionCache.get(key);
    if (entry && Date.now() < entry.expiry) return entry.doc;
    definitionCache.delete(key);
    return null;
}

function setCachedDefinition(key, doc) {
    definitionCache.set(key, { doc, expiry: Date.now() + DEFINITION_CACHE_TTL_MS });
    // Lazy eviction — cap size to prevent unbounded growth
    if (definitionCache.size > 500) {
        const now = Date.now();
        for (const [k, v] of definitionCache) {
            if (now >= v.expiry) definitionCache.delete(k);
        }
    }
}
/* ------------------------------------------------------------ */

const RETELL_EVENT_SWEEP_MS = Math.max(1000, parseInt(process.env.RETELL_EVENT_SWEEP_MS || '5000', 10));
const RETELL_EVENT_SWEEP_BATCH_SIZE = Math.max(1, parseInt(process.env.RETELL_EVENT_SWEEP_BATCH_SIZE || '200', 10));
const RETELL_EVENT_MATCH_RETRY_WINDOW_MS = Math.max(1000, parseInt(process.env.RETELL_EVENT_MATCH_RETRY_WINDOW_MS || '600000', 10));

let sweepIntervalHandle = null;

/**
 * Process a single Retell webhook event.
 *
 * New node-level flow (3 DB ops per event):
 *   1. Lead.findOneAndUpdate — set outcome + nodeStatus:'completed'
 *   2. CampaignNodeRun.$inc — increment completedLeads + outcomes.<outcome>
 *   3. Completion check — if completedLeads === totalLeads → enqueue node.complete
 *
 * The lead is identified from call metadata (campaignId + nodeId + leadId).
 */
async function processRetellEvent(retellEventId, embeddedPayload) {
    const event = await RetellEvent.findById(retellEventId);
    if (event && event.status === 'processed') return;

    const payload = event?.payloadJson || embeddedPayload;
    if (!payload) {
        logger.error('[RetellEventProcess] Event not found and no embedded payload', { retellEventId });
        throw new Error(`RetellEvent ${retellEventId} not found and no embedded payload`);
    }

    const metadata = payload.call?.metadata || payload.metadata;
    if (!metadata?.campaignId || !metadata?.nodeId || !metadata?.leadId) {
        // Cannot match without campaign metadata — check age and retry or fail
        const receivedAt = event?.receivedAt || event?.createdAt || Date.now();
        const eventAgeMs = Date.now() - new Date(receivedAt).getTime();
        if (eventAgeMs < RETELL_EVENT_MATCH_RETRY_WINDOW_MS) return; // retry later

        if (event) {
            event.status = 'failed';
            event.processedAt = new Date();
            await event.save();
        }
        logger.warn('[RetellEventProcess] Missing campaign metadata, event failed', { retellEventId });
        return;
    }

    const { campaignId, nodeId, leadId, nodeRunId } = metadata;

    // Determine outcome
    const outcome = determineOutcome(payload);
    const analysis = extractAnalysis(payload);

    // 1. Update Lead: set outcome + nodeStatus:'completed'
    const updatedLead = await Lead.findOneAndUpdate(
        { _id: leadId, campaignId, currentNodeId: nodeId, nodeStatus: 'in_progress' },
        {
            $set: {
                outcome,
                nodeStatus: 'completed',
                retellAnalysis: analysis
            }
        },
        { new: true }
    );

    if (!updatedLead) {
        // Lead may have already been processed (idempotency)
        logger.warn('[RetellEventProcess] Lead not found or already completed', { leadId, campaignId, nodeId });
        if (event) {
            event.status = 'processed';
            event.processedAt = new Date();
            await event.save();
        }
        return;
    }

    // Store call details on the RetellEvent itself
    if (event) {
        event.callId = payload.call?.call_id || payload.call_id;
        event.outcome = outcome;
    }

    // 1b. Immediately transition the lead to the next node based on its outcome.
    //     This ensures each lead progresses as soon as it completes, without waiting
    //     for the entire batch — fixing edge cases where late/failed webhooks would
    //     stall the whole node and incorrectly mark the campaign as complete.
    try {
        const tenantId = metadata.tenantId;
        const campaignVersion = Number(metadata.version);
        if (!tenantId || isNaN(campaignVersion)) {
            logger.warn('[RetellEventProcess] Missing tenantId or version in metadata, skipping per-lead transition', {
                leadId, campaignId, tenantId, version: metadata.version
            });
        } else {
        const defCacheKey = `${tenantId}:${campaignId}:${campaignVersion}`;
        let definition = getCachedDefinition(defCacheKey);
        if (!definition) {
            definition = await CampaignDefinition.findOne({ tenantId, campaignId, version: campaignVersion });
            if (definition) setCachedDefinition(defCacheKey, definition);
        }

        if (definition) {
            const outgoingEdges = getOutgoingEdges(definition.workflowJson, nodeId);
            const matchingEdge = outgoingEdges.find(e => e.outcome === outcome);

            if (matchingEdge && matchingEdge.toNodeId) {
                // Move this lead to the next node immediately
                await Lead.updateOne(
                    { _id: updatedLead._id },
                    { $set: { currentNodeId: matchingEdge.toNodeId, nodeStatus: 'pending', outcome: null } }
                );

                const nextNode = getNode(definition.workflowJson, matchingEdge.toNodeId);
                const delayMs = parseDelayToMs(matchingEdge.delay);
                const hasDelay = delayMs > 0;

                // Upsert the next CampaignNodeRun — increment totalLeads for this lead
                const nextNodeRun = await CampaignNodeRun.findOneAndUpdate(
                    {
                        campaignId,
                        campaignVersion,
                        nodeId: matchingEdge.toNodeId,
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
                            nodeId: matchingEdge.toNodeId,
                            agentId: nextNode?.agentId,
                            agentType: nextNode?.agentType ?? null,
                            fromNumber: nextNode?.fromNumber || null,
                            parentNodeId: nodeId,
                            sourceOutcome: outcome,
                            status: hasDelay ? 'waiting_delay' : 'dispatching',
                            completedLeads: 0,
                            outcomes: { successful: 0, unsuccessful: 0, not_answered: 0 },
                            delayExpiresAt: hasDelay ? new Date(Date.now() + delayMs) : null
                        }
                    },
                    { upsert: true, new: true }
                );

                // If the nodeRun was just created or transitioned from waiting_delay with no delay
                if (!hasDelay && nextNodeRun.status === 'waiting_delay') {
                    await CampaignNodeRun.updateOne(
                        { _id: nextNodeRun._id, status: 'waiting_delay' },
                        { $set: { status: 'dispatching' } }
                    );
                }

                // Enqueue a deduped dispatch job with a short accumulation window.
                // Using jobId ensures only one dispatch fires per node run; the delay
                // gives other in-flight events a chance to accumulate at the next node
                // so they can be batched into a single Retell batch call.
                if (!hasDelay) {
                    const isNextChat = nextNode?.agentType === 'chat';
                    const dispatchQueue = isNextChat ? queues.chatNodeDispatch : queues.campaignNodeDispatch;
                    await dispatchQueue.add(
                        `dispatch-${nextNodeRun._id}`,
                        { nodeRunId: nextNodeRun._id.toString() },
                        {
                            jobId: `node-dispatch-${nextNodeRun._id}`,
                            delay: parseInt(process.env.PER_LEAD_DISPATCH_DELAY_MS || '3000', 10)
                        }
                    );
                }

                logger.info('[RetellEventProcess] Lead transitioned to next node', {
                    leadId: updatedLead._id.toString(),
                    fromNodeId: nodeId,
                    toNodeId: matchingEdge.toNodeId,
                    outcome,
                    nextNodeRunId: nextNodeRun._id.toString(),
                    hasDelay
                });
            } else {
                // No matching outgoing edge — this is a terminal lead.
                // Clear its campaign assignment so it is not counted as stuck.
                await Lead.updateOne(
                    { _id: updatedLead._id },
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
                logger.info('[RetellEventProcess] Terminal lead cleared from campaign', {
                    leadId: updatedLead._id.toString(),
                    nodeId,
                    outcome
                });
            }
        } else {
            logger.warn('[RetellEventProcess] CampaignDefinition not found for per-lead transition', {
                tenantId,
                campaignId,
                version: metadata.version
            });
        }
        } // end tenantId/version guard
    } catch (transitionError) {
        // Non-fatal — log and continue so the nodeRun counters are still updated.
        logger.error('[RetellEventProcess] Error during per-lead transition', {
            leadId: updatedLead._id?.toString(),
            campaignId,
            nodeId,
            error: transitionError.message
        });
    }

    // 2. Atomically increment CampaignNodeRun counters
    const outcomeField = `outcomes.${outcome}`;
    const updatedNodeRun = await CampaignNodeRun.findOneAndUpdate(
        nodeRunId
            ? { _id: nodeRunId }
            : { campaignId, nodeId, status: 'active' },
        {
            $inc: { completedLeads: 1, [outcomeField]: 1 }
        },
        { new: true }
    );

    if (!updatedNodeRun) {
        logger.warn('[RetellEventProcess] CampaignNodeRun not found for increment', { campaignId, nodeId, nodeRunId });
    }

    // 3. Check if node is complete (all leads processed)
    if (updatedNodeRun && updatedNodeRun.completedLeads >= updatedNodeRun.totalLeads) {
        await queues.nodeComplete.add(
            `complete-${updatedNodeRun._id}`,
            { nodeRunId: updatedNodeRun._id.toString() },
            { jobId: `node-complete-${updatedNodeRun._id}` }
        );
        logger.info('[RetellEventProcess] Node complete, enqueued node.complete', {
            nodeRunId: updatedNodeRun._id.toString(),
            completedLeads: updatedNodeRun.completedLeads,
            totalLeads: updatedNodeRun.totalLeads
        });
    }

    // Mark event as processed
    if (event) {
        event.status = 'processed';
        event.processedAt = new Date();
        await event.save();
    }

    logger.info('[RetellEventProcess] Event processed', {
        retellEventId, leadId, outcome, nodeId, campaignId
    });
}

const worker = new Worker(QUEUE_NAMES.retellEventsProcess, async (job) => {
    const { retellEventId, payload } = job.data;
    await processRetellEvent(retellEventId, payload);
}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_EVENT_PROCESS || '10') });

worker.on('failed', (job, error) => {
    logger.error('[RetellEventProcess] Job failed', {
        jobId: job?.id,
        retellEventId: job?.data?.retellEventId,
        error: error?.message
    });
});

async function enqueuePendingRetellEvents() {
    const pendingEvents = await RetellEvent.find({ status: 'received' })
        .sort({ createdAt: 1 })
        .limit(RETELL_EVENT_SWEEP_BATCH_SIZE)
        .select('_id')
        .lean();

    if (pendingEvents.length === 0) return;

    const bulkJobs = pendingEvents.map(pe => ({
        name: `retell-event-${pe._id}`,
        data: { retellEventId: pe._id.toString() },
        opts: { removeOnComplete: true, removeOnFail: 50 }
    }));

    try {
        await queues.retellEventsProcess.addBulk(bulkJobs);
    } catch (error) {
        logger.error('[RetellEventProcess] Failed to bulk-enqueue pending events', {
            count: bulkJobs.length,
            error: error.message
        });
    }
}

worker.on('ready', async () => {
    try {
        await enqueuePendingRetellEvents();
        sweepIntervalHandle = setInterval(() => {
            enqueuePendingRetellEvents().catch((error) => {
                logger.error('[RetellEventProcess] Pending-event sweep error:', error.message);
            });
        }, RETELL_EVENT_SWEEP_MS);
    } catch (error) {
        logger.error('[RetellEventProcess] Failed to start pending-event sweep:', error.message);
    }
});

worker.on('closed', () => {
    if (sweepIntervalHandle) {
        clearInterval(sweepIntervalHandle);
        sweepIntervalHandle = null;
    }
});

module.exports = worker;
