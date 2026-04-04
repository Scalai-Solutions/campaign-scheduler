const { Worker } = require('bullmq');
const RetellEvent = require('../models/RetellEvent');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const Lead = require('../models/Lead');
const { determineOutcome, extractAnalysis } = require('../utils/batchingUtils');
const { connection, queues, BULL_PREFIX, QUEUE_NAMES } = require('../queues');
const logger = require('../utils/logger');

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

    for (const pendingEvent of pendingEvents) {
        try {
            await queues.retellEventsProcess.add(
                `retell-event-${pendingEvent._id}`,
                { retellEventId: pendingEvent._id.toString() },
                { removeOnComplete: true, removeOnFail: 50 }
            );
        } catch (error) {
            if (!String(error.message || '').toLowerCase().includes('job')) {
                logger.error('[RetellEventProcess] Failed to enqueue pending event', {
                    retellEventId: pendingEvent._id.toString(),
                    error: error.message
                });
            }
        }
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
