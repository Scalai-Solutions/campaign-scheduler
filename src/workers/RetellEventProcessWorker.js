const { Worker } = require('bullmq');
const RetellEvent = require('../models/RetellEvent');
const StepExecution = require('../models/StepExecution');
const CampaignRun = require('../models/CampaignRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const ScheduledTask = require('../models/ScheduledTask');
const NextStepIntent = require('../models/NextStepIntent');
const Lead = require('../models/Lead');
const { resolveNext, computeDueAt, makeTaskDedupeKey } = require('../campaignKernel');
const { 
    computeDispatchConfigHash,
    computeBatchCompatibilityKey,
    computeIntentDedupeKey,
    determineOutcome,
    extractAnalysis
} = require('../utils/batchingUtils');
const { connection, createConnection, queues, BULL_PREFIX, QUEUE_NAMES } = require('../queues');

const RETELL_EVENT_SWEEP_MS = Math.max(1000, parseInt(process.env.RETELL_EVENT_SWEEP_MS || '5000', 10));
const RETELL_EVENT_SWEEP_BATCH_SIZE = Math.max(1, parseInt(process.env.RETELL_EVENT_SWEEP_BATCH_SIZE || '200', 10));
const RETELL_EVENT_MATCH_RETRY_WINDOW_MS = Math.max(1000, parseInt(process.env.RETELL_EVENT_MATCH_RETRY_WINDOW_MS || '600000', 10));

// Separate Redis client for signaling the aggregation worker via list-based pub/sub.
// Uses createConnection() so it is also cluster-aware in production.
const redisClient = createConnection();

let sweepIntervalHandle = null;

function parseDelayToMs(delay) {
    if (delay == null) return 0;

    if (typeof delay === 'object') {
        const value = Number(delay.value);
        const unit = String(delay.unit || '').trim().toLowerCase();
        if (!Number.isFinite(value) || value < 0) return 0;

        if (['ms', 'millisecond', 'milliseconds'].includes(unit)) return value;
        if (['s', 'sec', 'secs', 'second', 'seconds'].includes(unit)) return value * 1000;
        if (['m', 'min', 'mins', 'minute', 'minutes'].includes(unit)) return value * 60 * 1000;
        if (['h', 'hr', 'hrs', 'hour', 'hours'].includes(unit)) return value * 60 * 60 * 1000;
        if (['d', 'day', 'days'].includes(unit)) return value * 24 * 60 * 60 * 1000;

        // Backward compatibility with workflow validator naming.
        if (unit === 'mins') return value * 60 * 1000;
        if (unit === 'hrs') return value * 60 * 60 * 1000;

        return 0;
    }

    if (typeof delay === 'number' && Number.isFinite(delay)) {
        // Backward compatibility: numeric delays are treated as seconds.
        return Math.max(0, delay * 1000);
    }

    if (typeof delay !== 'string') return 0;

    const raw = delay.trim().toLowerCase();
    if (!raw) return 0;

    if (/^\d+$/.test(raw)) {
        return parseInt(raw, 10) * 1000;
    }

    const match = raw.match(/^(\d+)\s*(ms|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$/);
    if (!match) return 0;

    const value = parseInt(match[1], 10);
    const unit = match[2];

    if (unit === 'ms') return value;
    if (['s', 'sec', 'secs', 'second', 'seconds'].includes(unit)) return value * 1000;
    if (['m', 'min', 'mins', 'minute', 'minutes'].includes(unit)) return value * 60 * 1000;
    if (['h', 'hr', 'hrs', 'hour', 'hours'].includes(unit)) return value * 60 * 60 * 1000;
    if (['d', 'day', 'days'].includes(unit)) return value * 24 * 60 * 60 * 1000;

    return 0;
}

async function findStepExecutionForEvent(payload, metadata) {
    let stepExecution;

    // Retell metadata key casing can vary by transport/version.
    const normalizedMetadata = metadata || {};
    const stepExecutionId = normalizedMetadata.stepExecutionId
        || normalizedMetadata.step_execution_id
        || normalizedMetadata.stepexecutionid;
    const runId = normalizedMetadata.runId
        || normalizedMetadata.run_id
        || normalizedMetadata.runid;
    const leadId = normalizedMetadata.leadId
        || normalizedMetadata.lead_id
        || normalizedMetadata.leadid;
    const nodeId = normalizedMetadata.nodeId
        || normalizedMetadata.node_id
        || normalizedMetadata.nodeid
        || normalizedMetadata.nextNodeId
        || normalizedMetadata.next_node_id
        || normalizedMetadata.nextnodeid;

    if (stepExecutionId) {
        stepExecution = await StepExecution.findById(stepExecutionId);
    }

    if (!stepExecution) {
        const callId = payload.call?.call_id || payload.call_id;
        if (callId) {
            stepExecution = await StepExecution.findOne({ 'retell.callId': callId });
        }
    }

    if (!stepExecution) {
        if (runId || leadId || nodeId) {
            const openStatusQuery = {
                status: { $in: ['waiting_result', 'queued', 'pending'] }
            };
            if (runId) openStatusQuery.runId = runId;
            if (leadId) openStatusQuery.leadId = leadId;
            if (nodeId) openStatusQuery.nodeId = nodeId;

            stepExecution = await StepExecution.findOne(openStatusQuery).sort({ createdAt: -1 });

            // Fallback: allow already-finalized steps so duplicates/late events are
            // handled idempotently instead of being retried repeatedly.
            if (!stepExecution) {
                const anyStatusQuery = {};
                if (runId) anyStatusQuery.runId = runId;
                if (leadId) anyStatusQuery.leadId = leadId;
                if (nodeId) anyStatusQuery.nodeId = nodeId;

                stepExecution = await StepExecution.findOne(anyStatusQuery).sort({ createdAt: -1 });
            }
        }
    }

    if (!stepExecution) {
        const batchCallId = payload.call?.batch_call_id || payload.batch_call_id;
        if (batchCallId) {
            let candidates = await StepExecution.find({
                'retell.batchCallId': batchCallId,
                status: { $in: ['waiting_result', 'queued', 'pending'] }
            }).sort({ createdAt: -1 }).limit(10);

            // Fallback: if active candidates are gone, include terminal states to
            // support idempotent duplicate webhook handling.
            if (candidates.length === 0) {
                candidates = await StepExecution.find({
                    'retell.batchCallId': batchCallId
                }).sort({ createdAt: -1 }).limit(10);
            }

            if (candidates.length === 1) {
                stepExecution = candidates[0];
            } else if (candidates.length > 1) {
                const toNumber = payload.call?.to_number || payload.to_number;
                if (toNumber) {
                    const leadIds = candidates.map(candidate => candidate.leadId).filter(Boolean);
                    const matchingLeads = await Lead.find({
                        _id: { $in: leadIds },
                        phone: toNumber
                    }).select('_id').lean();

                    if (matchingLeads.length > 0) {
                        const leadIdSet = new Set(matchingLeads.map((lead) => String(lead._id)));
                        const matchedCandidate = candidates.find((candidate) => leadIdSet.has(String(candidate.leadId)));
                        if (matchedCandidate) {
                            stepExecution = matchedCandidate;
                        }
                    }
                }

                if (!stepExecution) {
                    stepExecution = candidates[0];
                }
            }
        }
    }

    return stepExecution;
}

async function processRetellEvent(retellEventId, embeddedPayload) {
    const event = await RetellEvent.findById(retellEventId);

    if (event && event.status === 'processed') return;

    // Use the DB document's payload when available; fall back to the payload
    // that was embedded in the BullMQ job data (avoids tenant-DB ≠ scheduler-DB mismatch).
    const payload = event?.payloadJson || embeddedPayload;
    if (!payload) {
        console.error('[RetellEventProcess] Event not found and no embedded payload', { retellEventId });
        throw new Error(`RetellEvent ${retellEventId} not found and no embedded payload`);
    }
    if (!event) {
        console.warn('[RetellEventProcess] Event not found in local DB, using embedded payload', { retellEventId });
    }

    const metadata = payload.call?.metadata || payload.metadata; // Adjust based on Retell payload

    const stepExecution = await findStepExecutionForEvent(payload, metadata);

    if (stepExecution && ['completed', 'failed', 'timeout'].includes(stepExecution.status)) {
        // Idempotency: duplicate webhooks can legitimately arrive for a step that
        // has already been finalized by another event. Mark this event as processed
        // to prevent repeated "retry_later" churn for up to the retry window.
        if (event) {
            event.status = 'processed';
            event.processedAt = new Date();
            await event.save();
        }

        console.info('[RetellEventProcess] Duplicate/late event for terminal step execution', {
            retellEventId: event?._id?.toString() || retellEventId,
            stepExecutionId: stepExecution._id.toString(),
            stepStatus: stepExecution.status,
            callId: payload.call?.call_id || payload.call_id,
            batchCallId: payload.call?.batch_call_id || payload.batch_call_id
        });
        return;
    }

    if (!stepExecution) {
        const receivedAt = event?.receivedAt || event?.createdAt || Date.now();
        const eventAgeMs = Date.now() - new Date(receivedAt).getTime();
        const shouldKeepRetrying = eventAgeMs < RETELL_EVENT_MATCH_RETRY_WINDOW_MS;

        console.warn('[RetellEventProcess] No matching step execution found for event', {
            retellEventId: event?._id?.toString() || retellEventId,
            externalEventId: event?.externalEventId,
            callId: payload.call?.call_id || payload.call_id,
            batchCallId: payload.call?.batch_call_id || payload.batch_call_id,
            eventAgeMs,
            action: shouldKeepRetrying ? 'retry_later' : 'mark_failed'
        });

        if (shouldKeepRetrying) {
            return;
        }

        if (event) {
            event.status = 'failed';
            event.processedAt = new Date();
            await event.save();
        }
        return;
    }

    // Finalize outcome using Retell's classification
    // Tiered logic: unanswered > successful > unsuccessful
    let outcome = 'unsuccessful';
    const reason = payload.call?.disconnection_reason || payload.disconnection_reason;
    const callAnalysis = payload.call_analysis || payload.chat_analysis || {};

    // 1. Check for "not_answered" reasons (not connected)
    const unansweredReasons = ['dial_busy', 'dial_failed', 'dial_no_answer'];
    if (unansweredReasons.includes(reason)) {
        outcome = 'not_answered';
    } 
    // 2. Check for other unanswered (voicemail)
    else if (reason === 'voicemail') {
        outcome = 'not_answered';
    }
    // 3. Check for successful/unsuccessful using "call successful status"
    else {
        // Use determineOutcome utility for consistency
        outcome = determineOutcome(payload);
    }

    stepExecution.status = 'completed';
    stepExecution.outcome = outcome;
    stepExecution.endedAt = new Date();
    if (payload.call?.call_id) stepExecution.retell.callId = payload.call.call_id;
    
    // Extract and store call analysis
    const analysis = extractAnalysis(payload);
    if (analysis) {
        stepExecution.retell.analysis = analysis;
    }
    
    await stepExecution.save();

    // Resolve next node
    const run = await CampaignRun.findById(stepExecution.runId);
    const definition = await CampaignDefinition.findOne({
        tenantId: run.tenantId,
        campaignId: run.campaignId,
        version: run.campaignVersion
    });

    const { toNodeId, delay } = resolveNext(definition.workflowJson, stepExecution.nodeId, outcome);

    console.log(`[RetellEventProcess] Event ${event._id} (Run: ${run._id}) Outcome: ${outcome}. Next: ${toNodeId || 'End'}`);

    if (toNodeId) {
        console.log(`[RetellEventProcess] Starting creation of NextStepIntent for node: ${toNodeId}`);
        try {
            // Fetch next node definition to get agent config
            const nextNodeDef = definition.workflowJson.nodes.find(n => n.id === toNodeId);
            if (!nextNodeDef) {
                throw new Error(`Next node ${toNodeId} not found in definition`);
            }

            // Compute dispatch config hash from next node's agent config
            const agentConfig = nextNodeDef.agentConfig || {};
            const dispatchConfigHash = computeDispatchConfigHash(nextNodeDef, agentConfig);

            // Create intent dedup key (prevents replay duplicates)
            const intentDedupeKey = computeIntentDedupeKey(stepExecution._id.toString(), outcome);

            // Supersede any stale pending intents for this run+node from prior dispatch
            // cycles. These accumulate when earlier dispatches failed or were retried,
            // and their leftovers corrupt the workflow-manager timing display.
            await NextStepIntent.updateMany(
                {
                    runId: run._id,
                    nextNodeId: toNodeId,
                    status: { $in: ['pending_scheduled', 'pending_aggregation', 'pending_retry'] },
                    'metadata.intentDedupeKey': { $ne: intentDedupeKey }
                },
                {
                    $set: {
                        status: 'failed',
                        completedAt: new Date(),
                        outcome: 'failed',
                        'metadata.supersededReason': 'superseded_by_new_dispatch_cycle'
                    }
                }
            );

            // Calculate resolved delay (in ms) from workflow delay strings (e.g. 5m, 1h, 2d)
            const resolvedDelayMs = parseDelayToMs(delay);
            const now = new Date();
            const resolvedDelay = resolvedDelayMs > 0 ? resolvedDelayMs : 0;
            const dispatchTime = new Date(now.getTime() + resolvedDelayMs);

            console.log(`[RetellEventProcess] Resolved delay for ${toNodeId}: raw=${JSON.stringify(delay)} → ${resolvedDelayMs}ms, dispatchTime=${dispatchTime.toISOString()}`);

            // Determine batch compatibility key
            const batchCompatibilityKey = computeBatchCompatibilityKey(
                {
                    tenantId: run.tenantId,
                    campaignId: run.campaignId,
                    campaignVersion: run.campaignVersion,
                    nextNodeId: toNodeId,
                    nextNodeAgentId: nextNodeDef.agentId || nextNodeDef.id,
                    nextNodeAgentType: nextNodeDef.agentType || 'default'
                },
                dispatchConfigHash,
                resolvedDelayMs  // Pass millisecond delay as 3rd parameter
            );

            // Create or find existing intent (if replayed)
            const intent = await NextStepIntent.findOneAndUpdate(
                { 'metadata.intentDedupeKey': intentDedupeKey },
                {
                    $setOnInsert: {
                        tenantId: run.tenantId,
                        campaignId: run.campaignId,
                        campaignVersion: run.campaignVersion,
                        runId: run._id,
                        leadId: run.leadId,
                        currentNodeId: stepExecution.nodeId,
                        currentStepExecutionId: stepExecution._id,
                        currentOutcome: outcome,
                        completedAt: new Date(),
                        nextNodeId: toNodeId,
                        nextNodeAgentId: nextNodeDef.agentId || nextNodeDef.id,
                        nextNodeAgentType: nextNodeDef.agentType || 'default',
                        resolvedDelay,
                        dispatchTime,
                        dispatchConfigHash,
                        batchCompatibilityKey,
                        status: resolvedDelay > 0 ? 'pending_scheduled' : 'pending_aggregation',
                        metadata: {
                            createdAtMs: Date.now(),
                            intentDedupeKey,
                            originalEventId: event._id.toString(),
                            correlationId: metadata?.correlationId || `evt-${event._id}`
                        }
                    }
                },
                { upsert: true, new: true, setDefaultsOnInsert: true, runValidators: true }
            );

            const isExistingIntent = intent.metadata?.createdAtMs < Date.now() - 100;
            console.log(`[RetellEventProcess] NextStepIntent ${isExistingIntent ? 'deduped (existing)' : 'created new'}: ${intent._id}`);

            // If immediate dispatch (no delay), signal aggregation worker
            if (resolvedDelayMs === 0) {
                const signalKey = `{agg}:immediate:signal:${batchCompatibilityKey}`;
                await redisClient.lpush(signalKey, intent._id.toString());
                await redisClient.expire(signalKey, 3600); // 1-hour TTL on signal key
                console.log(`[RetellEventProcess] Signaled aggregation worker for batch key: ${batchCompatibilityKey}`);
            } else {
                console.log(`[RetellEventProcess] Intent scheduled for later (delay: ${resolvedDelayMs}ms), aggregation not signaled`);
            }

            // Update run progress
            run.currentNodeId = toNodeId;
            run.currentNodeStatus = 'pending';
            run.agentStatus = 'completed'; // For the node that just finished
            run.lastStepOutcome = outcome;
            run.nextStepIntentId = intent._id; // Track latest intent for this run
            await run.save();
            console.log(`[RetellEventProcess] CampaignRun ${run._id} updated successfully to node ${toNodeId}`);

        } catch (innerError) {
            console.error(`[RetellEventProcess] Error creating NextStepIntent:`, innerError);
            
            // Fallback to old ScheduledTask for compatibility
            console.log(`[RetellEventProcess] Falling back to ScheduledTask creation`);
            try {
                const now = new Date();
                const dueAt = computeDueAt(now, delay);
                const dueAtIso = dueAt.toISOString();
                const dedupeKey = makeTaskDedupeKey(run._id.toString(), toNodeId, dueAtIso);

                const nextTask = await ScheduledTask.findOneAndUpdate(
                    { dedupeKey },
                    {
                        $setOnInsert: {
                            tenantId: run.tenantId,
                            runId: run._id,
                            leadId: run.leadId,
                            nodeId: toNodeId,
                            dueAt: dueAt,
                            status: 'scheduled'
                        }
                    },
                    { upsert: true, new: true, setDefaultsOnInsert: true }
                );

                run.currentNodeId = toNodeId;
                run.currentNodeStatus = 'scheduled';
                run.agentStatus = 'completed';
                run.lastStepOutcome = outcome;
                await run.save();
            } catch (fallbackError) {
                console.error(`[RetellEventProcess] Fallback ScheduledTask also failed:`, fallbackError);
                throw innerError; // Throw original error
            }
        }
    } else {
        // Check if run is complete (no other pending steps)
        // For simplicity, we'll mark it completed if this path ends. 
        // In complex workflows, we might need a more global check.
        run.status = 'completed';
        run.currentNodeStatus = 'completed';
        run.agentStatus = 'completed'; 
        run.lastStepOutcome = outcome;
        await run.save();
    }

    if (event) {
        event.status = 'processed';
        event.processedAt = new Date();
        await event.save();
    }

}

const worker = new Worker(QUEUE_NAMES.retellEventsProcess, async (job) => {
    const { retellEventId, payload } = job.data;
    await processRetellEvent(retellEventId, payload);
}, { connection, prefix: BULL_PREFIX });

worker.on('failed', (job, error) => {
    console.error('[RetellEventProcess] Job failed', {
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
                {
                    removeOnComplete: true,
                    removeOnFail: 50
                }
            );
        } catch (error) {
            // Ignore duplicate job-id races across multiple scheduler instances.
            if (!String(error.message || '').toLowerCase().includes('job')) {
                console.error('[RetellEventProcess] Failed to enqueue pending event', {
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
                console.error('[RetellEventProcess] Pending-event sweep error:', error.message);
            });
        }, RETELL_EVENT_SWEEP_MS);
    } catch (error) {
        console.error('[RetellEventProcess] Failed to start pending-event sweep:', error.message);
    }
});

worker.on('closed', () => {
    if (sweepIntervalHandle) {
        clearInterval(sweepIntervalHandle);
        sweepIntervalHandle = null;
    }
});

module.exports = worker;
