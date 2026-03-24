const { Worker } = require('bullmq');
const RetellEvent = require('../models/RetellEvent');
const StepExecution = require('../models/StepExecution');
const CampaignRun = require('../models/CampaignRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const ScheduledTask = require('../models/ScheduledTask');
const NextStepIntent = require('../models/NextStepIntent');
const { resolveNext, computeDueAt, makeTaskDedupeKey } = require('../campaignKernel');
const { 
    computeDispatchConfigHash,
    computeBatchCompatibilityKey,
    computeIntentDedupeKey,
    determineOutcome,
    extractAnalysis
} = require('../utils/batchingUtils');
const { connection, createConnection, BULL_PREFIX, QUEUE_NAMES } = require('../queues');

// Separate Redis client for signaling the aggregation worker via list-based pub/sub.
// Uses createConnection() so it is also cluster-aware in production.
const redisClient = createConnection();

function parseDelayToMs(delay) {
    if (delay == null) return 0;
    if (typeof delay === 'number' && Number.isFinite(delay)) {
        // Backward compatibility: numeric delays are treated as seconds.
        return Math.max(0, delay * 1000);
    }
    if (typeof delay !== 'string') return 0;

    const match = delay.match(/^(\d+)([hdm])$/);
    if (!match) return 0;

    const value = parseInt(match[1], 10);
    const unit = match[2];

    if (unit === 'h') return value * 60 * 60 * 1000;
    if (unit === 'd') return value * 24 * 60 * 60 * 1000;
    if (unit === 'm') return value * 60 * 1000;

    return 0;
}

const worker = new Worker(QUEUE_NAMES.retellEventsProcess, async (job) => {
    const { retellEventId } = job.data;
    const event = await RetellEvent.findById(retellEventId);
    if (!event || event.status === 'processed') return;

    const payload = event.payloadJson;
    const metadata = payload.call?.metadata || payload.metadata; // Adjust based on Retell payload

    let stepExecution;
    if (metadata && metadata.stepExecutionId) {
        stepExecution = await StepExecution.findById(metadata.stepExecutionId);
    } else {
        // Fallback search by callId or batchCallId + phone
        const callId = payload.call?.call_id || payload.call_id;
        if (callId) {
            stepExecution = await StepExecution.findOne({ 'retell.callId': callId });
        }
    }

    if (!stepExecution || stepExecution.status === 'completed') {
        event.status = 'processed';
        event.processedAt = new Date();
        await event.save();
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

            // Calculate resolved delay (in ms) from workflow delay strings (e.g. 5m, 1h, 2d)
            const resolvedDelayMs = parseDelayToMs(delay);
            const now = new Date();
            const resolvedDelay = resolvedDelayMs > 0 ? resolvedDelayMs : 0;

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
                { upsert: true, new: true, setDefaultsOnInsert: true }
            );

            console.log(`[RetellEventProcess] NextStepIntent created: ${intent._id} (deduped: ${!intent._id.toString().endsWith('new')})`);

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

    event.status = 'processed';
    event.processedAt = new Date();
    await event.save();

}, { connection, prefix: BULL_PREFIX });

module.exports = worker;
