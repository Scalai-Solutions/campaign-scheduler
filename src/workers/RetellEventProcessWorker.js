const { Worker } = require('bullmq');
const RetellEvent = require('../models/RetellEvent');
const StepExecution = require('../models/StepExecution');
const CampaignRun = require('../models/CampaignRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const ScheduledTask = require('../models/ScheduledTask');
const { resolveNext, computeDueAt, makeTaskDedupeKey } = require('../campaignKernel');
const { connection } = require('../queues');

const worker = new Worker('retell.events.process', async (job) => {
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
    // successful/unsuccessful/not_answered
    let outcome = 'unsuccessful';
    if (payload.call?.disconnection_reason === 'finished' || payload.call_analysis?.call_completion_rating === 'Complete') {
        outcome = 'successful';
    } else if (payload.call?.disconnection_reason === 'voicemail') {
        outcome = 'not_answered';
    }
    // This classification is simplified; real logic would map more accurately.

    stepExecution.status = 'completed';
    stepExecution.outcome = outcome;
    stepExecution.endedAt = new Date();
    if (payload.call?.call_id) stepExecution.retell.callId = payload.call.call_id;
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
        const now = new Date();
        const dueAt = computeDueAt(now, delay);
        const dedupeKey = makeTaskDedupeKey(run._id.toString(), toNodeId, dueAt.toISOString());

        await ScheduledTask.findOneAndUpdate(
            { dedupeKey },
            {
                $setOnInsert: {
                    tenantId: run.tenantId,
                    runId: run._id,
                    leadId: run.leadId,
                    nodeId: toNodeId,
                    dueAt,
                    status: 'scheduled'
                }
            },
            { upsert: true }
        );

        // Update run progress
        run.currentNodeId = toNodeId;
        run.currentNodeStatus = 'scheduled';
        run.agentStatus = 'completed'; // For the node that just finished
        run.lastStepOutcome = outcome;
        await run.save();
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

}, { connection });

module.exports = worker;
