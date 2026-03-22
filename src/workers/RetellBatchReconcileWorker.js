const { Worker } = require('bullmq');
const StepExecution = require('../models/StepExecution');
const { connection, BULL_PREFIX } = require('../queues');
const Retell = require('retell-sdk');

const retellClient = new Retell({
    apiKey: process.env.RETELL_API_KEY,
});

const worker = new Worker('retell.batch.reconcile', async (job) => {
    const { batchCallId } = job.data;
    if (!batchCallId) return;

    const incompleteSteps = await StepExecution.find({
        'retell.batchCallId': batchCallId,
        status: 'waiting_result'
    });

    if (!incompleteSteps.length) return;

    try {
        // Note: Retell SDK doesn't currently support retrieveBatchCall.
        // Reconcile relies on individual call status webhooks or timeout logic below.
        // const batch = await retellClient.batchCall.retrieveBatchCall(batchCallId);

        // For each task in batch, if completed, finalize.
        // If batch is overall completed but some tasks missing, mark as failed/timeout.

        const maxAgeHours = parseInt(process.env.RETELL_RECONCILE_MAX_AGE_HOURS || '12');
        const thresholdDate = new Date();
        thresholdDate.setHours(thresholdDate.getHours() - maxAgeHours);

        for (const step of incompleteSteps) {
            if (step.updatedAt < thresholdDate) {
                step.status = 'timeout';
                step.outcome = 'unsuccessful';
                step.endedAt = new Date();
                await step.save();
                // Trigger next node logic if needed (usually a failure path)
            }
        }

    } catch (error) {
        console.error('Error in retell.batch.reconcile worker:', error);
    }
}, { connection, prefix: BULL_PREFIX });

module.exports = worker;
