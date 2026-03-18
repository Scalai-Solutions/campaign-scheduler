const { Worker } = require('bullmq');
const ScheduledTask = require('../models/ScheduledTask');
const CampaignRun = require('../models/CampaignRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const StepExecution = require('../models/StepExecution');
const { makeStepDedupeKey, getNode } = require('../campaignKernel');
const { connection, queues } = require('../queues');

const worker = new Worker('campaign.node.dispatch', async (job) => {
    const { scheduledTaskId } = job.data;

    const task = await ScheduledTask.findById(scheduledTaskId);
    if (!task || task.status !== 'leased') return; // Should be leased by scheduler

    const run = await CampaignRun.findById(task.runId);
    if (!run || run.status !== 'running') {
        task.status = 'failed';
        await task.save();
        return;
    }

    const definition = await CampaignDefinition.findOne({
        tenantId: run.tenantId,
        campaignId: run.campaignId,
        version: run.campaignVersion
    });

    if (!definition) {
        task.status = 'failed';
        await task.save();
        return;
    }

    const node = getNode(definition.workflowJson, task.nodeId);
    if (!node) {
        task.status = 'failed';
        await task.save();
        return;
    }

    const dedupeKey = makeStepDedupeKey(run._id.toString(), node.id, task.attempt);

    try {
        const stepExecution = await StepExecution.findOneAndUpdate(
            { dedupeKey },
            {
                $setOnInsert: {
                    tenantId: run.tenantId,
                    runId: run._id,
                    leadId: run.leadId,
                    nodeId: node.id,
                    agentId: node.agentId,
                    agentType: node.agentType,
                    status: 'queued',
                    attempt: task.attempt,
                    startedAt: new Date()
                }
            },
            { upsert: true, new: true }
        );

        // Update CampaignRun status for dashboard
        await CampaignRun.findByIdAndUpdate(run._id, {
            currentNodeId: node.id,
            currentNodeStatus: 'queued',
            agentStatus: 'dispatching_to_batch'
        });

        console.log(`[CampaignNodeDispatch] Dispatching node ${node.id} for run ${run._id} (Lead: ${run.leadId})`);

        // Grouping logic: simplicity for now, just enqueue directly.
        // In a high-scale system, we'd use a buffer or a separate process to batch.
        // For this implementation, we'll follow the requirement to enqueue retell.batch.dispatch.

        await queues.retellBatchDispatch.add(`dispatch-${stepExecution._id}`, {
            stepExecutionIds: [stepExecution._id],
            nodeId: node.id,
            agentId: node.agentId,
            agentType: node.agentType,
            tenantId: run.tenantId,
            campaignId: run.campaignId,
            version: run.campaignVersion
        });

        task.status = 'done';
        await task.save();

    } catch (error) {
        console.error('Error in campaign.node.dispatch worker:', error);
        throw error;
    }
}, { connection });

module.exports = worker;
