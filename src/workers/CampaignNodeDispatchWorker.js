const { Worker } = require('bullmq');
const ScheduledTask = require('../models/ScheduledTask');
const CampaignRun = require('../models/CampaignRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const StepExecution = require('../models/StepExecution');
const { makeStepDedupeKey, getNode, resolveNext, computeDueAt, makeTaskDedupeKey } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX } = require('../queues');

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

        console.log(`[CampaignNodeDispatch] Dispatching node ${node.id} for run ${run._id} (Lead: ${run.leadId}). agentType: ${node.agentType}`);

        if (node.agentType === 'chat') {
            // Placeholder: currently chat processing is near-instant in this logic. 
            // In the future, this would call a ChatDispatchWorker or connector-server.
            
            await StepExecution.findByIdAndUpdate(stepExecution._id, {
                status: 'completed',
                outcome: 'successful',
                endedAt: new Date()
            });

            // Resolve next node (logic extracted from RetellEventProcessWorker)
            const { toNodeId, delay } = resolveNext(definition.workflowJson, node.id, 'successful');

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

                await CampaignRun.findByIdAndUpdate(run._id, {
                    currentNodeId: toNodeId,
                    currentNodeStatus: 'scheduled',
                    agentStatus: 'completed',
                    lastStepOutcome: 'successful'
                });
            } else {
                await CampaignRun.findByIdAndUpdate(run._id, {
                    status: 'completed',
                    currentNodeStatus: 'completed',
                    agentStatus: 'completed',
                    lastStepOutcome: 'successful'
                });
            }
        } else {
            // For voice, use existing Retell batch logic.
            // Pass fromNumber (embedded at campaign creation time) so the dispatch worker
            // does not need to query the per-tenant phonenumbers collection.
            await queues.retellBatchDispatch.add(`dispatch-${stepExecution._id}`, {
                stepExecutionIds: [stepExecution._id],
                nodeId: node.id,
                agentId: node.agentId,
                agentType: node.agentType,
                tenantId: run.tenantId,
                campaignId: run.campaignId,
                version: run.campaignVersion,
                fromNumber: node.fromNumber || null
            });
        }

        task.status = 'done';
        await task.save();

    } catch (error) {
        console.error('Error in campaign.node.dispatch worker:', error);
        throw error;
    }
}, { connection, prefix: BULL_PREFIX });

module.exports = worker;
