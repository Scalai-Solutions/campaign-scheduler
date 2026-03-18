const { Worker } = require('bullmq');
const StepExecution = require('../models/StepExecution');
const CampaignRun = require('../models/CampaignRun');
const Lead = require('../models/Lead');
const { connection, queues } = require('../queues');
const Retell = require('retell-sdk');
const mongoose = require('mongoose');

const retellClient = new Retell({
    apiKey: process.env.RETELL_API_KEY,
});

const worker = new Worker('retell.batch.dispatch', async (job) => {
    const { stepExecutionIds, nodeId, agentId, agentType, tenantId, campaignId, version } = job.data;

    const steps = await StepExecution.find({ _id: { $in: stepExecutionIds } });
    if (!steps.length) return;

    const leads = await Lead.find({ _id: { $in: steps.map(s => s.leadId) } });

    const tasks = steps.map(step => {
        const lead = leads.find(l => l._id.equals(step.leadId));
        return {
            to_number: lead.phone, // Assuming phone is the field
            metadata: {
                tenantId,
                campaignId,
                version,
                runId: step.runId.toString(),
                leadId: lead._id.toString(),
                nodeId,
                stepExecutionId: step._id.toString()
            }
        };
    });

    try {
        // Fetch phone number for the agent
        let fromNumber = null;
        if (agentType === 'voice') {
            const phoneNumberDoc = await mongoose.connection.db.collection('phonenumbers').findOne({
                subaccountId: tenantId,
                $or: [
                    { outbound_agent_id: agentId },
                    { inbound_agent_id: agentId }
                ],
                status: 'active'
            });

            if (!phoneNumberDoc) {
                throw new Error(`No active outbound phone number found for agent ${agentId}`);
            }
            fromNumber = phoneNumberDoc.phoneNumber || phoneNumberDoc.phone_number;
        }

        const batchConfig = {
            base_agent_id: agentId,
            name: `Campaign ${campaignId} Node ${nodeId}`,
            tasks: tasks
        };

        if (fromNumber) {
            batchConfig.from_number = fromNumber;
        }

        const batchCall = await retellClient.batchCall.createBatchCall(batchConfig);
        console.log(`[RetellBatchDispatch] Created batch ${batchCall.batch_call_id} for ${steps.length} leads`);

        await StepExecution.updateMany(
            { _id: { $in: stepExecutionIds } },
            {
                status: 'waiting_result',
                'retell.batchCallId': batchCall.batch_call_id
            }
        );

        // Update CampaignRun status
        await CampaignRun.updateMany(
            { _id: { $in: steps.map(s => s.runId) } },
            { 
                currentNodeStatus: 'waiting_result',
                agentStatus: 'waiting_result'
            }
        );

        // Enqueue reconcile after 10m
        await queues.retellBatchReconcile.add(`reconcile-${batchCall.batch_call_id}`,
            { batchCallId: batchCall.batch_call_id },
            { delay: 10 * 60 * 1000 }
        );

    } catch (error) {
        console.error('Error in retell.batch.dispatch worker:', error);
        await StepExecution.updateMany(
            { _id: { $in: stepExecutionIds } },
            { status: 'failed' }
        );
        throw error;
    }
}, { connection });

module.exports = worker;
