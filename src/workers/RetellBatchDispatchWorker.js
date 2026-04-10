const { Worker } = require('bullmq');
const StepExecution = require('../models/StepExecution');
const CampaignRun = require('../models/CampaignRun');
const Lead = require('../models/Lead');
const { connection, queues, BULL_PREFIX } = require('../queues');
const Retell = require('retell-sdk');
const mongoose = require('mongoose');
const prefetchService = require('../services/prefetchService');

const retellClient = new Retell({
    apiKey: process.env.RETELL_API_KEY,
});

const worker = new Worker('retell.batch.dispatch', async (job) => {
    const { stepExecutionIds, nodeId, agentId, agentType, tenantId, campaignId, version, fromNumber: jobFromNumber } = job.data;

    const steps = await StepExecution.find({ _id: { $in: stepExecutionIds }, tenantId });
    if (!steps.length) return;

    const leads = await Lead.find({ _id: { $in: steps.map(s => s.leadId) }, tenantId });

    // Pre-fetch caller context + HubSpot data for all leads in parallel
    let prefetchMap = new Map();
    try {
        prefetchMap = await prefetchService.prefetchBatch(
            leads, tenantId, agentId,
            { timeoutMs: parseInt(process.env.PREFETCH_TIMEOUT_MS || '8000') }
        );
        console.log(`[RetellBatchDispatch] Batch prefetch completed: ${prefetchMap.size}/${leads.length} leads prefetched`);
    } catch (err) {
        console.error('[RetellBatchDispatch] Batch prefetch failed, proceeding without', err.message);
    }

    const tasks = steps.map(step => {
        const lead = leads.find(l => l._id.equals(step.leadId));
        if (!lead) return null; // guarded below
        const prefetchedVars = prefetchMap.get(lead._id.toString()) || {};
        return {
            to_number: lead.phone,
            retell_llm_dynamic_variables: {
                phone_number: lead.phone,
                agent_id: agentId,
                subaccount_id: tenantId,
                ...prefetchedVars,
            },
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
    }).filter(Boolean);

    console.log(`[RetellBatchDispatch] Dispatching node ${nodeId} for ${steps.length} leads. agentId: ${agentId}, agentType: ${agentType}`);

    try {
        // Resolve fromNumber:
        //   1. Use the value embedded in the job data (set by CampaignNodeDispatchWorker
        //      from the CampaignDefinition where campaignService._embedFromNumbers stored it).
        //   2. Fall back to a shared-DB phonenumbers query for standalone (sole_user) tenants
        //      whose data lives in scalai_db.  For subaccount tenants this always returns null;
        //      ensure fromNumber is embedded at campaign creation time to avoid dispatch errors.
        let fromNumber = jobFromNumber || null;

        if (!fromNumber) {
            const phoneNumberDoc = await mongoose.connection.db.collection('phonenumbers').findOne({
                subaccountId: tenantId,
                $or: [
                    { outbound_agent_id: agentId },
                    { inbound_agent_id: agentId }
                ],
                status: 'active'
            });
            if (phoneNumberDoc) {
                fromNumber = phoneNumberDoc.phoneNumber || phoneNumberDoc.phone_number || phoneNumberDoc.from_number || null;
            }
        }

        if (!fromNumber) {
            throw new Error(
                `No outbound phone number for agent ${agentId} (tenant: ${tenantId}). ` +
                'Ensure fromNumber is embedded in the campaign definition at creation time.'
            );
        }

        const batchConfig = {
            base_agent_id: agentId,
            name: `Campaign ${campaignId} Node ${nodeId}`,
            tasks: tasks,
            from_number: fromNumber // Ensure it's included, Retell requires it
        };

        const batchCall = await retellClient.batchCall.createBatchCall(batchConfig);
        console.log(`[RetellBatchDispatch] Created batch ${batchCall.batch_call_id} for ${steps.length} leads using from_number ${fromNumber}`);

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
}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_RETELL_DISPATCH || '3') });

module.exports = worker;
