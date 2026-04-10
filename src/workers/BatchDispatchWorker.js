const { Worker } = require('bullmq');
const mongoose = require('mongoose');
const logger = require('../utils/logger');

const BatchDispatch = require('../models/BatchDispatch');
const NextStepIntent = require('../models/NextStepIntent');
const CampaignRun = require('../models/CampaignRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const StepExecution = require('../models/StepExecution');
const { makeStepDedupeKey } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX } = require('../queues');
const prefetchService = require('../services/prefetchService');

/**
 * BatchDispatchWorker
 * Sends batches of intents to Retell for dispatching
 * 
 * Responsibilities:
 * 1. Validation: Verify all intents present, dependencies exist, no validation errors
 * 2. Preparation: Build task payloads with metadata (stepExecutionId, leadId, etc)
 * 3. Dispatch: Call Retell batch API
 * 4. Recording: Store Retell batch call ID and metadata
 * 5. Reconciliation: Enqueue reconciliation job
 */

function isTransactionUnsupportedError(error) {
    const message = (error && (error.message || String(error))).toLowerCase();
    return message.includes('transaction numbers are only allowed on a replica set member or mongos');
}

/**
 * Resolve the outbound phone number for a voice node.
 *
 * Priority:
 *   1. nodeDefFromNumber — embedded into the CampaignDefinition at campaign-creation
 *      time by campaignService._embedFromNumbers().  This is the preferred and reliable
 *      path for all subaccount tenants whose phonenumbers live in a per-tenant DB that
 *      the scheduler cannot reach.
 *   2. Direct scalai_db phonenumbers query — legacy fallback that works only when the
 *      phonenumbers collection lives in the shared DB (sole_user / standalone tenants).
 *
 * @param {string} tenantId
 * @param {string} agentId
 * @param {string|null} nodeDefFromNumber  Pre-resolved number from the workflow definition
 * @returns {Promise<string|null>}
 */
async function resolveFromNumber(tenantId, agentId, nodeDefFromNumber = null) {
    // Primary: use the pre-resolved value embedded at campaign-creation time
    if (nodeDefFromNumber) {
        return nodeDefFromNumber;
    }

    // Fallback: query the shared DB phonenumbers collection.
    // NOTE: For subaccount tenants, phonenumbers live in their isolated per-tenant DB,
    // NOT in scalai_db, so this query will return null for them.  The correct fix is
    // to ensure campaignService._embedFromNumbers() runs at campaign creation/update.
    logger.warn('resolveFromNumber: fromNumber not embedded in definition, falling back to shared DB query', {
        tenantId,
        agentId
    });

    const phoneNumberDoc = await mongoose.connection.db.collection('phonenumbers').findOne({
        subaccountId: tenantId,
        $or: [
            { outbound_agent_id: agentId },
            { inbound_agent_id: agentId }
        ],
        status: 'active'
    });

    return phoneNumberDoc
        ? (phoneNumberDoc.phoneNumber || phoneNumberDoc.phone_number || phoneNumberDoc.from_number || null)
        : null;
}

async function fetchLeadsForIntents(intents, session) {
    const query = Lead.find({ _id: { $in: intents.map(intent => intent.leadId) } });
    if (session) {
        query.session(session);
    }

    const leads = await query;
    return new Map(leads.map((lead) => [lead._id.toString(), lead]));
}

async function ensureStepExecutions(batch, intents, session) {
    const stepExecutionIds = [];
    const stepExecutionIdByIntentId = new Map();

    for (const intent of intents) {
        const attempt = (intent.retryCount || 0) + 1;
        const dedupeKey = makeStepDedupeKey(intent.runId.toString(), batch.nextNodeId, attempt);
        const query = StepExecution.findOneAndUpdate(
            { dedupeKey },
            {
                $setOnInsert: {
                    tenantId: intent.tenantId,
                    runId: intent.runId,
                    leadId: intent.leadId,
                    nodeId: batch.nextNodeId,
                    agentId: batch.nextNodeAgentId,
                    agentType: batch.nextNodeAgentType,
                    status: 'queued',
                    attempt,
                    startedAt: new Date(),
                    dedupeKey
                }
            },
            { upsert: true, new: true }
        );

        if (session) {
            query.session(session);
        }

        const stepExecution = await query;
        stepExecutionIds.push(stepExecution._id);
        stepExecutionIdByIntentId.set(intent._id.toString(), stepExecution._id.toString());
    }

    return { stepExecutionIds, stepExecutionIdByIntentId };
}

async function updateRunStatusesForBatch(intents, update, session) {
    const runIds = [...new Set(intents.map((intent) => intent.runId.toString()))];
    const query = CampaignRun.updateMany({ _id: { $in: runIds } }, update);
    if (session) {
        query.session(session);
    }
    await query;
}

function buildRetellTasks(batch, intents, nextNodeDef, leadsById, stepExecutionIdByIntentId, prefetchMap = new Map()) {
    const tasks = [];
    const failureReasonsByLead = {};

    for (const intent of intents) {
        const lead = leadsById.get(intent.leadId.toString());
        if (!lead || !lead.phone) {
            failureReasonsByLead[intent.leadId] = 'Phone number not found';
            continue;
        }

        const prefetchedVars = prefetchMap.get(lead._id.toString()) || {};

        tasks.push({
            phone_number: lead.phone,
            retell_llm_dynamic_variables: {
                phone_number: lead.phone,
                agent_id: nextNodeDef.agentId || '',
                subaccount_id: batch.tenantId,
                ...prefetchedVars,
            },
            metadata: {
                tenantId: batch.tenantId,
                campaignId: batch.campaignId,
                campaignVersion: batch.campaignVersion,
                runId: intent.runId.toString(),
                leadId: intent.leadId.toString(),
                nextNodeId: batch.nextNodeId,
                nextStepIntentId: intent._id.toString(),
                stepExecutionId: stepExecutionIdByIntentId.get(intent._id.toString()),
                correlationId: intent.metadata?.correlationId || `intent-${intent._id}`
            },
            retry_config: {
                max_retries: nextNodeDef.maxRetries || 3,
                timeout_ms: nextNodeDef.timeoutMs || 60000
            }
        });
    }

    return { tasks, failureReasonsByLead };
}

const worker = new Worker('batch.dispatch', async (job) => {
    const { batchDispatchId } = job.data;
    
    try {
        return await processWithTransaction(batchDispatchId);
    } catch (error) {
        if (!isTransactionUnsupportedError(error)) {
            throw error;
        }
        logger.warn('Mongo transactions unavailable, falling back to non-transactional batch dispatch', {
            error: error.message
        });
        return await processWithoutTransaction(batchDispatchId);
    }
}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_BATCH_DISPATCH || '3') });

async function processWithTransaction(batchDispatchId) {
    const session = await mongoose.startSession();
    session.startTransaction();

    try {
        // 1. Fetch batch
        const batch = await BatchDispatch.findById(batchDispatchId).session(session);
        if (!batch) {
            logger.error('Batch not found', { batchDispatchId });
            return;
        }

        if (batch.status !== 'pending') {
            logger.warn('Batch already processed', { batchDispatchId, status: batch.status });
            await session.abortTransaction();
            return;
        }

        logger.info('Processing batch dispatch', {
            batchDispatchId,
            leadCount: batch.leadCount,
            compatibilityKey: batch.batchCompatibilityKey
        });

        // 2. Validation: Fetch all intents
        const intents = await NextStepIntent.find({
            _id: { $in: batch.nextStepIntentIds }
        }).session(session);

        if (intents.length !== batch.nextStepIntentIds.length) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = 'Intent count mismatch';
            await batch.save({ session });
            await session.commitTransaction();
            logger.error('Intent validation failed', {
                batchDispatchId,
                expectedCount: batch.nextStepIntentIds.length,
                actualCount: intents.length
            });
            return;
        }

        // 3. Validation: All intents in expected status
        const invalidIntents = intents.filter(i => i.status !== 'batched');
        if (invalidIntents.length > 0) {
            batch.status = 'validation_failed';
            invalidIntents.forEach(i => {
                batch.failureReasonsByLead[i.leadId] = `Invalid status: ${i.status}`;
            });
            await batch.save({ session });
            await session.commitTransaction();
            logger.error('Intent status validation failed', {
                batchDispatchId,
                invalidCount: invalidIntents.length
            });
            return;
        }

        // 4. Validation: Fetch campaign definition
        const definition = await CampaignDefinition.findOne({
            tenantId: batch.tenantId,
            campaignId: batch.campaignId,
            version: batch.campaignVersion
        }).session(session);

        if (!definition) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = 'Campaign definition not found';
            await batch.save({ session });
            await session.commitTransaction();
            logger.error('Campaign definition not found', { batchDispatchId, ...batch._doc });
            return;
        }

        // 5. Find next node definition
        const nextNodeDef = definition.workflowJson.nodes.find(n => n.id === batch.nextNodeId);
        if (!nextNodeDef) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = `Next node ${batch.nextNodeId} not found`;
            await batch.save({ session });
            await session.commitTransaction();
            logger.error('Next node definition not found', { batchDispatchId, nodeId: batch.nextNodeId });
            return;
        }

        const { stepExecutionIds, stepExecutionIdByIntentId } = await ensureStepExecutions(batch, intents, session);

        // 6. Build task payloads for Retell
        const leadsById = await fetchLeadsForIntents(intents, session);

        // Pre-fetch caller context + HubSpot data for all leads in parallel
        let prefetchMap = new Map();
        try {
            const leadsArray = Array.from(leadsById.values());
            prefetchMap = await prefetchService.prefetchBatch(
                leadsArray, batch.tenantId, batch.nextNodeAgentId,
                { timeoutMs: parseInt(process.env.PREFETCH_TIMEOUT_MS || '8000') }
            );
            logger.info('Batch prefetch completed', {
                batchDispatchId, leadCount: leadsArray.length, prefetchedCount: prefetchMap.size
            });
        } catch (err) {
            logger.warn('Batch prefetch failed, proceeding without', {
                batchDispatchId, error: err.message
            });
        }

        const { tasks, failureReasonsByLead } = buildRetellTasks(batch, intents, nextNodeDef, leadsById, stepExecutionIdByIntentId, prefetchMap);
        Object.entries(failureReasonsByLead).forEach(([leadId, reason]) => {
            batch.failureReasonsByLead[leadId] = reason;
        });

        // fromNumber is embedded per-node in workflowJson at campaign creation time.
        // resolveFromNumber falls back to a shared-DB query for standalone tenants.
        const fromNumber = await resolveFromNumber(batch.tenantId, batch.nextNodeAgentId, nextNodeDef.fromNumber);
        if (!fromNumber) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = `No outbound phone number found for agent ${batch.nextNodeAgentId}. Ensure the campaign definition has fromNumber embedded.`;
            await batch.save({ session });
            await session.commitTransaction();
            logger.error('No outbound phone number for batch dispatch', {
                batchDispatchId,
                agentId: batch.nextNodeAgentId,
                tenantId: batch.tenantId,
                hint: 'fromNumber was not embedded in the workflow definition. Re-save the campaign to fix.'
            });
            return;
        }

        if (tasks.length === 0) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = 'No valid tasks to dispatch';
            await batch.save({ session });
            await session.commitTransaction();
            logger.error('No valid tasks in batch', { batchDispatchId });
            return;
        }

        // 7. Mark batch as dispatching
        batch.status = 'dispatching';
        batch.actualDispatchTime = new Date();
        await batch.save({ session });
        await StepExecution.updateMany(
            { _id: { $in: stepExecutionIds } },
            { status: 'waiting_result' },
            { session }
        );
        await updateRunStatusesForBatch(intents, {
            currentNodeStatus: 'waiting_result',
            agentStatus: 'waiting_result'
        }, session);
        await session.commitTransaction();

        logger.info('Batch marked dispatching', {
            batchDispatchId,
            taskCount: tasks.length
        });

        // 8. Call Retell API (outside transaction for idempotency)
        const retellClient = require('../services/retellClient'); // Assume exists
        let retellResponse;
        try {
            retellResponse = await retellClient.sendBatchCalls({
                baseAgentId: batch.nextNodeAgentId,
                fromNumber,
                name: `Campaign ${batch.campaignId} Node ${batch.nextNodeId}`,
                tasks
            });
        } catch (error) {
            logger.error('Retell API call failed', {
                batchDispatchId,
                error: error.message
            });

            // Mark batch as dispatch_failed
            batch.status = 'dispatch_failed';
            batch.failureReasonsByLead['_batch'] = `Retell API error: ${error.message}`;
            batch.metadata.retellError = error.message;
            await batch.save();
            await StepExecution.updateMany(
                { _id: { $in: stepExecutionIds } },
                { status: 'failed' }
            );

            throw error; // BullMQ will retry
        }

        // 9. Update batch with Retell response
        const batchSession = await mongoose.startSession();
        batchSession.startTransaction();

        try {
            batch.retellBatchCallId = retellResponse.batchCallId;
            batch.retellBatchCallMetadata = {
                sentAt: new Date(),
                taskCount: tasks.length,
                retellResponse: retellResponse.metadata || {}
            };
            batch.status = 'sent';

            // Bulk update intents to dispatch_sent
            await NextStepIntent.bulkWrite(
                batch.nextStepIntentIds.map(intentId => ({
                    updateOne: {
                        filter: { _id: intentId },
                        update: {
                            $set: {
                                status: 'dispatch_sent',
                                dispatchedAt: new Date(),
                                retellBatchCallId: retellResponse.batchCallId,
                                'metadata.retellDispatchedAt': Date.now()
                            }
                        }
                    }
                })),
                { session: batchSession }
            );

            await StepExecution.updateMany(
                { _id: { $in: stepExecutionIds } },
                {
                    status: 'waiting_result',
                    'retell.batchCallId': retellResponse.batchCallId
                },
                { session: batchSession }
            );

            await batch.save({ session: batchSession });
            await batchSession.commitTransaction();

            logger.info('Batch dispatched to Retell', {
                batchDispatchId,
                retellBatchCallId: retellResponse.batchCallId,
                taskCount: tasks.length
            });

            // 10. Enqueue reconciliation job (after 15 seconds to give Retell time to process)
            await queues.batchReconcile.add(
                `reconcile-${batchDispatchId}`,
                { batchDispatchId },
                {
                    delay: 15000, // 15 second delay
                    removeOnFail: false,
                    removeOnComplete: false
                }
            );

        } catch (error) {
            await batchSession.abortTransaction();
            if (!isTransactionUnsupportedError(error)) {
                logger.error('Transaction failed updating batch with Retell response', {
                    batchDispatchId,
                    error: error.message
                });
            }
            throw error;
        } finally {
            await batchSession.endSession();
        }

    } catch (error) {
        await session.abortTransaction();
        if (!isTransactionUnsupportedError(error)) {
            logger.error('BatchDispatchWorker failed', {
                batchDispatchId,
                error: error.message,
                stack: error.stack
            });
        }

        // Update batch status to failed
        if (!isTransactionUnsupportedError(error)) {
            try {
                const batch = await BatchDispatch.findById(batchDispatchId);
                if (batch && batch.status === 'dispatching') {
                    batch.status = 'dispatch_failed';
                    batch.failureReasonsByLead['_batch'] = error.message;
                    batch.metadata.lastError = error.message;
                    await batch.save();
                }
            } catch (saveError) {
                logger.error('Failed to update batch status', { error: saveError.message });
            }
        }

        throw error; // Rethrow for BullMQ retry
    } finally {
        await session.endSession();
    }
}

async function processWithoutTransaction(batchDispatchId) {
    try {
        // 1. Fetch batch
        const batch = await BatchDispatch.findById(batchDispatchId);
        if (!batch) {
            logger.error('Batch not found', { batchDispatchId });
            return;
        }

        if (batch.status !== 'pending') {
            logger.warn('Batch already processed', { batchDispatchId, status: batch.status });
            return;
        }

        logger.info('Processing batch dispatch (non-transactional)', {
            batchDispatchId,
            leadCount: batch.leadCount,
            compatibilityKey: batch.batchCompatibilityKey
        });

        // 2. Validation: Fetch all intents
        const intents = await NextStepIntent.find({
            _id: { $in: batch.nextStepIntentIds }
        });

        if (intents.length !== batch.nextStepIntentIds.length) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = 'Intent count mismatch';
            await batch.save();
            logger.error('Intent validation failed', {
                batchDispatchId,
                expectedCount: batch.nextStepIntentIds.length,
                actualCount: intents.length
            });
            return;
        }

        // 3. Validation: All intents in expected status
        const invalidIntents = intents.filter(i => i.status !== 'batched');
        if (invalidIntents.length > 0) {
            batch.status = 'validation_failed';
            invalidIntents.forEach(i => {
                batch.failureReasonsByLead[i.leadId] = `Invalid status: ${i.status}`;
            });
            await batch.save();
            logger.error('Intent status validation failed', {
                batchDispatchId,
                invalidCount: invalidIntents.length
            });
            return;
        }

        // 4. Validation: Fetch campaign definition
        const definition = await CampaignDefinition.findOne({
            tenantId: batch.tenantId,
            campaignId: batch.campaignId,
            version: batch.campaignVersion
        });

        if (!definition) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = 'Campaign definition not found';
            await batch.save();
            logger.error('Campaign definition not found', { batchDispatchId, ...batch._doc });
            return;
        }

        // 5. Find next node definition
        const nextNodeDef = definition.workflowJson.nodes.find(n => n.id === batch.nextNodeId);
        if (!nextNodeDef) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = `Next node ${batch.nextNodeId} not found`;
            await batch.save();
            logger.error('Next node definition not found', { batchDispatchId, nodeId: batch.nextNodeId });
            return;
        }

        const { stepExecutionIds, stepExecutionIdByIntentId } = await ensureStepExecutions(batch, intents);

        // 6. Build task payloads for Retell
        const leadsById = await fetchLeadsForIntents(intents);

        // Pre-fetch caller context + HubSpot data for all leads in parallel
        let prefetchMap = new Map();
        try {
            const leadsArray = Array.from(leadsById.values());
            prefetchMap = await prefetchService.prefetchBatch(
                leadsArray, batch.tenantId, batch.nextNodeAgentId,
                { timeoutMs: parseInt(process.env.PREFETCH_TIMEOUT_MS || '8000') }
            );
            logger.info('Batch prefetch completed (non-transactional)', {
                batchDispatchId, leadCount: leadsArray.length, prefetchedCount: prefetchMap.size
            });
        } catch (err) {
            logger.warn('Batch prefetch failed, proceeding without (non-transactional)', {
                batchDispatchId, error: err.message
            });
        }

        const { tasks, failureReasonsByLead } = buildRetellTasks(batch, intents, nextNodeDef, leadsById, stepExecutionIdByIntentId, prefetchMap);
        Object.entries(failureReasonsByLead).forEach(([leadId, reason]) => {
            batch.failureReasonsByLead[leadId] = reason;
        });

        const fromNumber = await resolveFromNumber(batch.tenantId, batch.nextNodeAgentId, nextNodeDef.fromNumber);
        if (!fromNumber) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = `No outbound phone number found for agent ${batch.nextNodeAgentId}. Ensure the campaign definition has fromNumber embedded.`;
            await batch.save();
            logger.error('No outbound phone number for batch dispatch (non-transactional)', {
                batchDispatchId,
                agentId: batch.nextNodeAgentId,
                tenantId: batch.tenantId,
                hint: 'fromNumber was not embedded in the workflow definition. Re-save the campaign to fix.'
            });
            return;
        }

        if (tasks.length === 0) {
            batch.status = 'validation_failed';
            batch.failureReasonsByLead['_batch'] = 'No valid tasks to dispatch';
            await batch.save();
            logger.error('No valid tasks in batch', { batchDispatchId });
            return;
        }

        // 7. Mark batch as dispatching
        batch.status = 'dispatching';
        batch.actualDispatchTime = new Date();
        await batch.save();
        await StepExecution.updateMany(
            { _id: { $in: stepExecutionIds } },
            { status: 'waiting_result' }
        );
        await updateRunStatusesForBatch(intents, {
            currentNodeStatus: 'waiting_result',
            agentStatus: 'waiting_result'
        });

        logger.info('Batch marked dispatching', {
            batchDispatchId,
            taskCount: tasks.length
        });

        // 8. Call Retell API
        const retellClient = require('../services/retellClient');
        let retellResponse;
        try {
            retellResponse = await retellClient.sendBatchCalls({
                baseAgentId: batch.nextNodeAgentId,
                fromNumber,
                name: `Campaign ${batch.campaignId} Node ${batch.nextNodeId}`,
                tasks
            });
        } catch (error) {
            logger.error('Retell API call failed', {
                batchDispatchId,
                error: error.message
            });

            batch.status = 'dispatch_failed';
            batch.failureReasonsByLead['_batch'] = `Retell API error: ${error.message}`;
            batch.metadata.retellError = error.message;
            await batch.save();
            await StepExecution.updateMany(
                { _id: { $in: stepExecutionIds } },
                { status: 'failed' }
            );

            throw error;
        }

        // 9. Update batch with Retell response
        batch.retellBatchCallId = retellResponse.batchCallId;
        batch.retellBatchCallMetadata = {
            sentAt: new Date(),
            taskCount: tasks.length,
            retellResponse: retellResponse.metadata || {}
        };
        batch.status = 'sent';

        // Bulk update intents to dispatch_sent
        await NextStepIntent.bulkWrite(
            batch.nextStepIntentIds.map(intentId => ({
                updateOne: {
                    filter: { _id: intentId },
                    update: {
                        $set: {
                            status: 'dispatch_sent',
                            dispatchedAt: new Date(),
                            retellBatchCallId: retellResponse.batchCallId,
                            'metadata.retellDispatchedAt': Date.now()
                        }
                    }
                }
            }))
        );

        await StepExecution.updateMany(
            { _id: { $in: stepExecutionIds } },
            {
                status: 'waiting_result',
                'retell.batchCallId': retellResponse.batchCallId
            }
        );

        await batch.save();

        logger.info('Batch dispatched to Retell (non-transactional)', {
            batchDispatchId,
            retellBatchCallId: retellResponse.batchCallId,
            taskCount: tasks.length
        });

        // 10. Enqueue reconciliation job
        await queues.batchReconcile.add(
            `reconcile-${batchDispatchId}`,
            { batchDispatchId },
            {
                delay: 15000,
                removeOnFail: false,
                removeOnComplete: false
            }
        );

    } catch (error) {
        logger.error('BatchDispatchWorker failed (non-transactional)', {
            batchDispatchId,
            error: error.message,
            stack: error.stack
        });

        try {
            const batch = await BatchDispatch.findById(batchDispatchId);
            if (batch && batch.status === 'dispatching') {
                batch.status = 'dispatch_failed';
                batch.failureReasonsByLead['_batch'] = error.message;
                batch.metadata.lastError = error.message;
                await batch.save();
            }
        } catch (saveError) {
            logger.error('Failed to update batch status', { error: saveError.message });
        }

        throw error;
    }
}

worker.on('failed', (job, err) => {
    logger.error('BatchDispatchWorker job failed', {
        jobId: job.id,
        batchDispatchId: job.data.batchDispatchId,
        error: err.message,
        attempts: job.attemptsMade
    });
});

worker.on('completed', (job) => {
    logger.debug('BatchDispatchWorker job completed', {
        jobId: job.id,
        batchDispatchId: job.data.batchDispatchId
    });
});

module.exports = worker;
