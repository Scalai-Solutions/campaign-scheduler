const { Worker } = require('bullmq');
const mongoose = require('mongoose');
const logger = require('../utils/logger');

const BatchDispatch = require('../models/BatchDispatch');
const NextStepIntent = require('../models/NextStepIntent');
const CampaignRun = require('../models/CampaignRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const { connection, queues, BULL_PREFIX } = require('../queues');

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
const worker = new Worker('batch.dispatch', async (job) => {
    const { batchDispatchId } = job.data;
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

        // 6. Build task payloads for Retell
        const tasks = [];
        for (const intent of intents) {
            // Ensure phone number exists
            const run = await CampaignRun.findById(intent.runId).session(session);
            if (!run || !run.leadPhoneNumber) {
                batch.failureReasonsByLead[intent.leadId] = 'Phone number not found';
                continue;
            }

            // Build Retell task
            const task = {
                phone_number: run.leadPhoneNumber,
                metadata: {
                    tenantId: batch.tenantId,
                    campaignId: batch.campaignId,
                    campaignVersion: batch.campaignVersion,
                    runId: intent.runId.toString(),
                    leadId: intent.leadId,
                    nextNodeId: batch.nextNodeId,
                    nextStepIntentId: intent._id.toString(),
                    correlationId: intent.metadata?.correlationId || `intent-${intent._id}`
                },
                agent: nextNodeDef.agentConfig || {},
                retry_config: {
                    max_retries: nextNodeDef.maxRetries || 3,
                    timeout_ms: nextNodeDef.timeoutMs || 60000
                }
            };

            tasks.push(task);
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
        await session.commitTransaction();

        logger.info('Batch marked dispatching', {
            batchDispatchId,
            taskCount: tasks.length
        });

        // 8. Call Retell API (outside transaction for idempotency)
        const retellClient = require('../services/retellClient'); // Assume exists
        let retellResponse;
        try {
            retellResponse = await retellClient.sendBatchCalls(tasks);
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
            logger.error('Transaction failed updating batch with Retell response', {
                batchDispatchId,
                error: error.message
            });
            throw error;
        } finally {
            await batchSession.endSession();
        }

    } catch (error) {
        await session.abortTransaction();
        logger.error('BatchDispatchWorker failed', {
            batchDispatchId,
            error: error.message,
            stack: error.stack
        });

        // Update batch status to failed
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

        throw error; // Rethrow for BullMQ retry
    } finally {
        await session.endSession();
    }

}, { connection, prefix: BULL_PREFIX });

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
