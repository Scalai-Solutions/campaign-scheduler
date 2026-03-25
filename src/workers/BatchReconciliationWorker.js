const { Worker } = require('bullmq');
const mongoose = require('mongoose');
const logger = require('../utils/logger');

const BatchDispatch = require('../models/BatchDispatch');
const NextStepIntent = require('../models/NextStepIntent');
const { connection, queues, BULL_PREFIX } = require('../queues');

function isTransactionUnsupportedError(error) {
    const message = (error && (error.message || String(error))).toLowerCase();
    return message.includes('transaction numbers are only allowed on a replica set member or mongos');
}

/**
 * BatchReconciliationWorker
 * Reconciles batch dispatch by polling Retell for results
 * 
 * Responsibilities:
 * 1. Poll Retell for batch call status
 * 2. Classify outcomes (completed, pending, failed)
 * 3. Determine semantic state (FULL_SUCCESS, ALL_PENDING, PARTIAL_FAILED)
 * 4. Handle timeout (10-minute SLA)
 * 5. Create retry batch for failed leads
 * 6. Update batch and intent statuses
 */
const worker = new Worker('batch.reconcile', async (job) => {
    const { batchDispatchId } = job.data;

    try {
        await reconcileBatch(batchDispatchId);

    } catch (error) {
        logger.error('BatchReconciliationWorker failed', {
            batchDispatchId,
            error: error.message,
            stack: error.stack
        });

        // Reschedule reconciliation with exponential backoff
        const queuedJob = await queues.batchReconcile.getJob(job.id);
        const nextDelay = Math.min((queuedJob?.attemptsMade || 0) * 30000, 300000); // Cap at 5 min
        await queues.batchReconcile.add(
            `reconcile-${batchDispatchId}`,
            { batchDispatchId },
            {
                delay: nextDelay,
                removeOnFail: false,
                removeOnComplete: false
            }
        );
    }

}, { connection, prefix: BULL_PREFIX });

async function reconcileBatch(batchDispatchId) {
    logger.info('Starting reconciliation for batch', { batchDispatchId });

    // 1. Fetch batch
    const batch = await BatchDispatch.findById(batchDispatchId);
    if (!batch) {
        logger.error('Batch not found for reconciliation', { batchDispatchId });
        return;
    }

    // Check if already reconciled
    if (['completed', 'permanently_failed', 'retry_scheduled'].includes(batch.status)) {
        logger.debug('Batch already reconciled', { batchDispatchId, status: batch.status });
        return;
    }

    // 2. Check if 10-minute timeout exceeded
    const timeoutThresholdMs = 10 * 60 * 1000; // 10 minutes
    const elapsedMs = Date.now() - batch.actualDispatchTime.getTime();
    const isTimeout = elapsedMs > timeoutThresholdMs;

    if (isTimeout) {
        logger.warn('Batch reconciliation timeout exceeded', {
            batchDispatchId,
            elapsedMs,
            timeoutThresholdMs
        });
        await handleTimeout(batch);
        return;
    }

    // 3. Poll Retell for batch status
    const retellClient = require('../services/retellClient');
    let retellStatus;
    try {
        retellStatus = await retellClient.getBatchCallStatus(batch.retellBatchCallId);
    } catch (error) {
        logger.error('Failed to poll Retell batch status', {
            batchDispatchId,
            retellBatchCallId: batch.retellBatchCallId,
            error: error.message
        });

        await queues.batchReconcile.add(
            `reconcile-${batchDispatchId}`,
            { batchDispatchId },
            {
                delay: 30000,
                removeOnFail: false,
                removeOnComplete: false
            }
        );
        return;
    }

    // 4. Classify call outcomes
    const intents = await NextStepIntent.find({
        _id: { $in: batch.nextStepIntentIds }
    });

    const outcomesByIntentId = {};
    const callResultsByIntentId = {};

    if (retellStatus.calls && Array.isArray(retellStatus.calls)) {
        for (const callResult of retellStatus.calls) {
            const intentId = callResult.metadata?.nextStepIntentId;
            if (intentId) {
                outcomesByIntentId[intentId] = classifyOutcome(callResult);
                callResultsByIntentId[intentId] = callResult;
            }
        }
    }

    const states = {
        completed: [],
        pending: [],
        failed: []
    };

    for (const intent of intents) {
        const intentIdStr = intent._id.toString();
        const outcome = outcomesByIntentId[intentIdStr] || 'pending';

        if (outcome === 'completed') {
            states.completed.push(intent);
        } else if (outcome === 'pending') {
            states.pending.push(intent);
        } else if (outcome === 'failed') {
            states.failed.push(intent);
        }
    }

    const semanticState = determineSemanticState(states);
    logger.info('Reconciliation semantic state', {
        batchDispatchId,
        state: semanticState,
        completed: states.completed.length,
        pending: states.pending.length,
        failed: states.failed.length
    });

    try {
        await reconcileBatchWithTransaction(batch, intents, states, callResultsByIntentId, semanticState);
    } catch (error) {
        if (!isTransactionUnsupportedError(error)) {
            throw error;
        }

        logger.warn('Mongo transactions unavailable, falling back to non-transactional reconciliation', {
            batchDispatchId,
            error: error.message
        });

        await reconcileBatchWithoutTransaction(batch, intents, states, callResultsByIntentId, semanticState);
    }
}

async function reconcileBatchWithTransaction(batch, intents, states, callResultsByIntentId, semanticState) {
    const session = await mongoose.startSession();
    session.startTransaction();

    try {
        if (semanticState === 'FULL_SUCCESS') {
            await handleFullSuccess(batch, intents, callResultsByIntentId, session);
        } else if (semanticState === 'ALL_PENDING') {
            await handleAllPending(batch, session);
        } else if (semanticState === 'PARTIAL_FAILED') {
            await handlePartialFailed(batch, states, callResultsByIntentId, session);
        }

        await session.commitTransaction();
        logger.info('Reconciliation completed', {
            batchDispatchId: batch._id,
            finalState: semanticState
        });
    } catch (error) {
        await session.abortTransaction();
        if (!isTransactionUnsupportedError(error)) {
            logger.error('Transaction failed during reconciliation', {
                batchDispatchId: batch._id,
                error: error.message
            });
        }
        throw error;
    } finally {
        await session.endSession();
    }
}

async function reconcileBatchWithoutTransaction(batch, intents, states, callResultsByIntentId, semanticState) {
    if (semanticState === 'FULL_SUCCESS') {
        await handleFullSuccess(batch, intents, callResultsByIntentId);
    } else if (semanticState === 'ALL_PENDING') {
        await handleAllPending(batch);
    } else if (semanticState === 'PARTIAL_FAILED') {
        await handlePartialFailed(batch, states, callResultsByIntentId);
    }

    logger.info('Reconciliation completed (non-transactional)', {
        batchDispatchId: batch._id,
        finalState: semanticState
    });
}

/**
 * Classify single call outcome from Retell result
 */
function classifyOutcome(callResult) {
    const status = callResult.status || callResult.call_status || 'pending';
    
    // Completed statuses
    if (['completed', 'succeeded', 'ended'].includes(status)) {
        return 'completed';
    }
    
    // Failed statuses
    if (['failed', 'error', 'cancelled', 'not_connected'].includes(status)) {
        return 'failed';
    }
    
    // Pending statuses
    return 'pending';
}

/**
 * Determine semantic batch state from partial outcomes
 */
function determineSemanticState(states) {
    const { completed, pending, failed } = states;
    const total = completed.length + pending.length + failed.length;

    if (failed.length === 0 && pending.length === 0) {
        return 'FULL_SUCCESS';
    } else if (failed.length === 0) {
        return 'ALL_PENDING';
    } else {
        return 'PARTIAL_FAILED';
    }
}

/**
 * Handle FULL_SUCCESS state: mark batch and intents as completed
 */
async function handleFullSuccess(batch, intents, callResultsByIntentId, session) {
    batch.status = 'completed';
    batch.metrics = batch.metrics || {};
    batch.metrics.successCount = intents.length;
    batch.metrics.failureCount = 0;
    batch.metadata.completionReason = 'full_success';

    await NextStepIntent.bulkWrite(
        intents.map(intent => ({
            updateOne: {
                filter: { _id: intent._id },
                update: {
                    $set: {
                        status: 'completed',
                        completedAt: new Date(),
                        outcome: 'successful',
                        'metadata.retellOutcome': callResultsByIntentId[intent._id.toString()],
                        'metadata.reconciliationReason': 'full_success'
                    }
                }
            }
        })),
        { session }
    );

    await batch.save({ session });
}

/**
 * Handle ALL_PENDING state: all intents still pending, reschedule reconciliation
 */
async function handleAllPending(batch, session) {
    batch.status = 'awaiting_results';
    batch.metadata.lastReconciliationAt = new Date();
    batch.metadata.reconciliationRetry = (batch.metadata.reconciliationRetry || 0) + 1;

    await batch.save({ session });

    // Reschedule reconciliation
    await queues.batchReconcile.add(
        `reconcile-${batch._id}`,
        { batchDispatchId: batch._id.toString() },
        {
            delay: 30000, // 30 seconds
            removeOnFail: false,
            removeOnComplete: false
        }
    );
}

/**
 * Handle PARTIAL_FAILED state: create retry batch, update successes
 */
async function handlePartialFailed(batch, states, callResultsByIntentId, session) {
    const { completed, failed } = states;
    batch.metrics = batch.metrics || {};

    // Update successful intents
    if (completed.length > 0) {
        await NextStepIntent.bulkWrite(
            completed.map(intent => ({
                updateOne: {
                    filter: { _id: intent._id },
                    update: {
                        $set: {
                            status: 'completed',
                            completedAt: new Date(),
                            outcome: 'successful',
                            'metadata.retellOutcome': callResultsByIntentId[intent._id.toString()],
                            'metadata.reconciliationReason': 'partial_failed_success'
                        }
                    }
                }
            })),
            { session }
        );
    }

    // Mark failed intents for retry if within retry limit
    for (const failedIntent of failed) {
        const retryCount = failedIntent.retryCount || 0;
        const maxRetries = failedIntent.metadata?.maxRetries || 3;

        if (retryCount < maxRetries) {
            // Mark for retry
            const nextRetryAt = new Date(Date.now() + (Math.pow(2, retryCount) * 30000)); // Exponential backoff
            await NextStepIntent.findByIdAndUpdate(
                failedIntent._id,
                {
                    $set: {
                        status: 'pending_retry',
                        retryCount: retryCount + 1,
                        nextRetryAt: nextRetryAt,
                        'metadata.retellFailureReason': callResultsByIntentId[failedIntent._id.toString()]?.error,
                        'metadata.reconciliationReason': 'partial_failed_will_retry'
                    }
                },
                { session }
            );

            batch.metrics.failureCount = (batch.metrics.failureCount || 0) + 1;
        } else {
            // Max retries exceeded, mark as permanently failed
            await NextStepIntent.findByIdAndUpdate(
                failedIntent._id,
                {
                    $set: {
                        status: 'failed_max_retries',
                        completedAt: new Date(),
                        outcome: 'failed',
                        'metadata.retellFailureReason': callResultsByIntentId[failedIntent._id.toString()]?.error,
                        'metadata.reconciliationReason': 'max_retries_exceeded'
                    }
                },
                { session }
            );

            batch.failureReasonsByLead[failedIntent.leadId] = 'Max retries exceeded';
            batch.metrics.failureCount = (batch.metrics.failureCount || 0) + 1;
        }
    }

    batch.metrics.successCount = completed.length;
    batch.status = 'partial_failed';
    batch.metadata.partialReason = 'some_calls_failed';
    batch.metadata.completionReason = `${completed.length}_success_${failed.length}_failed`;

    await batch.save({ session });
}

/**
 * Handle timeout: mark intents and batch as timed out
 */
async function handleTimeout(batch) {
    const session = await mongoose.startSession();
    session.startTransaction();

    try {
        // Mark all intents as timed out
        await NextStepIntent.bulkWrite(
            batch.nextStepIntentIds.map(intentId => ({
                updateOne: {
                    filter: { _id: intentId },
                    update: {
                        $set: {
                            status: 'timeout',
                            completedAt: new Date(),
                            outcome: 'timeout',
                            'metadata.reconciliationReason': 'webhook_timeout_10m'
                        }
                    }
                }
            })),
            { session }
        );

    batch.status = 'timeout';
        batch.metadata.partialReason = 'webhook_timeout_10m';
    batch.metrics = batch.metrics || {};
    batch.metrics.failureCount = batch.leadCount;

        await batch.save({ session });
        await session.commitTransaction();

        logger.info('Batch marked as timeout', {
            batchDispatchId: batch._id,
            leadCount: batch.leadCount
        });

    } catch (error) {
        await session.abortTransaction();
        if (isTransactionUnsupportedError(error)) {
            logger.warn('Mongo transactions unavailable, falling back to non-transactional timeout handling', {
                batchDispatchId: batch._id,
                error: error.message
            });

            await NextStepIntent.bulkWrite(
                batch.nextStepIntentIds.map(intentId => ({
                    updateOne: {
                        filter: { _id: intentId },
                        update: {
                            $set: {
                                status: 'timeout',
                                completedAt: new Date(),
                                outcome: 'timeout',
                                'metadata.reconciliationReason': 'webhook_timeout_10m'
                            }
                        }
                    }
                }))
            );

            batch.status = 'timeout';
            batch.metadata.partialReason = 'webhook_timeout_10m';
            batch.metrics = batch.metrics || {};
            batch.metrics.failureCount = batch.leadCount;
            await batch.save();

            logger.info('Batch marked as timeout (non-transactional)', {
                batchDispatchId: batch._id,
                leadCount: batch.leadCount
            });
            return;
        }

        logger.error('Failed to handle timeout', {
            batchDispatchId: batch._id,
            error: error.message
        });
        throw error;
    } finally {
        await session.endSession();
    }
}

worker.on('failed', (job, err) => {
    logger.error('BatchReconciliationWorker job failed', {
        jobId: job.id,
        batchDispatchId: job.data.batchDispatchId,
        error: err.message,
        attempts: job.attemptsMade
    });
});

worker.on('completed', (job) => {
    logger.debug('BatchReconciliationWorker job completed', {
        jobId: job.id,
        batchDispatchId: job.data.batchDispatchId
    });
});

module.exports = worker;
