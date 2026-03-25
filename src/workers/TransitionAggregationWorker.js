const { Worker } = require('bullmq');
const mongoose = require('mongoose');
const logger = require('../utils/logger');
const { createConnection, BULL_PREFIX } = require('../queues');

const NextStepIntent = require('../models/NextStepIntent');
const BatchDispatch = require('../models/BatchDispatch');
const { v4: uuidv4 } = require('uuid');

// Configuration from environment
const MICRO_BATCH_SIZE = parseInt(process.env.MICRO_BATCH_SIZE || '50');
const MICRO_BATCH_TIME_MS = parseInt(process.env.MICRO_BATCH_TIME_MS || '500');
const MICRO_BATCH_MAX_WAIT_MS = parseInt(process.env.MICRO_BATCH_MAX_WAIT_MS || '2000');
const MICRO_BATCH_POLL_INTERVAL_MS = parseInt(process.env.MICRO_BATCH_POLL_INTERVAL_MS || '100');
const SCHEDULED_PROMOTION_BATCH_SIZE = parseInt(process.env.SCHEDULED_PROMOTION_BATCH_SIZE || '100');

class TransitionAggregationWorker {
    constructor(connection, options = {}) {
        this.connection = connection;
        this.onFatalError = options.onFatalError;
        this.stopping = false;
        // Separate Redis client for distributed signal/claim keys.
        // Uses the same cluster-aware factory as BullMQ connections.
        this.redisClient = createConnection();

        // Worker processes claims on behalf of the work loop
        this.worker = new Worker('transition.aggregation.immediate', this.handleAggregation.bind(this), {
            connection,
            prefix: BULL_PREFIX
        });

        this.worker.on('failed', (job, err) => {
            logger.error('Aggregation worker failed', { jobId: job.id, error: err.message });
            this.handleFatalError(err, 'TransitionAggregationWorker.failed');
        });

        this.worker.on('error', (err) => {
            logger.error('Aggregation worker error', { error: err.message });
            this.handleFatalError(err, 'TransitionAggregationWorker.error');
        });

        this.worker.on('completed', (job) => {
            logger.debug('Aggregation job completed', { jobId: job.id });
        });
    }

    /**
     * Main work loop (runs continuously)
     * Discovers pending batch keys, atomically claims them, and aggregates intents
     */
    async workLoop(pollIntervalMs = MICRO_BATCH_POLL_INTERVAL_MS) {
        logger.info('TransitionAggregationWorker starting work loop', { 
            pollIntervalMs,
            batchSize: MICRO_BATCH_SIZE,
            batchTimeMs: MICRO_BATCH_TIME_MS,
            maxWaitMs: MICRO_BATCH_MAX_WAIT_MS,
            scheduledPromotionBatchSize: SCHEDULED_PROMOTION_BATCH_SIZE
        });

        while (!this.stopping) {
            try {
                await this.pollAndAggregate();
            } catch (error) {
                logger.error('Work loop error', { error: error.message });
                if (this.handleFatalError(error, 'TransitionAggregationWorker.workLoop')) {
                    break;
                }
            }

            // Sleep before next poll
            if (!this.stopping) {
                await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
            }
        }
    }

    isFatalInfrastructureError(error) {
        const message = (error && (error.message || String(error))).toLowerCase();
        const fatalMarkers = [
            'crossslot',
            'readonly',
            'noauth',
            'wrongpass',
            'noperm',
            'cluster support disabled'
        ];
        return fatalMarkers.some((marker) => message.includes(marker));
    }

    isTransactionUnsupportedError(error) {
        const message = (error && (error.message || String(error))).toLowerCase();
        return message.includes('transaction numbers are only allowed on a replica set member or mongos');
    }

    handleFatalError(error, source) {
        if (!this.isFatalInfrastructureError(error)) return false;
        this.stopping = true;
        if (this.onFatalError) {
            this.onFatalError(source, error);
        }
        return true;
    }

    /**
     * Scan all keys matching a pattern using SCAN (safe for ElastiCache Serverless / cluster).
     * KEYS is O(N) and blocked on ElastiCache Serverless; SCAN is the correct replacement.
     */
    async scanKeys(pattern) {
        const keys = [];
        let cursor = '0';
        do {
            const [nextCursor, batch] = await this.redisClient.scan(
                cursor,
                'MATCH', pattern,
                'COUNT', 100
            );
            cursor = nextCursor;
            keys.push(...batch);
        } while (cursor !== '0');
        return keys;
    }

    /**
     * Single iteration: discover batch keys, attempt claims, aggregate intents
     */
    async pollAndAggregate() {
        await this.promoteDueScheduledIntents();

        // Discover all active signal keys using SCAN (KEYS is blocked on ElastiCache Serverless)
        // Use hash tag {agg} to ensure all aggregation keys stay on the same Redis slot in cluster mode
        const signalKeys = await this.scanKeys('{agg}:immediate:signal:*');

        if (signalKeys.length === 0) {
            return; // No pending intents
        }

        // Attempt to claim and aggregate each batch key
        for (const signalKey of signalKeys) {
            const batchKey = signalKey.replace('{agg}:immediate:signal:', '');

            // Atomically claim this batch key
            const claimKey = `{agg}:immediate:claim:${batchKey}`;
            const claimId = uuidv4();

            // SET NX with expiry: only succeeds if no other worker claimed
            const claimed = await this.redisClient.set(
                claimKey,
                claimId,
                'EX', 5,  // 5-second lock (prevents duplicate claims)
                'NX'      // Only if not exists
            );

            if (!claimed) {
                continue; // Another worker claimed this, skip
            }

            try {
                await this.aggregateForBatchKey(signalKey, claimKey, claimId, batchKey);
            } catch (error) {
                logger.error('Aggregation failed for batch key', {
                    batchKey,
                    error: error.message
                });
                // Delete claim so another worker can retry
                await this.redisClient.del(claimKey);
            }
        }
    }

    /**
     * Promote due delayed intents into the immediate aggregation pipeline.
     */
    async promoteDueScheduledIntents() {
        const now = new Date();
        const dueIntents = await NextStepIntent.find({
            status: 'pending_scheduled',
            dispatchTime: { $lte: now }
        })
            .select('_id batchCompatibilityKey')
            .limit(SCHEDULED_PROMOTION_BATCH_SIZE)
            .lean();

        if (dueIntents.length === 0) {
            return;
        }

        let promotedCount = 0;

        for (const intent of dueIntents) {
            const promoted = await NextStepIntent.findOneAndUpdate(
                { _id: intent._id, status: 'pending_scheduled' },
                {
                    $set: {
                        status: 'pending_aggregation',
                        'metadata.aggregationReason': 'scheduled_due'
                    }
                },
                { new: false }
            );

            if (!promoted) {
                continue;
            }

            const signalKey = `{agg}:immediate:signal:${intent.batchCompatibilityKey}`;
            await this.redisClient.lpush(signalKey, intent._id.toString());
            await this.redisClient.expire(signalKey, 3600);
            promotedCount += 1;
        }

        if (promotedCount > 0) {
            logger.info('Promoted due scheduled intents for aggregation', {
                promotedCount
            });
        }
    }

    /**
     * Aggregate and dispatch for a specific batch key
     */
    async aggregateForBatchKey(signalKey, claimKey, claimId, batchKey) {
        // Drain pending intent IDs from Redis signal queue (up to configured batch size)
        const intentIds = [];
        let batchSize = 0;

        while (batchSize < MICRO_BATCH_SIZE) {
            const intentId = await this.redisClient.rpop(signalKey);
            if (!intentId) break;
            intentIds.push(intentId);
            batchSize++;
        }

        if (intentIds.length === 0) {
            // No intents queued, cleanup
            await this.redisClient.del(signalKey);
            await this.redisClient.del(claimKey);
            return;
        }

        // Bulk fetch intents from MongoDB (only fetch IDs we pulled from Redis)
        const intents = await NextStepIntent.find({
            _id: { $in: intentIds },
            status: 'pending_aggregation'
        }).lean();

        if (intents.length === 0) {
            // Intents disappeared or already batched
            await this.redisClient.del(claimKey);
            return;
        }

        // Verify claim still valid
        const currentClaim = await this.redisClient.get(claimKey);
        if (currentClaim !== claimId) {
            logger.warn('Claim expired during aggregation', { batchKey, claimId });
            return;
        }

        // Check if we should flush this batch
        const shouldFlush = await this.shouldFlush(batchKey, intents);

        if (shouldFlush) {
            // Create batch and enqueue dispatch
            try {
                await this.createBatchAndDispatch(batchKey, intents);
            } catch (error) {
                // Preserve intent IDs in Redis so aggregation can retry on transient failures.
                for (const intentId of intentIds) {
                    await this.redisClient.lpush(signalKey, intentId);
                }
                throw error;
            }
        } else {
            // Put intent IDs back on signal queue for next cycle
            for (const intentId of intentIds) {
                await this.redisClient.lpush(signalKey, intentId);
            }
        }

        // Release claim
        await this.redisClient.del(claimKey);
    }

    /**
     * Determine if batch should be flushed (dispatched) based on size/age
     */
    async shouldFlush(batchKey, intents) {
        const batchSize = intents.length;

        // Rule 1: Size threshold (50 leads by default)
        if (batchSize >= MICRO_BATCH_SIZE) {
            logger.debug('Flush: size threshold reached', { 
                batchKey, 
                size: batchSize,
                threshold: MICRO_BATCH_SIZE
            });
            return true;
        }

        // Rule 2: Age threshold (500ms elapsed, at least 10 leads)
        const oldestCreatedAtMs = Math.min(...intents.map(i => i.metadata?.createdAtMs || i.createdAt.getTime()));
        const ageMs = Date.now() - oldestCreatedAtMs;
        if (ageMs >= MICRO_BATCH_TIME_MS && batchSize >= 10) {
            logger.debug('Flush: age threshold reached', { 
                batchKey, 
                ageMs,
                threshold: MICRO_BATCH_TIME_MS,
                size: batchSize
            });
            return true;
        }

        // Rule 3: Queue saturation (signal key has accumulated 100+ items, flush small batches)
        const signalSize = await this.redisClient.llen(`{agg}:immediate:signal:${batchKey}`);
        if (signalSize > 100 && batchSize >= 5) {
            logger.debug('Flush: queue saturation', { 
                batchKey, 
                queueSize: signalSize, 
                batchSize
            });
            return true;
        }

        // Rule 4: Hard max wait (prevents single/small batches from stalling indefinitely)
        if (ageMs >= MICRO_BATCH_MAX_WAIT_MS) {
            logger.debug('Flush: max wait reached', {
                batchKey,
                ageMs,
                maxWaitMs: MICRO_BATCH_MAX_WAIT_MS,
                batchSize
            });
            return true;
        }

        return false;
    }

    /**
     * Create batch and enqueue dispatch
     */
    async createBatchAndDispatch(batchKey, intents) {
        try {
            return await this.createBatchAndDispatchWithTransaction(batchKey, intents);
        } catch (error) {
            if (!this.isTransactionUnsupportedError(error)) {
                throw error;
            }

            logger.warn('Mongo transactions unavailable, falling back to non-transactional batch creation', {
                error: error.message
            });

            return await this.createBatchAndDispatchWithoutTransaction(batchKey, intents);
        }
    }

    async createBatchAndDispatchWithTransaction(batchKey, intents) {
        const session = await mongoose.startSession();
        session.startTransaction();

        try {
            // Create batch record
            const batch = await BatchDispatch.create([{
                tenantId: intents[0].tenantId,
                campaignId: intents[0].campaignId,
                campaignVersion: intents[0].campaignVersion,
                batchCompatibilityKey: batchKey,
                nextNodeId: intents[0].nextNodeId,
                nextNodeAgentId: intents[0].nextNodeAgentId,
                nextNodeAgentType: intents[0].nextNodeAgentType,
                nextStepIntentIds: intents.map(i => i._id),
                leadCount: intents.length,
                status: 'pending',
                createdBy: 'transition_aggregator',
                metadata: {
                    createdAtMs: Date.now()
                }
            }], { session });

            // Bulk update intents to batched state
            await NextStepIntent.bulkWrite(
                intents.map(intent => ({
                    updateOne: {
                        filter: { _id: intent._id },
                        update: {
                            $set: {
                                batchDispatchId: batch[0]._id,
                                status: 'batched',
                                aggregatedAt: new Date(),
                                'metadata.aggregationReason': 'immediate_flush'
                            }
                        }
                    }
                })),
                { session }
            );

            await session.commitTransaction();

            logger.info('Batch created and intents aggregated', {
                batchId: batch[0]._id,
                batchKey,
                leadCount: intents.length
            });

            // Enqueue dispatch (outside transaction)
            const { queues } = require('../queues');
            await queues.batchDispatch.add(`batch-${batch[0]._id}`, {
                batchDispatchId: batch[0]._id.toString()
            }, {
                removeOnFail: false,
                removeOnComplete: false
            });

            return batch[0];

        } catch (error) {
            await session.abortTransaction();
            if (!this.isTransactionUnsupportedError(error)) {
                logger.error('Transaction failed during batch creation', {
                    error: error.message
                });
            }
            throw error;
        } finally {
            await session.endSession();
        }
    }

    async createBatchAndDispatchWithoutTransaction(batchKey, intents) {
        const batch = await BatchDispatch.create({
            tenantId: intents[0].tenantId,
            campaignId: intents[0].campaignId,
            campaignVersion: intents[0].campaignVersion,
            batchCompatibilityKey: batchKey,
            nextNodeId: intents[0].nextNodeId,
            nextNodeAgentId: intents[0].nextNodeAgentId,
            nextNodeAgentType: intents[0].nextNodeAgentType,
            nextStepIntentIds: intents.map(i => i._id),
            leadCount: intents.length,
            status: 'pending',
            createdBy: 'transition_aggregator',
            metadata: {
                createdAtMs: Date.now()
            }
        });

        await NextStepIntent.bulkWrite(
            intents.map(intent => ({
                updateOne: {
                    filter: { _id: intent._id },
                    update: {
                        $set: {
                            batchDispatchId: batch._id,
                            status: 'batched',
                            aggregatedAt: new Date(),
                            'metadata.aggregationReason': 'immediate_flush_no_txn'
                        }
                    }
                }
            }))
        );

        logger.info('Batch created and intents aggregated (non-transactional)', {
            batchId: batch._id,
            batchKey,
            leadCount: intents.length
        });

        const { queues } = require('../queues');
        await queues.batchDispatch.add(`batch-${batch._id}`, {
            batchDispatchId: batch._id.toString()
        }, {
            removeOnFail: false,
            removeOnComplete: false
        });

        return batch;
    }

    /**
     * Worker job handler (for queue-based dispatch if needed)
     */
    async handleAggregation(job) {
        const { batchCompatibilityKey } = job.data;
        logger.debug('Handling aggregation job', { batchKey: batchCompatibilityKey });
        // This could be used for explicit aggregation triggers
        // For now, work loop handles all aggregation
        return { processed: true };
    }

    /**
     * Start the worker
     */
    start() {
        this.stopping = false;
        logger.info('Starting TransitionAggregationWorker');
        this.workLoopPromise = this.workLoop(MICRO_BATCH_POLL_INTERVAL_MS);
    }

    /**
     * Stop the worker gracefully
     */
    async stop() {
        this.stopping = true;
        logger.info('Stopping TransitionAggregationWorker');
        if (this.workLoopPromise) {
            // Note: work loop runs indefinitely, so we just close connections
        }
        await this.worker.close();
        await this.redisClient.quit();
    }
}

module.exports = TransitionAggregationWorker;
