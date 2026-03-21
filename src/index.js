require('dotenv').config();
const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

// Validate required env vars
const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) throw new Error('MONGO_URI is required');

const INSTANCE_ID = process.env.SCHEDULER_INSTANCE_ID || `scheduler-${uuidv4()}`;
const LEASE_TTL_SECONDS = parseInt(process.env.SCHEDULER_LEASE_TTL_SECONDS || '60');
const POLL_INTERVAL_MS = parseInt(process.env.SCHEDULER_POLL_INTERVAL_MS || '1000');
const BATCH_SIZE = parseInt(process.env.DISPATCH_BATCH_SIZE || '100');

// Shared models
const ScheduledTask = require('./models/ScheduledTask');

// Shared queues & Redis connection (single source of truth)
const { queues, connection } = require('./queues');

// Initialize all workers (they each consume from their queue using the same Redis connection)
const { initWorkers, stopWorkers } = require('./workers');

let isShuttingDown = false;

async function poll() {
    try {
        const now = new Date();

        // Find tasks that are due or whose lease has expired
        const tasksToLease = await ScheduledTask.find({
            $or: [
                { status: 'scheduled', dueAt: { $lte: now } },
                { status: 'leased', leaseUntil: { $lte: now } }
            ]
        }).limit(BATCH_SIZE);
        
        if (tasksToLease.length > 0) {
            console.log(`[Scheduler] Found ${tasksToLease.length} tasks due for processing`);
        }

        for (const task of tasksToLease) {
            // Atomic lease: only one scheduler instance wins
            const leasedTask = await ScheduledTask.findOneAndUpdate(
                {
                    _id: task._id,
                    $or: [
                        { status: 'scheduled' },
                        { status: 'leased', leaseUntil: { $lte: now } }
                    ]
                },
                {
                    $set: {
                        status: 'leased',
                        leasedBy: INSTANCE_ID,
                        leaseUntil: new Date(Date.now() + LEASE_TTL_SECONDS * 1000)
                    }
                },
                { new: true }
            );

            if (leasedTask) {
                console.log(`[Scheduler] Leased task ${leasedTask._id} → node ${leasedTask.nodeId} for run ${leasedTask.runId}`);
                await queues.campaignNodeDispatch.add(`dispatch-${leasedTask._id}`, {
                    scheduledTaskId: leasedTask._id.toString()
                });
            } else {
                // This happens if another instance leased it first
                console.log(`[Scheduler] Atomically skipped task ${task._id} (already leased)`);
            }
        }
    } catch (error) {
        console.error('[Scheduler] Poll error:', error.message);
    } finally {
        // Jittered sleep to avoid thundering-herd across instances
        if (!isShuttingDown) {
            setTimeout(poll, POLL_INTERVAL_MS + Math.floor(Math.random() * 200));
        }
    }
}

async function start() {
    console.log(`[Scheduler] Starting instance ${INSTANCE_ID}...`);
    await mongoose.connect(MONGO_URI);
    console.log('[Scheduler] MongoDB connected');

    // Boot all BullMQ workers
    initWorkers();

    console.log(`[Scheduler] Workers online — beginning poll loop`);
    await poll();
}

// Log every 1 minute to show the service is alive
const heartbeatInterval = setInterval(() => {
    console.log(`[Scheduler] Heartbeat - ${new Date().toISOString()} - Alive`);
}, 60000);

start().catch((err) => {
    console.error('[Scheduler] Fatal startup error:', err);
    process.exit(1);
});

// Graceful shutdown
const gracefulShutdown = async (signal) => {
    console.log(`[Scheduler] ${signal} received, shutting down gracefully...`);
    isShuttingDown = true;

    // Clear heartbeat interval
    clearInterval(heartbeatInterval);

    try {
        // Stop workers first
        if (stopWorkers) {
            await stopWorkers();
        }

        // Close Redis connection
        await connection.quit();
        console.log('[Scheduler] Redis connection closed');

        // Disconnect from MongoDB
        await mongoose.disconnect();
        console.log('[Scheduler] MongoDB connection closed');

        console.log('[Scheduler] Graceful shutdown complete');
        process.exit(0);
    } catch (error) {
        console.error('[Scheduler] Error during graceful shutdown:', error.message);
        process.exit(1);
    }
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
