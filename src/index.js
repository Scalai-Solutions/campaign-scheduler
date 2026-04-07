require('dotenv').config();
const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

// Validate required env vars
const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) throw new Error('MONGO_URI is required');

const INSTANCE_ID = process.env.SCHEDULER_INSTANCE_ID || `scheduler-${uuidv4()}`;
const POLL_INTERVAL_MS = parseInt(process.env.SCHEDULER_POLL_INTERVAL_MS || '1000');
const BATCH_SIZE = parseInt(process.env.DISPATCH_BATCH_SIZE || '100');

// Shared models
const CampaignNodeRun = require('./models/CampaignNodeRun');

// Shared queues & Redis connection (single source of truth)
const { queues, connection } = require('./queues');

// Initialize all workers (they each consume from their queue using the same Redis connection)
const { initWorkers, stopWorkers } = require('./workers');

let isShuttingDown = false;
let shutdownPromise = null;

function isFatalInfrastructureError(error) {
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

async function poll() {
    try {
        const now = new Date();

        // Find CampaignNodeRuns whose delay has expired and are ready to dispatch.
        // Uses findOneAndUpdate in a loop for atomicity — only one scheduler instance
        // wins each node run transition.
        let dispatched = 0;
        while (dispatched < BATCH_SIZE) {
            const nodeRun = await CampaignNodeRun.findOneAndUpdate(
                {
                    status: 'waiting_delay',
                    delayExpiresAt: { $lte: now },
                    totalLeads: { $gt: 0 }
                },
                {
                    $set: { status: 'dispatching' }
                },
                { new: true }
            );

            if (!nodeRun) break; // No more ready node runs

            await queues.campaignNodeDispatch.add(
                `dispatch-${nodeRun._id}`,
                { nodeRunId: nodeRun._id.toString() }
            );

            console.log(`[Scheduler] Dispatched node run ${nodeRun._id} → node ${nodeRun.nodeId} (campaign ${nodeRun.campaignId})`);
            dispatched++;
        }

        if (dispatched > 0) {
            console.log(`[Scheduler] Dispatched ${dispatched} delayed node runs`);
        }
    } catch (error) {
        console.error('[Scheduler] Poll error:', error.message);
        if (isFatalInfrastructureError(error)) {
            await gracefulShutdown('FATAL_POLL_ERROR', error);
            return;
        }
    } finally {
        // Jittered sleep to avoid thundering-herd across instances
        if (!isShuttingDown) {
            setTimeout(poll, POLL_INTERVAL_MS + Math.floor(Math.random() * 200));
        }
    }
}

async function start() {
    console.log(`[Scheduler] Starting instance ${INSTANCE_ID}...`);
    await mongoose.connect(MONGO_URI, {
        maxPoolSize: parseInt(process.env.SCHEDULER_MONGO_POOL_SIZE || '20'),
        minPoolSize: 3,
    });
    console.log('[Scheduler] MongoDB connected');

    // Boot all BullMQ workers
    initWorkers({
        onFatalError: async (source, error) => {
            if (isFatalInfrastructureError(error)) {
                await gracefulShutdown(`FATAL_WORKER_ERROR:${source}`, error);
            }
        }
    });

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
const gracefulShutdown = async (signal, cause) => {
    if (shutdownPromise) return shutdownPromise;

    shutdownPromise = (async () => {
        console.log(`[Scheduler] ${signal} received, shutting down gracefully...`);
        if (cause) {
            console.error('[Scheduler] Shutdown cause:', cause.message || cause);
        }
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
    })();

    return shutdownPromise;
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('uncaughtException', (error) => {
    console.error('[Scheduler] Uncaught exception:', error);
    gracefulShutdown('UNCAUGHT_EXCEPTION', error);
});
process.on('unhandledRejection', (reason) => {
    console.error('[Scheduler] Unhandled rejection:', reason);
    gracefulShutdown('UNHANDLED_REJECTION', reason);
});
