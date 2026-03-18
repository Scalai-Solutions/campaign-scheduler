const { Queue } = require('bullmq');
const Redis = require('ioredis');

// REDIS_URL can be passed directly (e.g. from docker-compose environment override)
// or built from individual parts if needed.
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const connection = new Redis(REDIS_URL, {
    maxRetriesPerRequest: null,
});

connection.on('error', (err) => {
    console.error('[CampaignQueues/Scheduler] Redis connection error:', err.message);
});

const queues = {
    campaignNodeDispatch: new Queue('campaign.node.dispatch', { connection }),
    retellBatchDispatch: new Queue('retell.batch.dispatch', { connection }),
    retellEventsProcess: new Queue('retell.events.process', { connection }),
    retellBatchReconcile: new Queue('retell.batch.reconcile', { connection }),
};

module.exports = {
    queues,
    connection,
};
