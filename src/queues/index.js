const { Queue } = require('bullmq');
const Redis = require('ioredis');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const BULL_PREFIX = process.env.BULL_PREFIX || '{bull}';

const QUEUE_NAMES = {
    campaignNodeDispatch: 'campaign.node.dispatch',
    retellEventsProcess:  'retell.events.process',
    batchReconcile:       'batch.reconcile',
    nodeComplete:         'node.complete',
    campaignCompletion:   'campaign.completion',
    // Chat-agent campaign queues
    chatNodeDispatch:     'campaign.chat.dispatch',
    chatEventsProcess:    'chat.events.process',
    chatBatchReconcile:   'chat.batch.reconcile',
};

// BullMQ's Lua scripts atomically touch multiple keys per queue
// (e.g. {prefix}:queue:wait, {prefix}:queue:active, {prefix}:queue:delayed …).
// Without hash tags, each key hashes to a different Redis Cluster slot and the
// atomic Lua script is rejected with CROSSSLOT.
//
// Wrapping the prefix in curly braces forces Redis to hash only the text inside
// {} when computing the hash slot, so every key for every queue lands on the
// SAME slot → no more CROSSSLOT errors.
//
// This works for ALL Redis deployments:
//   • Single-node / docker-compose   → hash tags are a no-op, keys just carry the prefix
//   • ElastiCache Serverless          → internal sharding honours hash tags, CROSSSLOT resolved
//   • ElastiCache cluster-mode enabled (true cluster) → slot pinning works correctly
//
// NOTE: Do NOT use ioredis.Cluster here. ElastiCache Serverless and cluster-mode-
// disabled replication groups do not expose the CLUSTER SLOTS command, so
// Redis.Cluster connection fails with "ERR This instance has cluster support disabled".
// A plain ioredis connection with hash-tagged keys handles all environments uniformly.
function createConnection() {
    return new Redis(REDIS_URL, { maxRetriesPerRequest: null });
}

const connection = createConnection();

connection.on('error', (err) => {
    console.error('[CampaignQueues/Scheduler] Redis connection error:', err.message);
});

const queueOptions = { connection, prefix: BULL_PREFIX };

const queues = {
    campaignNodeDispatch: new Queue(QUEUE_NAMES.campaignNodeDispatch, queueOptions),
    retellEventsProcess:  new Queue(QUEUE_NAMES.retellEventsProcess,  queueOptions),
    batchReconcile:       new Queue(QUEUE_NAMES.batchReconcile,       queueOptions),
    nodeComplete:         new Queue(QUEUE_NAMES.nodeComplete,         queueOptions),
    campaignCompletion:   new Queue(QUEUE_NAMES.campaignCompletion,   queueOptions),
    // Chat-agent campaign queues
    chatNodeDispatch:   new Queue(QUEUE_NAMES.chatNodeDispatch,   queueOptions),
    chatEventsProcess:  new Queue(QUEUE_NAMES.chatEventsProcess,  queueOptions),
    chatBatchReconcile: new Queue(QUEUE_NAMES.chatBatchReconcile, queueOptions),
};

module.exports = {
    queues,
    connection,
    createConnection,
    BULL_PREFIX,
    QUEUE_NAMES,
};
