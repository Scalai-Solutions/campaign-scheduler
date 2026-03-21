const campaignNodeDispatchWorker = require('./CampaignNodeDispatchWorker');
const retellBatchDispatchWorker = require('./RetellBatchDispatchWorker');
const retellEventProcessWorker = require('./RetellEventProcessWorker');
const retellBatchReconcileWorker = require('./RetellBatchReconcileWorker');
const TransitionAggregationWorker = require('./TransitionAggregationWorker');
const batchDispatchWorker = require('./BatchDispatchWorker');
const batchReconciliationWorker = require('./BatchReconciliationWorker');
const { connection } = require('../queues');

let transitionAggregationWorker = null;

function initWorkers() {
    console.log('[Workers] CampaignNodeDispatch started');
    console.log('[Workers] RetellBatchDispatch started');
    console.log('[Workers] RetellEventProcess started');
    console.log('[Workers] RetellBatchReconcile started');
    console.log('[Workers] BatchDispatch started');
    console.log('[Workers] BatchReconciliation started');

    // Initialize transition aggregation worker (micro-batching)
    const microBatchingEnabled = process.env.ENABLE_MICRO_BATCHING !== 'false';
    if (microBatchingEnabled) {
        try {
            transitionAggregationWorker = new TransitionAggregationWorker(connection);
            transitionAggregationWorker.start();
            console.log('[Workers] TransitionAggregationWorker started (micro-batching enabled)');
        } catch (error) {
            console.error('[Workers] Failed to start TransitionAggregationWorker:', error.message);
        }
    } else {
        console.log('[Workers] TransitionAggregationWorker disabled (ENABLE_MICRO_BATCHING=false)');
    }
}

async function stopWorkers() {
    if (transitionAggregationWorker) {
        try {
            await transitionAggregationWorker.stop();
            console.log('[Workers] TransitionAggregationWorker stopped gracefully');
        } catch (error) {
            console.error('[Workers] Error stopping TransitionAggregationWorker:', error.message);
        }
    }
}

module.exports = { initWorkers, stopWorkers };
