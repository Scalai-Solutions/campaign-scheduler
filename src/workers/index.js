const campaignNodeDispatchWorker = require('./CampaignNodeDispatchWorker');
const retellBatchDispatchWorker = require('./RetellBatchDispatchWorker');
const retellEventProcessWorker = require('./RetellEventProcessWorker');
const retellBatchReconcileWorker = require('./RetellBatchReconcileWorker');
const TransitionAggregationWorker = require('./TransitionAggregationWorker');
const batchDispatchWorker = require('./BatchDispatchWorker');
const batchReconciliationWorker = require('./BatchReconciliationWorker');
const { connection } = require('../queues');

let transitionAggregationWorker = null;

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

function attachFatalHandlers(worker, workerName, onFatalError) {
    if (!worker || typeof worker.on !== 'function' || !onFatalError) return;

    worker.on('error', (error) => {
        if (isFatalInfrastructureError(error)) {
            onFatalError(workerName, error);
        }
    });

    worker.on('failed', (_job, error) => {
        if (isFatalInfrastructureError(error)) {
            onFatalError(workerName, error);
        }
    });
}

function initWorkers(options = {}) {
    const { onFatalError } = options;

    console.log('[Workers] CampaignNodeDispatch started');
    console.log('[Workers] RetellBatchDispatch started');
    console.log('[Workers] RetellEventProcess started');
    console.log('[Workers] RetellBatchReconcile started');
    console.log('[Workers] BatchDispatch started');
    console.log('[Workers] BatchReconciliation started');

    attachFatalHandlers(campaignNodeDispatchWorker, 'CampaignNodeDispatchWorker', onFatalError);
    attachFatalHandlers(retellBatchDispatchWorker, 'RetellBatchDispatchWorker', onFatalError);
    attachFatalHandlers(retellEventProcessWorker, 'RetellEventProcessWorker', onFatalError);
    attachFatalHandlers(retellBatchReconcileWorker, 'RetellBatchReconcileWorker', onFatalError);
    attachFatalHandlers(batchDispatchWorker, 'BatchDispatchWorker', onFatalError);
    attachFatalHandlers(batchReconciliationWorker, 'BatchReconciliationWorker', onFatalError);

    // Initialize transition aggregation worker (micro-batching)
    const microBatchingEnabled = process.env.ENABLE_MICRO_BATCHING !== 'false';
    if (microBatchingEnabled) {
        try {
            transitionAggregationWorker = new TransitionAggregationWorker(connection, { onFatalError });
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
    const closeSafely = async (worker, workerName) => {
        if (!worker || typeof worker.close !== 'function') return;
        try {
            await worker.close();
            console.log(`[Workers] ${workerName} stopped gracefully`);
        } catch (error) {
            console.error(`[Workers] Error stopping ${workerName}:`, error.message);
        }
    };

    await closeSafely(campaignNodeDispatchWorker, 'CampaignNodeDispatchWorker');
    await closeSafely(retellBatchDispatchWorker, 'RetellBatchDispatchWorker');
    await closeSafely(retellEventProcessWorker, 'RetellEventProcessWorker');
    await closeSafely(retellBatchReconcileWorker, 'RetellBatchReconcileWorker');
    await closeSafely(batchDispatchWorker, 'BatchDispatchWorker');
    await closeSafely(batchReconciliationWorker, 'BatchReconciliationWorker');

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
