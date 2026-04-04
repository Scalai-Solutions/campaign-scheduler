const campaignNodeDispatchWorker = require('./CampaignNodeDispatchWorker');
const retellEventProcessWorker = require('./RetellEventProcessWorker');
const nodeCompletionWorker = require('./NodeCompletionWorker');
const batchReconciliationWorker = require('./BatchReconciliationWorker');
const campaignCompletionWorker = require('./CampaignCompletionWorker');

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
    console.log('[Workers] RetellEventProcess started');
    console.log('[Workers] NodeCompletion started');
    console.log('[Workers] BatchReconciliation started');
    console.log('[Workers] CampaignCompletion started');

    attachFatalHandlers(campaignNodeDispatchWorker, 'CampaignNodeDispatchWorker', onFatalError);
    attachFatalHandlers(retellEventProcessWorker, 'RetellEventProcessWorker', onFatalError);
    attachFatalHandlers(nodeCompletionWorker, 'NodeCompletionWorker', onFatalError);
    attachFatalHandlers(batchReconciliationWorker, 'BatchReconciliationWorker', onFatalError);
    attachFatalHandlers(campaignCompletionWorker, 'CampaignCompletionWorker', onFatalError);
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
    await closeSafely(retellEventProcessWorker, 'RetellEventProcessWorker');
    await closeSafely(nodeCompletionWorker, 'NodeCompletionWorker');
    await closeSafely(batchReconciliationWorker, 'BatchReconciliationWorker');
    await closeSafely(campaignCompletionWorker, 'CampaignCompletionWorker');
}

module.exports = { initWorkers, stopWorkers };
