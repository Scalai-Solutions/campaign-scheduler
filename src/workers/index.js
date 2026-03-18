const campaignNodeDispatchWorker = require('./CampaignNodeDispatchWorker');
const retellBatchDispatchWorker = require('./RetellBatchDispatchWorker');
const retellEventProcessWorker = require('./RetellEventProcessWorker');
const retellBatchReconcileWorker = require('./RetellBatchReconcileWorker');

function initWorkers() {
    console.log('[Workers] CampaignNodeDispatch started');
    console.log('[Workers] RetellBatchDispatch started');
    console.log('[Workers] RetellEventProcess started');
    console.log('[Workers] RetellBatchReconcile started');
}

module.exports = { initWorkers };
