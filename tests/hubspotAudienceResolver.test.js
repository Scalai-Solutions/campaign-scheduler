const assert = require('assert');
const { __testing } = require('../src/services/hubspotAudienceResolver');

const {
    scanCallableLeadsFromListPages,
    determineListScanStoppedReason
} = __testing;

const OUTCOME_PROP = 'voone_call_outcome_074cb498_99e9_4776_8ac5_2cc9e8555df8';

function makeRecord(id, { outcome = '', phone = '+34600000001' } = {}) {
    return {
        id: String(id),
        properties: {
            firstname: 'Test',
            lastname: String(id),
            phone,
            [OUTCOME_PROP]: outcome
        }
    };
}

function makePage(records, nextAfter = null, totalInList = 1000) {
    return {
        records,
        nextAfter,
        totalInList,
        objectTypeId: '0-1',
        objectTypeName: 'contacts'
    };
}

function runScan(pages, target = 150) {
    return scanCallableLeadsFromListPages(pages, {
        target,
        outcomeProperty: OUTCOME_PROP,
        vooneCallOutcomeIntent: 'callable',
        selectedSource: { listId: '1277' },
        maxScanPages: 80
    });
}

(function testDenseTaggedRegionReturns150() {
    const tagged = Array.from({ length: 392 }, (_, i) => makeRecord(`t-${i}`, { outcome: 'unsuccessful', phone: `+346100${String(i).padStart(5, '0')}` }));
    const callable = Array.from({ length: 357 }, (_, i) => makeRecord(`c-${i}`, { phone: `+346200${String(i).padStart(5, '0')}` }));
    const page1 = makePage([...tagged.slice(0, 250), ...callable.slice(0, 0)], 'page2');
    const page2 = makePage([...tagged.slice(250), ...callable.slice(0, 142)], 'page3');
    const page3 = makePage(callable.slice(142, 250), 'page4');
    const page4 = makePage(callable.slice(250, 357), null);

    const result = runScan([page1, page2, page3, page4]);
    assert.strictEqual(result.leads.length, 150, 'should collect full batch of 150');
    assert.strictEqual(result.stoppedReason, 'target_met');
    assert.strictEqual(result.stats.skippedTaggedCount, 392);
})();

(function testSparseRegionScansPast750() {
    const tagged = Array.from({ length: 662 }, (_, i) => makeRecord(`t-${i}`, { outcome: 'successful', phone: `+347100${String(i).padStart(5, '0')}` }));
    const callableEarly = Array.from({ length: 88 }, (_, i) => makeRecord(`e-${i}`, { phone: `+347200${String(i).padStart(5, '0')}` }));
    const callableLate = Array.from({ length: 62 }, (_, i) => makeRecord(`l-${i}`, { phone: `+347300${String(i).padStart(5, '0')}` }));

    const pages = [
        makePage([...tagged.slice(0, 250)], 'p2'),
        makePage([...tagged.slice(250, 500)], 'p3'),
        makePage([...tagged.slice(500, 662), ...callableEarly], 'p4'),
        makePage(callableLate, null)
    ];

    const result = runScan(pages);
    assert.strictEqual(result.leads.length, 150, 'should scan beyond first 750 to reach 150');
    assert.strictEqual(result.stoppedReason, 'target_met');
    assert.strictEqual(result.stats.pagesConsumed, 4);
})();

(function testListEndPartialBatch() {
    const callable = Array.from({ length: 88 }, (_, i) => makeRecord(`c-${i}`, { phone: `+348200${String(i).padStart(5, '0')}` }));
    const result = runScan([makePage(callable, null)], 150);

    assert.strictEqual(result.leads.length, 88);
    assert.strictEqual(result.scanExhaustedList, true);
    assert.strictEqual(result.stoppedReason, 'list_end');
    assert.strictEqual(result.nextHubspotCursor, null);
})();

(function testCursorAdvancesPastTaggedContacts() {
    const tagged = Array.from({ length: 100 }, (_, i) => makeRecord(`t-${i}`, { outcome: 'unsuccessful', phone: `+349100${String(i).padStart(5, '0')}` }));
    const callable = Array.from({ length: 150 }, (_, i) => makeRecord(`c-${i}`, { phone: `+349200${String(i).padStart(5, '0')}` }));

    const result = runScan([
        makePage(tagged, 'next-page'),
        makePage(callable, null)
    ]);

    assert.strictEqual(result.leads.length, 150);
    assert.strictEqual(result.nextHubspotCursor, null);
    assert.strictEqual(result.stats.pagesConsumed, 2);
})();

(function testStoppedReasonHelper() {
    assert.strictEqual(determineListScanStoppedReason({ validCount: 150, target: 150, listEnd: false, scanCapHit: false }), 'target_met');
    assert.strictEqual(determineListScanStoppedReason({ validCount: 88, target: 150, listEnd: true, scanCapHit: false }), 'list_end');
    assert.strictEqual(determineListScanStoppedReason({ validCount: 50, target: 150, listEnd: false, scanCapHit: true }), 'scan_cap');
})();

console.log('hubspotAudienceResolver tests passed');
