const { Worker } = require('bullmq');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { getNode, getOutgoingEdges, parseDelayToMs } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX } = require('../queues');
const retellClient = require('../services/retellClient');
const prefetchService = require('../services/prefetchService');
const logger = require('../utils/logger');

// Reconcile delay tuning — all overridable via env without redeploying code.
const RETELL_CALL_CONCURRENCY   = parseInt(process.env.RETELL_CALL_CONCURRENCY    || '20');
const AVG_CALL_DURATION_MS      = parseInt(process.env.AVG_CALL_DURATION_MS       || String(3 * 60 * 1000));
const BATCH_RECONCILE_GRACE_MS  = parseInt(process.env.BATCH_RECONCILE_GRACE_MS   || String(30 * 60 * 1000));
const BATCH_RECONCILE_BUFFER_MS = parseInt(process.env.BATCH_RECONCILE_BUFFER_MS  || String(30 * 60 * 1000));
const BATCH_RECONCILE_FLOOR_MS  = parseInt(process.env.BATCH_RECONCILE_FLOOR_MS   || String(20 * 60 * 1000));

/**
 * Compute how long to wait before running the safety-net reconciliation job.
 *
 * Two constraints are balanced:
 *  1. Duration-based  — wait until all real calls should be finished + webhook grace.
 *  2. Pre-dispatch    — fire at least BUFFER_MS before the scheduler triggers the
 *                       earliest delayed next-node, so stragglers are closed before
 *                       node 2 reads the lead set.
 *
 * Floor prevents the value from being uselessly small for tiny batches.
 */
function computeReconcileDelayMs(leadCount, outgoingEdges) {
    const rounds = Math.ceil(leadCount / RETELL_CALL_CONCURRENCY);
    const batchDurationMs = rounds * AVG_CALL_DURATION_MS;
    const durationBasedMs = batchDurationMs + BATCH_RECONCILE_GRACE_MS;

    const delayedEdgeMs = outgoingEdges
        .map(e => parseDelayToMs(e.delay))
        .filter(d => d > 0);
    const minEdgeDelayMs = delayedEdgeMs.length > 0 ? Math.min(...delayedEdgeMs) : Infinity;
    const preDispatchMs  = minEdgeDelayMs === Infinity ? Infinity : minEdgeDelayMs - BATCH_RECONCILE_BUFFER_MS;

    return Math.max(BATCH_RECONCILE_FLOOR_MS, Math.min(durationBasedMs, preDispatchMs));
}

/**
 * Record invalid leads on the node run (outcomes.failed + failedLeads array)
 * and fully remove them from the campaign flow.
 *
 * @param {string} resolvedNodeRunId
 * @param {string} campaignId
 * @param {Array<{leadId, phone, reason}>} entries
 */
async function recordAndRemoveInvalidLeads(resolvedNodeRunId, campaignId, entries) {
    if (!entries || entries.length === 0) return;
    const normalized = entries.map(e => ({
        leadId: e.leadId,
        phone: e.phone || '',
        reason: e.reason,
        failedAt: new Date()
    }));
    const invalidIds = normalized.map(e => e.leadId);

    await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, {
        $inc: { 'outcomes.failed': normalized.length },
        $push: { failedLeads: { $each: normalized } }
    });

    await Lead.updateMany(
        { _id: { $in: invalidIds }, campaignId },
        {
            $unset: {
                campaignId: '',
                campaignVersion: '',
                currentNodeId: '',
                outcome: '',
                nodeStatus: ''
            }
        }
    );
}

/**
 * CampaignNodeDispatchWorker
 *
 * Handles the `campaign.node.dispatch` queue.
 *
 * Job data: { nodeRunId }
 *
 * Flow:
 *   1. Load CampaignNodeRun → abort if not 'dispatching'
 *   2. Load CampaignDefinition → resolve node from workflow
 *   3. Lead.find({ campaignId, currentNodeId, nodeStatus: 'pending' }) — all leads assigned to this node
 *   4. Single Retell batchCall for voice / instant completion for chat
 *   5. Lead.updateMany → nodeStatus: 'in_progress'
 *   6. CampaignNodeRun → status: 'active', batchCallId, totalLeads
 *   7. Pre-create CampaignNodeRun stubs (waiting_delay) for each outgoing edge
 *   8. Enqueue batch.reconcile as a safety-net
 */
const worker = new Worker('campaign.node.dispatch', async (job) => {
    const { nodeRunId, campaignNodeRunId } = job.data;
    const resolvedNodeRunId = nodeRunId || campaignNodeRunId;

    if (!resolvedNodeRunId) {
        logger.warn('[NodeDispatch] Missing nodeRunId in job payload', { jobId: job.id });
        return;
    }

    // 1. Load and guard the CampaignNodeRun
    const nodeRun = await CampaignNodeRun.findById(resolvedNodeRunId);
    if (!nodeRun || nodeRun.status !== 'dispatching') return;

    // 2. Load workflow definition
    const definition = await CampaignDefinition.findOne({
        tenantId: nodeRun.tenantId,
        campaignId: nodeRun.campaignId,
        version: nodeRun.campaignVersion
    });
    if (!definition) {
        await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, { status: 'cancelled' });
        logger.error('[NodeDispatch] No CampaignDefinition found', { nodeRunId: resolvedNodeRunId });
        return;
    }

    const node = getNode(definition.workflowJson, nodeRun.nodeId);
    if (!node) {
        await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, { status: 'cancelled' });
        logger.error('[NodeDispatch] Node not found in workflow', { nodeRunId: resolvedNodeRunId, nodeId: nodeRun.nodeId });
        return;
    }

    // 3. Gather all leads assigned to this node that are pending dispatch
    const leads = await Lead.find({
        campaignId: nodeRun.campaignId,
        currentNodeId: nodeRun.nodeId,
        nodeStatus: 'pending'
    }).lean();

    if (leads.length === 0) {
        await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, { status: 'completed', totalLeads: 0, completedLeads: 0 });
        logger.warn('[NodeDispatch] No pending leads for node', { nodeRunId: resolvedNodeRunId, nodeId: nodeRun.nodeId });
        return;
    }

    logger.info('[NodeDispatch] Dispatching node', {
        nodeRunId: resolvedNodeRunId, nodeId: node.id, agentType: node.agentType, leadCount: leads.length
    });

    // Pre-fetch caller context + HubSpot data for all leads in parallel
    let prefetchMap = new Map();
    try {
        prefetchMap = await prefetchService.prefetchBatch(
            leads, nodeRun.tenantId, node.agentId,
            { timeoutMs: parseInt(process.env.PREFETCH_TIMEOUT_MS || '8000') }
        );
        logger.info('[NodeDispatch] Batch prefetch completed', {
            nodeRunId: resolvedNodeRunId, leadCount: leads.length, prefetchedCount: prefetchMap.size
        });
    } catch (err) {
        logger.warn('[NodeDispatch] Batch prefetch failed, proceeding without', {
            nodeRunId: resolvedNodeRunId, error: err.message
        });
    }

    let batchCallId = null;
    let validLeads = leads;

    if (node.agentType === 'voice') {
        // 4a. Voice: single Retell batchCall
        const fromNumber = node.fromNumber || null;
        if (!fromNumber) {
            await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, { status: 'cancelled' });
            throw new Error(`No fromNumber for voice node ${node.id} (tenant: ${nodeRun.tenantId})`);
        }

        // Pre-validation: filter out leads with invalid E.164 numbers before hitting Retell
        const preInvalid = [];
        validLeads = [];
        for (const lead of leads) {
            if (lead.phone && /^\+\d{10,15}$/.test(lead.phone)) {
                validLeads.push(lead);
            } else {
                preInvalid.push({
                    leadId: lead._id,
                    phone: lead.phone || '',
                    reason: 'Invalid phone number (not E.164)'
                });
            }
        }

        if (preInvalid.length > 0) {
            await recordAndRemoveInvalidLeads(resolvedNodeRunId, nodeRun.campaignId, preInvalid);
            logger.warn('[NodeDispatch] Removed leads with invalid phone numbers before dispatch', {
                nodeRunId: resolvedNodeRunId.toString(),
                invalidCount: preInvalid.length,
                invalidPhones: preInvalid.map(e => e.phone)
            });
        }

        if (validLeads.length === 0) {
            await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, {
                status: 'completed',
                totalLeads: leads.length,
                completedLeads: leads.length
            });
            logger.warn('[NodeDispatch] All leads invalid, node completed', { nodeRunId: resolvedNodeRunId });
            return;
        }

        const tasks = validLeads.map(lead => ({
            to_number: lead.phone,
            retell_llm_dynamic_variables: {
                phone_number: lead.phone || '',
                agent_id: node.agentId || '',
                subaccount_id: nodeRun.tenantId || '',
                ...(prefetchMap.get(lead._id.toString()) || {})
            },
            metadata: {
                tenantId: nodeRun.tenantId,
                campaignId: nodeRun.campaignId,
                version: nodeRun.campaignVersion,
                nodeId: node.id,
                nodeRunId: resolvedNodeRunId.toString(),
                leadId: lead._id.toString()
            }
        }));

        const result = await retellClient.sendBatchCalls({
            baseAgentId: node.agentId,
            fromNumber,
            name: `Campaign ${nodeRun.campaignId} Node ${node.id}`,
            tasks
        });
        batchCallId = result.batchCallId;

        // Handle any additional invalids caught by Retell's own validation during the retry loop
        if (result.invalidTasks && result.invalidTasks.length > 0) {
            const retellInvalid = result.invalidTasks
                .map(t => ({
                    leadId: t.metadata?.leadId,
                    phone: t.to_number || t.phone_number || '',
                    reason: t.failureReason || 'Rejected by Retell (invalid phone number)'
                }))
                .filter(e => e.leadId);

            if (retellInvalid.length > 0) {
                await recordAndRemoveInvalidLeads(resolvedNodeRunId, nodeRun.campaignId, retellInvalid);
                const retellInvalidIds = new Set(retellInvalid.map(e => String(e.leadId)));
                validLeads = validLeads.filter(l => !retellInvalidIds.has(String(l._id)));
                logger.warn('[NodeDispatch] Removed Retell-rejected leads from batch', {
                    nodeRunId: resolvedNodeRunId,
                    invalidCount: retellInvalid.length
                });
            }
        }

        // If Retell rejected all tasks, complete the node
        if (!batchCallId) {
            await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, {
                status: 'completed',
                totalLeads: leads.length,
                completedLeads: leads.length
            });
            logger.warn('[NodeDispatch] Retell rejected all leads, node completed', { nodeRunId: resolvedNodeRunId });
            return;
        }
    } else {
        // 4b. Chat: mark all leads as completed immediately (placeholder for future chat dispatch)
        const leadIds = leads.map(l => l._id);
        await Lead.updateMany(
            { _id: { $in: leadIds } },
            { $set: { nodeStatus: 'completed', outcome: 'successful' } }
        );
        await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, {
            status: 'completed',
            totalLeads: leads.length,
            completedLeads: leads.length,
            'outcomes.successful': leads.length
        });
        // Enqueue node completion to advance leads to next nodes
        await queues.nodeComplete.add(`complete-${resolvedNodeRunId}`, { nodeRunId: resolvedNodeRunId.toString() });
        return;
    }

    // 5. Mark valid leads as in_progress (voice path)
    const leadIds = validLeads.map(l => l._id);
    await Lead.updateMany(
        { _id: { $in: leadIds } },
        { $set: { nodeStatus: 'in_progress' } }
    );

    // 6. Activate the CampaignNodeRun — totalLeads is valid (dispatched) lead count
    await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, {
        status: 'active',
        batchCallId,
        totalLeads: validLeads.length
    });

    // 7. Pre-create waiting_delay stubs for each outgoing edge
    const outgoingEdges = getOutgoingEdges(definition.workflowJson, node.id);
    if (outgoingEdges.length > 0) {
        const edgeOps = outgoingEdges.map(edge => {
            const delayMs = parseDelayToMs(edge.delay);
            return {
                updateOne: {
                    filter: {
                        campaignId: nodeRun.campaignId,
                        campaignVersion: nodeRun.campaignVersion,
                        nodeId: edge.toNodeId,
                        parentNodeId: node.id,
                        sourceOutcome: edge.outcome
                    },
                    update: {
                        $setOnInsert: {
                            tenantId: nodeRun.tenantId,
                            campaignId: nodeRun.campaignId,
                            campaignVersion: nodeRun.campaignVersion,
                            nodeId: edge.toNodeId,
                            agentId: getNode(definition.workflowJson, edge.toNodeId)?.agentId,
                            agentType: getNode(definition.workflowJson, edge.toNodeId)?.agentType,
                            fromNumber: getNode(definition.workflowJson, edge.toNodeId)?.fromNumber || null,
                            status: 'waiting_delay',
                            delayExpiresAt: delayMs > 0 ? new Date(Date.now() + delayMs) : null,
                            parentNodeId: node.id,
                            sourceOutcome: edge.outcome
                        }
                    },
                    upsert: true
                }
            };
        });
        await CampaignNodeRun.bulkWrite(edgeOps, { ordered: false });
    }

    // 8. Safety-net reconciliation — delay adapts to batch size and next-node timing.
    const reconcileDelayMs = computeReconcileDelayMs(leads.length, outgoingEdges);
    await queues.batchReconcile.add(
        `reconcile-${batchCallId}`,
        { nodeRunId: resolvedNodeRunId.toString(), batchCallId },
        { delay: reconcileDelayMs }
    );
    logger.info('[NodeDispatch] Scheduled reconcile', {
        nodeRunId: resolvedNodeRunId, reconcileDelayMs, leadCount: leads.length
    });

    logger.info('[NodeDispatch] Voice batch dispatched', {
        nodeRunId: resolvedNodeRunId, batchCallId, leadCount: leads.length, edges: outgoingEdges.length
    });
}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_NODE_DISPATCH || '5') });

module.exports = worker;
