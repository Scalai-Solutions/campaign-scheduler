const { Worker } = require('bullmq');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { getNode, getOutgoingEdges, parseDelayToMs } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX } = require('../queues');
const retellClient = require('../services/retellClient');
const logger = require('../utils/logger');

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

    let batchCallId = null;

    if (node.agentType === 'voice') {
        // 4a. Voice: single Retell batchCall
        const fromNumber = node.fromNumber || null;
        if (!fromNumber) {
            await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, { status: 'cancelled' });
            throw new Error(`No fromNumber for voice node ${node.id} (tenant: ${nodeRun.tenantId})`);
        }

        const tasks = leads.map(lead => ({
            to_number: lead.phone,
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

    // 5. Mark leads as in_progress (voice path)
    const leadIds = leads.map(l => l._id);
    await Lead.updateMany(
        { _id: { $in: leadIds } },
        { $set: { nodeStatus: 'in_progress' } }
    );

    // 6. Activate the CampaignNodeRun
    await CampaignNodeRun.findByIdAndUpdate(resolvedNodeRunId, {
        status: 'active',
        batchCallId,
        totalLeads: leads.length
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

    // 8. Safety-net reconciliation after 10 minutes
    await queues.batchReconcile.add(
        `reconcile-${batchCallId}`,
        { nodeRunId: resolvedNodeRunId.toString(), batchCallId },
        { delay: 10 * 60 * 1000 }
    );

    logger.info('[NodeDispatch] Voice batch dispatched', {
        nodeRunId: resolvedNodeRunId, batchCallId, leadCount: leads.length, edges: outgoingEdges.length
    });
}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_NODE_DISPATCH || '5') });

module.exports = worker;
