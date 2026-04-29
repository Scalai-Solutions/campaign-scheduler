const { Worker } = require('bullmq');
const mongoose = require('mongoose');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignChatSession = require('../models/CampaignChatSession');
const CampaignDefinition = require('../models/CampaignDefinition');
const Lead = require('../models/Lead');
const { getNode, getOutgoingEdges, parseDelayToMs } = require('../campaignKernel');
const { connection, queues, BULL_PREFIX } = require('../queues');
const retellClient = require('../services/retellClient');
const wahaClient = require('../services/wahaClient');
const { normalizeE164Phone } = require('../utils/phoneValidation');
const logger = require('../utils/logger');

/**
 * How long to wait before the chat reconciliation job fires.
 *
 * Chat outcomes take substantially longer than voice calls because the lead
 * must see the message, read it, and reply — then Retell closes the chat after
 * `end_chat_after_silence_ms` (default 6 min).
 *
 * Default 2h; override with CHAT_RECONCILE_DELAY_MS env var.
 */
const CHAT_RECONCILE_DELAY_MS = parseInt(
    process.env.CHAT_RECONCILE_DELAY_MS || String(2 * 60 * 60 * 1000), 10
);

/**
 * TTL (seconds) for the Redis cache entry that tells the connector-server a
 * phone number is in an active campaign chat session.
 */
const CAMPAIGN_CHAT_CACHE_TTL_SECS = parseInt(
    process.env.CAMPAIGN_CHAT_CACHE_TTL_SECS || String(24 * 60 * 60), 10
);

const CHAT_AGENT_MESSAGE_CACHE_TTL_MS = parseInt(
    process.env.CHAT_AGENT_MESSAGE_CACHE_TTL_MS || String(5 * 60 * 1000), 10
);

const RETELL_CREATE_CHAT_MAX_RETRIES = Math.max(
    1,
    parseInt(process.env.RETELL_CREATE_CHAT_MAX_RETRIES || '3', 10)
);

const RETELL_CREATE_CHAT_RETRY_DELAY_MS = Math.max(
    0,
    parseInt(process.env.RETELL_CREATE_CHAT_RETRY_DELAY_MS || '1500', 10)
);

const chatAgentMessageCache = new Map();

function getCachedChatAgentMessage(agentId) {
    const entry = chatAgentMessageCache.get(agentId);
    if (entry && entry.expiresAt > Date.now()) return entry.value;
    chatAgentMessageCache.delete(agentId);
    return null;
}

function setCachedChatAgentMessage(agentId, value) {
    chatAgentMessageCache.set(agentId, {
        value,
        expiresAt: Date.now() + CHAT_AGENT_MESSAGE_CACHE_TTL_MS
    });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Returns true for Retell 4xx API errors — these are permanent failures
 * (unknown agentId, auth, bad payload) and should not be retried.
 * Network errors and 5xx are transient and worth retrying.
 */
function isNonRetryableRetellError(err) {
    return /^Retell Chat API error 4\d{2}:/.test(err?.message || '');
}

function getCampaignChatCacheKeys(tenantId, phone) {
    const raw = String(phone || '').trim();
    if (!raw) return [];

    const digits = raw.replace(/^\+/, '');
    return [...new Set([
        `campaign:chat:active:${tenantId}:${raw}`,
        `campaign:chat:active:${tenantId}:${digits}`,
        `campaign:chat:active:${tenantId}:+${digits}`
    ])];
}

async function supersedeExistingCampaignChatSession({ currentNodeRunId, campaignId, phone }) {
    const existingSession = await CampaignChatSession.findOne(
        {
            campaignId,
            phone,
            status: { $in: ['active', 'pending'] },
            nodeRunId: { $ne: currentNodeRunId }
        },
        {
            _id: 1,
            tenantId: 1,
            campaignId: 1,
            nodeId: 1,
            nodeRunId: 1,
            leadId: 1,
            status: 1
        }
    ).lean();

    if (!existingSession) return null;

    const now = new Date();

    await CampaignChatSession.updateOne(
        { _id: existingSession._id, status: { $in: ['active', 'pending'] } },
        {
            $set: {
                status: 'timeout',
                outcome: 'not_answered',
                lastActivityAt: now
            }
        }
    );

    const supersededNodeRun = await CampaignNodeRun.findByIdAndUpdate(
        existingSession.nodeRunId,
        {
            $inc: {
                completedLeads: 1,
                'outcomes.not_answered': 1
            }
        },
        { new: true }
    );

    try {
        const pipeline = connection.pipeline();
        for (const cacheKey of getCampaignChatCacheKeys(existingSession.tenantId, phone)) {
            pipeline.del(cacheKey);
        }
        await pipeline.exec();
    } catch (cacheErr) {
        logger.warn('[ChatNodeDispatch] Failed to evict Redis cache for superseded session', {
            currentNodeRunId,
            previousNodeRunId: existingSession.nodeRunId?.toString(),
            phone,
            error: cacheErr.message
        });
    }

    if (
        supersededNodeRun &&
        supersededNodeRun.completedLeads >= supersededNodeRun.totalLeads &&
        supersededNodeRun.totalLeads > 0
    ) {
        await queues.nodeComplete.add(
            `complete-${supersededNodeRun._id}`,
            { nodeRunId: supersededNodeRun._id.toString() },
            { jobId: `node-complete-${supersededNodeRun._id}` }
        );
    }

    return existingSession;
}

/**
 * Resolve the outbound first message text for a chat node.
 *
 * Strategy (intentionally pluggable for future enhancements):
 *   1. Use `node.firstMessage` if explicitly set on the workflow node.
 *   2. Fall back to `node.beginMessage` / `node.initialMessage` compatibility keys.
 *   3. Fall back to WAHA_DEFAULT_FIRST_MESSAGE env var.
 *   4. Ultimately fall back to an empty string with a warning.
 *
 * Future options: fetch from Retell agent config, dedicated campaign field, etc.
 *
 * @param {object} node - Workflow node object
 * @returns {string}
 */
function resolveFirstMessage(node) {
    if (node.firstMessage && typeof node.firstMessage === 'string' && node.firstMessage.trim()) {
        return node.firstMessage.trim();
    }

    if (node.beginMessage && typeof node.beginMessage === 'string' && node.beginMessage.trim()) {
        return node.beginMessage.trim();
    }

    if (node.initialMessage && typeof node.initialMessage === 'string' && node.initialMessage.trim()) {
        return node.initialMessage.trim();
    }

    const envDefault = process.env.WAHA_DEFAULT_FIRST_MESSAGE;
    if (envDefault && envDefault.trim()) {
        return envDefault.trim();
    }

    logger.warn('[ChatNodeDispatch] No firstMessage configured for chat node — outbound message will be empty', {
        nodeId: node.id,
        agentId: node.agentId
    });
    return '';
}

/**
 * Record invalid leads at the node-run level and remove them from the campaign
 * flow. Mirrors the same logic in CampaignNodeDispatchWorker.
 */
async function recordAndRemoveInvalidLeads(nodeRunId, campaignId, entries) {
    if (!entries || entries.length === 0) return;
    const normalised = entries.map(e => ({
        leadId: e.leadId,
        phone: e.phone || '',
        reason: e.reason,
        failedAt: new Date()
    }));
    const invalidIds = normalised.map(e => e.leadId);

    await CampaignNodeRun.findByIdAndUpdate(nodeRunId, {
        $inc: {
            completedLeads: normalised.length,
            'outcomes.failed': normalised.length
        },
        $push: { failedLeads: { $each: normalised } }
    });

    await Lead.updateMany(
        { _id: { $in: invalidIds }, campaignId },
        {
            $unset: {
                campaignId: '',
                campaignVersion: '',
                currentNodeId: '',
                outcome: '',
                nodeStatus: '',
                chatSessionId: ''
            }
        }
    );
}

/**
 * ChatNodeDispatchWorker
 *
 * Handles the `campaign.chat.dispatch` queue.
 * Processes chat-type campaign nodes:
 *   1. Load CampaignNodeRun (guard status = 'dispatching', agentType = 'chat')
 *   2. Load CampaignDefinition + resolve node
 *   3. Load all pending leads for this node
 *   4. Validate phone numbers (E.164)
 *   5. Resolve first-message text via resolveFirstMessage()
 *   6. Send outbound WhatsApp via WAHA sequentially (rate-limited)
 *   7. Create Retell chat session for each successfully messaged lead
 *   8. Bulk-insert CampaignChatSession documents + write Redis cache entries
 *   9. Lead.updateMany → nodeStatus: 'in_progress'
 *  10. Activate CampaignNodeRun
 *  11. Pre-create next-node stubs (same as voice worker)
 *  12. Enqueue chat.batch.reconcile safety-net
 */
const worker = new Worker('campaign.chat.dispatch', async (job) => {
    const { nodeRunId } = job.data;

    if (!nodeRunId) {
        logger.warn('[ChatNodeDispatch] Missing nodeRunId in job payload', { jobId: job.id });
        return;
    }

    // 1. Load and guard the CampaignNodeRun
    const nodeRun = await CampaignNodeRun.findById(nodeRunId);
    if (!nodeRun || nodeRun.status !== 'dispatching') return;

    if (nodeRun.agentType !== 'chat') {
        logger.error('[ChatNodeDispatch] Worker received non-chat nodeRun — routing error', {
            nodeRunId, agentType: nodeRun.agentType
        });
        return;
    }

    // 2. Load workflow definition
    const definition = await CampaignDefinition.findOne({
        tenantId: nodeRun.tenantId,
        campaignId: nodeRun.campaignId,
        version: nodeRun.campaignVersion
    });
    if (!definition) {
        await CampaignNodeRun.findByIdAndUpdate(nodeRunId, { status: 'cancelled' });
        logger.error('[ChatNodeDispatch] No CampaignDefinition found', { nodeRunId });
        return;
    }

    const node = getNode(definition.workflowJson, nodeRun.nodeId);
    if (!node) {
        await CampaignNodeRun.findByIdAndUpdate(nodeRunId, { status: 'cancelled' });
        logger.error('[ChatNodeDispatch] Node not found in workflow', { nodeRunId, nodeId: nodeRun.nodeId });
        return;
    }

    // 2b. Live firstMessage fallback — campaigns saved before _embedChatFirstMessages
    //     was deployed won't have firstMessage pre-embedded in workflowJson.
    //     Query chatagents directly (same MongoDB) so we always have the value.
    if (!node.firstMessage && !node.beginMessage && !node.initialMessage && node.agentId) {
        try {
            const cachedFirstMessage = getCachedChatAgentMessage(node.agentId);
            if (cachedFirstMessage) {
                node.firstMessage = cachedFirstMessage;
            } else {
                const chatAgentDoc = await mongoose.connection.db
                    .collection('chatagents')
                    .findOne({ agentId: node.agentId }, { projection: { beginMessage: 1 } });
                if (chatAgentDoc?.beginMessage) {
                    node.firstMessage = chatAgentDoc.beginMessage;
                    setCachedChatAgentMessage(node.agentId, chatAgentDoc.beginMessage);
                }
            }

            if (node.firstMessage) {
                logger.info('[ChatNodeDispatch] Resolved firstMessage from chatagents live lookup', {
                    nodeId: node.id, agentId: node.agentId
                });
            }
        } catch (err) {
            logger.warn('[ChatNodeDispatch] Failed to resolve firstMessage from chatagents', {
                nodeId: node.id, agentId: node.agentId, error: err.message
            });
        }
    }

    // 3. Gather pending leads
    const leads = await Lead.find({
        campaignId: nodeRun.campaignId,
        currentNodeId: nodeRun.nodeId,
        nodeStatus: 'pending'
    })
        .select('_id phone')
        .lean();

    if (leads.length === 0) {
        await CampaignNodeRun.findByIdAndUpdate(nodeRunId, {
            status: 'completed', totalLeads: 0, completedLeads: 0
        });
        logger.warn('[ChatNodeDispatch] No pending leads for chat node', { nodeRunId, nodeId: nodeRun.nodeId });
        return;
    }

    logger.info('[ChatNodeDispatch] Dispatching chat node', {
        nodeRunId, nodeId: node.id, agentId: node.agentId, leadCount: leads.length
    });

    // 4. Phone validation
    const preInvalid = [];
    const validLeads = [];
    for (const lead of leads) {
        const normalised = normalizeE164Phone(lead.phone);
        if (normalised) {
            validLeads.push({ ...lead, phone: normalised });
        } else {
            preInvalid.push({ leadId: lead._id, phone: lead.phone || '', reason: 'Invalid phone number' });
        }
    }

    if (preInvalid.length > 0) {
        await recordAndRemoveInvalidLeads(nodeRunId, nodeRun.campaignId, preInvalid);
        logger.warn('[ChatNodeDispatch] Removed leads with invalid phones', {
            nodeRunId, invalidCount: preInvalid.length
        });
    }

    if (validLeads.length === 0) {
        await CampaignNodeRun.findByIdAndUpdate(nodeRunId, {
            status: 'completed',
            totalLeads: leads.length,
            completedLeads: leads.length
        });
        logger.warn('[ChatNodeDispatch] All leads invalid, chat node completed', { nodeRunId });
        return;
    }

    // 5. Resolve first message
    const firstMessage = resolveFirstMessage(node);

    // 6 & 7. Sequential WAHA send + Retell chat creation
    //
    // Accumulate results before any bulk DB writes so a mid-loop failure does
    // not leave the DB in a partial state (only sessions fully set up are saved).

    const sessionDocsToInsert = [];  // new sessions — need DB insert + Redis write
    const cacheRefreshDocs    = [];  // already-sent sessions — Redis write only (no re-send)
    const successfulLeadIds   = [];
    const wahaSendFailed      = [];  // leads where WAHA send failed

    for (let i = 0; i < validLeads.length; i++) {
        const lead = validLeads[i];

        // ── Idempotency checkpoint ────────────────────────────────────────
        // On BullMQ job replay (stalled/restarted worker), check if we already
        // sent the first message on a prior attempt.  The unique index on
        // (campaignId, nodeId, phone) makes this lookup deterministic.
        const existingSession = await CampaignChatSession.findOne(
            { campaignId: nodeRun.campaignId, nodeId: node.id, nodeRunId, phone: lead.phone },
            { firstMessageSentAt: 1, retellChatId: 1, agentId: 1,
              campaignVersion: 1, nodeRunId: 1, leadId: 1, tenantId: 1 }
        ).lean();

        if (existingSession?.firstMessageSentAt) {
            // Already fully dispatched on a prior run — skip WAHA + Retell.
            // Still refresh the Redis cache (it may have expired) so the
            // connector can continue routing replies correctly.
            logger.info('[ChatNodeDispatch] Idempotency: first message already sent, skipping re-send', {
                nodeRunId, leadId: lead._id.toString(), phone: lead.phone
            });
            successfulLeadIds.push(lead._id);
            cacheRefreshDocs.push({
                tenantId:        existingSession.tenantId,
                campaignId:      nodeRun.campaignId,
                campaignVersion: existingSession.campaignVersion,
                nodeId:          node.id,
                nodeRunId:       existingSession.nodeRunId,
                leadId:          existingSession.leadId,
                phone:           lead.phone,
                agentId:         existingSession.agentId,
                retellChatId:    existingSession.retellChatId
            });
            continue;
        }
        // ─────────────────────────────────────────────────────────────────

        // ── Campaign-level deduplication ──────────────────────────────────
        // Prevent duplicate active chats for the same phone across different nodes.
        // Check if this phone already has an active/pending chat elsewhere in the campaign.
        const supersededSession = await supersedeExistingCampaignChatSession({
            currentNodeRunId: nodeRunId,
            campaignId: nodeRun.campaignId,
            phone: lead.phone
        });

        if (supersededSession) {
            logger.warn('[ChatNodeDispatch] Superseded older campaign chat session before redispatch', {
                nodeRunId,
                leadId: lead._id.toString(),
                phone: lead.phone,
                existingNodeId: supersededSession.nodeId,
                existingNodeRunId: supersededSession.nodeRunId,
                existingStatus: supersededSession.status
            });
        }
        // ─────────────────────────────────────────────────────────────────

        // ── 6. Send WhatsApp message ──────────────────────────────────────
        let wahaMessageId = null;
        let wahaSendOk = false;
        try {
            const sendResult = await wahaClient.sendMessage({
                subaccountId: nodeRun.tenantId,
                phone: lead.phone,
                message: firstMessage
            });
            wahaMessageId = sendResult.messageId;
            wahaSendOk = sendResult.success;
        } catch (err) {
            logger.warn('[ChatNodeDispatch] WAHA send failed, skipping lead', {
                nodeRunId, leadId: lead._id.toString(), phone: lead.phone, error: err.message
            });
        }

        if (!wahaSendOk) {
            wahaSendFailed.push({ leadId: lead._id, phone: lead.phone, reason: 'WAHA send failed' });
            continue;
        }

        // ── 7. Create Retell chat session ────────────────────────────────
        let retellChatId = null;
        let retellError = null;
        for (let attempt = 1; attempt <= RETELL_CREATE_CHAT_MAX_RETRIES; attempt++) {
            try {
                const chatResult = await retellClient.createChat({
                    agentId: node.agentId,
                    dynamicVariables: {
                        phone_number: String(lead.phone),
                        agent_id:     String(node.agentId),
                        subaccount_id: String(nodeRun.tenantId),
                        subaccountId:  String(nodeRun.tenantId)
                    },
                    metadata: {
                        channel:        'whatsapp_campaign',
                        tenantId:       String(nodeRun.tenantId),
                        campaignId:     String(nodeRun.campaignId),
                        version:        String(nodeRun.campaignVersion),
                        nodeId:         String(node.id),
                        nodeRunId:      nodeRunId.toString(),
                        leadId:         lead._id.toString()
                    }
                });
                retellChatId = chatResult.chatId;
                retellError = null;
                break;
            } catch (err) {
                retellError = err;
                const isLastAttempt = attempt === RETELL_CREATE_CHAT_MAX_RETRIES;
                logger.warn('[ChatNodeDispatch] Retell createChat attempt failed', {
                    nodeRunId,
                    leadId: lead._id.toString(),
                    attempt,
                    maxAttempts: RETELL_CREATE_CHAT_MAX_RETRIES,
                    error: err.message
                });
                if (isNonRetryableRetellError(err)) {
                    logger.warn('[ChatNodeDispatch] Retell createChat non-retryable error, aborting retries', {
                        nodeRunId, leadId: lead._id.toString(), attempt, error: err.message
                    });
                    break;
                }
                if (!isLastAttempt && RETELL_CREATE_CHAT_RETRY_DELAY_MS > 0) {
                    await sleep(RETELL_CREATE_CHAT_RETRY_DELAY_MS);
                }
            }
        }

        if (!retellChatId) {
            // WAHA send already succeeded — the lead has the first message.
            // Do NOT mark them failed. Instead create a pending chat session so:
            //  (a) idempotency won't re-send WAHA on the next job replay
            //  (b) the Redis entry lets the connector detect this as a campaign lead
            //  (c) on the lead's first reply the connector creates the Retell chat lazily
            //  (d) the reconciliation worker will time it out if the lead never replies
            logger.warn('[ChatNodeDispatch] Retell createChat failed — preserving as pending session', {
                nodeRunId, leadId: lead._id.toString(), error: retellError?.message
            });
            successfulLeadIds.push(lead._id);
            sessionDocsToInsert.push({
                tenantId:           nodeRun.tenantId,
                campaignId:         nodeRun.campaignId,
                campaignVersion:    nodeRun.campaignVersion,
                nodeId:             node.id,
                nodeRunId,
                leadId:             lead._id,
                phone:              lead.phone,
                agentId:            node.agentId,
                retellChatId:       null,
                wahaSession:        `wa-${nodeRun.tenantId}`,
                status:             'pending',
                firstMessageSentAt: new Date(),
                lastActivityAt:     new Date()
            });
            continue;
        }

        // Collect for bulk operations
        successfulLeadIds.push(lead._id);
        sessionDocsToInsert.push({
            tenantId:        nodeRun.tenantId,
            campaignId:      nodeRun.campaignId,
            campaignVersion: nodeRun.campaignVersion,
            nodeId:          node.id,
            nodeRunId,
            leadId:          lead._id,
            phone:           lead.phone,
            agentId:         node.agentId,
            retellChatId,
            wahaSession:     `wa-${nodeRun.tenantId}`,
            status:          'active',
            firstMessageSentAt: new Date(),
            lastActivityAt:     new Date()
        });
    }

    // Record leads where WAHA/Retell failed
    if (wahaSendFailed.length > 0) {
        await recordAndRemoveInvalidLeads(nodeRunId, nodeRun.campaignId, wahaSendFailed);
    }

    if (successfulLeadIds.length === 0) {
        await CampaignNodeRun.findByIdAndUpdate(nodeRunId, {
            status: 'completed',
            totalLeads: leads.length,
            completedLeads: leads.length
        });
        logger.warn('[ChatNodeDispatch] All leads failed WAHA/Retell, chat node completed', { nodeRunId });
        return;
    }

    // 8a. Persist CampaignChatSession documents.
    // Primary path is insertMany for throughput.
    // If a legacy unique index (campaignId,nodeId,phone) exists in Mongo, a
    // new run can throw duplicate-key. In that case upsert by legacy key so
    // dispatch remains consistent instead of leaving partial state.
    try {
        await CampaignChatSession.insertMany(sessionDocsToInsert, {
            ordered: false,
            rawResult: true
        });
    } catch (err) {
        const duplicateKey = /E11000 duplicate key/.test(err?.message || '');
        const legacyIndex = /campaignId_1_nodeId_1_phone_1/.test(err?.message || '');

        if (duplicateKey && legacyIndex && sessionDocsToInsert.length > 0) {
            logger.warn('[ChatNodeDispatch] Legacy CampaignChatSession unique index detected, applying upsert fallback', {
                nodeRunId,
                error: err.message
            });

            const now = new Date();
            const fallbackOps = sessionDocsToInsert.map((doc) => ({
                updateOne: {
                    filter: {
                        campaignId: doc.campaignId,
                        nodeId: doc.nodeId,
                        phone: doc.phone
                    },
                    update: {
                        $set: {
                            tenantId: doc.tenantId,
                            campaignId: doc.campaignId,
                            campaignVersion: doc.campaignVersion,
                            nodeId: doc.nodeId,
                            nodeRunId: doc.nodeRunId,
                            leadId: doc.leadId,
                            phone: doc.phone,
                            agentId: doc.agentId,
                            retellChatId: doc.retellChatId,
                            wahaSession: doc.wahaSession,
                            status: doc.status,
                            firstMessageSentAt: doc.firstMessageSentAt,
                            lastActivityAt: doc.lastActivityAt,
                            updatedAt: now
                        },
                        $setOnInsert: {
                            createdAt: now
                        }
                    },
                    upsert: true
                }
            }));

            await CampaignChatSession.bulkWrite(fallbackOps, { ordered: false });
        } else {
            // insertMany with ordered:false throws if all docs fail; log and continue.
            logger.warn('[ChatNodeDispatch] Some CampaignChatSession docs failed to insert', {
                nodeRunId,
                error: err.message
            });
        }
    }

    // 8b. Write Redis cache entries so the connector-server can detect campaign chats.
    // Key: campaign:chat:active:{tenantId}:{phone}
    // Value: JSON with enough context for the connector to route the reply.
    // Both new sessions AND cache-refresh docs (idempotency replay) are written here.
    try {
        const allCacheDocs = [...sessionDocsToInsert, ...cacheRefreshDocs];
        const redisPipeline = connection.pipeline();
        for (const doc of allCacheDocs) {
            const payload = JSON.stringify({
                retellChatId:    doc.retellChatId,
                agentId:         doc.agentId,
                campaignId:      doc.campaignId,
                nodeId:          doc.nodeId,
                nodeRunId:       doc.nodeRunId.toString(),
                leadId:          doc.leadId.toString(),
                tenantId:        doc.tenantId,
                campaignVersion: doc.campaignVersion
            });

            for (const cacheKey of getCampaignChatCacheKeys(doc.tenantId, doc.phone)) {
                redisPipeline.set(cacheKey, payload, 'EX', CAMPAIGN_CHAT_CACHE_TTL_SECS);
            }
        }
        await redisPipeline.exec();
    } catch (redisErr) {
        // Non-fatal — the connector will just fall through to normal routing
        logger.warn('[ChatNodeDispatch] Failed to write Redis cache for campaign chat sessions', {
            nodeRunId, error: redisErr.message
        });
    }

    // 9. Mark successful leads as in_progress
    await Lead.updateMany(
        { _id: { $in: successfulLeadIds } },
        { $set: { nodeStatus: 'in_progress' } }
    );

    // 10. Activate CampaignNodeRun: totalLeads covers all original leads (same math as voice)
    await CampaignNodeRun.findByIdAndUpdate(nodeRunId, {
        status:      'active',
        totalLeads:  leads.length
    });

    // 11. Pre-create waiting_delay stubs for each outgoing edge
    const outgoingEdges = getOutgoingEdges(definition.workflowJson, node.id);
    if (outgoingEdges.length > 0) {
        const edgeOps = outgoingEdges.map(edge => {
            const delayMs = parseDelayToMs(edge.delay);
            const nextNode = getNode(definition.workflowJson, edge.toNodeId);
            return {
                updateOne: {
                    filter: {
                        campaignId:      nodeRun.campaignId,
                        campaignVersion: nodeRun.campaignVersion,
                        nodeId:          edge.toNodeId,
                        parentNodeId:    node.id,
                        sourceOutcome:   edge.outcome
                    },
                    update: {
                        $setOnInsert: {
                            tenantId:        nodeRun.tenantId,
                            campaignId:      nodeRun.campaignId,
                            campaignVersion: nodeRun.campaignVersion,
                            nodeId:          edge.toNodeId,
                            agentId:         nextNode?.agentId,
                            agentType:       nextNode?.agentType,
                            fromNumber:      nextNode?.fromNumber || null,
                            status:          'waiting_delay',
                            delayExpiresAt:  delayMs > 0 ? new Date(Date.now() + delayMs) : null,
                            parentNodeId:    node.id,
                            sourceOutcome:   edge.outcome
                        }
                    },
                    upsert: true
                }
            };
        });
        await CampaignNodeRun.bulkWrite(edgeOps, { ordered: false });
    }

    // 12. Safety-net reconciliation — much longer window than voice
    await queues.chatBatchReconcile.add(
        `chat-reconcile-${nodeRunId}`,
        { nodeRunId: nodeRunId.toString() },
        {
            jobId: `chat-reconcile-${nodeRunId}`,
            delay: CHAT_RECONCILE_DELAY_MS
        }
    );

    logger.info('[ChatNodeDispatch] Chat node dispatched', {
        nodeRunId,
        nodeId:      node.id,
        agentId:     node.agentId,
        totalLeads:  leads.length,
        sentCount:   successfulLeadIds.length,
        failedCount: preInvalid.length + wahaSendFailed.length
    });

}, { connection, prefix: BULL_PREFIX, concurrency: parseInt(process.env.WORKER_CONCURRENCY_CHAT_DISPATCH || '3') });

worker.on('failed', (job, error) => {
    logger.error('[ChatNodeDispatch] Job failed', {
        jobId:     job?.id,
        nodeRunId: job?.data?.nodeRunId,
        error:     error?.message
    });
});

module.exports = worker;
