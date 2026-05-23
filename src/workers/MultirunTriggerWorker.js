const { Worker } = require('bullmq');
const { v4: uuidv4 } = require('uuid');

const CampaignDefinition = require('../models/CampaignDefinition');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const CampaignExecution = require('../models/CampaignExecution');
const MultirunCampaign = require('../models/MultirunCampaign');
const Lead = require('../models/Lead');
const RetellEvent = require('../models/RetellEvent');
const CampaignChatSession = require('../models/CampaignChatSession');

const hubspotAudienceResolver = require('../services/hubspotAudienceResolver');
const multirunAggregationService = require('../services/multirunAggregationService');
const { connection, queues, BULL_PREFIX, QUEUE_NAMES } = require('../queues');
const logger = require('../utils/logger');

const DATABASE_SERVER_URL = process.env.DATABASE_SERVER_URL || 'http://database-server:3000';

function getTimeZoneOffsetMinutes(date, timeZone) {
    try {
        const parts = new Intl.DateTimeFormat('en-US', {
            timeZone,
            timeZoneName: 'shortOffset'
        }).formatToParts(date);

        const zone = parts.find((part) => part.type === 'timeZoneName')?.value || 'GMT+0';
        const match = zone.match(/GMT([+-])(\d{1,2})(?::?(\d{2}))?/i);
        if (!match) return 0;

        const sign = match[1] === '-' ? -1 : 1;
        const hours = parseInt(match[2], 10) || 0;
        const minutes = parseInt(match[3] || '0', 10) || 0;
        return sign * (hours * 60 + minutes);
    } catch {
        return 0;
    }
}

function isValidIanaTimezone(timeZone) {
    if (!timeZone || typeof timeZone !== 'string') return false;
    try {
        new Intl.DateTimeFormat('en-US', { timeZone }).format(new Date());
        return true;
    } catch {
        return false;
    }
}

function getZonedDateParts(date, timeZone) {
    const parts = new Intl.DateTimeFormat('en-GB', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    }).formatToParts(date);

    const get = (type) => parseInt(parts.find((part) => part.type === type)?.value || '0', 10);
    return {
        year: get('year'),
        month: get('month'),
        day: get('day')
    };
}

function zonedTimeToUtcDate({ year, month, day, hour, minute }, timeZone) {
    const guess = new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
    const offsetMinutes = getTimeZoneOffsetMinutes(guess, timeZone);
    return new Date(guess.getTime() - offsetMinutes * 60 * 1000);
}

function computeNextRunAt(multirunConfig) {
    if (!multirunConfig) return null;
    const timezone = isValidIanaTimezone(multirunConfig.timezone)
        ? multirunConfig.timezone
        : 'Europe/Madrid';
    const runsPerDay = Math.max(1, Math.min(3, Number(multirunConfig.runsPerDay || 1)));

    const dailyRunTimes = [...new Set(
        Array.isArray(multirunConfig.dailyRunTimes) ? multirunConfig.dailyRunTimes : []
    )]
        .map((value) => String(value).trim())
        .filter((value) => /^([01]\d|2[0-3]):[0-5]\d$/.test(value))
        .sort()
        .slice(0, runsPerDay);

    if (dailyRunTimes.length === 0) return null;

    const now = new Date();
    const nowParts = getZonedDateParts(now, timezone);

    const candidates = [];
    const sortedTimes = [...dailyRunTimes].sort();

    for (const dayOffset of [0, 1, 2]) {
        for (const hhmm of sortedTimes) {
            const [hourStr, minuteStr] = String(hhmm).split(':');
            const hour = parseInt(hourStr, 10);
            const minute = parseInt(minuteStr, 10);
            if (!Number.isInteger(hour) || !Number.isInteger(minute)) continue;

            const local = zonedTimeToUtcDate({
                year: nowParts.year,
                month: nowParts.month,
                day: nowParts.day,
                hour,
                minute
            }, timezone);

            candidates.push(new Date(local.getTime() + dayOffset * 24 * 60 * 60 * 1000));
        }
    }

    return candidates.filter((candidate) => candidate > now).sort((a, b) => a.getTime() - b.getTime())[0] || null;
}

async function notifyCampaignLifecycle(tenantId, campaignId, payload) {
    try {
        const response = await fetch(`${DATABASE_SERVER_URL}/internal/campaigns/${campaignId}/multirun-lifecycle`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tenantId, ...payload })
        });

        if (!response.ok) {
            const body = await response.text();
            throw new Error(`Lifecycle update failed (${response.status}): ${body}`);
        }
    } catch (error) {
        logger.warn('[MultirunTrigger] Failed lifecycle sync to database-server', {
            tenantId,
            campaignId,
            error: error.message
        });
    }
}

const worker = new Worker(QUEUE_NAMES.multirunTrigger, async (job) => {
    const { tenantId, campaignId } = job.data;
    const now = new Date();

    const multirunCampaign = await MultirunCampaign.findOne({ tenantId, campaignId, isLive: true });
    if (!multirunCampaign) {
        logger.warn('[MultirunTrigger] Campaign not live or missing', { tenantId, campaignId });
        return;
    }

    const activeNodeRuns = await CampaignNodeRun.countDocuments({
        tenantId,
        campaignId,
        status: { $in: ['waiting_delay', 'dispatching', 'active'] },
        totalLeads: { $gt: 0 }
    });

    if (activeNodeRuns > 0) {
        logger.info('[MultirunTrigger] Existing active run detected, skipping overlapping launch', {
            tenantId,
            campaignId,
            activeNodeRuns
        });

        const nextRunAt = computeNextRunAt(multirunCampaign.multirunConfig);
        await MultirunCampaign.updateOne(
            { tenantId, campaignId },
            { $set: { nextRunAt, updatedAt: new Date(), status: 'running' } }
        );

        await notifyCampaignLifecycle(tenantId, campaignId, {
            status: 'running',
            isLive: true,
            nextRunAt,
            stoppedAt: null,
            stoppedBy: null
        });
        return;
    }

    const definition = await CampaignDefinition.findOne({ tenantId, campaignId }).sort({ version: -1 });
    if (!definition) {
        logger.error('[MultirunTrigger] CampaignDefinition missing — stopping campaign', { tenantId, campaignId });
        await MultirunCampaign.updateOne(
            { tenantId, campaignId },
            { $set: { isLive: false, status: 'failed', nextRunAt: null, updatedAt: new Date() } }
        );
        await notifyCampaignLifecycle(tenantId, campaignId, {
            status: 'failed',
            isLive: false,
            nextRunAt: null
        });
        return;
    }

    const workflow = definition.workflowJson || {};
    const entryNodeId = workflow.entryNodeId || workflow.nodes?.[0]?.id;
    const entryNode = (workflow.nodes || []).find((node) => node.id === entryNodeId);
    if (!entryNodeId || !entryNode) {
        logger.error('[MultirunTrigger] Workflow entry node missing — stopping campaign', { tenantId, campaignId });
        await MultirunCampaign.updateOne(
            { tenantId, campaignId },
            { $set: { isLive: false, status: 'failed', nextRunAt: null, updatedAt: new Date() } }
        );
        await notifyCampaignLifecycle(tenantId, campaignId, {
            status: 'failed',
            isLive: false,
            nextRunAt: null
        });
        return;
    }

    const { leads, snapshot } = await hubspotAudienceResolver.resolveAudience(
        tenantId,
        multirunCampaign.pipelineConfig || {}
    );

    const configuredLeadsPerRun = Number(multirunCampaign.multirunConfig?.leadsPerRun || 200);
    const leadsPerRun = Math.max(1, Math.min(200, configuredLeadsPerRun));
    const executionLeads = leads.slice(0, leadsPerRun);

    const executionId = uuidv4();
    await CampaignExecution.create({
        executionId,
        tenantId,
        campaignId,
        campaignVersion: definition.version,
        scheduledAt: now,
        startedAt: now,
        status: 'running',
        snapshotMeta: {
            ...snapshot,
            leadsPerRun,
            selectedLeadsCount: executionLeads.length,
            totalResolvedLeads: Array.isArray(leads) ? leads.length : 0
        }
    });

    await Promise.all([
        CampaignNodeRun.deleteMany({ tenantId, campaignId }),
        Lead.updateMany(
            { tenantId, campaignId },
            {
                $unset: {
                    campaignId: '',
                    campaignVersion: '',
                    currentNodeId: '',
                    outcome: '',
                    nodeStatus: '',
                    retellAnalysis: '',
                    chatSessionId: ''
                }
            }
        ),
        RetellEvent.deleteMany({
            $or: [
                { 'payloadJson.call.metadata.campaignId': campaignId },
                { 'payloadJson.metadata.campaignId': campaignId }
            ]
        }),
        CampaignChatSession.deleteMany({ tenantId, campaignId })
    ]);

    const leadByPhone = new Map();
    for (const lead of executionLeads) {
        if (!leadByPhone.has(lead.phone)) {
            leadByPhone.set(lead.phone, lead);
        }
    }
    const uniquePhones = [...leadByPhone.keys()];

    if (uniquePhones.length === 0) {
        const nextRunAt = computeNextRunAt(multirunCampaign.multirunConfig);

        await CampaignExecution.updateOne(
            { executionId },
            {
                $set: {
                    status: 'completed',
                    completedAt: new Date(),
                    metrics: {
                        totalContacted: 0,
                        totalSuccess: 0,
                        totalFailure: 0,
                        byNode: []
                    }
                }
            }
        );

        await MultirunCampaign.updateOne(
            { tenantId, campaignId },
            {
                $set: {
                    status: 'saved',
                    campaignVersion: definition.version,
                    lastRunAt: now,
                    nextRunAt,
                    updatedAt: new Date()
                }
            }
        );

        await notifyCampaignLifecycle(tenantId, campaignId, {
            status: 'saved',
            lastRunAt: now,
            nextRunAt,
            isLive: true
        });

        logger.info('[MultirunTrigger] Execution completed with zero leads', {
            tenantId,
            campaignId,
            executionId
        });
        return;
    }

    await Lead.bulkWrite(
        uniquePhones.map((phone) => {
            const sourceLead = leadByPhone.get(phone) || {};
            const source = sourceLead.source || {};
            const setFields = {
                campaignId,
                campaignVersion: definition.version,
                currentNodeId: entryNodeId,
                nodeStatus: 'pending',
                outcome: null
            };

            if (source.provider === 'hubspot') {
                const isContactRecord = source.objectTypeName === 'contacts' || source.objectTypeId === '0-1';
                setFields['attrs.hubspot'] = {
                    provider: 'hubspot',
                    mode: source.mode,
                    recordId: source.recordId || sourceLead.sourceRecordId || null,
                    contactId: isContactRecord ? (source.recordId || sourceLead.sourceRecordId || null) : null,
                    listId: source.listId || null,
                    objectTypeId: source.objectTypeId || null,
                    objectTypeName: source.objectTypeName || null
                };
            }

            return {
                updateOne: {
                    filter: { tenantId, phone },
                    update: {
                        $set: setFields,
                        $setOnInsert: { tenantId, phone }
                    },
                    upsert: true
                }
            };
        }),
        { ordered: false }
    );

    const nodeRun = await CampaignNodeRun.findOneAndUpdate(
        {
            campaignId,
            campaignVersion: definition.version,
            nodeId: entryNodeId,
            parentNodeId: null,
            sourceOutcome: null
        },
        {
            $set: {
                tenantId,
                campaignId,
                campaignVersion: definition.version,
                executionId,
                nodeId: entryNodeId,
                agentId: entryNode.agentId,
                agentType: entryNode.agentType || 'voice',
                fromNumber: entryNode.fromNumber || null,
                parentNodeId: null,
                sourceOutcome: null,
                status: 'dispatching',
                totalLeads: uniquePhones.length,
                completedLeads: 0,
                outcomes: { successful: 0, unsuccessful: 0, not_answered: 0, failed: 0 },
                updatedAt: new Date()
            },
            $setOnInsert: {
                createdAt: new Date()
            }
        },
        { upsert: true, new: true }
    );

    const dispatchQueue = (entryNode.agentType || 'voice') === 'chat'
        ? queues.chatNodeDispatch
        : queues.campaignNodeDispatch;

    await dispatchQueue.add(
        `dispatch-${nodeRun._id}`,
        { nodeRunId: nodeRun._id.toString(), executionId },
        { jobId: `node-dispatch-${nodeRun._id}` }
    );

    const nextRunAt = computeNextRunAt(multirunCampaign.multirunConfig);

    await MultirunCampaign.updateOne(
        { tenantId, campaignId },
        {
            $set: {
                status: 'running',
                campaignVersion: definition.version,
                lastRunAt: now,
                nextRunAt,
                updatedAt: new Date()
            }
        }
    );

    await multirunAggregationService.refreshExecutionAndCampaign(
        tenantId,
        campaignId,
        executionId
    );

    await notifyCampaignLifecycle(tenantId, campaignId, {
        status: 'running',
        isLive: true,
        lastRunAt: now,
        nextRunAt,
        stoppedAt: null,
        stoppedBy: null
    });

    logger.info('[MultirunTrigger] Execution launched', {
        tenantId,
        campaignId,
        executionId,
        leads: uniquePhones.length,
        nodeRunId: nodeRun._id.toString()
    });
}, {
    connection,
    prefix: BULL_PREFIX,
    concurrency: parseInt(process.env.WORKER_CONCURRENCY_MULTIRUN_TRIGGER || '2', 10)
});

worker.on('failed', async (job, error) => {
    logger.error('[MultirunTrigger] Job failed', {
        jobId: job?.id,
        campaignId: job?.data?.campaignId,
        tenantId: job?.data?.tenantId,
        error: error?.message
    });

    if (job?.data?.tenantId && job?.data?.campaignId) {
        // Stop the campaign so the scheduler does not requeue it indefinitely.
        // An operator can re-enable it after fixing the underlying problem.
        await MultirunCampaign.updateOne(
            { tenantId: job.data.tenantId, campaignId: job.data.campaignId },
            {
                $set: {
                    isLive: false,
                    nextRunAt: null,
                    status: 'failed',
                    updatedAt: new Date()
                }
            }
        );
        await notifyCampaignLifecycle(job.data.tenantId, job.data.campaignId, {
            status: 'failed',
            isLive: false,
            nextRunAt: null
        }).catch(() => {});
    }
});

module.exports = worker;
