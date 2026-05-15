const CampaignExecution = require('../models/CampaignExecution');
const CampaignNodeRun = require('../models/CampaignNodeRun');
const MultirunCampaign = require('../models/MultirunCampaign');

class MultirunAggregationService {
    static async refreshExecutionAndCampaign(tenantId, campaignId, executionId) {
        if (!executionId) {
            return null;
        }

        const execution = await CampaignExecution.findOne({ executionId, tenantId, campaignId }).lean();
        if (!execution) {
            return null;
        }

        return this.aggregateExecutionMetrics(executionId);
    }

    static async aggregateExecutionMetrics(executionId) {
        const execution = await CampaignExecution.findOne({ executionId });
        if (!execution) return null;

        const nodeRuns = await CampaignNodeRun.find({
            tenantId: execution.tenantId,
            campaignId: execution.campaignId,
            executionId
        }).lean();

        const byNode = nodeRuns.map((nodeRun) => {
            const successful = nodeRun?.outcomes?.successful || 0;
            const unsuccessful = nodeRun?.outcomes?.unsuccessful || 0;
            const failed = nodeRun?.outcomes?.failed || 0;
            const notAnswered = nodeRun?.outcomes?.not_answered || 0;

            return {
                nodeId: nodeRun.nodeId,
                contacted: nodeRun.completedLeads || 0,
                success: successful,
                failure: unsuccessful + failed,
                notAnswered
            };
        });

        const metrics = byNode.reduce((acc, node) => {
            acc.totalContacted += node.contacted;
            acc.totalSuccess += node.success;
            acc.totalFailure += node.failure;
            acc.byNode.push(node);
            return acc;
        }, {
            totalContacted: 0,
            totalSuccess: 0,
            totalFailure: 0,
            byNode: []
        });

        const activeNodeRuns = await CampaignNodeRun.countDocuments({
            tenantId: execution.tenantId,
            campaignId: execution.campaignId,
            executionId,
            status: { $in: ['waiting_delay', 'dispatching', 'active'] },
            totalLeads: { $gt: 0 }
        });

        const update = {
            metrics,
            updatedAt: new Date()
        };

        if (activeNodeRuns === 0 && ['pending', 'running'].includes(execution.status)) {
            update.status = 'completed';
            update.completedAt = new Date();
        }

        await CampaignExecution.updateOne({ executionId }, { $set: update });
        await this.aggregateCampaignCumulativeMetrics(execution.tenantId, execution.campaignId);

        return { executionId, metrics, activeNodeRuns };
    }

    static async aggregateCampaignCumulativeMetrics(tenantId, campaignId) {
        const executions = await CampaignExecution.find({ tenantId, campaignId }).lean();

        const cumulative = executions.reduce((acc, execution) => {
            const metrics = execution.metrics || {};
            acc.totalContacted += metrics.totalContacted || 0;
            acc.totalSuccess += metrics.totalSuccess || 0;
            acc.totalFailure += metrics.totalFailure || 0;

            for (const nodeMetric of (metrics.byNode || [])) {
                const key = nodeMetric.nodeId;
                const current = acc.byNodeMap.get(key) || {
                    nodeId: key,
                    contacted: 0,
                    success: 0,
                    failure: 0,
                    notAnswered: 0
                };
                current.contacted += nodeMetric.contacted || 0;
                current.success += nodeMetric.success || 0;
                current.failure += nodeMetric.failure || 0;
                current.notAnswered += nodeMetric.notAnswered || 0;
                acc.byNodeMap.set(key, current);
            }

            return acc;
        }, {
            totalContacted: 0,
            totalSuccess: 0,
            totalFailure: 0,
            byNodeMap: new Map()
        });

        const byNode = Array.from(cumulative.byNodeMap.values());

        await MultirunCampaign.updateOne(
            { tenantId, campaignId },
            {
                $set: {
                    cumulativeMetrics: {
                        totalContacted: cumulative.totalContacted,
                        totalSuccess: cumulative.totalSuccess,
                        totalFailure: cumulative.totalFailure,
                        byNode
                    },
                    updatedAt: new Date()
                }
            }
        );

        return {
            totalContacted: cumulative.totalContacted,
            totalSuccess: cumulative.totalSuccess,
            totalFailure: cumulative.totalFailure,
            byNode
        };
    }
}

module.exports = MultirunAggregationService;
