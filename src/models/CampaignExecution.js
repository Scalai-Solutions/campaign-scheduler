const mongoose = require('mongoose');
const { Schema } = mongoose;

const ExecutionNodeMetricSchema = new Schema({
    nodeId: { type: String, required: true },
    contacted: { type: Number, default: 0 },
    success: { type: Number, default: 0 },
    failure: { type: Number, default: 0 },
    notAnswered: { type: Number, default: 0 }
}, { _id: false });

const ExecutionMetricsSchema = new Schema({
    totalContacted: { type: Number, default: 0 },
    totalSuccess: { type: Number, default: 0 },
    totalFailure: { type: Number, default: 0 },
    byNode: { type: [ExecutionNodeMetricSchema], default: [] }
}, { _id: false });

const ExecutionSnapshotSchema = new Schema({
    filtersUsed: { type: Schema.Types.Mixed },
    sourceId: { type: String },
    fetchedCount: { type: Number, default: 0 },
    validCount: { type: Number, default: 0 },
    invalidCount: { type: Number, default: 0 },
    skippedCount: { type: Number, default: 0 }
}, { _id: false });

const CampaignExecutionSchema = new Schema({
    executionId: { type: String, required: true, unique: true },
    tenantId: { type: String, required: true },
    campaignId: { type: String, required: true },
    campaignVersion: { type: Number },
    scheduledAt: { type: Date },
    startedAt: { type: Date },
    completedAt: { type: Date },
    status: {
        type: String,
        enum: ['pending', 'running', 'completed', 'failed', 'cancelled'],
        default: 'pending'
    },
    snapshotMeta: { type: ExecutionSnapshotSchema, default: {} },
    metrics: { type: ExecutionMetricsSchema, default: {} }
}, {
    timestamps: true
});

CampaignExecutionSchema.index({ tenantId: 1, campaignId: 1 });
CampaignExecutionSchema.index({ campaignId: 1, scheduledAt: -1 });
CampaignExecutionSchema.index({ tenantId: 1, campaignId: 1, status: 1 });

module.exports = mongoose.model('CampaignExecution', CampaignExecutionSchema);
