const mongoose = require('mongoose');
const { Schema } = mongoose;

const CumulativeNodeSchema = new Schema({
    nodeId: { type: String, required: true },
    contacted: { type: Number, default: 0 },
    success: { type: Number, default: 0 },
    failure: { type: Number, default: 0 },
    notAnswered: { type: Number, default: 0 }
}, { _id: false });

const MultirunCampaignSchema = new Schema({
    tenantId: { type: String, required: true },
    campaignId: { type: String, required: true },
    campaignType: { type: String, enum: ['multirun'], default: 'multirun' },
    campaignVersion: { type: Number, default: 1 },
    multirunConfig: { type: Schema.Types.Mixed, default: null },
    pipelineConfig: { type: Schema.Types.Mixed, default: null },
    isLive: { type: Boolean, default: true },
    status: { type: String, default: 'saved' },
    lastRunAt: { type: Date, default: null },
    nextRunAt: { type: Date, default: null },
    stoppedAt: { type: Date, default: null },
    stoppedBy: { type: String, default: null },
    audienceCursor: { type: String, default: null },
    cumulativeMetrics: {
        totalContacted: { type: Number, default: 0 },
        totalSuccess: { type: Number, default: 0 },
        totalFailure: { type: Number, default: 0 },
        byNode: { type: [CumulativeNodeSchema], default: [] }
    }
}, {
    collection: 'multiruncampaigns',
    timestamps: true
});

MultirunCampaignSchema.index({ tenantId: 1, campaignId: 1 }, { unique: true });
MultirunCampaignSchema.index({ isLive: 1, nextRunAt: 1 });
MultirunCampaignSchema.index({ tenantId: 1, isLive: 1, nextRunAt: 1 });

module.exports = mongoose.model('MultirunCampaign', MultirunCampaignSchema);
