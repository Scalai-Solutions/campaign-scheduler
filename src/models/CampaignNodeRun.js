const mongoose = require('mongoose');
const { Schema } = mongoose;

const CampaignNodeRunSchema = new Schema({
    tenantId: { type: String, required: true },
    campaignId: { type: String, required: true },
    campaignVersion: { type: Number, required: true },
    nodeId: { type: String, required: true },
    agentId: { type: String },
    agentType: { type: String, enum: ['voice', 'chat'] },
    fromNumber: { type: String },
    status: {
        type: String,
        enum: ['waiting_delay', 'dispatching', 'active', 'completed', 'cancelled'],
        default: 'dispatching'
    },
    totalLeads: { type: Number, default: 0 },
    completedLeads: { type: Number, default: 0 },
    outcomes: {
        successful: { type: Number, default: 0 },
        unsuccessful: { type: Number, default: 0 },
        not_answered: { type: Number, default: 0 },
        failed: { type: Number, default: 0 }
    },
    failedLeads: [{
        leadId: { type: Schema.Types.ObjectId, ref: 'Lead', required: true },
        phone: { type: String },
        reason: { type: String, required: true },
        failedAt: { type: Date, default: Date.now }
    }],
    batchCallId: { type: String },
    delayExpiresAt: { type: Date },
    sourceOutcome: { type: String },
    parentNodeId: { type: String }
}, {
    timestamps: true
});

CampaignNodeRunSchema.index(
    { campaignId: 1, campaignVersion: 1, nodeId: 1, parentNodeId: 1, sourceOutcome: 1 },
    { unique: true, partialFilterExpression: { parentNodeId: { $exists: true } } }
);
CampaignNodeRunSchema.index({ status: 1, delayExpiresAt: 1 });
CampaignNodeRunSchema.index({ tenantId: 1, campaignId: 1 });

module.exports = mongoose.model('CampaignNodeRun', CampaignNodeRunSchema);
