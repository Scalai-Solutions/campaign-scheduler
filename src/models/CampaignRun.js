const mongoose = require('mongoose');
const { Schema } = mongoose;

const CampaignRunSchema = new Schema({
    tenantId: { type: String, required: true },
    campaignId: { type: String, required: true },
    campaignVersion: { type: Number, required: true },
    leadId: { type: Schema.Types.ObjectId, ref: 'Lead', required: true },
    status: {
        type: String,
        enum: ['running', 'paused', 'completed', 'cancelled'],
        default: 'running'
    },
    currentNodeId: { type: String },
    currentNodeStatus: { type: String },
    agentStatus: { type: String },
    lastStepOutcome: { type: String },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
}, {
    timestamps: true
});

CampaignRunSchema.index({ tenantId: 1, campaignId: 1, campaignVersion: 1, leadId: 1 }, { unique: true });

module.exports = mongoose.model('CampaignRun', CampaignRunSchema);
