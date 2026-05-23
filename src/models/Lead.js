const mongoose = require('mongoose');
const { Schema } = mongoose;

const LeadSchema = new Schema({
    tenantId: { type: String, required: true },
    phone: { type: String, required: true },
    email: { type: String },
    attrs: { type: Schema.Types.Mixed },
    retellAnalysis: { type: Schema.Types.Mixed },
    campaignId: { type: String },
    campaignVersion: { type: Number },
    currentNodeId: { type: String },
    outcome: { type: String },
    nodeStatus: {
        type: String,
        enum: ['pending', 'in_progress', 'completed', null],
        default: null
    },
    // Populated when the lead is currently inside a chat node.
    // Points to the active CampaignChatSession document.
    chatSessionId: { type: Schema.Types.ObjectId, ref: 'CampaignChatSession', default: null },
    createdAt: { type: Date, default: Date.now }
}, {
    timestamps: true
});

LeadSchema.index({ tenantId: 1, phone: 1 }, { unique: true });
LeadSchema.index({ campaignId: 1, currentNodeId: 1, outcome: 1, nodeStatus: 1 });
LeadSchema.index({ campaignId: 1, currentNodeId: 1, nodeStatus: 1 });

module.exports = mongoose.model('Lead', LeadSchema);
