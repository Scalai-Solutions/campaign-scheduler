const mongoose = require('mongoose');
const { Schema } = mongoose;

const CampaignDefinitionSchema = new Schema({
    tenantId: { type: String, required: true },
    campaignId: { type: String, required: true },
    version: { type: Number, required: true },
    workflowJson: { type: Schema.Types.Mixed, required: true },
    createdAt: { type: Date, default: Date.now }
}, {
    timestamps: true
});

CampaignDefinitionSchema.index({ tenantId: 1, campaignId: 1, version: 1 }, { unique: true });

module.exports = mongoose.model('CampaignDefinition', CampaignDefinitionSchema);
