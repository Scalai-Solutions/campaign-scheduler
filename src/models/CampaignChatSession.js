const mongoose = require('mongoose');
const { Schema } = mongoose;

/**
 * CampaignChatSession
 *
 * One document per lead per chat node in a campaign run.
 * Maps a phone number to an active Retell chat so that incoming
 * WhatsApp replies can be routed directly to the correct chat agent
 * without going through the general confidence-scoring router.
 *
 * Lifecycle: pending → active → completed | timeout
 */
const CampaignChatSessionSchema = new Schema({
    tenantId:     { type: String, required: true },
    campaignId:   { type: String, required: true },
    campaignVersion: { type: Number, required: true },
    nodeId:       { type: String, required: true },
    nodeRunId:    { type: Schema.Types.ObjectId, required: true },
    leadId:       { type: Schema.Types.ObjectId, required: true },
    phone:        { type: String, required: true },      // normalised E.164 without '+'
    agentId:      { type: String, required: true },      // Retell chat agent id
    retellChatId: { type: String },                      // set after Retell chat is created
    wahaSession:  { type: String },                      // WAHA session name, e.g. "wa-{subaccountId}"
    status: {
        type: String,
        enum: ['pending', 'active', 'completed', 'timeout', 'failed'],
        default: 'pending'
    },
    outcome:      { type: String },                      // successful | unsuccessful | not_answered (set on completion)
    chatAnalysis: { type: Schema.Types.Mixed },          // Retell post-chat analysis object
    firstMessageSentAt: { type: Date },
    lastActivityAt:     { type: Date },
}, {
    timestamps: true
});

// Per-run uniqueness: one chat session per (campaign, node, run, phone).
CampaignChatSessionSchema.index(
    { campaignId: 1, nodeId: 1, nodeRunId: 1, phone: 1 },
    { unique: true }
);
// Campaign-wide lookup used by connectors to detect campaign chats
CampaignChatSessionSchema.index({ tenantId: 1, phone: 1, status: 1 });
// Retell chat_id → session (for webhook correlation)
CampaignChatSessionSchema.index({ retellChatId: 1 }, { unique: true, sparse: true });
// Reconciliation sweep: find active sessions older than threshold
CampaignChatSessionSchema.index({ status: 1, lastActivityAt: 1 });
// Node-level bulk queries
CampaignChatSessionSchema.index({ nodeRunId: 1, status: 1 });

module.exports = mongoose.model('CampaignChatSession', CampaignChatSessionSchema);
