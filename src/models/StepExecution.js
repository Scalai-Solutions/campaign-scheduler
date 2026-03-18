const mongoose = require('mongoose');
const { Schema } = mongoose;

const StepExecutionSchema = new Schema({
    tenantId: { type: String, required: true },
    runId: { type: Schema.Types.ObjectId, ref: 'CampaignRun', required: true },
    leadId: { type: Schema.Types.ObjectId, ref: 'Lead', required: true },
    nodeId: { type: String, required: true },
    agentId: { type: String, required: true },
    agentType: { type: String, enum: ['voice', 'chat'], required: true },
    status: {
        type: String,
        enum: ['pending', 'queued', 'waiting_result', 'completed', 'failed', 'timeout'],
        default: 'pending'
    },
    attempt: { type: Number, default: 1 },
    outcome: { type: String },
    retell: {
        batchCallId: { type: String },
        callId: { type: String }
    },
    startedAt: { type: Date },
    endedAt: { type: Date },
    dedupeKey: { type: String, required: true, unique: true }
}, {
    timestamps: true
});

StepExecutionSchema.index({ dedupeKey: 1 }, { unique: true });
StepExecutionSchema.index({ 'retell.batchCallId': 1 });
StepExecutionSchema.index({ 'retell.callId': 1 });
StepExecutionSchema.index({ tenantId: 1, runId: 1, nodeId: 1 });

module.exports = mongoose.model('StepExecution', StepExecutionSchema);
