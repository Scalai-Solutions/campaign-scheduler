const mongoose = require('mongoose');
const { Schema } = mongoose;

const ScheduledTaskSchema = new Schema({
    tenantId: { type: String, required: true },
    runId: { type: Schema.Types.ObjectId, ref: 'CampaignRun', required: true },
    leadId: { type: Schema.Types.ObjectId, ref: 'Lead', required: true },
    nodeId: { type: String, required: true },
    dueAt: { type: Date, required: true },
    status: {
        type: String,
        enum: ['scheduled', 'leased', 'done', 'failed'],
        default: 'scheduled'
    },
    leaseUntil: { type: Date },
    leasedBy: { type: String },
    attempt: { type: Number, default: 1 },
    maxAttempts: { type: Number, default: 3 },
    dedupeKey: { type: String, required: true, unique: true }
}, {
    timestamps: true
});

ScheduledTaskSchema.index({ status: 1, dueAt: 1 });
ScheduledTaskSchema.index({ leaseUntil: 1 });
ScheduledTaskSchema.index({ dedupeKey: 1 }, { unique: true });

module.exports = mongoose.model('ScheduledTask', ScheduledTaskSchema);
