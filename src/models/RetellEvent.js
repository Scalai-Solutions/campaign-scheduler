const mongoose = require('mongoose');
const { Schema } = mongoose;

const RetellEventSchema = new Schema({
    provider: { type: String, default: 'retell' },
    externalEventId: { type: String },
    externalRef: { type: String }, // e.g. callId or batchCallId
    payloadJson: { type: Schema.Types.Mixed, required: true },
    receivedAt: { type: Date, default: Date.now },
    processedAt: { type: Date },
    status: {
        type: String,
        enum: ['received', 'processed', 'failed'],
        default: 'received'
    }
}, {
    timestamps: true
});

RetellEventSchema.index({ provider: 1, externalEventId: 1 }, { unique: true, sparse: true });

module.exports = mongoose.model('RetellEvent', RetellEventSchema);
