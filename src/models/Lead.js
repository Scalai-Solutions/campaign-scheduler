const mongoose = require('mongoose');
const { Schema } = mongoose;

const LeadSchema = new Schema({
    tenantId: { type: String, required: true },
    phone: { type: String, required: true },
    email: { type: String },
    attrs: { type: Schema.Types.Mixed },
    createdAt: { type: Date, default: Date.now }
}, {
    timestamps: true
});

LeadSchema.index({ tenantId: 1, phone: 1 }, { unique: true });

module.exports = mongoose.model('Lead', LeadSchema);
