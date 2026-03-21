const mongoose = require('mongoose');
const { Schema } = mongoose;

const BatchDispatchSchema = new Schema({
    // Tenant/Campaign identifiers
    tenantId: { type: String, required: true },
    campaignId: { type: String, required: true },
    campaignVersion: { type: Number, required: true },

    // Batch composition
    batchCompatibilityKey: {
        type: String,
        required: true,
        index: true
    },
    nextNodeId: { type: String, required: true },
    nextNodeAgentId: { type: String, required: true },
    nextNodeAgentType: {
        type: String,
        enum: ['voice', 'chat'],
        required: true
    },

    // Members (array of per-lead intent IDs)
    nextStepIntentIds: [{
        type: Schema.Types.ObjectId,
        ref: 'NextStepIntent'
    }],
    leadCount: {
        type: Number,
        required: true
    },

    // Dispatch timing
    createdAt: {
        type: Date,
        default: Date.now,
        index: true
    },
    scheduledDispatchTime: {
        type: Date,
        default: null
    },
    actualDispatchTime: {
        type: Date,
        default: null
    },

    // Retell integration
    retellBatchCallId: {
        type: String,
        default: null,
        index: true
    },
    retellBatchCallMetadata: {
        type: Schema.Types.Mixed,
        default: null
    },

    // Status lifecycle
    status: {
        type: String,
        enum: [
            'pending',           // Waiting for batch formation / flush
            'scheduled',         // Will dispatch at future time
            'dispatching',       // Currently sending to Retell
            'sent',              // Successfully sent to Retell
            'partial_failed',    // Some leads failed, some succeeded
            'all_failed',        // All leads failed
            'awaiting_results',  // Sent to Retell, waiting for webhooks
            'completed',         // All leads have final outcomes
            'permanently_failed' // Unrecoverable (max retries exceeded)
        ],
        default: 'pending',
        index: true
    },

    // Retry state
    retryCount: {
        type: Number,
        default: 0
    },
    lastRetryAt: {
        type: Date,
        default: null
    },
    nextRetryAt: {
        type: Date,
        default: null
    },

    // Per-lead failure tracking (for partial retry)
    failureReasonsByLead: {
        type: Map,
        of: String,
        default: new Map()
    },

    // Metrics/Observability
    metrics: {
        timeToBatch: Number,        // ms: created until dispatch sent
        timeToResult: Number,       // ms: dispatched until final outcomes
        successCount: { type: Number, default: 0 },
        failureCount: { type: Number, default: 0 },
        notAnsweredCount: { type: Number, default: 0 },
        pendingCount: { type: Number, default: 0 },
        completedAt: Date
    },

    // Trace info
    createdBy: {
        type: String,
        enum: ['transition_aggregator', 'scheduler', 'manual_retry', 'batch_retry'],
        default: 'transition_aggregator'
    },

    // Metadata
    metadata: {
        dedupeKey: String, // For batch idempotency
        taskCount: Number,
        reason: String, // immediate_flush, time_threshold, size_threshold, etc.
        aggregationReason: String,
        batchDedupeKey: String, // 8-char hash of sorted intent IDs
        parentBatchId: String, // If this is a retry batch
        retellError: String,
        timeoutAt: Date,
        partialReason: String, // webhook_timeout_10m, staggered_completion
        createdAtMs: Number
    }
}, {
    timestamps: true
});

// Indexes
BatchDispatchSchema.index({ tenantId: 1, status: 1 });
BatchDispatchSchema.index({ batchCompatibilityKey: 1, status: 1 });
BatchDispatchSchema.index({ retellBatchCallId: 1 }, { sparse: true });
BatchDispatchSchema.index({ createdAt: 1 }); // For cleanup queries
BatchDispatchSchema.index({ status: 1, nextRetryAt: 1 }, { sparse: true }); // For retry polling

module.exports = mongoose.model('BatchDispatch', BatchDispatchSchema);
