const mongoose = require('mongoose');
const { Schema } = mongoose;

const NextStepIntentSchema = new Schema({
    // Tenant/Campaign identifiers
    tenantId: { type: String, required: true },
    campaignId: { type: String, required: true },
    campaignVersion: { type: Number, required: true },
    runId: { type: Schema.Types.ObjectId, ref: 'CampaignRun', required: true },
    leadId: { type: Schema.Types.ObjectId, ref: 'Lead', required: true },

    // Current execution state (from completed node)
    currentNodeId: { type: String, required: true },
    currentStepExecutionId: { type: Schema.Types.ObjectId, ref: 'StepExecution', required: true },
    currentOutcome: {
        type: String,
        enum: ['successful', 'unsuccessful', 'not_answered'],
        required: true
    },
    completedAt: { type: Date, required: true },

    // Resolved next transition
    nextNodeId: { type: String, required: true },
    nextNodeAgentId: { type: String, required: true },
    nextNodeAgentType: {
        type: String,
        enum: ['voice', 'chat'],
        required: true
    },
    resolvedDelay: {
        type: String,
        default: null // e.g., '1h', '2d', null for immediate
    },
    dispatchConfigHash: {
        type: String,
        required: true // 8-char hash of dispatch policy + agent config
    },

    // Dispatch timing
    dispatchTime: {
        type: Date,
        required: true // When to dispatch (now for immediate, future for scheduled)
    },

    // Batching compatibility key (for strict grouping)
    batchCompatibilityKey: {
        type: String,
        required: true,
        index: true // For efficient grouping queries
    },

    // Batch assignment
    batchDispatchId: {
        type: Schema.Types.ObjectId,
        ref: 'BatchDispatch',
        default: null
    },
    batchPosition: {
        type: Number,
        default: null // Position in batch (for debugging)
    },

    // Status tracking (fine-grained lifecycle)
    status: {
        type: String,
        enum: [
            'pending_scheduled',      // Waiting until dispatchTime before entering aggregation
            'pending_aggregation',    // Created, waiting for batch formation
            'batched',                // Assigned to a BatchDispatch
            'dispatching',            // Batch is sending to Retell
            'dispatch_sent',          // Successfully sent to Retell
            'dispatch_failed',        // Failed to send (will be retried)
            'pending_retry',          // Waiting for retry after reconciliation failure
            'awaiting_result',        // Waiting for call result/webhook
            'completed',              // Final outcome received
            'failed_max_retries',     // Retry limit exceeded
            'timeout',                // Timed out waiting for final outcome
            'failed'                  // Unrecoverable failure
        ],
        default: 'pending_aggregation',
        index: true
    },

    // Retry tracking
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

    // Outcome (once completed)
    outcome: {
        type: String,
        enum: ['successful', 'unsuccessful', 'not_answered', 'failed', 'timeout'],
        default: null
    },

    // Observability
    createdAt: {
        type: Date,
        default: Date.now
    },
    updatedAt: {
        type: Date,
        default: Date.now
    },
    aggregatedAt: {
        type: Date,
        default: null // When assigned to batch
    },
    dispatchedAt: {
        type: Date,
        default: null // When batch was sent to Retell
    },
    completedAt: {
        type: Date,
        default: null // When webhook outcome received
    },

    // Metadata for debugging and tracing
    metadata: {
        intentDedupeKey: String, // For idempotency check
        createdAtMs: Number,
        aggregationReason: String, // size_threshold, time_flush, scheduled_flush
        retellBatchCallId: String,
        callId: String,
        disconnectionReason: String,
        callAnalysis: Schema.Types.Mixed,
        dispatchAttempt: Number,
        retryBatchId: String, // If this intent was retried as part of partial batch
        retryHistory: [{
            reason: String,
            attemptNumber: Number,
            timestamp: Date,
            error: String
        }]
    }
}, {
    timestamps: true
});

// Indexes
NextStepIntentSchema.index({ tenantId: 1, status: 1 });
NextStepIntentSchema.index({ batchCompatibilityKey: 1, status: 1 });
NextStepIntentSchema.index({ tenantId: 1, dispatchTime: 1, status: 1 });
NextStepIntentSchema.index({ batchDispatchId: 1 });
NextStepIntentSchema.index({ 'metadata.intentDedupeKey': 1 }, { sparse: true }); // Idempotency
NextStepIntentSchema.index({ tenantId: 1, campaignId: 1, leadId: 1 }); // For per-lead tracing
NextStepIntentSchema.index({ nextRetryAt: 1 }, { sparse: true }); // For retry polling

module.exports = mongoose.model('NextStepIntent', NextStepIntentSchema);
