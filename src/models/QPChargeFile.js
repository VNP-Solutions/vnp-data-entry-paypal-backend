const mongoose = require("mongoose");

const qpChargeFileSchema = new mongoose.Schema(
  {
    file_name: {
      type: String,
      required: true,
    },
    file_type: {
      type: String,
      enum: ["XLSX", "CSV"],
    },
    storage_path: {
      type: String,
      required: true,
    },
    status: {
      type: String,
      enum: [
        "IMPORTED",
        "QUEUED",
        "PROCESSING",
        "PAUSED",
        "COMPLETED",
        "COMPLETED_WITH_ERRORS",
        "FAILED",
        "CANCELLED",
      ],
      default: "IMPORTED",
    },
    queue_order: { type: Number, default: null, index: true },
    queued_at: { type: Date, default: null },
    total_rows: { type: Number, default: 0 },
    valid_rows: { type: Number, default: 0 },
    invalid_rows: { type: Number, default: 0 },
    processed_rows: { type: Number, default: 0 },
    success_count: { type: Number, default: 0 },
    declined_count: { type: Number, default: 0 },
    error_count: { type: Number, default: 0 },
    skipped_count: { type: Number, default: 0 },

    compiled_storage_path: { type: String },
    last_run_id: { type: String, index: true },

    /** Cooperative pause: loop checks this each row; cleared when entering PAUSED */
    pause_requested: { type: Boolean, default: false },
    /** Heartbeat for crash detection (updated during bulk run) */
    bulk_last_activity_at: { type: Date, default: null },

    created_by: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
    },
    updated_by: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
    },
    deleted_at: {
      type: Date,
      default: null,
    },
    deleted_by: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
    },
  },
  {
    timestamps: true,
  },
);

qpChargeFileSchema.index({ status: 1 });
qpChargeFileSchema.index({ status: 1, queue_order: 1 });
qpChargeFileSchema.index({ created_by: 1 });

module.exports = mongoose.model("QPChargeFile", qpChargeFileSchema);
