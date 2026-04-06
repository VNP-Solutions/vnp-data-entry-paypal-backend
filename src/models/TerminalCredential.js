const mongoose = require("mongoose");

const terminalCredentialSchema = new mongoose.Schema(
  {
    // Optional hotel identifier - can be null for standalone terminals
    // Multiple credentials can have the same hotel_id or null
    hotel_id: {
      type: String,
      required: false,
      default: null,
    },
    username: {
      type: String,
      required: true,
      unique: true,
    },
    terminal_key: {
      type: String,
      required: true, // Must be encrypted before saving
    },
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

// Indexes for performance - hotel_id is NOT unique, allows multiple nulls
terminalCredentialSchema.index({ hotel_id: 1, deleted_at: 1 }); // For filtering by hotel
// For charge lookup by QP username - username already has unique constraint
terminalCredentialSchema.index({ username: 1, deleted_at: 1 });

module.exports = mongoose.model("TerminalCredential", terminalCredentialSchema);
