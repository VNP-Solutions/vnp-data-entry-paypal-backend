const mongoose = require('mongoose');

const terminalCredentialSchema = new mongoose.Schema({
  hotel_id: {
    type: String,
    required: true,
    unique: true
  },
  username: {
    type: String,
    required: true
  },
  terminal_key: {
    type: String,
    required: true // Must be encrypted before saving
  },
  created_by: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User'
  },
  updated_by: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User'
  },
  deleted_at: {
    type: Date,
    default: null
  },
  deleted_by: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User'
  }
}, {
  timestamps: true
});

// For soft deletion and fast constraint checking
terminalCredentialSchema.index({ hotel_id: 1, deleted_at: 1 });

module.exports = mongoose.model('TerminalCredential', terminalCredentialSchema);
