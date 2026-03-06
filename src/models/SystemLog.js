const mongoose = require('mongoose');

const systemLogSchema = new mongoose.Schema({
  timestamp: {
    type: Date,
    default: Date.now
  },
  level: {
    type: String,
    enum: ['INFO', 'WARN', 'ERROR'],
    default: 'INFO'
  },
  request_id: {
    type: String,
    index: true
  },
  run_id: {
    type: String,
    index: true
  },
  actor_user_id: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User'
  },
  action: {
    type: String,
    required: true
  },
  entity_type: {
    type: String
  },
  entity_id: {
    type: String
  },
  message: {
    type: String
  },
  metadata: {
    type: mongoose.Schema.Types.Mixed
  }
}, {
  timestamps: true // Automatically creates createdAt and updatedAt (if needed additionally)
});

// Indexes for common log queries
systemLogSchema.index({ action: 1, timestamp: -1 });

module.exports = mongoose.model('SystemLog', systemLogSchema);
