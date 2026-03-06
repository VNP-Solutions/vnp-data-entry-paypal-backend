const mongoose = require('mongoose');

const qpPaymentAttemptSchema = new mongoose.Schema({
  charge_instance_id: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'QPChargeInstance',
    required: true,
    index: true
  },
  request_id: {
    type: String,
    required: true,
    index: true
  },
  run_id: {
    type: String,
    index: true
  },
  request_payload_redacted: {
    type: mongoose.Schema.Types.Mixed // JSON
  },
  response_status_code: {
    type: Number
  },
  response_body: {
    type: mongoose.Schema.Types.Mixed // JSON full provider response bucket
  },
  result: {
    type: String,
    enum: ['SUCCESS', 'DECLINED', 'ERROR']
  },
  created_by: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User'
  }
}, {
  timestamps: true // Gives us `createdAt` and `updatedAt` for free
});

module.exports = mongoose.model('QPPaymentAttempt', qpPaymentAttemptSchema);
