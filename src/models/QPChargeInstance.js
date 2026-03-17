/**
 * QPChargeInstance.js
 * Schema for a single charge row (one reservation/card) within a QP charge file.
 * Stores payment identity (hotel_id, hotel_name, reservation_id), amount, currency, billing address,
 * encrypted card data, status (PENDING/SUCCESS/DECLINED/ERROR/etc.), and provider response fields.
 *
 * BOOKMARK LIST (landmarks in this file – schema sections)
 * ------------------------------------
 * charge_file_id, parent_file_name, row_number
 *   Links instance to QPChargeFile and row index.
 * Carry-through fields (From XLSX)
 *   ota, vnp_work_id, portfolio from template.
 * Payment Identity
 *   hotel_id, hotel_name, reservation_id, amount_numeric, currency, user_id.
 * Billing Address
 *   address_1, address_2, city, state, postal_code, country_code.
 * Card Details (Masked and Encrypted)
 *   card_number, card_last4, expiry_month, expiry_year, cvv.
 * Status
 *   status (enum), status_reason.
 * Idempotency / duplicates
 *   charge_key, is_duplicate.
 * Provider summary
 *   provider_transaction_id, provider_message, provider_code.
 * Trace fields
 *   last_request_id, last_run_id, requested_at, completed_at, last_response_payload.
 */

const mongoose = require('mongoose');

const qpChargeInstanceSchema = new mongoose.Schema({
  charge_file_id: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'QPChargeFile',
    required: true,
    index: true
  },
  parent_file_name: {
    type: String
  },
  row_number: {
    type: Number,
    required: true
  },
  
  // Carry-through fields (From XLSX)
  ota: { type: String },
  vnp_work_id: { type: String },
  portfolio: { type: String },
  
  // Payment Identity
  hotel_id: { type: String, index: true },
  hotel_name: { type: String },
  reservation_id: { type: String, index: true },
  amount_numeric: { type: Number },
  currency: { type: String, default: 'USD' },
  user_id: { type: String }, // Extracted from excel, not objectId
  
  // Billing Address
  billing_address: {
    address_1: { type: String },
    address_2: { type: String },
    city: { type: String },
    state: { type: String },
    postal_code: { type: String },
    country_code: { type: String, default: 'US' }
  },
  
  // Card Details (Masked and Encrypted)
  card_number: { type: String }, // Encrypted
  card_last4: { type: String },
  expiry_month: { type: Number },
  expiry_year: { type: Number },
  cvv: { type: String }, // Encrypted
  
  // Status
  status: {
    type: String,
    enum: ['PENDING', 'PROCESSING', 'SUCCESS', 'DECLINED', 'ERROR', 'INVALID', 'SKIPPED'],
    default: 'PENDING',
    index: true
  },
  status_reason: { type: String },
  
  // Idempotency / duplicates
  charge_key: { type: String, index: true },
  is_duplicate: { type: Boolean, default: false },
  
  // Provider summary
  provider_transaction_id: { type: String },
  provider_message: { type: String },
  provider_code: { type: String },
  
  // Trace fields
  last_request_id: { type: String },
  last_run_id: { type: String, index: true },
  requested_at: { type: Date },
  completed_at: { type: Date },

  // Full QP response (for details modal)
  last_response_payload: { type: mongoose.Schema.Types.Mixed },

  // Standard Tracking
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

// Compound indexes for fast lookups
qpChargeInstanceSchema.index({ charge_file_id: 1, row_number: 1 });
qpChargeInstanceSchema.index({ hotel_id: 1, reservation_id: 1 });

module.exports = mongoose.model('QPChargeInstance', qpChargeInstanceSchema);
