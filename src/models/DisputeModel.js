const mongoose = require("mongoose");

const disputeSchema = new mongoose.Schema(
  {
    // Reference to the original payment record
    stripeExcelDataId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "StripeExcelData",
      required: true,
    },
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
    // Stripe Payment Intent and Charge references
    stripePaymentIntentId: {
      type: String,
      required: true,
    },
    stripeLatestChargeId: {
      type: String,
      required: true,
    },
    // Stripe Dispute Fields
    stripeDisputeId: {
      type: String,
      required: true,
      unique: true,
    },
    stripeDisputeStatus: {
      type: String, // warning_needs_response, warning_under_review, warning_closed, needs_response, under_review, charge_refunded, won, lost
      required: true,
    },
    stripeDisputeReason: {
      type: String, // duplicate, fraudulent, subscription_canceled, product_unacceptable, product_not_received, unrecognized, credit_not_processed, general, incorrect_account_details, insufficient_funds, bank_cannot_process, debit_not_authorized, customer_initiated
      required: true,
    },
    stripeDisputeAmount: {
      type: Number,
      required: true,
    },
    stripeDisputeCurrency: {
      type: String,
      required: true,
    },
    stripeDisputeCreatedAt: {
      type: Date,
      required: true,
    },
    stripeDisputeEvidenceDueBy: {
      type: Date,
      default: null,
    },
    stripeDisputeEvidenceSubmitted: {
      type: Boolean,
      default: false,
    },
    stripeDisputeEvidenceDetails: {
      type: Object,
      default: {},
    },
    stripeDisputeMetadata: {
      type: Object,
      default: {},
    },
    stripeDisputeEvidenceFileId: {
      type: String,
      default: null,
    },
    stripeDisputeNetworkReasonCode: {
      type: String,
      default: null,
    },
    stripeDisputeIsChargeRefundable: {
      type: Boolean,
      default: null,
    },
    stripeDisputeBalanceTransactions: {
      type: [String], // Array of balance transaction IDs
      default: [],
    },
    // Additional tracking fields
    disputeResolutionNotes: {
      type: String,
      default: null,
    },
    internalStatus: {
      type: String,
      enum: [
        "new",
        "investigating",
        "evidence_submitted",
        "awaiting_response",
        "resolved",
      ],
      default: "new",
    },
    assignedTo: {
      type: String, // Email or user ID of person handling the dispute
      default: null,
    },
    lastUpdatedBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      default: null,
    },
    // Hotel/Property information (copied from original record for quick access)
    hotelName: {
      type: String,
      default: null,
    },
    reservationId: {
      type: String,
      default: null,
    },
    guestName: {
      type: String,
      default: null,
    },
    checkIn: {
      type: String,
      default: null,
    },
    checkOut: {
      type: String,
      default: null,
    },
    connectedAccount: {
      type: String,
      default: null,
    },
  },
  {
    timestamps: true,
  }
);

// Indexes for faster queries
disputeSchema.index({ stripeDisputeId: 1 });
disputeSchema.index({ stripeExcelDataId: 1 });
disputeSchema.index({ stripeLatestChargeId: 1 });
disputeSchema.index({ stripePaymentIntentId: 1 });
disputeSchema.index({ userId: 1 });
disputeSchema.index({ stripeDisputeStatus: 1 });
disputeSchema.index({ stripeDisputeCreatedAt: 1 });
disputeSchema.index({ internalStatus: 1 });

// Virtual to get the original payment record
disputeSchema.virtual("originalPayment", {
  ref: "StripeExcelData",
  localField: "stripeExcelDataId",
  foreignField: "_id",
  justOne: true,
});

// Ensure virtual fields are serialized
disputeSchema.set("toJSON", { virtuals: true });

module.exports = mongoose.model("Dispute", disputeSchema);
