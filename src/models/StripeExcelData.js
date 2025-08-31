const mongoose = require("mongoose");

const stripeExcelDataSchema = new mongoose.Schema(
  {
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
    // File tracking fields
    uploadId: {
      type: String,
      required: true,
    },
    fileName: {
      type: String,
      required: true,
    },
    uploadStatus: {
      type: String,
      enum: ["processing", "completed", "failed", "partial"],
      default: "processing",
    },
    rowNumber: {
      type: Number,
      required: true,
    },
    // Excel Data Fields - These are the standard field names in our database
    "Expedia ID": {
      type: String,
      default: null,
    },
    Batch: {
      type: String,
      default: null,
    },
    OTA: {
      type: String,
      default: null,
    },
    "Posting Type": {
      type: String,
      default: null,
    },
    Portfolio: {
      type: String,
      default: null,
    },
    "Hotel Name": {
      type: String,
      default: null,
    },
    "Reservation ID": {
      type: String,
      default: null,
    },
    "Hotel Confirmation Code": {
      type: String,
      default: null,
    },
    Name: {
      type: String,
      default: null,
    },
    "Check In": {
      type: String,
      default: null,
    },
    "Check Out": {
      type: String,
      default: null,
    },
    Curency: {
      type: String,
      default: null,
    },
    "Amount to charge": {
      type: String,
      default: null,
    },
    "Charge status": {
      type: String,
      default: null,
    },
    "Card Number": {
      type: String,
      default: null,
    },
    "Card Expire": {
      type: String,
      default: null,
    },
    "Card CVV": {
      type: String,
      default: null,
    },
    "Soft Descriptor": {
      type: String,
      default: null,
    },
    "VNP Work ID": {
      type: String,
      default: null,
    },
    Status: {
      type: String,
      default: null,
    },
    // Stripe-specific field
    "Connected Account": {
      type: String,
      default: null,
    },
    // Stripe Payment Details Fields
    stripeOrderId: {
      type: String,
      default: null,
    },
    stripeCaptureId: {
      type: String,
      default: null,
    },
    stripeNetworkTransactionId: {
      type: String,
      default: null,
    },
    stripeFee: {
      type: String,
      default: null,
    },
    stripeNetAmount: {
      type: String,
      default: null,
    },
    stripeCardBrand: {
      type: String,
      default: null,
    },
    stripeCardType: {
      type: String,
      default: null,
    },
    stripeAvsCode: {
      type: String,
      default: null,
    },
    stripeCvvCode: {
      type: String,
      default: null,
    },
    stripeCreateTime: {
      type: String,
      default: null,
    },
    stripeUpdateTime: {
      type: String,
      default: null,
    },
    stripeStatus: {
      type: String,
      default: null,
    },
    stripeAmount: {
      type: String,
      default: null,
    },
    stripeCurrency: {
      type: String,
      default: null,
    },
    stripeCardLastDigits: {
      type: String,
      default: null,
    },
    stripeCaptureStatus: {
      type: String,
      default: null,
    },
    stripeCustomId: {
      type: String,
      default: null,
    },
    // Stripe Refund Details Fields
    stripeRefundId: {
      type: String,
      default: null,
    },
    stripeRefundStatus: {
      type: String,
      default: null,
    },
    stripeRefundAmount: {
      type: String,
      default: null,
    },
    stripeRefundCurrency: {
      type: String,
      default: null,
    },
    stripeRefundGrossAmount: {
      type: String,
      default: null,
    },
    stripeRefundFee: {
      type: String,
      default: null,
    },
    stripeRefundNetAmount: {
      type: String,
      default: null,
    },
    stripeTotalRefunded: {
      type: String,
      default: null,
    },
    stripeRefundCreateTime: {
      type: String,
      default: null,
    },
    stripeRefundUpdateTime: {
      type: String,
      default: null,
    },
    stripeRefundInvoiceId: {
      type: String,
      default: null,
    },
    stripeRefundCustomId: {
      type: String,
      default: null,
    },
    stripeRefundNote: {
      type: String,
      default: null,
    },
    otaId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "OTA",
      default: null,
    },
  },
  {
    timestamps: true,
  }
);

// Indexes for faster queries
stripeExcelDataSchema.index({ userId: 1 });
stripeExcelDataSchema.index({ uploadId: 1, rowNumber: 1 });
stripeExcelDataSchema.index({ "Expedia ID": 1, userId: 1 });

module.exports = mongoose.model("StripeExcelData", stripeExcelDataSchema);
