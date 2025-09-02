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
    "Connected Account": {
      type: String,
      default: null,
    },
    // Stripe Payment Details Fields
    stripePaymentIntentId: {
      type: String, // maps payment.id
      default: null,
    },
    stripeLatestChargeId: {
      type: String, // maps payment.latest_charge
      default: null,
    },
    stripePaymentMethodId: {
      type: String, // maps payment.payment_method
      default: null,
    },
    stripeTransferDestination: {
      type: String, // maps payment.transfer_data.destination
      default: null,
    },
    stripeTransferGroup: {
      type: String, // maps payment.transfer_group
      default: null,
    },
    stripeApplicationFeeAmount: {
      type: Number, // maps payment.application_fee_amount
      default: null,
    },
    stripeAmount: {
      type: Number, // maps payment.amount
      default: null,
    },
    stripeAmountReceived: {
      type: Number, // maps payment.amount_received
      default: null,
    },
    stripeCurrency: {
      type: String, // maps payment.currency
      default: null,
    },
    stripeStatus: {
      type: String, // maps payment.status
      default: null,
    },
    stripeCaptureMethod: {
      type: String, // maps payment.capture_method
      default: null,
    },
    stripeConfirmationMethod: {
      type: String, // maps payment.confirmation_method
      default: null,
    },
    stripeCreatedAt: {
      type: Date, // maps payment.created (unix timestamp)
      default: null,
    },
    stripeClientSecret: {
      type: String, // maps payment.client_secret
      default: null,
    },
    stripePaymentMethodTypes: {
      type: [String], // maps payment.payment_method_types
      default: [],
    },
    stripeAutomaticPaymentMethods: {
      type: Object, // maps payment.automatic_payment_methods
      default: {},
    },
    stripeDescription: {
      type: String, // maps payment.description
      default: null,
    },
    stripeMetadata: {
      type: Object, // maps payment.metadata
      default: {},
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
