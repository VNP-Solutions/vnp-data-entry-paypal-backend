const mongoose = require("mongoose");

const uploadSessionSchema = new mongoose.Schema(
  {
    uploadId: {
      type: String,
      required: true,
      unique: true,
    },
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
    fileName: {
      type: String,
      required: true,
    },
    originalFileName: {
      type: String,
      required: true,
    },
    s3Key: {
      type: String,
      required: true,
    },
    totalRows: {
      type: Number,
      required: true,
    },
    processedRows: {
      type: Number,
      default: 0,
    },
    status: {
      type: String,
      enum: ["uploading", "processing", "completed", "failed", "cancelled"],
      default: "uploading",
    },
    errorMessage: {
      type: String,
      default: null,
    },
    startedAt: {
      type: Date,
      default: Date.now,
    },
    completedAt: {
      type: Date,
      default: null,
    },
    vnpWorkId: {
      type: String,
      default: null,
    },
    headers: [
      {
        type: String,
      },
    ],
    batchSize: {
      type: Number,
      default: 100,
    },
    retryCount: {
      type: Number,
      default: 0,
    },
    maxRetries: {
      type: Number,
      default: 3,
    },
    // OTA (Online Travel Agency) Fields
    ota: {
      type: String,
      enum: ["Expedia", "Booking.com", "Agoda"],
      default: null,
    },
    otaId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "OTA",
      default: null,
    },
    // Payment Gateway
    paymentGateway: {
      type: String,
      enum: ["paypal", "stripe"],
      default: "paypal",
    },
  },
  {
    timestamps: true,
  }
);

// Indexes
uploadSessionSchema.index({ userId: 1, status: 1 });

module.exports = mongoose.model("UploadSession", uploadSessionSchema);
