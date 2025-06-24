const mongoose = require('mongoose');

const excelDataSchema = new mongoose.Schema({
    userId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'User',
        required: true
    },
    // File tracking fields
    uploadId: {
        type: String,
        required: true
    },
    fileName: {
        type: String,
        required: true
    },
    uploadStatus: {
        type: String,
        enum: ['processing', 'completed', 'failed', 'partial'],
        default: 'processing'
    },
    rowNumber: {
        type: Number,
        required: true
    },
    // Excel Data Fields - These are the standard field names in our database
    'Expedia ID': {
        type: String,
        default: null
    },
    'Batch': {
        type: String,
        default: null
    },
    'Posting Type': {
        type: String,
        default: null
    },
    'Portfolio': {
        type: String,
        default: null
    },
    'Hotel Name': {
        type: String,
        default: null
    },
    'Reservation ID': {
        type: String,
        default: null
    },
    'Hotel Confirmation Code': {
        type: String,
        default: null
    },
    'Name': {
        type: String,
        default: null
    },
    'Check In': {
        type: String,
        default: null
    },
    'Check Out': {
        type: String,
        default: null
    },
    'Curency': {
        type: String,
        default: null
    },
    'Amount to charge': {
        type: String,
        default: null
    },
    'Charge status': {
        type: String,
        default: null
    },
    'Card first 4': {
        type: String,
        default: null
    },
    'Card last 12': {
        type: String,
        default: null
    },
    'Card Expire': {
        type: String,
        default: null
    },
    'Card CVV': {
        type: String,
        default: null
    },
    'Soft Descriptor': {
        type: String,
        default: null
    },
    'VNP Work ID': {
        type: String,
        default: null
    },
    'Status': {
        type: String,
        default: null
    },
    // PayPal Payment Details Fields
    paypalOrderId: {
        type: String,
        default: null
    },
    paypalCaptureId: {
        type: String,
        default: null
    },
    paypalNetworkTransactionId: {
        type: String,
        default: null
    },
    paypalFee: {
        type: String,
        default: null
    },
    paypalNetAmount: {
        type: String,
        default: null
    },
    paypalCardBrand: {
        type: String,
        default: null
    },
    paypalCardType: {
        type: String,
        default: null
    },
    paypalAvsCode: {
        type: String,
        default: null
    },
    paypalCvvCode: {
        type: String,
        default: null
    },
    paypalCreateTime: {
        type: String,
        default: null
    },
    paypalUpdateTime: {
        type: String,
        default: null
    },
    paypalStatus: {
        type: String,
        default: null
    },
    paypalAmount: {
        type: String,
        default: null
    },
    paypalCurrency: {
        type: String,
        default: null
    },
    paypalCardLastDigits: {
        type: String,
        default: null
    }
}, {
    timestamps: true
});

// Indexes for faster queries
excelDataSchema.index({ userId: 1 });
excelDataSchema.index({ uploadId: 1, rowNumber: 1 }, { unique: true });
excelDataSchema.index({ 'Expedia ID': 1, userId: 1 });

module.exports = mongoose.model('ExcelData', excelDataSchema); 