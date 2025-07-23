const mongoose = require('mongoose');

const otaSchema = new mongoose.Schema({
    name: {
        type: String,
        required: true,
        unique: true,
        enum: ['Expedia', 'Booking.com', 'Agoda']
    },
    displayName: {
        type: String,
        required: true
    },
    customer: {
        type: String,
        required: true
    },
    billingAddress: {
        zipCode: {
            type: String,
            required: true
        },
        countryCode: {
            type: String,
            required: true,
            default: 'US'
        },
        addressLine1: {
            type: String,
            default: ''
        },
        addressLine2: {
            type: String,
            default: ''
        },
        city: {
            type: String,
            default: ''
        },
        state: {
            type: String,
            default: ''
        }
    },
    isActive: {
        type: Boolean,
        default: true
    },
    createdAt: {
        type: Date,
        default: Date.now
    },
    updatedAt: {
        type: Date,
        default: Date.now
    }
});

// Update the updatedAt field before saving
otaSchema.pre('save', function(next) {
    this.updatedAt = Date.now();
    next();
});

// Create index for faster queries
otaSchema.index({ name: 1 });
otaSchema.index({ isActive: 1 });

module.exports = mongoose.model('OTA', otaSchema); 