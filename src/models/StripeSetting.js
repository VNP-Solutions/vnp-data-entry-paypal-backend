const mongoose = require('mongoose');

const stripeSettingSchema = new mongoose.Schema({
    vnpRatio: {
        type: Number,
        required: true,
        min: 0,
        max: 100,
        default: 15
    }
}, {
    timestamps: true
});

module.exports = mongoose.model('StripeSetting', stripeSettingSchema);

