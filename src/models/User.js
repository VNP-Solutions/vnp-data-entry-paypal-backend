const mongoose = require('mongoose');

const userSchema = new mongoose.Schema({
    name: {
        type: String,
        required: [true, 'Name is required'],
        trim: true,
        minlength: [2, 'Name must be at least 2 characters long'],
        maxlength: [50, 'Name cannot exceed 50 characters']
    },
    email: {
        type: String,
        required: [true, 'Email is required'],
        unique: true,
        lowercase: true,
        trim: true,
        match: [
            /^\w+([.-]?\w+)*@\w+([.-]?\w+)*(\.\w{2,3})+$/,
            'Please enter a valid email address'
        ]
    },
    password: {
        type: String,
        required: [true, 'Password is required'],
        minlength: [6, 'Password must be at least 6 characters long']
    },
    resetPasswordToken: {
        type: String,
        default: undefined
    },
    resetPasswordExpires: {
        type: Date,
        default: undefined
    },
    // OTP fields for Two-Factor Authentication
    otpCode: {
        type: String,
        default: undefined
    },
    otpExpires: {
        type: Date,
        default: undefined
    },
    otpAttempts: {
        type: Number,
        default: 0
    },
    otpVerified: {
        type: Boolean,
        default: false
    },
    pendingLoginSession: {
        type: String,
        default: undefined
    },
    pendingLoginExpires: {
        type: Date,
        default: undefined
    },
    isActive: {
        type: Boolean,
        default: true
    },
    lastLogin: {
        type: Date,
        default: Date.now
    }
}, {
    timestamps: true
});

// Pre-save middleware to hash password
userSchema.pre('save', async function(next) {
    if (!this.isModified('password')) {
        return next();
    }
    
    try {
        const bcrypt = require('bcryptjs');
        const salt = await bcrypt.genSalt(10);
        this.password = await bcrypt.hash(this.password, salt);
        next();
    } catch (error) {
        next(error);
    }
});

// Method to compare password
userSchema.methods.comparePassword = async function(candidatePassword) {
    const bcrypt = require('bcryptjs');
    return bcrypt.compare(candidatePassword, this.password);
};

// Virtual for user's full profile (excluding password)
userSchema.virtual('profile').get(function() {
    return {
        id: this._id,
        name: this.name,
        email: this.email,
        isActive: this.isActive,
        lastLogin: this.lastLogin,
        createdAt: this.createdAt,
        updatedAt: this.updatedAt
    };
});

// Ensure virtual fields are serialized
userSchema.set('toJSON', {
    virtuals: true,
    transform: function(doc, ret) {
        delete ret.password;
        delete ret.resetPasswordToken;
        delete ret.resetPasswordExpires;
        delete ret.otpCode;
        delete ret.otpExpires;
        delete ret.otpAttempts;
        delete ret.pendingLoginSession;
        delete ret.pendingLoginExpires;
        return ret;
    }
});

module.exports = mongoose.model('User', userSchema); 