const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const User = require('../models/User');
const nodemailer = require('nodemailer');
const dotenv = require('dotenv');
dotenv.config();

// JWT Secret (in production, use environment variable)
const JWT_SECRET = process.env.JWT_SECRET;

// Email configuration (you'll need to configure this with your email service)
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER || 'your-email@gmail.com',
        pass: process.env.EMAIL_PASS || 'your-app-password'
    }
});

// Register new user
const register = async (req, res) => {
    try {
        const { name, email, password } = req.body;

        // Validate input
        if (!name || !email || !password) {
            return res.status(400).json({
                status: 'error',
                message: 'Name, email, and password are required'
            });
        }

        // Check if email already exists
        const existingUser = await User.findOne({ email });
        if (existingUser) {
            return res.status(400).json({
                status: 'error',
                message: 'User with this email already exists'
            });
        }

        // Create new user (let the model's pre-save middleware handle password hashing)
        const user = new User({
            name,
            email,
            password: password  // Pass the plain password, let the model hash it
        });

        await user.save();

        // Generate JWT token
        const token = jwt.sign(
            { userId: user._id, email: user.email },
            JWT_SECRET,
            { expiresIn: '24h' }
        );

        res.status(201).json({
            status: 'success',
            message: 'User registered successfully',
            data: {
                user: {
                    id: user._id,
                    name: user.name,
                    email: user.email
                },
                token
            }
        });

    } catch (error) {
        console.error('Registration error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Generate 6-digit OTP
function generateOTP() {
    return Math.floor(100000 + Math.random() * 900000).toString();
}

// Generate temporary session token for pending login
function generatePendingSessionToken() {
    return crypto.randomBytes(32).toString('hex');
}

// Send OTP email
async function sendOTPEmail(email, otp, userName) {
    console.log(`\x1b[36m🔑 OTP for ${email}: ${otp}\x1b[0m`);
    
    const mailOptions = {
        from: process.env.EMAIL_USER || 'your-email@gmail.com',
        to: email,
        subject: 'Login Verification Code',
        html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <h2 style="color: #333;">Login Verification</h2>
                <p>Hello ${userName},</p>
                <p>You are attempting to log in to your account. Please use the verification code below:</p>
                <div style="background-color: #f4f4f4; padding: 20px; text-align: center; margin: 20px 0; border-radius: 5px;">
                    <h1 style="font-size: 32px; letter-spacing: 5px; margin: 0; color: #007bff;">${otp}</h1>
                </div>
                <p><strong>This code will expire in 5 minutes.</strong></p>
                <p>If you didn't attempt to log in, please ignore this email and consider changing your password.</p>
                <hr style="margin: 30px 0;">
                <p style="color: #666; font-size: 12px;">This is an automated message, please do not reply.</p>
            </div>
        `
    };

    try {
        await transporter.sendMail(mailOptions);
        return true;
    } catch (error) {
        console.error('Error sending OTP email:', error);
        throw new Error('Failed to send verification code');
    }
}

// Step 1: Email and Password Verification
const login = async (req, res) => {
    try {
        const { email, password } = req.body;

        // Validate input
        if (!email || !password) {
            return res.status(403).json({
                status: 'error',
                message: 'Email and password are required'
            });
        }

        // Find user by email
        const user = await User.findOne({ email });
        if (!user) {
            return res.status(403).json({
                status: 'error',
                message: 'Invalid email or password'
            });
        }
        

        // Check password
        const isPasswordValid = await bcrypt.compare(password, user.password);
        
        if (!isPasswordValid) {
            return res.status(403).json({
                status: 'error',
                message: 'Invalid email or password'
            });
        }

        // Generate OTP and session token
        const otp = generateOTP();
        const pendingSessionToken = generatePendingSessionToken();
        const otpExpires = new Date(Date.now() + 5 * 60 * 1000); // 5 minutes
        const sessionExpires = new Date(Date.now() + 15 * 60 * 1000); // 15 minutes (longer than OTP)

        // Save OTP details to user
        user.otpCode = otp;
        user.otpExpires = otpExpires;
        user.otpAttempts = 0;
        user.otpVerified = false;
        user.pendingLoginSession = pendingSessionToken;
        user.pendingLoginExpires = sessionExpires;
        await user.save();

        // Send OTP email
        try {
            await sendOTPEmail(email, otp, user.name);
        } catch (emailError) {
            console.error('Failed to send OTP email:', emailError);
            return res.status(500).json({
                status: 'error',
                message: 'Failed to send verification code. Please try again.'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'Verification code sent to your email',
            data: {
                step: 'otp_required',
                sessionToken: pendingSessionToken,
                email: email,
                expiresIn: 300, // 5 minutes in seconds
                message: 'Please check your email for the 6-digit verification code'
            }
        });

    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Send password reset email
async function sendPasswordResetEmail(email, resetUrl, userName) {
    const mailOptions = {
        from: process.env.EMAIL_USER || 'your-email@gmail.com',
        to: email,
        subject: 'Password Reset Request',
        html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <h2 style="color: #333; text-align: center;">Password Reset Request</h2>
                <p>Hello ${userName},</p>
                <p>You have requested to reset your password for your account. Click the button below to reset your password:</p>
                
                <div style="text-align: center; margin: 30px 0;">
                    <a href="${resetUrl}" style="background-color: #007bff; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block; font-weight: bold;">Reset Password</a>
                </div>
                
                <p><strong>This link will expire in 1 hour.</strong></p>
                
                <p>If the button doesn't work, copy and paste this URL into your browser:</p>
                <p style="word-break: break-all; color: #666; font-size: 14px;">${resetUrl}</p>
                
                <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
                <p style="color: #666; font-size: 12px;">
                    <strong>Security Notice:</strong><br>
                    • If you didn't request this password reset, please ignore this email<br>
                    • Your password won't be changed unless you click the link above<br>
                    • For security reasons, this link will expire in 1 hour<br>
                    • Never share this email or link with anyone
                </p>
                <p style="color: #666; font-size: 12px; text-align: center;">This is an automated message, please do not reply.</p>
            </div>
        `
    };

    try {
        await transporter.sendMail(mailOptions);
        return true;
    } catch (error) {
        console.error('Error sending password reset email:', error);
        throw new Error('Failed to send password reset email');
    }
}

// Forgot password with security improvements
const forgotPassword = async (req, res) => {
    try {
        const { email } = req.body;

        if (!email) {
            return res.status(400).json({
                status: 'error',
                message: 'Email is required'
            });
        }

        // Validate email format
        const emailRegex = /^\w+([.-]?\w+)*@\w+([.-]?\w+)*(\.\w{2,3})+$/;
        if (!emailRegex.test(email)) {
            return res.status(400).json({
                status: 'error',
                message: 'Please provide a valid email address'
            });
        }

        // Find user by email
        const user = await User.findOne({ email });
        
        // SECURITY: Always return success to prevent email enumeration attacks
        // Don't reveal whether the email exists in the system
        const response = {
            status: 'success',
            message: 'If an account with that email exists, we have sent a password reset link',
            data: {
                email: email,
                instruction: 'Please check your email inbox and spam folder for the reset link'
            }
        };

        // If user doesn't exist, still return success but don't send email
        if (!user) {
            return res.status(200).json(response);
        }

        // Check for recent reset attempts (rate limiting)
        const now = Date.now();
        const fiveMinutesAgo = now - (5 * 60 * 1000);
        
        if (user.resetPasswordExpires && user.resetPasswordExpires > fiveMinutesAgo) {
            // User requested reset within last 5 minutes, but don't reveal this
            return res.status(200).json(response);
        }

        // Generate secure reset token
        const resetToken = crypto.randomBytes(32).toString('hex');
        const resetTokenExpiry = now + 3600000; // 1 hour

        // Save reset token to user
        user.resetPasswordToken = resetToken;
        user.resetPasswordExpires = resetTokenExpiry;
        
        // Clear any existing OTP session data for security
        user.otpCode = undefined;
        user.otpExpires = undefined;
        user.otpAttempts = 0;
        user.pendingLoginSession = undefined;
        user.pendingLoginExpires = undefined;
        
        await user.save();

        // Create reset URL (use environment variable for frontend URL)
        const frontendUrl = process.env.FRONTEND_URL;
        const resetUrl = `${frontendUrl}/reset-password?token=${resetToken}`;
        
        // Send password reset email
        try {
            await sendPasswordResetEmail(email, resetUrl, user.name);
        } catch (emailError) {
            console.error('Failed to send password reset email:', emailError);
            
            // Clear the reset token if email failed
            user.resetPasswordToken = undefined;
            user.resetPasswordExpires = undefined;
            await user.save();
            
            return res.status(500).json({
                status: 'error',
                message: 'Failed to send password reset email. Please try again later.'
            });
        }

        res.status(200).json(response);

    } catch (error) {
        console.error('Forgot password error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Reset password with enhanced security and validation
const resetPassword = async (req, res) => {
    try {
        const { token } = req.params;
        const { newPassword, confirmPassword } = req.body;

        // Validate input
        if (!newPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'New password is required'
            });
        }

        if (!confirmPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'Password confirmation is required'
            });
        }

        if (newPassword !== confirmPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'Passwords do not match'
            });
        }

        // Validate password strength
        if (newPassword.length < 8) {
            return res.status(400).json({
                status: 'error',
                message: 'Password must be at least 8 characters long'
            });
        }

        // Check for password complexity (optional but recommended)
        const hasUpperCase = /[A-Z]/.test(newPassword);
        const hasLowerCase = /[a-z]/.test(newPassword);
        const hasNumbers = /\d/.test(newPassword);
        const hasSpecialChar = /[!@#$%^&*(),.?":{}|<>]/.test(newPassword);

        if (!hasUpperCase || !hasLowerCase || !hasNumbers) {
            return res.status(400).json({
                status: 'error',
                message: 'Password must contain at least one uppercase letter, one lowercase letter, and one number'
            });
        }

        // Find user by reset token
        const user = await User.findOne({
            resetPasswordToken: token,
            resetPasswordExpires: { $gt: Date.now() }
        });

        if (!user) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid or expired reset token. Please request a new password reset.'
            });
        }

        // Check if new password is same as current password
        const isSamePassword = await bcrypt.compare(newPassword, user.password);
        if (isSamePassword) {
            return res.status(400).json({
                status: 'error',
                message: 'New password must be different from your current password'
            });
        }

        // Update user password (let the model's pre-save middleware handle hashing)
        user.password = newPassword;
        user.resetPasswordToken = undefined;
        user.resetPasswordExpires = undefined;
        user.lastLogin = new Date(); // Update last login time
        
        // Clear any existing OTP session data for security
        user.otpCode = undefined;
        user.otpExpires = undefined;
        user.otpAttempts = 0;
        user.otpVerified = false;
        user.pendingLoginSession = undefined;
        user.pendingLoginExpires = undefined;
        
        await user.save();

        // Send confirmation email (optional but good UX)
        try {
            const confirmationMailOptions = {
                from: process.env.EMAIL_USER || 'your-email@gmail.com',
                to: user.email,
                subject: 'Password Successfully Reset',
                html: `
                    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                        <h2 style="color: #28a745; text-align: center;">Password Reset Successful</h2>
                        <p>Hello ${user.name},</p>
                        <p>Your password has been successfully reset. You can now log in to your account with your new password.</p>
                        <p><strong>Security reminder:</strong></p>
                        <ul style="color: #666;">
                            <li>Keep your password secure and don't share it with anyone</li>
                            <li>Use a unique password that you don't use elsewhere</li>
                            <li>If you didn't reset your password, please contact support immediately</li>
                        </ul>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="${process.env.FRONTEND_URL || 'http://localhost:3000'}/login" style="background-color: #007bff; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block; font-weight: bold;">Login to Your Account</a>
                        </div>
                        <p style="color: #666; font-size: 12px; text-align: center;">This is an automated message, please do not reply.</p>
                    </div>
                `
            };
            
            await transporter.sendMail(confirmationMailOptions);
        } catch (emailError) {
            console.error('Failed to send password reset confirmation email:', emailError);
            // Don't fail the request if confirmation email fails
        }

        res.status(200).json({
            status: 'success',
            message: 'Password reset successfully. You can now log in with your new password.',
            data: {
                email: user.email,
                message: 'A confirmation email has been sent to your email address'
            }
        });

    } catch (error) {
        console.error('Reset password error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Get current user profile
const getProfile = async (req, res) => {
    try {
        const user = await User.findById(req.user.userId).select('-password');
        
        if (!user) {
            return res.status(404).json({
                status: 'error',
                message: 'User not found'
            });
        }

        res.status(200).json({
            status: 'success',
            data: { user }
        });

    } catch (error) {
        console.error('Get profile error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Update user profile
const updateProfile = async (req, res) => {
    try {
        const { name, email } = req.body;
        const userId = req.user.userId;

        const updateData = {};
        if (name) updateData.name = name;
        if (email) {
            // Check if email is already taken by another user
            const existingUser = await User.findOne({ email, _id: { $ne: userId } });
            if (existingUser) {
                return res.status(400).json({
                    status: 'error',
                    message: 'Email is already taken'
                });
            }
            updateData.email = email;
        }

        const user = await User.findByIdAndUpdate(
            userId,
            updateData,
            { new: true, runValidators: true }
        ).select('-password');

        res.status(200).json({
            status: 'success',
            message: 'Profile updated successfully',
            data: { user }
        });

    } catch (error) {
        console.error('Update profile error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Step 2: OTP Verification
const verifyOTP = async (req, res) => {
    try {
        const { sessionToken, otp } = req.body;

        // Validate input
        if (!sessionToken || !otp) {
            return res.status(400).json({
                status: 'error',
                message: 'Session token and OTP are required'
            });
        }

        // Validate OTP format (6 digits)
        if (!/^\d{6}$/.test(otp)) {
            return res.status(400).json({
                status: 'error',
                message: 'OTP must be a 6-digit number'
            });
        }

        // Find user by pending session token
        const user = await User.findOne({ 
            pendingLoginSession: sessionToken,
            otpCode: { $ne: null }
        });

        if (!user) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid session token or OTP request not found'
            });
        }

        // Check if session has expired
        if (!user.pendingLoginExpires || user.pendingLoginExpires < new Date()) {
            // Clear all session data if session expired
            user.otpCode = undefined;
            user.otpExpires = undefined;
            user.otpAttempts = 0;
            user.pendingLoginSession = undefined;
            user.pendingLoginExpires = undefined;
            await user.save();

            return res.status(401).json({
                status: 'error',
                message: 'Login session has expired. Please start the login process again'
            });
        }

        // Check if OTP has expired (but keep session alive for resend)
        if (!user.otpExpires || user.otpExpires < new Date()) {
            // Clear only OTP data, keep session alive for resend
            user.otpCode = undefined;
            user.otpExpires = undefined;
            user.otpAttempts = 0;
            // Keep pendingLoginSession and pendingLoginExpires for resend
            await user.save();

            return res.status(401).json({
                status: 'error',
                message: 'OTP has expired. Please request a new verification code using resend',
                canResend: true
            });
        }

        // Check for too many attempts
        if (user.otpAttempts >= 5) {
            // Clear all session data after too many attempts
            user.otpCode = undefined;
            user.otpExpires = undefined;
            user.otpAttempts = 0;
            user.pendingLoginSession = undefined;
            user.pendingLoginExpires = undefined;
            await user.save();

            return res.status(429).json({
                status: 'error',
                message: 'Too many failed attempts. Please start the login process again'
            });
        }

        // Verify OTP
        if (user.otpCode !== otp) {
            // Increment attempts
            user.otpAttempts += 1;
            await user.save();

            const remainingAttempts = 5 - user.otpAttempts;
            return res.status(401).json({
                status: 'error',
                message: `Invalid OTP. ${remainingAttempts} attempts remaining`
            });
        }

        // OTP is valid - complete login
        user.otpCode = undefined;
        user.otpExpires = undefined;
        user.otpAttempts = 0;
        user.otpVerified = true;
        user.pendingLoginSession = undefined;
        user.pendingLoginExpires = undefined;
        user.lastLogin = new Date();
        await user.save();

        // Generate JWT token
        const token = jwt.sign(
            { userId: user._id, email: user.email },
            JWT_SECRET,
            { expiresIn: '24h' }
        );

        res.status(200).json({
            status: 'success',
            message: 'Login successful',
            data: {
                user: {
                    id: user._id,
                    name: user.name,
                    email: user.email,
                    lastLogin: user.lastLogin
                },
                token,
                tokenExpiresIn: '24h'
            }
        });

    } catch (error) {
        console.error('OTP verification error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Resend OTP
const resendOTP = async (req, res) => {
    try {
        const { sessionToken } = req.body;

        // Validate input
        if (!sessionToken) {
            return res.status(400).json({
                status: 'error',
                message: 'Session token is required'
            });
        }

        // Find user by pending session token
        const user = await User.findOne({ 
            pendingLoginSession: sessionToken
        });

        if (!user) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid session token. Please start the login process again'
            });
        }

        // Check if session has expired
        if (!user.pendingLoginExpires || user.pendingLoginExpires < new Date()) {
            // Clear all session data if session expired
            user.otpCode = undefined;
            user.otpExpires = undefined;
            user.otpAttempts = 0;
            user.pendingLoginSession = undefined;
            user.pendingLoginExpires = undefined;
            await user.save();

            return res.status(401).json({
                status: 'error',
                message: 'Login session has expired. Please start the login process again'
            });
        }

        // Generate new OTP
        const otp = generateOTP();
        const otpExpires = new Date(Date.now() + 5 * 60 * 1000); // 5 minutes

        // Update user with new OTP
        user.otpCode = otp;
        user.otpExpires = otpExpires;
        user.otpAttempts = 0; // Reset attempts
        await user.save();

        // Send new OTP email
        try {
            await sendOTPEmail(user.email, otp, user.name);
        } catch (emailError) {
            console.error('Failed to resend OTP email:', emailError);
            return res.status(500).json({
                status: 'error',
                message: 'Failed to resend verification code. Please try again.'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'New verification code sent to your email',
            data: {
                email: user.email,
                expiresIn: 300, // 5 minutes in seconds
                sessionExpiresIn: Math.floor((user.pendingLoginExpires - new Date()) / 1000), // Remaining session time
                message: 'Please check your email for the new 6-digit verification code'
            }
        });

    } catch (error) {
        console.error('Resend OTP error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Send invitation
const sendInvitation = async (req, res) => {
    try {
        const { email, name } = req.body;
        const inviterUserId = req.user.userId; // From auth middleware

        // Validate input
        if (!email || !name) {
            return res.status(400).json({
                status: 'error',
                message: 'Email and name are required'
            });
        }

        // Validate email format
        const emailRegex = /^\w+([.-]?\w+)*@\w+([.-]?\w+)*(\.\w{2,3})+$/;
        if (!emailRegex.test(email)) {
            return res.status(400).json({
                status: 'error',
                message: 'Please provide a valid email address'
            });
        }

        // Get inviter information
        const inviter = await User.findById(inviterUserId);
        if (!inviter) {
            return res.status(401).json({
                status: 'error',
                message: 'Inviter not found'
            });
        }

        // Check if user already exists with this email
        const existingUser = await User.findOne({ email });
        
        if (existingUser) {
            // If it's an active user (completed invitation/registration), don't allow new invitation
            if (existingUser.isActive && !existingUser.isInvited) {
                return res.status(400).json({
                    status: 'error',
                    message: 'A user with this email already exists and is active'
                });
            }
            
            // If it's a pending invitation, replace it automatically
            if (existingUser.isInvited) {
                console.log(`🔄 Replacing existing invitation for ${email}`);
                await User.findByIdAndDelete(existingUser._id);
                // Continue with creating new invitation
            }
        }

        // Generate invitation token and temporary password
        const invitationToken = generateInvitationToken();
        const tempPassword = generateTempPassword();

        // Create invited user with temporary credentials
        const invitedUser = new User({
            name,
            email,
            password: tempPassword, // This will be hashed by the pre-save middleware
            invitationToken,
            tempPassword: tempPassword, // Store plain text temp password for validation
            isInvited: true,
            invitedBy: inviterUserId,
            isActive: false // Account will be activated after completing invitation
        });

        await invitedUser.save();

        // Create invitation URL
        const frontendUrl = process.env.FRONTEND_URL;
        const invitationUrl = `${frontendUrl}/invitation?token=${invitationToken}&email=${encodeURIComponent(email)}`;

        // Send invitation email
        try {
            await sendInvitationEmail(email, name, invitationUrl, tempPassword, inviter.name);
        } catch (emailError) {
            console.error('Failed to send invitation email:', emailError);
            
            // Delete the created user if email fails
            await User.findByIdAndDelete(invitedUser._id);
            
            return res.status(500).json({
                status: 'error',
                message: 'Failed to send invitation email. Please try again.'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'Invitation sent successfully',
            data: {
                invitedUser: {
                    id: invitedUser._id,
                    name: invitedUser.name,
                    email: invitedUser.email,
                    invitedBy: inviter.name
                },
                status: 'pending',
                message: `Invitation has been sent to ${email}`
            }
        });

    } catch (error) {
        console.error('Send invitation error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Validate invitation with temporary password
const validateInvitation = async (req, res) => {
    try {
        const { email, tempPassword, token } = req.body;

        // Validate input
        if (!email || !tempPassword || !token) {
            return res.status(400).json({
                status: 'error',
                message: 'Email, temporary password, and token are required'
            });
        }

        // Find user by email and invitation token
        const user = await User.findOne({
            email: email.toLowerCase(),
            invitationToken: token,
            isInvited: true
        });

        if (!user) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid invitation'
            });
        }

        // Verify temporary password
        if (user.tempPassword !== tempPassword) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid temporary password'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'Invitation validated successfully',
            data: {
                valid: true,
                user: {
                    id: user._id,
                    name: user.name,
                    email: user.email
                },
                nextStep: 'Please set your new password to complete the registration'
            }
        });

    } catch (error) {
        console.error('Validate invitation error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Complete invitation by setting new password
const completeInvitation = async (req, res) => {
    try {
        const { email, tempPassword, token, newPassword, confirmPassword } = req.body;

        // Validate input
        if (!email || !tempPassword || !token || !newPassword || !confirmPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'All fields are required'
            });
        }

        if (newPassword !== confirmPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'Passwords do not match'
            });
        }

        // Validate password strength
        if (newPassword.length < 8) {
            return res.status(400).json({
                status: 'error',
                message: 'Password must be at least 8 characters long'
            });
        }

        // Check for password complexity
        const hasUpperCase = /[A-Z]/.test(newPassword);
        const hasLowerCase = /[a-z]/.test(newPassword);
        const hasNumbers = /\d/.test(newPassword);

        if (!hasUpperCase || !hasLowerCase || !hasNumbers) {
            return res.status(400).json({
                status: 'error',
                message: 'Password must contain at least one uppercase letter, one lowercase letter, and one number'
            });
        }

        // Find user by email and invitation token
        const user = await User.findOne({
            email: email.toLowerCase(),
            invitationToken: token,
            isInvited: true
        });

        if (!user) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid invitation'
            });
        }

        // Verify temporary password
        if (user.tempPassword !== tempPassword) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid temporary password'
            });
        }

        // Check if new password is same as temporary password
        const isSameTempPassword = await bcrypt.compare(newPassword, user.password);
        if (isSameTempPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'New password must be different from the temporary password'
            });
        }

        // Update user with new password and complete invitation
        user.password = newPassword; // Will be hashed by pre-save middleware
        user.invitationToken = undefined;
        user.tempPassword = undefined;
        user.isInvited = false;
        user.isActive = true;
        user.invitationCompletedAt = new Date();
        user.lastLogin = new Date();

        await user.save();

        // Generate JWT token
        const jwtToken = jwt.sign(
            { userId: user._id, email: user.email },
            JWT_SECRET,
            { expiresIn: '24h' }
        );

        // Send welcome email (optional)
        try {
            const welcomeMailOptions = {
                from: process.env.EMAIL_USER || 'your-email@gmail.com',
                to: user.email,
                subject: 'Welcome! Your account is now active',
                html: `
                    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                        <h2 style="color: #28a745; text-align: center;">🎉 Welcome to Our Platform!</h2>
                        <p>Hello ${user.name},</p>
                        <p>Congratulations! You have successfully completed your account setup. Your account is now active and ready to use.</p>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="${process.env.FRONTEND_URL || 'http://localhost:3000'}/dashboard" style="background-color: #007bff; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block; font-weight: bold;">Go to Dashboard</a>
                        </div>
                        <p>If you have any questions, feel free to reach out to our support team.</p>
                        <p style="color: #666; font-size: 12px; text-align: center;">This is an automated message, please do not reply.</p>
                    </div>
                `
            };
            
            await transporter.sendMail(welcomeMailOptions);
        } catch (emailError) {
            console.error('Failed to send welcome email:', emailError);
            // Don't fail the request if welcome email fails
        }

        res.status(200).json({
            status: 'success',
            message: 'Invitation completed successfully! Welcome aboard!',
            data: {
                user: {
                    id: user._id,
                    name: user.name,
                    email: user.email,
                    lastLogin: user.lastLogin,
                    completedAt: user.invitationCompletedAt
                },
                token: jwtToken,
                tokenExpiresIn: '24h'
            }
        });

    } catch (error) {
        console.error('Complete invitation error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Verify password (re-auth for sensitive actions)
const verifyPassword = async (req, res) => {
    try {
        if (!req.user || !req.user.userId) {
            return res.status(401).json({
                status: 'error',
                message: 'Authentication required'
            });
        }
        const { password } = req.body;
        if (!password || typeof password !== 'string') {
            return res.status(400).json({
                status: 'error',
                message: 'Password is required'
            });
        }
        const user = await User.findById(req.user.userId).select('password');
        if (!user) {
            return res.status(404).json({
                status: 'error',
                message: 'User not found'
            });
        }
        if (!user.password) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid password'
            });
        }
        let isValid = false;
        try {
            isValid = await user.comparePassword(password);
        } catch (compareErr) {
            console.error('Verify password compare error:', compareErr);
            return res.status(401).json({
                status: 'error',
                message: 'Invalid password'
            });
        }
        if (!isValid) {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid password'
            });
        }
        res.status(200).json({ status: 'success' });
    } catch (error) {
        console.error('Verify password error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error'
        });
    }
};

module.exports = {
    register,
    login,
    forgotPassword,
    resetPassword,
    getProfile,
    updateProfile,
    verifyOTP,
    resendOTP,
    validateInvitation,
    completeInvitation,
    verifyPassword
}; 