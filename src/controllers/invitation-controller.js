const crypto = require('crypto');
const User = require('../models/User');
const nodemailer = require('nodemailer');
const dotenv = require('dotenv');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const JWT_SECRET = process.env.JWT_SECRET;
dotenv.config();

// Email configuration
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER || 'your-email@gmail.com',
        pass: process.env.EMAIL_PASS || 'your-app-password'
    }
});

// Generate temporary password (8 characters: letters and numbers)
function generateTempPassword() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';
    let tempPassword = '';
    for (let i = 0; i < 8; i++) {
        tempPassword += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return tempPassword;
}

// Generate invitation token
function generateInvitationToken() {
    return crypto.randomBytes(32).toString('hex');
}

// Send invitation email
async function sendInvitationEmail(email, name, invitationUrl, tempPassword, inviterName) {
    const mailOptions = {
        from: process.env.EMAIL_USER || 'your-email@gmail.com',
        to: email,
        subject: `You're invited to join our platform by ${inviterName}`,
        html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; background-color: #f9f9f9; padding: 20px;">
                <div style="background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                    <h2 style="color: #333; text-align: center; margin-bottom: 30px;">🎉 You're Invited!</h2>
                    
                    <p style="font-size: 16px; color: #333;">Hello <strong>${name}</strong>,</p>
                    
                    <p style="font-size: 14px; color: #666; line-height: 1.6;">
                        <strong>${inviterName}</strong> has invited you to join our platform. We're excited to have you on board!
                    </p>
                    
                    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin: 25px 0; border-left: 4px solid #007bff;">
                        <h3 style="margin: 0 0 15px 0; color: #333; font-size: 16px;">Your Account Details:</h3>
                        <p style="margin: 5px 0; color: #666;"><strong>Email:</strong> ${email}</p>
                        <p style="margin: 5px 0; color: #666;"><strong>Temporary Password:</strong> 
                            <span style="font-family: 'Courier New', monospace; background-color: #e9ecef; padding: 4px 8px; border-radius: 4px; font-weight: bold;">${tempPassword}</span>
                        </p>
                    </div>
                    
                    <div style="text-align: center; margin: 30px 0;">
                        <a href="${invitationUrl}" style="background-color: #007bff; color: white; padding: 15px 30px; text-decoration: none; border-radius: 8px; display: inline-block; font-weight: bold; font-size: 16px; box-shadow: 0 2px 5px rgba(0,123,255,0.3);">
                            Accept Invitation & Set Password
                        </a>
                    </div>
                    
                    <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 8px; margin: 20px 0;">
                        <h4 style="margin: 0 0 10px 0; color: #856404;">🔒 Next Steps:</h4>
                        <ol style="margin: 0; padding-left: 20px; color: #856404;">
                            <li>Click the button above to access the platform</li>
                            <li>Use your email and temporary password to validate your invitation</li>
                            <li>Set a new secure password of your choice</li>
                            <li>Start exploring the platform!</li>
                        </ol>
                    </div>
                    
                    <p style="font-size: 12px; color: #666; margin-top: 30px;">
                        <strong>Security Notice:</strong> If you didn't expect this invitation, please ignore this email.
                    </p>
                    
                    <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
                    <p style="color: #666; font-size: 12px; text-align: center;">This is an automated message, please do not reply.</p>
                </div>
            </div>
        `
    };

    try {
        await transporter.sendMail(mailOptions);
        return true;
    } catch (error) {
        console.error('Error sending invitation email:', error);
        throw new Error('Failed to send invitation email');
    }
}

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
        const frontendUrl = process.env.FRONTEND_URL || `${req.protocol}://${req.get('host')}`;
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

// Validate invitation with token and temporary password (Security Step)
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
                message: 'Invalid invitation token or email'
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

// Get list of users invited by current user
const getMyInvitations = async (req, res) => {
    try {
        const inviterUserId = req.user.userId; // From auth middleware
        
        // Get pagination parameters
        const page = req.query.page || 1;
        const limit = req.query.limit || 10;
        const skip = (parseInt(page) - 1) * parseInt(limit);

        // Get inviter information
        const inviter = await User.findById(inviterUserId);
        if (!inviter) {
            return res.status(401).json({
                status: 'error',
                message: 'User not found'
            });
        }

        // Get total count for pagination
        const total = await User.countDocuments({
            invitedBy: inviterUserId
        });

        // Find users invited by this user with pagination
        const invitedUsers = await User.find({
            invitedBy: inviterUserId
        })
        .select('name email isInvited isActive createdAt invitationCompletedAt lastLogin')
        .sort({ createdAt: -1 }) // Sort by invitation date (newest first)
        .skip(skip)
        .limit(parseInt(limit));

        // Format the response data
        const formattedInvitations = invitedUsers.map(user => {
            let status = 'unknown';
            let statusColor = 'gray';
            
            if (user.isInvited) {
                status = 'pending';
                statusColor = 'orange';
            } else if (user.isActive && user.invitationCompletedAt) {
                status = 'completed';
                statusColor = 'green';
            } else if (user.isActive) {
                status = 'active';
                statusColor = 'blue';
            }

            return {
                id: user._id,
                name: user.name,
                email: user.email,
                status: status,
                statusColor: statusColor,
                invitedAt: user.createdAt,
                completedAt: user.invitationCompletedAt || null,
                lastLogin: user.lastLogin || null,
                isActive: user.isActive,
                isPending: user.isInvited
            };
        });

        // Calculate statistics (for all invitations, not just current page)
        const allInvitedUsers = await User.find({
            invitedBy: inviterUserId
        }).select('isInvited isActive invitationCompletedAt');

        const stats = {
            total: total,
            pending: allInvitedUsers.filter(user => user.isInvited).length,
            completed: allInvitedUsers.filter(user => user.isActive && user.invitationCompletedAt).length,
            active: allInvitedUsers.filter(user => user.isActive && !user.invitationCompletedAt).length
        };

        res.status(200).json({
            status: 'success',
            message: 'Invitations retrieved successfully',
            data: {
                inviter: {
                    id: inviter._id,
                    name: inviter.name,
                    email: inviter.email
                },
                statistics: stats,
                invitations: formattedInvitations,
                pagination: {
                    total,
                    page: parseInt(page),
                    limit: parseInt(limit),
                    pages: Math.ceil(total / parseInt(limit))
                }
            }
        });

    } catch (error) {
        console.error('Get my invitations error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Complete invitation by setting new password (Secure 3-step process)
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
                message: 'Invalid invitation token or email'
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

module.exports = {
    sendInvitation,
    validateInvitation,
    getMyInvitations,
    completeInvitation
}; 