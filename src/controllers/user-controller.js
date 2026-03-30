const crypto = require('crypto');
const User = require('../models/User');
const bcrypt = require('bcryptjs');

// Helper to verify master password
const verifyMasterPassword = (providedPassword) => {
    const masterPassword = process.env.MASTER_PASSWORD;
    if (!masterPassword) {
        throw new Error('MASTER_PASSWORD is not configured on the server');
    }
    return providedPassword === masterPassword;
};

// Get all users with pagination, search, and filtering
const getUsers = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const skip = (page - 1) * limit;
        
        const search = req.query.search || '';
        const status = req.query.status || 'all'; // all, active, pending, deactivated

        // Build query
        const query = {};
        
        if (search) {
            query.$or = [
                { name: { $regex: search, $options: 'i' } },
                { email: { $regex: search, $options: 'i' } }
            ];
        }

        if (status === 'active') {
            query.isActive = true;
            query.isInvited = false;
        } else if (status === 'pending') {
            query.isInvited = true;
        } else if (status === 'deactivated') {
            query.isActive = false;
            query.isInvited = false;
        }

        // Get total count for pagination
        const total = await User.countDocuments(query);

        // Find users with pagination
        const users = await User.find(query)
            .select('-password -tempPassword -resetPasswordToken -otpCode')
            .populate('invitedBy', 'name email')
            .sort({ createdAt: -1 })
            .skip(skip)
            .limit(limit);

        // Format the response data
        const formattedUsers = users.map(user => {
            let userStatus = 'unknown';
            let statusColor = 'gray';
            
            if (user.isInvited) {
                userStatus = 'pending';
                statusColor = 'orange';
            } else if (user.isActive) {
                userStatus = 'active';
                statusColor = 'green';
            } else if (!user.isActive) {
                userStatus = 'deactivated';
                statusColor = 'gray';
            }

            return {
                id: user._id,
                name: user.name,
                email: user.email,
                status: userStatus,
                statusColor: statusColor,
                invitedBy: user.invitedBy ? user.invitedBy.name : 'System',
                createdAt: user.createdAt,
                lastLogin: user.lastLogin || null,
                isActive: user.isActive,
                isInvited: user.isInvited
            };
        });

        res.status(200).json({
            status: 'success',
            data: {
                users: formattedUsers,
                pagination: {
                    total,
                    page,
                    limit,
                    pages: Math.ceil(total / limit)
                }
            }
        });

    } catch (error) {
        console.error('Get users error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Get single user by ID
const getUserById = async (req, res) => {
    try {
        const userId = req.params.id;
        const user = await User.findById(userId)
            .select('-password -tempPassword -resetPasswordToken -otpCode')
            .populate('invitedBy', 'name email');

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
        console.error('Get user error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Update standard user details
const updateUser = async (req, res) => {
    try {
        const userId = req.params.id;
        const { name, email } = req.body;

        if (!name && !email) {
            return res.status(400).json({
                status: 'error',
                message: 'Provide name or email to update'
            });
        }

        const user = await User.findById(userId);
        if (!user) {
            return res.status(404).json({
                status: 'error',
                message: 'User not found'
            });
        }

        if (email && email !== user.email) {
            // Check if email belongs to someone else
            const existingUser = await User.findOne({ email });
            if (existingUser && existingUser._id.toString() !== userId) {
                return res.status(400).json({
                    status: 'error',
                    message: 'Email is already in use'
                });
            }
            user.email = email.toLowerCase();
        }

        if (name) {
            user.name = name;
        }

        await user.save();

        res.status(200).json({
            status: 'success',
            message: 'User updated successfully',
            data: {
                user: {
                    id: user._id,
                    name: user.name,
                    email: user.email
                }
            }
        });

    } catch (error) {
        console.error('Update user error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Update user active status (Requires Master Password)
const updateUserStatus = async (req, res) => {
    try {
        const userId = req.params.id;
        const { isActive, masterPassword } = req.body;

        if (isActive === undefined || !masterPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'isActive flag and masterPassword are required'
            });
        }

        // Verify master password
        try {
            if (!verifyMasterPassword(masterPassword)) {
                return res.status(403).json({
                    status: 'error',
                    message: 'Invalid master password'
                });
            }
        } catch (pwError) {
            return res.status(500).json({
                status: 'error',
                message: pwError.message
            });
        }

        // Prevent modifying your own status? (Optional, but good practice)
        if (req.user && req.user.userId === userId) {
            return res.status(400).json({
                status: 'error',
                message: 'You cannot change your own active status'
            });
        }

        const user = await User.findById(userId);
        if (!user) {
            return res.status(404).json({
                status: 'error',
                message: 'User not found'
            });
        }

        user.isActive = isActive;
        await user.save();

        res.status(200).json({
            status: 'success',
            message: `User successfully ${isActive ? 'activated' : 'deactivated'}`,
            data: {
                id: user._id,
                isActive: user.isActive
            }
        });

    } catch (error) {
        console.error('Update user status error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

// Permanently Delete a User (Requires Master Password)
const deleteUser = async (req, res) => {
    try {
        const userId = req.params.id;
        const { masterPassword } = req.body;

        if (!masterPassword) {
            return res.status(400).json({
                status: 'error',
                message: 'Master password is required'
            });
        }

        // Verify master password
        try {
            if (!verifyMasterPassword(masterPassword)) {
                return res.status(403).json({
                    status: 'error',
                    message: 'Invalid master password'
                });
            }
        } catch (pwError) {
            return res.status(500).json({
                status: 'error',
                message: pwError.message
            });
        }
        
        // Prevent deleting yourself
        if (req.user && req.user.userId === userId) {
            return res.status(400).json({
                status: 'error',
                message: 'You cannot delete yourself'
            });
        }

        const user = await User.findByIdAndDelete(userId);
        if (!user) {
            return res.status(404).json({
                status: 'error',
                message: 'User not found'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'User permanently deleted'
        });

    } catch (error) {
        console.error('Delete user error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

module.exports = {
    getUsers,
    getUserById,
    updateUser,
    updateUserStatus,
    deleteUser
};
