const jwt = require('jsonwebtoken');
const User = require('../models/User');

const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key';

// Middleware to verify JWT token
const authenticateToken = async (req, res, next) => {
    try {
        const authHeader = req.headers['authorization'];
        const token = authHeader && authHeader.split(' ')[1]; // Bearer TOKEN

        if (!token) {
            return res.status(401).json({
                status: 'error',
                message: 'Access token is required'
            });
        }

        // Verify token
        const decoded = jwt.verify(token, JWT_SECRET);
        
        // Check if user still exists
        const user = await User.findById(decoded.userId).select('-password');
        if (!user) {
            return res.status(401).json({
                status: 'error',
                message: 'User not found'
            });
        }

        // Check if user is active
        if (!user.isActive) {
            return res.status(401).json({
                status: 'error',
                message: 'User account is deactivated'
            });
        }

        // Add user info to request
        req.user = decoded;
        req.userData = user;
        
        next();
    } catch (error) {
        if (error.name === 'JsonWebTokenError') {
            return res.status(401).json({
                status: 'error',
                message: 'Invalid token'
            });
        } else if (error.name === 'TokenExpiredError') {
            return res.status(401).json({
                status: 'error',
                message: 'Token expired'
            });
        } else {
            console.error('Auth middleware error:', error);
            return res.status(500).json({
                status: 'error',
                message: 'Internal server error'
            });
        }
    }
};

// Optional authentication middleware (doesn't fail if no token)
const optionalAuth = async (req, res, next) => {
    try {
        const authHeader = req.headers['authorization'];
        const token = authHeader && authHeader.split(' ')[1];

        if (token) {
            const decoded = jwt.verify(token, JWT_SECRET);
            const user = await User.findById(decoded.userId).select('-password');
            
            if (user && user.isActive) {
                req.user = decoded;
                req.userData = user;
            }
        }
        
        next();
    } catch (error) {
        // Continue without authentication if token is invalid
        next();
    }
};

// Role-based authorization middleware
const authorizeRoles = (...roles) => {
    return (req, res, next) => {
        if (!req.user) {
            return res.status(401).json({
                status: 'error',
                message: 'Authentication required'
            });
        }

        if (!roles.includes(req.userData.role)) {
            return res.status(403).json({
                status: 'error',
                message: 'Access denied. Insufficient permissions'
            });
        }

        next();
    };
};

module.exports = {
    authenticateToken,
    optionalAuth,
    authorizeRoles
}; 