// Import required modules
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs-extra');
require('dotenv').config();

// Import controllers and middleware
const fileController = require('./src/controllers/file-manage-controller');
const authController = require('./src/controllers/auth-controller');
const paypalController = require('./src/controllers/paypal-integration');
const { authenticateToken } = require('./src/middleware/auth');

// Import database connection
const connectDB = require('./src/config/database');

// Create an instance of Express
const app = express();

// Connect to MongoDB
connectDB();

// Custom HTTP request logging middleware
const requestLogger = (req, res, next) => {
    const startTime = Date.now();
    const timestamp = new Date().toISOString();
    
    // Store original end method
    const originalEnd = res.end;
    
    // Override end method to capture response time
    res.end = function(...args) {
        const endTime = Date.now();
        const duration = endTime - startTime;
        
        // Color coding for status codes
        let statusColor = '';
        if (res.statusCode >= 200 && res.statusCode < 300) {
            statusColor = '\x1b[32m'; // Green for success
        } else if (res.statusCode >= 300 && res.statusCode < 400) {
            statusColor = '\x1b[33m'; // Yellow for redirects
        } else if (res.statusCode >= 400 && res.statusCode < 500) {
            statusColor = '\x1b[31m'; // Red for client errors
        } else if (res.statusCode >= 500) {
            statusColor = '\x1b[35m'; // Magenta for server errors
        }
        
        // Reset color
        const resetColor = '\x1b[0m';
        
        // Method color
        const methodColor = '\x1b[36m'; // Cyan for method
        
        // Log the request
        console.log(
            `[${timestamp}] ` +
            `${methodColor}${req.method}${resetColor} ` +
            `${req.originalUrl} ` +
            `${statusColor}${res.statusCode}${resetColor} ` +
            `- ${duration}ms`
        );
        
        // Call original end method
        originalEnd.apply(this, args);
    };
    
    next();
};

// Apply request logging middleware
app.use(requestLogger);

// Enable CORS
app.use(cors());

// Enable JSON parsing for request bodies
app.use(express.json());

// Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

// Ensure the files directory exists
const filesDir = path.join(__dirname, 'public', 'files');
fs.ensureDirSync(filesDir);

// Authentication Routes
app.post('/api/auth/register', authController.register);
app.post('/api/auth/login', authController.login);
app.post('/api/auth/verify-otp', authController.verifyOTP);
app.post('/api/auth/resend-otp', authController.resendOTP);
app.post('/api/auth/forgot-password', authController.forgotPassword);
app.get('/api/auth/validate-reset-token/:token', authController.validateResetToken);
app.post('/api/auth/reset-password/:token', authController.resetPassword);

// Protected Routes (require authentication)
app.get('/api/auth/profile', authenticateToken, authController.getProfile);
app.put('/api/auth/profile', authenticateToken, authController.updateProfile);

// File Management API Routes (protected with S3 integration)
app.post('/api/upload', authenticateToken, fileController.upload.single('file'), fileController.uploadFile);
app.get('/api/get-row-data', authenticateToken, fileController.getRowData);
app.post('/api/update-sheet', authenticateToken, fileController.updateSheet);
app.get('/api/user-files', authenticateToken, fileController.getUserFiles);
app.get('/api/file-headers', authenticateToken, fileController.getFileHeaders);
app.delete('/api/delete-file/:documentId', authenticateToken, fileController.deleteFile);

// Upload Management API Routes (protected)
app.get('/api/upload/status/:uploadId', authenticateToken, fileController.getUploadStatus);
app.get('/api/upload/sessions', authenticateToken, fileController.getUserUploadSessions);
app.post('/api/upload/resume/:uploadId', authenticateToken, fileController.resumeUpload);
app.delete('/api/upload/cleanup', authenticateToken, fileController.cleanupFailedUploads);

// PayPal Payment API Routes (protected)
app.post('/api/paypal/process-payment', authenticateToken, paypalController.processPayment);

// Health check route
app.get('/api/health', (req, res) => {
    res.status(200).json({
        status: 'success',
        message: 'Server is running',
        timestamp: new Date().toISOString()
    });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('❌ Error:', err.stack);
    res.status(500).json({
        status: 'error',
        message: 'Something went wrong!',
        error: process.env.NODE_ENV === 'development' ? err.message : 'Internal server error'
    });
});

// 404 handler
app.use('*', (req, res) => {
    res.status(404).json({
        status: 'error',
        message: 'Route not found'
    });
});

// Define a port
const PORT = process.env.PORT || 3001;

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});