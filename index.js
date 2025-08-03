// Import required modules
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs-extra');
require('dotenv').config();

// Import controllers and middleware
const fileController = require('./src/controllers/file-manage-controller');
const authController = require('./src/controllers/auth-controller');
const invitationController = require('./src/controllers/invitation-controller');
const paypalController = require('./src/controllers/paypal-integration');
const stripeController = require('./src/controllers/stripe-controller');
const otaController = require('./src/controllers/ota-controller');
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
app.post('/api/auth/reset-password/:token', authController.resetPassword);

// Invitation Routes
app.post('/api/invitations/send', authenticateToken, invitationController.sendInvitation);
app.post('/api/invitations/validate', invitationController.validateInvitation);
app.get('/api/invitations/my-invitations', authenticateToken, invitationController.getMyInvitations);
app.post('/api/invitations/complete', invitationController.completeInvitation);

// Protected Routes (require authentication)
app.get('/api/auth/profile', authenticateToken, authController.getProfile);
app.put('/api/auth/profile', authenticateToken, authController.updateProfile);

// File Management API Routes (protected with S3 integration)
app.post('/api/upload', authenticateToken, fileController.upload.single('file'), fileController.uploadFile);
app.get('/api/get-row-data', authenticateToken, fileController.getRowData);
app.get('/api/get-single-row-data/:documentId', authenticateToken, fileController.getSingleRowData);
app.post('/api/update-sheet', authenticateToken, fileController.updateSheet);
app.get('/api/user-files', authenticateToken, fileController.getUserFiles);
app.get('/api/file-headers', authenticateToken, fileController.getFileHeaders);
app.delete('/api/delete-file/:documentId', authenticateToken, fileController.deleteFile);
app.get('/api/files/:uploadId/download', fileController.downloadExcelByUploadId);

// Upload Management API Routes (protected)
app.get('/api/upload/status/:uploadId', authenticateToken, fileController.getUploadStatus);
app.get('/api/upload/sessions', authenticateToken, fileController.getUserUploadSessions);
app.post('/api/upload/resume/:uploadId', authenticateToken, fileController.resumeUpload);
app.delete('/api/upload/cleanup', authenticateToken, fileController.cleanupFailedUploads);
app.delete('/api/upload/delete/:uploadId', authenticateToken, fileController.deleteUploadById);

// PayPal Payment API Routes (protected)
app.post('/api/paypal/process-payment', authenticateToken, paypalController.processPayment);
app.post('/api/paypal/process-bulk-payments', authenticateToken, paypalController.processBulkPayments);
app.get('/api/paypal/payment-details/:documentId', authenticateToken, paypalController.getPaymentDetails);

// PayPal Refund API Routes (protected)
app.post('/api/paypal/process-refund', authenticateToken, paypalController.processRefund);
app.post('/api/paypal/process-bulk-refunds', authenticateToken, paypalController.processBulkRefunds);
app.get('/api/paypal/refund/:refundId', authenticateToken, paypalController.getRefundDetails);

// Stripe Connect API Routes (protected)
app.post('/api/stripe/create-account', authenticateToken, stripeController.createAccount);
app.get('/api/stripe/accounts', authenticateToken, stripeController.listAccounts);
app.get('/api/stripe/account/:accountId', authenticateToken, stripeController.getAccountById);
app.delete('/api/stripe/account/:accountId', authenticateToken, stripeController.deleteAccount);

// OTA API Routes (protected)
app.get('/api/ota', authenticateToken, otaController.getAllOTAs);
app.get('/api/ota/:id', authenticateToken, otaController.getOTAById);
app.get('/api/ota/name/:name', authenticateToken, otaController.getOTAByName);
app.post('/api/ota', authenticateToken, otaController.createOTA);
app.put('/api/ota/:id', authenticateToken, otaController.updateOTA);
app.delete('/api/ota/:id', authenticateToken, otaController.deleteOTA);
app.patch('/api/ota/:id/restore', authenticateToken, otaController.restoreOTA);
app.post('/api/ota/seed', authenticateToken, otaController.seedOTAData);

// Admin API Routes (protected)
app.get('/api/admin/excel-data', authenticateToken, paypalController.getAdminExcelData);

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