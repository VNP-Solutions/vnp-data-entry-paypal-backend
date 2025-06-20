const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const multer = require('multer');
const multerS3 = require('multer-s3');

// Create S3 client with AWS SDK v3
const s3Client = new S3Client({
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
    region: process.env.AWS_REGION || 'us-east-1'
});

// Configure multer for S3 upload
const upload = multer({
    storage: multerS3({
        s3: s3Client,
        bucket: process.env.AWS_S3_BUCKET,
        metadata: function (req, file, cb) {
            cb(null, { fieldName: file.fieldname });
        },
        key: function (req, file, cb) {
            // Generate unique filename with timestamp
            const timestamp = Date.now();
            const fileName = `uploads/${req.user.userId}/${timestamp}-${file.originalname}`;
            cb(null, fileName);
        }
    }),
    fileFilter: (req, file, cb) => {
        // Only allow Excel files
        if (file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
            file.mimetype === 'application/vnd.ms-excel' ||
            file.originalname.endsWith('.xlsx') ||
            file.originalname.endsWith('.xls')) {
            cb(null, true);
        } else {
            cb(new Error('Only Excel files are allowed'), false);
        }
    },
    limits: {
        fileSize: 10 * 1024 * 1024 // 10MB limit
    }
});

// S3 service functions using AWS SDK v3
const s3Service = {
    // Upload file to S3
    uploadFile: async (file, key) => {
        const command = new PutObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: key,
            Body: file.buffer,
            ContentType: file.mimetype
        });

        try {
            const result = await s3Client.send(command);
            return `https://${process.env.AWS_S3_BUCKET}.s3.${process.env.AWS_REGION || 'us-east-1'}.amazonaws.com/${key}`;
        } catch (error) {
            throw new Error(`S3 upload failed: ${error.message}`);
        }
    },

    // Download file from S3
    downloadFile: async (key) => {
        const command = new GetObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: key
        });

        try {
            const result = await s3Client.send(command);
            
            // Convert stream to buffer for AWS SDK v3
            const chunks = [];
            for await (const chunk of result.Body) {
                chunks.push(chunk);
            }
            return Buffer.concat(chunks);
        } catch (error) {
            throw new Error(`S3 download failed: ${error.message}`);
        }
    },

    // Delete file from S3
    deleteFile: async (key) => {
        const command = new DeleteObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: key
        });

        try {
            await s3Client.send(command);
            return true;
        } catch (error) {
            throw new Error(`S3 delete failed: ${error.message}`);
        }
    },

    // Get file URL (for temporary access)
    getSignedUrl: async (key, expiresIn = 3600) => {
        const command = new GetObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: key
        });

        try {
            return await getSignedUrl(s3Client, command, { expiresIn });
        } catch (error) {
            throw new Error(`S3 signed URL generation failed: ${error.message}`);
        }
    }
};

module.exports = {
    upload,
    s3Service,
    s3Client
}; 