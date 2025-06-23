const mongoose = require('mongoose');
const XLSX = require('xlsx');
const ExcelData = require('../models/ExcelData');
const UploadSession = require('../models/UploadSession');
const { upload, s3Service } = require('../config/s3');
const { encryptCardData, decryptCardData } = require('../utils/encryption');

// Helper function to advance a column name
function nextCol(col) {
    const ords = col.split('').map(c => c.charCodeAt(0) - 65);
    let carry = 1;
    for (let i = ords.length - 1; i >= 0; i--) {
        const v = ords[i] + carry;
        ords[i] = v % 26;
        carry = Math.floor(v / 26);
    }
    if (carry) ords.unshift(carry - 1);
    return ords.map(n => String.fromCharCode(n + 65)).join('');
}

// Generate unique upload ID
function generateUploadId() {
    return `upload_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Optimized batch processing with efficient duplicate checking - GLOBAL DUPLICATE PREVENTION
async function processBatchOptimized(excelDataRecords, uploadId, batchNumber, existingDuplicatesSet) {
    try {
        // Filter out duplicates using Set lookup (O(1) instead of O(n))
        // Now checking globally across all users to prevent multiple payments
        const filteredRecords = excelDataRecords.filter(record => {
            const duplicateKey = `${record['Portfolio']}|${record['Reservation ID']}`;
            return !existingDuplicatesSet.has(duplicateKey);
        });

        if (filteredRecords.length === 0) {
            return 0;
        }

        // Use bulkWrite for better performance with enhanced error handling
        try {
            const result = await ExcelData.bulkWrite(
                filteredRecords.map(record => ({
                    insertOne: { document: record }
                })), 
                { ordered: false } // Continue processing even if some records fail
            );
            
            return result.insertedCount;
        } catch (bulkWriteError) {
            // Handle duplicate key errors gracefully (database-level safety net)
            if (bulkWriteError.code === 11000 || bulkWriteError.name === 'BulkWriteError') {
                const successfulInserts = bulkWriteError.result ? bulkWriteError.result.insertedCount : 0;
                const duplicateErrors = bulkWriteError.writeErrors ? bulkWriteError.writeErrors.filter(err => err.code === 11000).length : 0;
                
                return successfulInserts;
            }
            throw bulkWriteError; // Re-throw if it's not a duplicate key error
        }
    } catch (error) {
        console.error(`Error processing batch ${batchNumber}:`, error);
        throw error;
    }
}

// Efficient duplicate checking using Set for O(1) lookup - GLOBAL CHECK ACROSS ALL USERS
async function getExistingDuplicatesSet() {
    try {
        // Get all existing Portfolio + Reservation ID combinations from ALL users
        const existingRecords = await ExcelData.find(
            {}, // No userId filter - check across ALL users
            { 'Portfolio': 1, 'Reservation ID': 1, userId: 1, uploadId: 1, fileName: 1 }
        ).lean(); // Use lean() for better performance

        // Create a Set for O(1) lookup with detailed duplicate info
        const duplicatesSet = new Set();
        const duplicateDetails = new Map(); // Store additional info about existing records
        
        existingRecords.forEach(record => {
            const key = `${record['Portfolio']}|${record['Reservation ID']}`;
            duplicatesSet.add(key);
            
            // Store details about the existing record for better error reporting
            if (!duplicateDetails.has(key)) {
                duplicateDetails.set(key, {
                    existingUserId: record.userId,
                    existingUploadId: record.uploadId,
                    existingFileName: record.fileName,
                    portfolio: record['Portfolio'],
                    reservationId: record['Reservation ID']
                });
            }
        });

        return { duplicatesSet, duplicateDetails };
    } catch (error) {
        console.error('Error getting existing duplicates:', error);
        throw error;
    }
}

// Check for existing upload session
async function checkExistingUpload(fileName, userId) {
    const existingSession = await UploadSession.findOne({
        userId: userId,
        fileName: fileName,
        status: { $in: ['uploading', 'processing'] }
    });
    
    if (existingSession) {
        return {
            exists: true,
            session: existingSession
        };
    }
    
    return { exists: false };
}

// Upload file API with optimized time complexity
const uploadFile = async (req, res) => {
    const session = await mongoose.startSession();
    session.startTransaction();
    
    try {
        // Check if file was uploaded
        if (!req.file) {
            return res.status(400).json({
                status: 'error',
                message: 'No file uploaded'
            });
        }

        const fileName = req.file.originalname;
        const userId = req.user.userId;
        const vnpWorkId = req.body.vnpWorkId;
        const skipDuplicateCheck = req.body.skipDuplicateCheck === 'true';

        // Check for existing upload session
        const existingCheck = await checkExistingUpload(fileName, userId);
        if (existingCheck.exists) {
            return res.status(409).json({
                status: 'error',
                message: 'File is already being processed',
                data: {
                    uploadId: existingCheck.session.uploadId,
                    status: existingCheck.session.status,
                    processedRows: existingCheck.session.processedRows,
                    totalRows: existingCheck.session.totalRows
                }
            });
        }



        // Download file from S3 to process
        const fileBuffer = await s3Service.downloadFile(req.file.key);
        
        // Read the Excel file
        const workbook = XLSX.read(fileBuffer);
        const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
        const cellA1 = firstSheet['A1'] ? firstSheet['A1'].v : null;

        // Validate the content of cell A1
        if (cellA1 !== 'Expedia ID') {
            await s3Service.deleteFile(req.file.key);
            return res.status(400).json({
                status: 'error',
                message: 'Invalid VNP Work file. Cell A1 must contain "Expedia ID"'
            });
        }

        // Get sheet range and headers
        const range = firstSheet['!ref'];
        const totalRows = XLSX.utils.decode_range(range).e.r + 1;
        const totalCols = XLSX.utils.decode_range(range).e.c + 1;

        // Get headers from the first row
        const headers = [];
        for (let col = 0; col < totalCols; col++) {
            const cell = firstSheet[XLSX.utils.encode_cell({ r: 0, c: col })];
            if (cell) {
                const headerName = cell.v.toString().trim();
                headers.push(headerName);
            }
        }



        // Generate unique upload ID
        const uploadId = generateUploadId();

        // Create upload session
        const uploadSession = new UploadSession({
            uploadId: uploadId,
            userId: userId,
            fileName: fileName,
            originalFileName: fileName,
            s3Key: req.file.key,
            totalRows: totalRows - 1,
            status: 'processing',
            vnpWorkId: vnpWorkId,
            headers: headers,
            batchSize: 100
        });

        await uploadSession.save({ session });

        // Get existing duplicates Set for O(1) lookup (only if not skipping)
        let existingDuplicatesSet = new Set();
        let duplicateDetails = new Map();
        let duplicateAnalysis = null;
        let duplicateRows = [];
        
        if (!skipDuplicateCheck) {
    
            const { duplicatesSet, duplicateDetails: details } = await getExistingDuplicatesSet();
            existingDuplicatesSet = duplicatesSet;
            duplicateDetails = details;
        }

        // Process data in optimized batches
        const batchSize = 100;
        let totalProcessed = 0;
        let totalDuplicates = 0;
        let batchNumber = 1;
        const allRecords = []; // Collect all records for duplicate analysis

        // Process file in chunks to avoid memory issues
        for (let startRow = 1; startRow < totalRows; startRow += batchSize) {
            const endRow = Math.min(startRow + batchSize - 1, totalRows - 1);
            const excelDataRecords = [];
            
            // Process current batch
            for (let row = startRow; row <= endRow; row++) {
                const rowData = [];
                
                for (let col = 0; col < totalCols; col++) {
                    const cell = firstSheet[XLSX.utils.encode_cell({ r: row, c: col })];
                    if (cell) {
                        rowData.push(cell.v !== undefined ? cell.v.toString() : '');
                    } else {
                        rowData.push('');
                    }
                }

                const rowObject = headers.reduce((acc, header, index) => {
                    acc[header.trim()] = rowData[index]?.trim() || null;
                    return acc;
                }, {});

                if (Object.keys(rowObject).length > 0 && rowObject['Expedia ID']) {
                    const mappedData = {
                        userId: userId,
                        uploadId: uploadId,
                        fileName: fileName,
                        uploadStatus: 'processing',
                        rowNumber: row,
                        'Expedia ID': rowObject['Expedia ID'],
                        'Batch': rowObject['Batch'],
                        'Posting Type': rowObject['Posting Type'],
                        'Portfolio': rowObject['Portfolio'],
                        'Hotel Name': rowObject['Hotel Name'],
                        'Reservation ID': rowObject['Reservation ID'],
                        'Hotel Confirmation Code': rowObject['Hotel Confirmation Code'],
                        'Name': rowObject['Name'],
                        'Check In': rowObject['Check In'],
                        'Check Out': rowObject['Check Out'],
                        'Curency': rowObject['Curency'],
                        'Amount to charge': rowObject['Amount to charge'],
                        'Charge status': rowObject['Charge status'],
                        'Card first 4': rowObject['Card first 4'],
                        'Card last 12': rowObject['Card last 12'],
                        'Card Expire': rowObject['Card Expire'],
                        'Card CVV': rowObject['Card CVV'],
                        'Soft Descriptor': rowObject['Soft Descriptor'] || rowObject['BT MAID'],
                        'VNP Work ID': rowObject['VNP Work ID'],
                        'Status': rowObject['Status']
                    };

                    const encryptedData = encryptCardData(mappedData);
                    excelDataRecords.push(encryptedData);
                    
                    // Collect for duplicate analysis
                    allRecords.push({
                        portfolio: rowObject['Portfolio'],
                        reservationId: rowObject['Reservation ID'],
                        expediaId: rowObject['Expedia ID'],
                        rowNumber: row + 1,
                        isDuplicate: !skipDuplicateCheck && existingDuplicatesSet.has(`${rowObject['Portfolio']}|${rowObject['Reservation ID']}`)
                    });
                }
            }

            // Process batch if there are records
            if (excelDataRecords.length > 0) {
                const processedCount = await processBatchOptimized(excelDataRecords, uploadId, batchNumber, existingDuplicatesSet);
                // For duplicate analysis, collect which rows were skipped
                if (!skipDuplicateCheck) {
                    excelDataRecords.forEach((record, idx) => {
                        const orig = allRecords[allRecords.length - excelDataRecords.length + idx];
                        const duplicateKey = `${record['Portfolio']}|${record['Reservation ID']}`;
                        if (existingDuplicatesSet.has(duplicateKey)) {
                            duplicateRows.push({
                                rowNumber: orig.rowNumber,
                                portfolio: orig.portfolio,
                                reservationId: orig.reservationId,
                                expediaId: orig.expediaId
                            });
                        }
                    });
                }
                totalProcessed += processedCount;
                totalDuplicates += (excelDataRecords.length - processedCount);
                
                // Update session progress
                await UploadSession.findOneAndUpdate(
                    { uploadId: uploadId },
                    { 
                        processedRows: totalProcessed,
                        status: totalProcessed >= (totalRows - 1) ? 'completed' : 'processing'
                    },
                    { session }
                );
            }

            batchNumber++;
            
            // Minimal delay to prevent overwhelming the database
            if (batchNumber % 10 === 0) {
                await new Promise(resolve => setTimeout(resolve, 50));
            }
        }

        // Create duplicate analysis if needed
        if (!skipDuplicateCheck) {
            const totalRecords = allRecords.length;
            // Find all duplicates for reporting with detailed information
            duplicateRows = allRecords.filter(r => r.isDuplicate).map(r => {
                const duplicateKey = `${r.portfolio}|${r.reservationId}`;
                const existingRecord = duplicateDetails.get(duplicateKey);
                
                return {
                    rowNumber: r.rowNumber,
                    portfolio: r.portfolio,
                    reservationId: r.reservationId,
                    expediaId: r.expediaId,
                    // Include info about who originally uploaded this record
                    originallyUploadedBy: existingRecord ? {
                        userId: existingRecord.existingUserId,
                        uploadId: existingRecord.existingUploadId,
                        fileName: existingRecord.existingFileName
                    } : null,
                    reason: 'Duplicate reservation already exists in system (prevents multiple payments)'
                };
            });
            
            duplicateAnalysis = {
                totalRecords: totalRecords,
                duplicateRecords: totalDuplicates,
                newRecords: totalProcessed,
                duplicatePercentage: totalRecords > 0 ? Math.round((totalDuplicates / totalRecords) * 100) : 0,
                duplicates: duplicateRows,
                summary: {
                    willBeSkipped: totalDuplicates,
                    willBeInserted: totalProcessed,
                    recommendation: totalDuplicates > 0 ? 
                        `${totalDuplicates} records were skipped to prevent duplicate payments. These Portfolio + Reservation ID combinations already exist in the system (uploaded by other users or previous uploads).` :
                        'No duplicates found. All records were inserted.',
                    globalDuplicateCheck: true,
                    preventMultiplePayments: true
                }
            };
        }

        // Mark session as completed
        await UploadSession.findOneAndUpdate(
            { uploadId: uploadId },
            { 
                status: 'completed',
                completedAt: new Date()
            },
            { session }
        );

        // Delete file from S3 after successful processing
        await s3Service.deleteFile(req.file.key);

        // Commit transaction
        await session.commitTransaction();



        res.status(200).json({
            status: 'success',
            message: 'File uploaded and processed successfully',
            data: {
                uploadId: uploadId,
                fileName: fileName,
                totalRows: totalRows - 1,
                totalColumns: totalCols,
                rowsProcessed: totalProcessed,
                headers: headers,
                status: 'completed',
                duplicateAnalysis: duplicateAnalysis
            }
        });

    } catch (error) {
        // Rollback transaction on error
        await session.abortTransaction();
        
        console.error('Upload error:', error);
        
        // Update session status to failed if it exists
        if (req.file && req.file.key) {
            try {
                await UploadSession.findOneAndUpdate(
                    { s3Key: req.file.key },
                    { 
                        status: 'failed',
                        errorMessage: error.message
                    }
                );
            } catch (sessionError) {
                console.error('Error updating session status:', sessionError);
            }
        }

        res.status(500).json({
            status: 'error',
            message: 'Error processing file',
            error: error.message
        });
    } finally {
        session.endSession();
    }
};

// Get all row data for a user
const getRowData = async (req, res) => {
    try {
        const { limit = 10, page = 1, chargeStatus, search } = req.query;
        const userId = req.user.userId;

        const skip = (parseInt(page) - 1) * parseInt(limit);

        // Build query object
        const query = { userId: userId };

        // Add filter for Charge status if provided
        if (chargeStatus && chargeStatus.trim() !== '') {
            query['Charge status'] = { $regex: chargeStatus, $options: 'i' }; // Case-insensitive match
        }

        // Add search functionality across multiple fields if provided
        if (search && search.trim() !== '') {
            const searchRegex = { $regex: search, $options: 'i' }; // Case-insensitive search
            query.$or = [
                { 'Reservation ID': searchRegex },
                { 'Expedia ID': searchRegex },
                { 'Hotel Name': searchRegex },
                { 'Name': searchRegex },
                { 'Portfolio': searchRegex },
                { 'Batch': searchRegex },    
            ];
        }

        // Get paginated data for the user with filters
        const rowData = await ExcelData.find(query)
            .skip(skip)
            .limit(parseInt(limit))
            .sort({ createdAt: -1 });

        // Get total count for pagination with same filters
        const totalCount = await ExcelData.countDocuments(query);

        res.status(200).json({
            status: 'success',
            data: {
                rows: rowData.map(row => {
                    // Remove MongoDB internal fields and userId, return only the Excel data
                    const { _id, userId, __v, createdAt, updatedAt, ...excelData } = row.toObject();
                    
                    // Decrypt sensitive card data before returning
                    const decryptedData = decryptCardData(excelData);
                    
                    return {
                        id: _id,
                        ...decryptedData,
                        createdAt: createdAt
                    };
                }),
                pagination: {
                    currentPage: parseInt(page),
                    totalPages: Math.ceil(totalCount / parseInt(limit)),
                    totalCount: totalCount,
                    limit: parseInt(limit)
                },
                filters: {
                    chargeStatus: chargeStatus || null,
                    search: search || null
                }
            }
        });

    } catch (error) {
        console.error('Get row data error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error retrieving row data',
            error: error.message
        });
    }
};
// Get single row data for a user
const getSingleRowData = async (req, res) => {
    try {
        const { documentId } = req.params;
        const userId = req.user.userId;

        const rowData = await ExcelData.findOne({
            _id: documentId,
            userId: userId
        });

        if (!rowData) {
            return res.status(404).json({
                status: 'error',
                message: 'Row data not found'
            });
        }

        // Remove MongoDB internal fields and userId, return only the Excel data
        const { _id, userId: userIdField, __v, createdAt, updatedAt, ...excelData } = rowData.toObject();
        
        // Decrypt sensitive card data before returning
        const decryptedData = decryptCardData(excelData);
        
        res.status(200).json({
            status: 'success',
            data: {
                id: _id,
                ...decryptedData,
                createdAt: createdAt
            }
        });
        
    } catch (error) {
        console.error('Get single row data error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error retrieving single row data',
            error: error.message
        });
    }
};

// Update sheet data in database
const updateSheet = async (req, res) => {
    try {
        const { documentId, ...updateData } = req.body;
        const userId = req.user.userId;

        if (!documentId) {
            return res.status(400).json({
                status: 'error',
                message: 'Document ID is required'
            });
        }

        // Map the update data using the same logic
        const mappedUpdateData = {
            'Expedia ID': updateData['Expedia ID'],
            'Batch': updateData['Batch'],
            'Posting Type': updateData['Posting Type'],
            'Portfolio': updateData['Portfolio'],
            'Hotel Name': updateData['Hotel Name'],
            'Reservation ID': updateData['Reservation ID'],
            'Hotel Confirmation Code': updateData['Hotel Confirmation Code'],
            'Name': updateData['Name'],
            'Check In': updateData['Check In'],
            'Check Out': updateData['Check Out'],
            'Curency': updateData['Curency'],
            'Amount to charge': updateData['Amount to charge'],
            'Charge status': updateData['Charge status'],
            'Card first 4': updateData['Card first 4'],
            'Card last 12': updateData['Card last 12'],
            'Card Expire': updateData['Card Expire'],
            'Card CVV': updateData['Card CVV'],
            'Soft Descriptor': updateData['Soft Descriptor'],
            'VNP Work ID': updateData['VNP Work ID'],
            'Status': updateData['Status']
        };

        // Remove undefined values
        Object.keys(mappedUpdateData).forEach(key => {
            if (mappedUpdateData[key] === undefined) {
                delete mappedUpdateData[key];
            }
        });

        // Encrypt sensitive card data before updating
        const encryptedUpdateData = encryptCardData(mappedUpdateData);

        // Update the document with new data
        const updateResult = await ExcelData.findOneAndUpdate(
            {
                _id: documentId,
                userId: userId
            },
            {
                $set: encryptedUpdateData
            },
            { new: true }
        );

        if (!updateResult) {
            return res.status(404).json({
                status: 'error',
                message: 'Document not found'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'Data updated successfully',
            data: (() => {
                const { _id, userId, __v, createdAt, updatedAt, ...excelData } = updateResult.toObject();
                // Decrypt sensitive card data before returning
                return decryptCardData(excelData);
            })()
        });

    } catch (error) {
        console.error('Update sheet error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error updating data',
            error: error.message
        });
    }
};

// Get all data for a user
const getUserFiles = async (req, res) => {
    try {
        const userId = req.user.userId;

        // Get total count of records for the user
        const totalCount = await ExcelData.countDocuments({ userId: userId });

        // Get recent data (last 10 records)
        const recentData = await ExcelData.find({ userId: userId })
            .sort({ createdAt: -1 })
            .limit(10);

        res.status(200).json({
            status: 'success',
            data: {
                totalRecords: totalCount,
                recentData: recentData.map(record => {
                    const { _id, userId, __v, createdAt, updatedAt, ...excelData } = record.toObject();
                    // Decrypt sensitive card data before returning
                    const decryptedData = decryptCardData(excelData);
                    return {
                        id: _id,
                        ...decryptedData,
                        createdAt: createdAt
                    };
                })
            }
        });

    } catch (error) {
        console.error('Get user files error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error retrieving user data',
            error: error.message
        });
    }
};

// Delete specific data record
const deleteFile = async (req, res) => {
    try {
        const { documentId } = req.params;
        const userId = req.user.userId;

        if (!documentId) {
            return res.status(400).json({
                status: 'error',
                message: 'Document ID is required'
            });
        }

        // Delete the specific document
        const deleteResult = await ExcelData.findOneAndDelete({
            _id: documentId,
            userId: userId
        });

        if (!deleteResult) {
            return res.status(404).json({
                status: 'error',
                message: 'Document not found'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'Data deleted successfully'
        });

    } catch (error) {
        console.error('Delete file error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error deleting data',
            error: error.message
        });
    }
};

// Get headers from user's data
const getFileHeaders = async (req, res) => {
    try {
        const userId = req.user.userId;

        // Get one record to extract headers from the data object
        const sampleRecord = await ExcelData.findOne({ userId: userId });

        if (!sampleRecord) {
            return res.status(404).json({
                status: 'error',
                message: 'No data found for user'
            });
        }

        // Extract headers from the document properties (excluding MongoDB fields)
        const { _id, userId: user, __v, createdAt, updatedAt, ...excelData } = sampleRecord.toObject();
        const headers = Object.keys(excelData);

        res.status(200).json({
            status: 'success',
            data: {
                headers: headers,
                totalHeaders: headers.length
            }
        });

    } catch (error) {
        console.error('Get file headers error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error retrieving headers',
            error: error.message
        });
    }
};

// Get upload status
const getUploadStatus = async (req, res) => {
    try {
        const { uploadId } = req.params;
        const userId = req.user.userId;

        const uploadSession = await UploadSession.findOne({
            uploadId: uploadId,
            userId: userId
        });

        if (!uploadSession) {
            return res.status(404).json({
                status: 'error',
                message: 'Upload session not found'
            });
        }

        res.status(200).json({
            status: 'success',
            data: {
                uploadId: uploadSession.uploadId,
                fileName: uploadSession.fileName,
                status: uploadSession.status,
                totalRows: uploadSession.totalRows,
                processedRows: uploadSession.processedRows,
                progress: uploadSession.status === 'completed' ? 100 : 
                    (uploadSession.totalRows > 0 ? 
                        Math.round((uploadSession.processedRows / uploadSession.totalRows) * 100) : 0),
                startedAt: uploadSession.startedAt,
                completedAt: uploadSession.completedAt,
                errorMessage: uploadSession.errorMessage,
            }
        });

    } catch (error) {
        console.error('Error getting upload status:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error getting upload status',
            error: error.message
        });
    }
};

// Get all upload sessions for user
const getUserUploadSessions = async (req, res) => {
    try {
        const userId = req.user.userId;
        const { status, limit = 20, page = 1, search } = req.query;

        const query = { userId: userId };
        if (status) {
            query.status = status;
        }

        if (search && search.trim() !== '') {
            const searchRegex = { $regex: search, $options: 'i' }; // Case-insensitive search
            query.$or = [
                { 'fileName': searchRegex },
            ];
        }
        const sessions = await UploadSession.find(query)
            .sort({ createdAt: -1 })
            .limit(parseInt(limit))
            .skip((parseInt(page) - 1) * parseInt(limit));

        const total = await UploadSession.countDocuments(query);

        res.status(200).json({
            status: 'success',
            data: {
                sessions: sessions.map(session => ({
                    uploadId: session.uploadId,
                    fileName: session.fileName,
                    status: session.status,
                    totalRows: session.totalRows,
                    processedRows: session.processedRows,
                    progress: session.status === 'completed' ? 100 : 
                        (session.totalRows > 0 ? 
                            Math.round((session.processedRows / session.totalRows) * 100) : 0),
                    startedAt: session.startedAt,
                    completedAt: session.completedAt,
                    vnpWorkId: session.vnpWorkId
                })),
                pagination: {
                    total,
                    page: parseInt(page),
                    limit: parseInt(limit),
                    pages: Math.ceil(total / parseInt(limit))
                }
            }
        });

    } catch (error) {
        console.error('Error getting user upload sessions:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error getting upload sessions',
            error: error.message
        });
    }
};

// Resume failed upload
const resumeUpload = async (req, res) => {
    const session = await mongoose.startSession();
    session.startTransaction();

    try {
        const { uploadId } = req.params;
        const userId = req.user.userId;

        const uploadSession = await UploadSession.findOne({
            uploadId: uploadId,
            userId: userId,
            status: 'failed'
        });

        if (!uploadSession) {
            return res.status(404).json({
                status: 'error',
                message: 'Failed upload session not found'
            });
        }

        // Check if retry limit exceeded
        if (uploadSession.retryCount >= uploadSession.maxRetries) {
            return res.status(400).json({
                status: 'error',
                message: 'Maximum retry attempts exceeded'
            });
        }

        // Download file from S3
        const fileBuffer = await s3Service.downloadFile(uploadSession.s3Key);
        const workbook = XLSX.read(fileBuffer);
        const firstSheet = workbook.Sheets[workbook.SheetNames[0]];

        // Get sheet range
        const range = firstSheet['!ref'];
        const totalRows = XLSX.utils.decode_range(range).e.r + 1;
        const totalCols = XLSX.utils.decode_range(range).e.c + 1;

        // Get headers from the first row
        const headers = [];
        for (let col = 0; col < totalCols; col++) {
            const cell = firstSheet[XLSX.utils.encode_cell({ r: 0, c: col })];
            if (cell) {
                const headerName = cell.v.toString().trim();
                headers.push(headerName);
            }
        }

        // Update session status to processing
        await UploadSession.findOneAndUpdate(
            { uploadId: uploadId },
            { 
                status: 'processing',
                retryCount: uploadSession.retryCount + 1
            },
            { session }
        );

        // Delete existing partial data for this upload
        await ExcelData.deleteMany({ uploadId: uploadId }, { session });

        // Get existing duplicates Set for global checking during resume
        const { duplicatesSet: existingDuplicatesSet } = await getExistingDuplicatesSet();

        // Process remaining data from where it left off
        const batchSize = uploadSession.batchSize || 100;
        let totalProcessed = 0;
        let batchNumber = 1;

        for (let startRow = 1; startRow < totalRows; startRow += batchSize) {
            const endRow = Math.min(startRow + batchSize - 1, totalRows - 1);
            const excelDataRecords = [];
            
            for (let row = startRow; row <= endRow; row++) {
                const rowData = [];
                
                for (let col = 0; col < totalCols; col++) {
                    const cell = firstSheet[XLSX.utils.encode_cell({ r: row, c: col })];
                    if (cell) {
                        rowData.push(cell.v !== undefined ? cell.v.toString() : '');
                    } else {
                        rowData.push('');
                    }
                }

                const rowObject = headers.reduce((acc, header, index) => {
                    acc[header.trim()] = rowData[index]?.trim() || null;
                    return acc;
                }, {});

                if (Object.keys(rowObject).length > 0 && rowObject['Expedia ID']) {
                    const mappedData = {
                        userId: userId,
                        uploadId: uploadId,
                        fileName: uploadSession.fileName,
                        uploadStatus: 'processing',
                        rowNumber: row,
                        'Expedia ID': rowObject['Expedia ID'],
                        'Batch': rowObject['Batch'],
                        'Posting Type': rowObject['Posting Type'],
                        'Portfolio': rowObject['Portfolio'],
                        'Hotel Name': rowObject['Hotel Name'],
                        'Reservation ID': rowObject['Reservation ID'],
                        'Hotel Confirmation Code': rowObject['Hotel Confirmation Code'],
                        'Name': rowObject['Name'],
                        'Check In': rowObject['Check In'],
                        'Check Out': rowObject['Check Out'],
                        'Curency': rowObject['Curency'],
                        'Amount to charge': rowObject['Amount to charge'],
                        'Charge status': rowObject['Charge status'],
                        'Card first 4': rowObject['Card first 4'],
                        'Card last 12': rowObject['Card last 12'],
                        'Card Expire': rowObject['Card Expire'],
                        'Card CVV': rowObject['Card CVV'],
                        'Soft Descriptor': rowObject['Soft Descriptor'] || rowObject['BT MAID'],
                        'VNP Work ID': rowObject['VNP Work ID'],
                        'Status': rowObject['Status']
                    };

                    const encryptedData = encryptCardData(mappedData);
                    excelDataRecords.push(encryptedData);
                }
            }

            if (excelDataRecords.length > 0) {
                const processedCount = await processBatchOptimized(excelDataRecords, uploadId, batchNumber, existingDuplicatesSet);
                totalProcessed += processedCount;
                
                await UploadSession.findOneAndUpdate(
                    { uploadId: uploadId },
                    { 
                        processedRows: totalProcessed,
                        status: totalProcessed >= (totalRows - 1) ? 'completed' : 'processing'
                    },
                    { session }
                );
            }

            batchNumber++;
            
            if (batchNumber % 5 === 0) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        // Mark session as completed
        await UploadSession.findOneAndUpdate(
            { uploadId: uploadId },
            { 
                status: 'completed',
                completedAt: new Date()
            },
            { session }
        );

        // Delete file from S3
        await s3Service.deleteFile(uploadSession.s3Key);

        await session.commitTransaction();

        res.status(200).json({
            status: 'success',
            message: 'Upload resumed and completed successfully',
            data: {
                uploadId: uploadId,
                fileName: uploadSession.fileName,
                totalRows: totalRows - 1,
                rowsProcessed: totalProcessed,
                status: 'completed'
            }
        });

    } catch (error) {
        await session.abortTransaction();
        
        console.error('Resume upload error:', error);
        
        res.status(500).json({
            status: 'error',
            message: 'Error resuming upload',
            error: error.message
        });
    } finally {
        session.endSession();
    }
};

// Clean up failed uploads
const cleanupFailedUploads = async (req, res) => {
    try {
        const userId = req.user.userId;
        const { uploadId } = req.query;

        const query = { 
            userId: userId, 
            status: 'failed' 
        };
        
        if (uploadId) {
            query.uploadId = uploadId;
        }

        const failedSessions = await UploadSession.find(query);

        for (const session of failedSessions) {
            try {
                // Delete from S3
                await s3Service.deleteFile(session.s3Key);
                
                // Delete from database
                await ExcelData.deleteMany({ uploadId: session.uploadId });
                await UploadSession.findByIdAndDelete(session._id);
                
    
            } catch (cleanupError) {
                console.error(`Error cleaning up session ${session.uploadId}:`, cleanupError);
            }
        }

        res.status(200).json({
            status: 'success',
            message: `Cleaned up ${failedSessions.length} failed upload(s)`,
            data: {
                cleanedCount: failedSessions.length
            }
        });

    } catch (error) {
        console.error('Error cleaning up failed uploads:', error);
        res.status(500).json({
            status: 'error',
            message: 'Error cleaning up failed uploads',
            error: error.message
        });
    }
};

module.exports = {
    upload,
    uploadFile,
    getRowData,
    getSingleRowData,
    updateSheet,
    getUserFiles,
    deleteFile,
    getFileHeaders,
    getUploadStatus,
    getUserUploadSessions,
    resumeUpload,
    cleanupFailedUploads
};
