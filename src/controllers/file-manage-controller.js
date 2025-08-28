const mongoose = require("mongoose");
const XLSX = require("xlsx");
const ExcelData = require("../models/ExcelData");
const StripeExcelData = require("../models/StripeExcelData");
const OTA = require("../models/OTA");
const UploadSession = require("../models/UploadSession");
const { upload, s3Service } = require("../config/s3");
const { encryptCardData, decryptCardData } = require("../utils/encryption");
const User = require("../models/User");
const ExcelJS = require("exceljs");

// Generate unique upload ID
function generateUploadId() {
  return `upload_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Simplified batch processing without duplicate checking
async function processBatch(
  excelDataRecords,
  uploadId,
  batchNumber,
  paymentGateway = "paypal"
) {
  try {
    if (excelDataRecords.length === 0) {
      console.log(
        `\x1b[33m⚠️  Batch ${batchNumber}: No records to process\x1b[0m`
      );
      return 0;
    }

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    console.log(
      `\x1b[36m📤 Processing batch ${batchNumber}: ${
        excelDataRecords.length
      } records (${paymentGateway.toUpperCase()})\x1b[0m`
    );

    // Use bulkWrite for better performance
    try {
      const result = await DataModel.bulkWrite(
        excelDataRecords.map((record) => ({
          insertOne: { document: record },
        })),
        { ordered: false } // Continue processing even if some records fail
      );

      console.log(
        `\x1b[32m✅ Batch ${batchNumber} SUCCESS: Saved ${result.insertedCount}/${excelDataRecords.length} records to database\x1b[0m`
      );
      return result.insertedCount;
    } catch (bulkWriteError) {
      // Handle bulk write errors gracefully
      if (
        bulkWriteError.code === 11000 ||
        bulkWriteError.name === "BulkWriteError"
      ) {
        const successfulInserts = bulkWriteError.result
          ? bulkWriteError.result.insertedCount
          : 0;
        const failedInserts = excelDataRecords.length - successfulInserts;
        console.log(
          `\x1b[33m⚠️  Batch ${batchNumber} PARTIAL SUCCESS: Saved ${successfulInserts}/${excelDataRecords.length} records (${failedInserts} duplicates/errors skipped)\x1b[0m`
        );
        return successfulInserts;
      }
      console.log(
        `\x1b[31m❌ Batch ${batchNumber} ERROR: Failed to save records - ${bulkWriteError.message}\x1b[0m`
      );
      throw bulkWriteError;
    }
  } catch (error) {
    console.log(
      `\x1b[31m❌ Batch ${batchNumber} CRITICAL ERROR: ${error.message}\x1b[0m`
    );
    console.error(`Error processing batch ${batchNumber}:`, error);
    throw error;
  }
}

// Check for existing upload session
async function checkExistingUpload(fileName, userId) {
  const existingSession = await UploadSession.findOne({
    userId: userId,
    fileName: fileName,
    status: { $in: ["uploading", "processing"] },
  });

  if (existingSession) {
    return {
      exists: true,
      session: existingSession,
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
        status: "error",
        message: "No file uploaded",
      });
    }

    const originalFileName = req.file.originalname;
    const userId = req.user.userId;
    const vnpWorkId = req.body.vnpWorkId;

    // Add timestamp to filename
    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, "-")
      .slice(0, -5); // Format: YYYY-MM-DDTHH-MM-SS
    const fileExtension = originalFileName.substring(
      originalFileName.lastIndexOf(".")
    );
    const fileNameWithoutExt = originalFileName.substring(
      0,
      originalFileName.lastIndexOf(".")
    );
    const fileName = `${fileNameWithoutExt}_${timestamp}${fileExtension}`;

    // Check for existing upload session (using original filename to avoid conflicts)
    const existingCheck = await checkExistingUpload(originalFileName, userId);
    if (existingCheck.exists) {
      return res.status(409).json({
        status: "error",
        message: "File is already being processed",
        data: {
          uploadId: existingCheck.session.uploadId,
          status: existingCheck.session.status,
          processedRows: existingCheck.session.processedRows,
          totalRows: existingCheck.session.totalRows,
        },
      });
    }

    // Download file from S3 to process
    const fileBuffer = await s3Service.downloadFile(req.file.key);

    // Read the Excel file
    const workbook = XLSX.read(fileBuffer);
    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
    const cellA1 = firstSheet["A1"] ? firstSheet["A1"].v : null;

    // Validate the content of cell A1
    if (cellA1 !== "Expedia ID") {
      await s3Service.deleteFile(req.file.key);
      return res.status(400).json({
        status: "error",
        message: 'Invalid VNP Work file. Cell A1 must contain "Expedia ID"',
      });
    }

    // Get sheet range and headers
    const range = firstSheet["!ref"];
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

    // Auto-detect payment gateway based on Excel headers
    // If "Connected Account" column exists, it's for Stripe, otherwise PayPal
    const paymentGateway = headers.includes("Connected Account")
      ? "stripe"
      : "paypal";

    console.log(
      `Detected payment gateway: ${paymentGateway} based on headers:`,
      headers
    );

    // Generate unique upload ID
    const uploadId = generateUploadId();

    console.log(
      `\x1b[34m🚀 STARTING UPLOAD: File "${fileName}" - ${
        totalRows - 1
      } rows, ${paymentGateway.toUpperCase()} gateway\x1b[0m`
    );

    // Create upload session
    const uploadSession = new UploadSession({
      uploadId: uploadId,
      userId: userId,
      fileName: fileName,
      originalFileName: originalFileName,
      s3Key: req.file.key,
      totalRows: totalRows - 1,
      status: "processing",
      vnpWorkId: vnpWorkId,
      headers: headers,
      batchSize: 100,
      paymentGateway: paymentGateway,
      // Add OTA fields to session for resume functionality
      ota: null, // OTA will be determined from Excel data
      otaId: null,
    });

    await uploadSession.save({ session });

    // Process data in batches
    const batchSize = 100;
    let totalProcessed = 0;
    let batchNumber = 1;

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
            rowData.push(cell.v !== undefined ? cell.v.toString() : "");
          } else {
            rowData.push("");
          }
        }

        const rowObject = headers.reduce((acc, header, index) => {
          acc[header.trim()] = rowData[index]?.trim() || null;
          return acc;
        }, {});

        if (Object.keys(rowObject).length > 0 && rowObject["Expedia ID"]) {
          // Normalize Card Expire to YYYY-MM format
          let cardExpire = rowObject["Card Expire"];
          if (cardExpire) {
            // If it's a number (Excel date serial), convert to YYYY-MM
            if (
              !isNaN(cardExpire) &&
              cardExpire !== "" &&
              cardExpire !== null
            ) {
              // Excel's epoch starts at 1899-12-30
              const serial = Number(cardExpire);
              if (serial > 59) {
                // Excel bug: 1900 is not a leap year
                // Excel incorrectly treats 1900 as a leap year
                // So, dates after 1900-02-28 are offset by +1
                // But for just YYYY-MM, this is fine
              }
              const excelEpoch = new Date(1899, 11, 30);
              const date = new Date(
                excelEpoch.getTime() + serial * 24 * 60 * 60 * 1000
              );
              const year = date.getFullYear();
              const month = (date.getMonth() + 1).toString().padStart(2, "0");
              cardExpire = `${year}-${month}`;
            } else {
              // Try to parse as date string (YYYY-MM, MM/YYYY, MM-YYYY, etc.)
              let match = null;
              // YYYY-MM
              if (/^\d{4}-\d{2}$/.test(cardExpire)) {
                // already correct
              } else if (
                (match = cardExpire.match(/^(\d{1,2})[\/-](\d{4})$/))
              ) {
                // M/YYYY or MM/YYYY or M-YYYY or MM-YYYY
                cardExpire = `${match[2]}-${match[1].padStart(2, "0")}`;
              } else if (
                (match = cardExpire.match(/^(\d{4})[\/-](\d{1,2})$/))
              ) {
                // YYYY/M or YYYY/MM or YYYY-M or YYYY-MM
                cardExpire = `${match[1]}-${match[2].padStart(2, "0")}`;
              } else if (
                (match = cardExpire.match(/^(\d{1,2})[\/-](\d{2})$/))
              ) {
                // M/YY or MM/YY or M-YY or MM-YY
                cardExpire = `20${match[2]}-${match[1].padStart(2, "0")}`;
              } else if (
                (match = cardExpire.match(/^(\d{2})[\/-](\d{2})[\/-](\d{4})$/))
              ) {
                // DD-MM-YYYY or DD/MM/YYYY
                cardExpire = `${match[3]}-${match[2]}`;
              } else {
                // fallback: try Date.parse
                const d = new Date(cardExpire);
                if (!isNaN(d)) {
                  const year = d.getFullYear();
                  const month = (d.getMonth() + 1).toString().padStart(2, "0");
                  cardExpire = `${year}-${month}`;
                }
              }
            }
          }

          // Process OTA field from Excel sheet
          let otaRecord = null;
          const otaFromExcel = rowObject["OTA"]?.trim();

          if (otaFromExcel) {
            try {
              console.log(`Processing OTA from Excel: "${otaFromExcel}"`);
              otaRecord = await OTA.findOne({
                name: otaFromExcel,
                isActive: true,
              });

              if (otaRecord) {
                console.log(
                  `Found OTA record for ${otaFromExcel}:`,
                  otaRecord._id
                );
              } else {
                console.log(`No OTA record found for: "${otaFromExcel}"`);
              }
            } catch (otaError) {
              console.error("Error finding OTA record:", otaError);
            }
          }

          const mappedData = {
            userId: userId,
            uploadId: uploadId,
            fileName: fileName,
            uploadStatus: "processing",
            rowNumber: row,
            "Expedia ID": rowObject["Expedia ID"],
            Batch: rowObject["Batch"],
            OTA: rowObject["OTA"], // Store the OTA name from Excel
            "Posting Type": rowObject["Posting Type"],
            Portfolio: rowObject["Portfolio"],
            "Hotel Name": rowObject["Hotel Name"],
            "Reservation ID": rowObject["Reservation ID"],
            "Hotel Confirmation Code": rowObject["Hotel Confirmation Code"],
            Name: rowObject["Name"],
            "Check In": rowObject["Check In"],
            "Check Out": rowObject["Check Out"],
            Curency: rowObject["Curency"],
            "Amount to charge": rowObject["Amount to charge"],
            "Charge status": rowObject["Charge status"],
            "Card Number": rowObject["Card Number"],
            "Card Expire": cardExpire,
            "Card CVV": rowObject["Card CVV"],
            "Soft Descriptor":
              rowObject["Soft Descriptor"] || rowObject["BT MAID"],
            "VNP Work ID": rowObject["VNP Work ID"],
            Status: rowObject["Status"],
            // Add OTA fields based on Excel data
            ota: otaRecord?.name || otaFromExcel || null,
            otaId: otaRecord?._id || null,
          };

          // Add Stripe-specific field if payment gateway is stripe
          if (paymentGateway === "stripe") {
            mappedData["Connected Account"] =
              rowObject["Connected Account"] || null;
          }

          const encryptedData = encryptCardData(mappedData);
          excelDataRecords.push(encryptedData);
        }
      }

      // Process batch if there are records
      if (excelDataRecords.length > 0) {
        const processedCount = await processBatch(
          excelDataRecords,
          uploadId,
          batchNumber,
          paymentGateway
        );
        totalProcessed += processedCount;

        // Update session progress
        await UploadSession.findOneAndUpdate(
          { uploadId: uploadId },
          {
            processedRows: totalProcessed,
            status:
              totalProcessed >= totalRows - 1 ? "completed" : "processing",
          },
          { session }
        );
      }

      batchNumber++;

      // Minimal delay to prevent overwhelming the database
      if (batchNumber % 10 === 0) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }

    // Mark session as completed
    await UploadSession.findOneAndUpdate(
      { uploadId: uploadId },
      {
        status: "completed",
        completedAt: new Date(),
      },
      { session }
    );

    console.log(
      `\x1b[32m✅ UPLOAD COMPLETE: File "${fileName}" processed successfully - ${totalProcessed} records saved to database\x1b[0m`
    );

    // Delete file from S3 after successful processing
    await s3Service.deleteFile(req.file.key);

    // Commit transaction
    await session.commitTransaction();

    res.status(200).json({
      status: "success",
      message: "File uploaded and processed successfully",
      data: {
        uploadId: uploadId,
        fileName: fileName,
        totalRows: totalRows - 1,
        totalColumns: totalCols,
        rowsProcessed: totalProcessed,
        headers: headers,
        status: "completed",
      },
    });
  } catch (error) {
    // Rollback transaction on error
    await session.abortTransaction();

    console.log(
      `\x1b[31m❌ UPLOAD ERROR: Failed to process file - ${error.message}\x1b[0m`
    );
    console.error("Upload error:", error);

    // Update session status to failed if it exists
    if (req.file && req.file.key) {
      try {
        await UploadSession.findOneAndUpdate(
          { s3Key: req.file.key },
          {
            status: "failed",
            errorMessage: error.message,
          }
        );
      } catch (sessionError) {
        console.error("Error updating session status:", sessionError);
      }
    }

    res.status(500).json({
      status: "error",
      message: "Error processing file",
      error: error.message,
    });
  } finally {
    session.endSession();
  }
};

// Get all row data from all users
const getRowData = async (req, res) => {
  try {
    const {
      limit = 10,
      page = 1,
      chargeStatus,
      search,
      paymentGateway = "paypal",
    } = req.query;
    const userId = req.user.userId;

    const skip = (parseInt(page) - 1) * parseInt(limit);

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Build query object - show data from all users
    const query = {};

    // Add filter for Charge status if provided (skip if "All" is selected)
    if (chargeStatus && chargeStatus.trim() !== "" && chargeStatus !== "All") {
      query["Charge status"] = chargeStatus;
    }

    // Add search functionality across multiple fields if provided
    if (search && search.trim() !== "") {
      const searchRegex = { $regex: search, $options: "i" }; // Case-insensitive search
      query.$or = [
        { "Reservation ID": searchRegex },
        { "Expedia ID": searchRegex },
        { "Hotel Name": searchRegex },
        { Name: searchRegex },
        { Portfolio: searchRegex },
        { Batch: searchRegex },
      ];
    }

    // Get paginated data for the user with filters
    const rowData = await DataModel.find(query)
      .populate("otaId", "name displayName customer billingAddress isActive") // Populate OTA data
      .skip(skip)
      .limit(parseInt(limit))
      .sort({ createdAt: -1 });

    // Get total count for pagination with same filters
    const totalCount = await DataModel.countDocuments(query);

    res.status(200).json({
      status: "success",
      data: {
        rows: rowData.map((row) => {
          // Remove MongoDB internal fields and userId, return only the Excel data
          const { _id, userId, __v, createdAt, updatedAt, ...excelData } =
            row.toObject();

          // Decrypt sensitive card data before returning
          const decryptedData = decryptCardData(excelData);

          return {
            id: _id,
            ...decryptedData,
            createdAt: createdAt,
          };
        }),
        pagination: {
          currentPage: parseInt(page),
          totalPages: Math.ceil(totalCount / parseInt(limit)),
          totalCount: totalCount,
          limit: parseInt(limit),
        },
        filters: {
          chargeStatus: chargeStatus || null,
          search: search || null,
          paymentGateway: paymentGateway,
        },
      },
    });
  } catch (error) {
    console.error("Get row data error:", error);
    res.status(500).json({
      status: "error",
      message: "Error retrieving row data",
      error: error.message,
    });
  }
};

// Get all stripe row data from all users
const getStripeRowData = async (req, res) => {
  try {
    const { limit = 10, page = 1, chargeStatus, search } = req.query;
    const userId = req.user.userId;

    const skip = (parseInt(page) - 1) * parseInt(limit);

    // Always use StripeExcelData model for this endpoint
    const DataModel = StripeExcelData;

    // Build query object - show data from all users
    const query = {};

    // Add filter for Charge status if provided (skip if "All" is selected)
    if (chargeStatus && chargeStatus.trim() !== "" && chargeStatus !== "All") {
      query["Charge status"] = chargeStatus;
    }

    // Add search functionality across multiple fields if provided
    if (search && search.trim() !== "") {
      const searchRegex = { $regex: search, $options: "i" }; // Case-insensitive search
      query.$or = [
        { "Reservation ID": searchRegex },
        { "Expedia ID": searchRegex },
        { "Hotel Name": searchRegex },
        { Name: searchRegex },
        { Portfolio: searchRegex },
        { Batch: searchRegex },
      ];
    }

    // Get paginated data for the user with filters
    const rowData = await DataModel.find(query)
      .populate("otaId", "name displayName customer billingAddress isActive") // Populate OTA data
      .skip(skip)
      .limit(parseInt(limit))
      .sort({ createdAt: -1 });

    // Get total count for pagination with same filters
    const totalCount = await DataModel.countDocuments(query);

    res.status(200).json({
      status: "success",
      data: {
        rows: rowData.map((row) => {
          // Remove MongoDB internal fields and userId, return only the Excel data
          const { _id, userId, __v, createdAt, updatedAt, ...excelData } =
            row.toObject();

          // Decrypt sensitive card data before returning
          const decryptedData = decryptCardData(excelData);

          return {
            id: _id,
            ...decryptedData,
            createdAt: createdAt,
          };
        }),
        pagination: {
          currentPage: parseInt(page),
          totalPages: Math.ceil(totalCount / parseInt(limit)),
          totalCount: totalCount,
          limit: parseInt(limit),
        },
        filters: {
          chargeStatus: chargeStatus || null,
          search: search || null,
          paymentGateway: "stripe",
        },
      },
    });
  } catch (error) {
    console.error("Get stripe row data error:", error);
    res.status(500).json({
      status: "error",
      message: "Error retrieving stripe row data",
      error: error.message,
    });
  }
};

// Get single row data for a user
const getSingleRowData = async (req, res) => {
  try {
    const { documentId } = req.params;
    const { paymentGateway = "paypal" } = req.query;
    const userId = req.user.userId;

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    const rowData = await DataModel.findOne({
      _id: documentId,
      userId: userId,
    }).populate("otaId", "name displayName customer billingAddress isActive"); // Populate OTA data

    if (!rowData) {
      return res.status(404).json({
        status: "error",
        message: "Row data not found",
      });
    }

    // Remove MongoDB internal fields and userId, return only the Excel data
    const {
      _id,
      userId: userIdField,
      __v,
      createdAt,
      updatedAt,
      ...excelData
    } = rowData.toObject();

    // Decrypt sensitive card data before returning
    const decryptedData = decryptCardData(excelData);

    res.status(200).json({
      status: "success",
      data: {
        id: _id,
        ...decryptedData,
        createdAt: createdAt,
      },
    });
  } catch (error) {
    console.error("Get single row data error:", error);
    res.status(500).json({
      status: "error",
      message: "Error retrieving single row data",
      error: error.message,
    });
  }
};

// Update sheet data in database
const updateSheet = async (req, res) => {
  try {
    const { documentId } = req.params;
    // console.log("documentId", documentId);
    const updateData = req.body;
    const { paymentGateway = "paypal" } = req.query;
    // console.log("updateData", updateData);
    const userId = req.user.userId;

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    if (!documentId) {
      return res.status(400).json({
        status: "error",
        message: "Document ID is required",
      });
    }

    // Map the update data using the same logic
    const mappedUpdateData = {
      "Expedia ID": updateData["Expedia ID"],
      Batch: updateData["Batch"],
      "Posting Type": updateData["Posting Type"],
      Portfolio: updateData["Portfolio"],
      "Hotel Name": updateData["Hotel Name"],
      "Reservation ID": updateData["Reservation ID"],
      "Hotel Confirmation Code": updateData["Hotel Confirmation Code"],
      Name: updateData["Name"],
      "Check In": updateData["Check In"],
      "Check Out": updateData["Check Out"],
      Curency: updateData["Curency"],
      "Amount to charge": updateData["Amount to charge"],
      "Charge status": updateData["Charge status"],
      "Card Number": updateData["Card Number"],
      "Card Expire": updateData["Card Expire"],
      "Card CVV": updateData["Card CVV"],
      "Soft Descriptor": updateData["Soft Descriptor"],
      "VNP Work ID": updateData["VNP Work ID"],
      Status: updateData["Status"],
    };

    // Add Stripe-specific field if payment gateway is stripe
    if (paymentGateway === "stripe") {
      mappedUpdateData["Connected Account"] = updateData["Connected Account"];
    }

    // Remove undefined values
    Object.keys(mappedUpdateData).forEach((key) => {
      if (mappedUpdateData[key] === undefined) {
        delete mappedUpdateData[key];
      }
    });

    // Encrypt sensitive card data before updating
    const encryptedUpdateData = encryptCardData(mappedUpdateData);

    console.log(
      `\x1b[36m📝 Updating record: Document ID ${documentId} (${paymentGateway.toUpperCase()})\x1b[0m`
    );

    // Update the document with new data
    const updateResult = await DataModel.findOneAndUpdate(
      {
        _id: documentId,
      },
      {
        $set: encryptedUpdateData,
      },
      { new: true }
    ).populate("otaId", "name displayName customer billingAddress isActive"); // Populate OTA data

    if (!updateResult) {
      console.log(
        `\x1b[31m❌ UPDATE FAILED: Document not found - ID: ${documentId}\x1b[0m`
      );
      return res.status(404).json({
        status: "error",
        message: "Document not found",
      });
    }

    console.log(
      `\x1b[32m✅ UPDATE SUCCESS: Record updated successfully - ID: ${documentId}, Expedia ID: ${
        updateResult["Expedia ID"] || "N/A"
      }\x1b[0m`
    );

    res.status(200).json({
      status: "success",
      message: "Data updated successfully",
      data: (() => {
        const { _id, userId, __v, createdAt, updatedAt, ...excelData } =
          updateResult.toObject();
        // Decrypt sensitive card data before returning
        return decryptCardData(excelData);
      })(),
    });
  } catch (error) {
    console.log(
      `\x1b[31m❌ UPDATE ERROR: Failed to update record - Document ID: ${req.params.documentId}, Error: ${error.message}\x1b[0m`
    );
    console.error("Update sheet error:", error);
    res.status(500).json({
      status: "error",
      message: "Error updating data",
      error: error.message,
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
      .populate("otaId", "name displayName customer billingAddress isActive") // Populate OTA data
      .sort({ createdAt: -1 })
      .limit(10);

    res.status(200).json({
      status: "success",
      data: {
        totalRecords: totalCount,
        recentData: recentData.map((record) => {
          const { _id, userId, __v, createdAt, updatedAt, ...excelData } =
            record.toObject();
          // Decrypt sensitive card data before returning
          const decryptedData = decryptCardData(excelData);
          return {
            id: _id,
            ...decryptedData,
            createdAt: createdAt,
          };
        }),
      },
    });
  } catch (error) {
    console.error("Get user files error:", error);
    res.status(500).json({
      status: "error",
      message: "Error retrieving user data",
      error: error.message,
    });
  }
};

// Delete specific data record
const deleteFile = async (req, res) => {
  try {
    const { documentId } = req.params;
    const { paymentGateway = "paypal" } = req.query;
    const userId = req.user.userId;

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    if (!documentId) {
      return res.status(400).json({
        status: "error",
        message: "Document ID is required",
      });
    }

    console.log(
      `\x1b[36m🗑️  Deleting record: Document ID ${documentId} (${paymentGateway.toUpperCase()})\x1b[0m`
    );

    // Delete the specific document
    const deleteResult = await DataModel.findOneAndDelete({
      _id: documentId,
      userId: userId,
    });

    if (!deleteResult) {
      console.log(
        `\x1b[31m❌ DELETE FAILED: Document not found - ID: ${documentId}\x1b[0m`
      );
      return res.status(404).json({
        status: "error",
        message: "Document not found",
      });
    }

    console.log(
      `\x1b[32m✅ DELETE SUCCESS: Record deleted successfully - ID: ${documentId}, Expedia ID: ${
        deleteResult["Expedia ID"] || "N/A"
      }\x1b[0m`
    );

    res.status(200).json({
      status: "success",
      message: "Data deleted successfully",
    });
  } catch (error) {
    console.log(
      `\x1b[31m❌ DELETE ERROR: Failed to delete record - Document ID: ${req.params.documentId}, Error: ${error.message}\x1b[0m`
    );
    console.error("Delete file error:", error);
    res.status(500).json({
      status: "error",
      message: "Error deleting data",
      error: error.message,
    });
  }
};

// Delete entire upload and all associated data
const deleteUploadById = async (req, res) => {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const { uploadId } = req.params;
    const userId = req.user.userId;

    if (!uploadId) {
      return res.status(400).json({
        status: "error",
        message: "Upload ID is required",
      });
    }

    // First, verify that the upload session belongs to the user
    const uploadSession = await UploadSession.findOne({
      uploadId: uploadId,
    });

    if (!uploadSession) {
      return res.status(404).json({
        status: "error",
        message: "Upload session not found or access denied",
      });
    }

    // Choose the appropriate model based on the upload session's payment gateway
    const DataModel =
      uploadSession.paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Count how many records will be deleted for confirmation
    const recordCount = await DataModel.countDocuments({
      uploadId: uploadId,
    });

    console.log(
      `\x1b[36m🗑️  Deleting upload: Upload ID ${uploadId} with ${recordCount} records (${uploadSession.paymentGateway.toUpperCase()})\x1b[0m`
    );

    // Delete all data records for this upload
    const excelDataDeleteResult = await DataModel.deleteMany(
      {
        uploadId: uploadId,
      },
      { session }
    );

    console.log(
      `\x1b[32m✅ DELETE SUCCESS: Removed ${excelDataDeleteResult.deletedCount}/${recordCount} records from database\x1b[0m`
    );

    // Delete the upload session
    const sessionDeleteResult = await UploadSession.findOneAndDelete(
      {
        uploadId: uploadId,
      },
      { session }
    );

    if (sessionDeleteResult) {
      console.log(
        `\x1b[32m✅ DELETE SUCCESS: Upload session removed - File: ${sessionDeleteResult.fileName}\x1b[0m`
      );
    }

    // Try to clean up any remaining S3 files (for failed uploads or exports)
    if (uploadSession.s3Key) {
      try {
        await s3Service.deleteFile(uploadSession.s3Key);
      } catch (s3Error) {
        console.log(
          `S3 file cleanup for ${uploadSession.s3Key}: ${s3Error.message}`
        );
        // Don't fail the entire operation if S3 cleanup fails
      }
    }

    // Also try to delete any export files for this upload
    try {
      const exportKey = `exports/${uploadId}/${uploadSession.originalFileName}`;
      await s3Service.deleteFile(exportKey);
    } catch (s3Error) {
      console.log(`S3 export file cleanup: ${s3Error.message}`);
      // Don't fail the entire operation if S3 cleanup fails
    }

    // Commit the transaction
    await session.commitTransaction();

    res.status(200).json({
      status: "success",
      message: "Upload and all associated data deleted successfully",
      data: {
        uploadId: uploadId,
        fileName: uploadSession.fileName,
        deletedRecords: excelDataDeleteResult.deletedCount,
        expectedRecords: recordCount,
      },
    });
  } catch (error) {
    // Rollback transaction on error
    await session.abortTransaction();

    console.log(
      `\x1b[31m❌ DELETE UPLOAD ERROR: Failed to delete upload - Upload ID: ${req.params.uploadId}, Error: ${error.message}\x1b[0m`
    );
    console.error("Delete upload error:", error);
    res.status(500).json({
      status: "error",
      message: "Error deleting upload",
      error: error.message,
    });
  } finally {
    session.endSession();
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
        status: "error",
        message: "No data found for user",
      });
    }

    // Extract headers from the document properties (excluding MongoDB fields)
    const {
      _id,
      userId: user,
      __v,
      createdAt,
      updatedAt,
      ...excelData
    } = sampleRecord.toObject();
    const headers = Object.keys(excelData);

    res.status(200).json({
      status: "success",
      data: {
        headers: headers,
        totalHeaders: headers.length,
      },
    });
  } catch (error) {
    console.error("Get file headers error:", error);
    res.status(500).json({
      status: "error",
      message: "Error retrieving headers",
      error: error.message,
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
      userId: userId,
    });

    if (!uploadSession) {
      return res.status(404).json({
        status: "error",
        message: "Upload session not found",
      });
    }

    res.status(200).json({
      status: "success",
      data: {
        uploadId: uploadSession.uploadId,
        fileName: uploadSession.fileName,
        status: uploadSession.status,
        totalRows: uploadSession.totalRows,
        processedRows: uploadSession.processedRows,
        progress:
          uploadSession.status === "completed"
            ? 100
            : uploadSession.totalRows > 0
            ? Math.round(
                (uploadSession.processedRows / uploadSession.totalRows) * 100
              )
            : 0,
        startedAt: uploadSession.startedAt,
        completedAt: uploadSession.completedAt,
        errorMessage: uploadSession.errorMessage,
      },
    });
  } catch (error) {
    console.error("Error getting upload status:", error);
    res.status(500).json({
      status: "error",
      message: "Error getting upload status",
      error: error.message,
    });
  }
};

// Get all upload sessions from all users
const getUserUploadSessions = async (req, res) => {
  try {
    const userId = req.user.userId;
    const { status, limit = 20, page = 1, search } = req.query;

    const query = {}; // Show sessions from all users
    if (status) {
      query.status = status;
    }

    if (search && search.trim() !== "") {
      const searchRegex = { $regex: search, $options: "i" }; // Case-insensitive search
      query.$or = [{ fileName: searchRegex }];
    }
    const sessions = await UploadSession.find(query)
      .populate("userId", "email name username") // Populate user information
      .sort({ createdAt: -1 })
      .limit(parseInt(limit))
      .skip((parseInt(page) - 1) * parseInt(limit));

    const total = await UploadSession.countDocuments(query);

    // Add chargedCount for each session
    const chargedCounts = await Promise.all(
      sessions.map(async (session) => {
        const paymentGateway = session.paymentGateway || "paypal";
        const DataModel =
          paymentGateway === "stripe" ? StripeExcelData : ExcelData;

        return await DataModel.countDocuments({
          uploadId: session.uploadId,
          "Charge status": "Charged",
        });
      })
    );

    res.status(200).json({
      status: "success",
      data: {
        sessions: sessions.map((session, idx) => ({
          uploadId: session.uploadId,
          fileName: session.fileName,
          status: session.status,
          totalRows: session.totalRows,
          processedRows: session.processedRows,
          progress:
            session.status === "completed"
              ? 100
              : session.totalRows > 0
              ? Math.round((session.processedRows / session.totalRows) * 100)
              : 0,
          startedAt: session.startedAt,
          completedAt: session.completedAt,
          chargedCount: chargedCounts[idx],
          uploadedBy: {
            userId: session.userId._id,
            email: session.userId.email,
            name:
              session.userId.name || session.userId.username || "Unknown User",
          },
        })),
        pagination: {
          total,
          page: parseInt(page),
          limit: parseInt(limit),
          pages: Math.ceil(total / parseInt(limit)),
        },
      },
    });
  } catch (error) {
    console.error("Error getting all upload sessions:", error);
    res.status(500).json({
      status: "error",
      message: "Error getting upload sessions",
      error: error.message,
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
      status: "failed",
    });

    if (!uploadSession) {
      return res.status(404).json({
        status: "error",
        message: "Failed upload session not found",
      });
    }

    // Check if retry limit exceeded
    if (uploadSession.retryCount >= uploadSession.maxRetries) {
      return res.status(400).json({
        status: "error",
        message: "Maximum retry attempts exceeded",
      });
    }

    // Download file from S3
    const fileBuffer = await s3Service.downloadFile(uploadSession.s3Key);
    const workbook = XLSX.read(fileBuffer);
    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];

    // Get sheet range
    const range = firstSheet["!ref"];
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

    // Auto-detect payment gateway based on Excel headers
    // If "Connected Account" column exists, it's for Stripe, otherwise PayPal
    const paymentGateway = headers.includes("Connected Account")
      ? "stripe"
      : "paypal";

    console.log(
      `Detected payment gateway: ${paymentGateway} based on headers:`,
      headers
    );

    console.log(
      `\x1b[35m🔄 RESUMING UPLOAD: Upload ID ${uploadId} (Retry ${
        uploadSession.retryCount + 1
      }/${uploadSession.maxRetries}) - ${paymentGateway.toUpperCase()}\x1b[0m`
    );

    // Update session status to processing
    await UploadSession.findOneAndUpdate(
      { uploadId: uploadId },
      {
        status: "processing",
        retryCount: uploadSession.retryCount + 1,
      },
      { session }
    );

    console.log(
      `\x1b[36m📊 RESUME: Processing ${
        totalRows - 1
      } total rows in batches of ${batchSize}\x1b[0m`
    );

    // Choose the appropriate model based on the upload session's payment gateway
    const DataModel =
      uploadSession.paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Delete existing partial data for this upload
    await DataModel.deleteMany({ uploadId: uploadId }, { session });

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
            rowData.push(cell.v !== undefined ? cell.v.toString() : "");
          } else {
            rowData.push("");
          }
        }

        const rowObject = headers.reduce((acc, header, index) => {
          acc[header.trim()] = rowData[index]?.trim() || null;
          return acc;
        }, {});

        if (Object.keys(rowObject).length > 0 && rowObject["Expedia ID"]) {
          // Normalize Card Expire to YYYY-MM format
          let cardExpire = rowObject["Card Expire"];
          if (cardExpire) {
            // If it's a number (Excel date serial), convert to YYYY-MM
            if (
              !isNaN(cardExpire) &&
              cardExpire !== "" &&
              cardExpire !== null
            ) {
              // Excel's epoch starts at 1899-12-30
              const serial = Number(cardExpire);
              if (serial > 59) {
                // Excel bug: 1900 is not a leap year
                // Excel incorrectly treats 1900 as a leap year
                // So, dates after 1900-02-28 are offset by +1
                // But for just YYYY-MM, this is fine
              }
              const excelEpoch = new Date(1899, 11, 30);
              const date = new Date(
                excelEpoch.getTime() + serial * 24 * 60 * 60 * 1000
              );
              const year = date.getFullYear();
              const month = (date.getMonth() + 1).toString().padStart(2, "0");
              cardExpire = `${year}-${month}`;
            } else {
              // Try to parse as date string (YYYY-MM, MM/YYYY, MM-YYYY, etc.)
              let match = null;
              // YYYY-MM
              if (/^\d{4}-\d{2}$/.test(cardExpire)) {
                // already correct
              } else if (
                (match = cardExpire.match(/^(\d{1,2})[\/-](\d{4})$/))
              ) {
                // M/YYYY or MM/YYYY or M-YYYY or MM-YYYY
                cardExpire = `${match[2]}-${match[1].padStart(2, "0")}`;
              } else if (
                (match = cardExpire.match(/^(\d{4})[\/-](\d{1,2})$/))
              ) {
                // YYYY/M or YYYY/MM or YYYY-M or YYYY-MM
                cardExpire = `${match[1]}-${match[2].padStart(2, "0")}`;
              } else if (
                (match = cardExpire.match(/^(\d{1,2})[\/-](\d{2})$/))
              ) {
                // M/YY or MM/YY or M-YY or MM-YY
                cardExpire = `20${match[2]}-${match[1].padStart(2, "0")}`;
              } else if (
                (match = cardExpire.match(/^(\d{2})[\/-](\d{2})[\/-](\d{4})$/))
              ) {
                // DD-MM-YYYY or DD/MM/YYYY
                cardExpire = `${match[3]}-${match[2]}`;
              } else {
                // fallback: try Date.parse
                const d = new Date(cardExpire);
                if (!isNaN(d)) {
                  const year = d.getFullYear();
                  const month = (d.getMonth() + 1).toString().padStart(2, "0");
                  cardExpire = `${year}-${month}`;
                }
              }
            }
          }

          // Process OTA field from Excel sheet in resume upload
          let otaRecord = null;
          const otaFromExcel = rowObject["OTA"]?.trim();

          if (otaFromExcel) {
            try {
              console.log(
                `Processing OTA from Excel (resume): "${otaFromExcel}"`
              );
              otaRecord = await OTA.findOne({
                name: otaFromExcel,
                isActive: true,
              });

              if (otaRecord) {
                console.log(
                  `Found OTA record for ${otaFromExcel}:`,
                  otaRecord._id
                );
              } else {
                console.log(`No OTA record found for: "${otaFromExcel}"`);
              }
            } catch (otaError) {
              console.error("Error finding OTA record in resume:", otaError);
            }
          }

          const mappedData = {
            userId: userId,
            uploadId: uploadId,
            fileName: uploadSession.fileName,
            uploadStatus: "processing",
            rowNumber: row,
            "Expedia ID": rowObject["Expedia ID"],
            Batch: rowObject["Batch"],
            OTA: rowObject["OTA"], // Store the OTA name from Excel
            "Posting Type": rowObject["Posting Type"],
            Portfolio: rowObject["Portfolio"],
            "Hotel Name": rowObject["Hotel Name"],
            "Reservation ID": rowObject["Reservation ID"],
            "Hotel Confirmation Code": rowObject["Hotel Confirmation Code"],
            Name: rowObject["Name"],
            "Check In": rowObject["Check In"],
            "Check Out": rowObject["Check Out"],
            Curency: rowObject["Curency"],
            "Amount to charge": rowObject["Amount to charge"],
            "Charge status": rowObject["Charge status"],
            "Card Number": rowObject["Card Number"],
            "Card Expire": cardExpire,
            "Card CVV": rowObject["Card CVV"],
            "Soft Descriptor":
              rowObject["Soft Descriptor"] || rowObject["BT MAID"],
            "VNP Work ID": rowObject["VNP Work ID"],
            Status: rowObject["Status"],
            // Add OTA fields based on Excel data
            ota: otaRecord?.name || otaFromExcel || null,
            otaId: otaRecord?._id || null,
          };

          // Add Stripe-specific field if payment gateway is stripe
          if (paymentGateway === "stripe") {
            mappedData["Connected Account"] =
              rowObject["Connected Account"] || null;
          }

          const encryptedData = encryptCardData(mappedData);
          excelDataRecords.push(encryptedData);
        }
      }

      if (excelDataRecords.length > 0) {
        const processedCount = await processBatch(
          excelDataRecords,
          uploadId,
          batchNumber,
          paymentGateway
        );
        totalProcessed += processedCount;

        await UploadSession.findOneAndUpdate(
          { uploadId: uploadId },
          {
            processedRows: totalProcessed,
            status:
              totalProcessed >= totalRows - 1 ? "completed" : "processing",
          },
          { session }
        );
      }

      batchNumber++;

      if (batchNumber % 5 === 0) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Mark session as completed
    await UploadSession.findOneAndUpdate(
      { uploadId: uploadId },
      {
        status: "completed",
        completedAt: new Date(),
      },
      { session }
    );

    console.log(
      `\x1b[32m✅ RESUME COMPLETE: Upload ID ${uploadId} successfully resumed and completed - ${totalProcessed} records processed\x1b[0m`
    );

    // Delete file from S3
    await s3Service.deleteFile(uploadSession.s3Key);

    await session.commitTransaction();

    res.status(200).json({
      status: "success",
      message: "Upload resumed and completed successfully",
      data: {
        uploadId: uploadId,
        fileName: uploadSession.fileName,
        totalRows: totalRows - 1,
        rowsProcessed: totalProcessed,
        status: "completed",
      },
    });
  } catch (error) {
    await session.abortTransaction();

    console.log(
      `\x1b[31m❌ RESUME ERROR: Failed to resume upload - Upload ID: ${req.params.uploadId}, Error: ${error.message}\x1b[0m`
    );
    console.error("Resume upload error:", error);

    res.status(500).json({
      status: "error",
      message: "Error resuming upload",
      error: error.message,
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
      status: "failed",
    };

    if (uploadId) {
      query.uploadId = uploadId;
    }

    const failedSessions = await UploadSession.find(query);

    for (const session of failedSessions) {
      try {
        // Delete from S3
        await s3Service.deleteFile(session.s3Key);

        // Delete from database - clean up from both models to be safe
        const paymentGateway = session.paymentGateway || "paypal";
        const DataModel =
          paymentGateway === "stripe" ? StripeExcelData : ExcelData;

        await DataModel.deleteMany({ uploadId: session.uploadId });

        // Also clean up from the other model if there are any records (edge case)
        const OtherModel =
          paymentGateway === "stripe" ? ExcelData : StripeExcelData;
        await OtherModel.deleteMany({ uploadId: session.uploadId });

        await UploadSession.findByIdAndDelete(session._id);
      } catch (cleanupError) {
        console.error(
          `Error cleaning up session ${session.uploadId}:`,
          cleanupError
        );
      }
    }

    res.status(200).json({
      status: "success",
      message: `Cleaned up ${failedSessions.length} failed upload(s)`,
      data: {
        cleanedCount: failedSessions.length,
      },
    });
  } catch (error) {
    console.error("Error cleaning up failed uploads:", error);
    res.status(500).json({
      status: "error",
      message: "Error cleaning up failed uploads",
      error: error.message,
    });
  }
};

// Download Excel for a single uploadId
const downloadExcelByUploadId = async (req, res) => {
  try {
    const { uploadId } = req.params;
    if (!uploadId) {
      return res
        .status(400)
        .json({ status: "error", message: "uploadId is required" });
    }
    // Get upload session to determine payment gateway
    const uploadSession = await UploadSession.findOne({ uploadId });
    if (!uploadSession) {
      return res
        .status(404)
        .json({ status: "error", message: "Upload session not found" });
    }

    const paymentGateway = uploadSession.paymentGateway || "paypal";
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Get one row to get userId
    const firstRow = await DataModel.findOne({ uploadId }).lean();
    if (!firstRow) {
      return res
        .status(404)
        .json({ status: "error", message: "No data found for this uploadId" });
    }
    // Get user email (all rows have same userId)
    const user = await User.findById(firstRow.userId);
    const userEmail = user ? user.email : "";
    // Get original file name from UploadSession
    const fileName = uploadSession
      ? uploadSession.originalFileName
      : `export_${uploadId}.xlsx`;
    // Define base columns
    const baseColumns = [
      "Expedia ID",
      "Batch",
      "OTA",
      "Posting Type",
      "Portfolio",
      "Hotel Name",
      "Reservation ID",
      "Hotel Confirmation Code",
      "Name",
      "Check In",
      "Check Out",
      "Curency",
      "Amount to charge",
      "Charge status",
      "Card Number",
      "Card Expire",
      "Card CVV",
      "Soft Descriptor",
      "VNP Work ID",
      "Status",
    ];

    // Add payment gateway specific columns
    let columns = [...baseColumns];

    if (paymentGateway === "stripe") {
      columns.push("Connected Account");
      // Stripe Payment Fields
      columns.push(
        "Stripe Order ID",
        "Stripe Capture ID",
        "Stripe Network Transaction ID",
        "Stripe Fee",
        "Stripe Net Amount",
        "Stripe Card Brand",
        "Stripe Card Type",
        "Stripe AVS Code",
        "Stripe CVV Code",
        "Stripe Create Time",
        "Stripe Update Time",
        "Stripe Status",
        "Stripe Amount",
        "Stripe Currency",
        "Stripe Card Last Digits",
        "Stripe Capture Status",
        "Stripe Custom ID",
        // Stripe Refund Fields
        "Stripe Refund ID",
        "Stripe Refund Status",
        "Stripe Refund Amount",
        "Stripe Refund Currency",
        "Stripe Refund Gross Amount",
        "Stripe Refund Fee",
        "Stripe Refund Net Amount",
        "Stripe Refund Create Time",
        "Stripe Refund Update Time",
        "Stripe Refund Invoice ID",
        "Stripe Refund Custom ID",
        "Stripe Refund Note"
      );
    } else {
      // PayPal Payment Fields
      columns.push(
        "PayPal Order ID",
        "PayPal Capture ID",
        "PayPal Network Transaction ID",
        "PayPal Fee",
        "PayPal Net Amount",
        "PayPal Card Brand",
        "PayPal Card Type",
        "PayPal AVS Code",
        "PayPal CVV Code",
        "PayPal Create Time",
        "PayPal Update Time",
        "PayPal Status",
        "PayPal Amount",
        "PayPal Currency",
        "PayPal Card Last Digits",
        "PayPal Capture Status",
        "PayPal Custom ID",
        // PayPal Refund Fields
        "PayPal Refund ID",
        "PayPal Refund Status",
        "PayPal Refund Amount",
        "PayPal Refund Currency",
        "PayPal Refund Gross Amount",
        "PayPal Refund Fee",
        "PayPal Refund Net Amount",
        "PayPal Refund Create Time",
        "PayPal Refund Update Time",
        "PayPal Refund Invoice ID",
        "PayPal Refund Custom ID",
        "PayPal Refund Note"
      );
    }
    // Create Excel workbook and worksheet
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Sheet1");
    worksheet.columns = columns.map((col) => ({ header: col, key: col }));
    // Stream rows from MongoDB
    const cursor = DataModel.find({ uploadId }).lean().cursor();
    for await (const row of cursor) {
      const decrypted = decryptCardData(row);

      // Base row data
      const rowData = {
        "Expedia ID": decrypted["Expedia ID"] || "",
        Batch: decrypted["Batch"] || "",
        OTA: decrypted["OTA"] || "",
        "Posting Type": decrypted["Posting Type"] || "",
        Portfolio: decrypted["Portfolio"] || "",
        "Hotel Name": decrypted["Hotel Name"] || "",
        "Reservation ID": decrypted["Reservation ID"] || "",
        "Hotel Confirmation Code": decrypted["Hotel Confirmation Code"] || "",
        Name: decrypted["Name"] || "",
        "Check In": decrypted["Check In"] || "",
        "Check Out": decrypted["Check Out"] || "",
        Curency: decrypted["Curency"] || "",
        "Amount to charge": decrypted["Amount to charge"] || "",
        "Charge status": decrypted["Charge status"] || "",
        "Card Number": decrypted["Card Number"] || "",
        "Card Expire": decrypted["Card Expire"] || "",
        "Card CVV": decrypted["Card CVV"] || "",
        "Soft Descriptor": decrypted["Soft Descriptor"] || "",
        "VNP Work ID": userEmail,
        Status: decrypted["Status"] || "",
      };

      // Add payment gateway specific fields
      if (paymentGateway === "stripe") {
        rowData["Connected Account"] = decrypted["Connected Account"] || "";
        // Add Stripe fields using correct database field names
        rowData["Stripe Order ID"] = decrypted["stripeOrderId"] || "";
        rowData["Stripe Capture ID"] = decrypted["stripeCaptureId"] || "";
        rowData["Stripe Network Transaction ID"] =
          decrypted["stripeNetworkTransactionId"] || "";
        rowData["Stripe Fee"] = decrypted["stripeFee"] || "";
        rowData["Stripe Net Amount"] = decrypted["stripeNetAmount"] || "";
        rowData["Stripe Card Brand"] = decrypted["stripeCardBrand"] || "";
        rowData["Stripe Card Type"] = decrypted["stripeCardType"] || "";
        rowData["Stripe AVS Code"] = decrypted["stripeAvsCode"] || "";
        rowData["Stripe CVV Code"] = decrypted["stripeCvvCode"] || "";
        rowData["Stripe Create Time"] = decrypted["stripeCreateTime"] || "";
        rowData["Stripe Update Time"] = decrypted["stripeUpdateTime"] || "";
        rowData["Stripe Status"] = decrypted["stripeStatus"] || "";
        rowData["Stripe Amount"] = decrypted["stripeAmount"] || "";
        rowData["Stripe Currency"] = decrypted["stripeCurrency"] || "";
        rowData["Stripe Card Last Digits"] =
          decrypted["stripeCardLastDigits"] || "";
        rowData["Stripe Capture Status"] =
          decrypted["stripeCaptureStatus"] || "";
        rowData["Stripe Custom ID"] = decrypted["stripeCustomId"] || "";
        // Add Stripe Refund fields
        rowData["Stripe Refund ID"] = decrypted["stripeRefundId"] || "";
        rowData["Stripe Refund Status"] = decrypted["stripeRefundStatus"] || "";
        rowData["Stripe Refund Amount"] = decrypted["stripeRefundAmount"] || "";
        rowData["Stripe Refund Currency"] =
          decrypted["stripeRefundCurrency"] || "";
        rowData["Stripe Refund Gross Amount"] =
          decrypted["stripeRefundGrossAmount"] || "";
        rowData["Stripe Refund Fee"] = decrypted["stripeRefundFee"] || "";
        rowData["Stripe Refund Net Amount"] =
          decrypted["stripeRefundNetAmount"] || "";
        rowData["Stripe Refund Create Time"] =
          decrypted["stripeRefundCreateTime"] || "";
        rowData["Stripe Refund Update Time"] =
          decrypted["stripeRefundUpdateTime"] || "";
        rowData["Stripe Refund Invoice ID"] =
          decrypted["stripeRefundInvoiceId"] || "";
        rowData["Stripe Refund Custom ID"] =
          decrypted["stripeRefundCustomId"] || "";
        rowData["Stripe Refund Note"] = decrypted["stripeRefundNote"] || "";
      } else {
        // Add PayPal fields using correct database field names
        rowData["PayPal Order ID"] = decrypted["paypalOrderId"] || "";
        rowData["PayPal Capture ID"] = decrypted["paypalCaptureId"] || "";
        rowData["PayPal Network Transaction ID"] =
          decrypted["paypalNetworkTransactionId"] || "";
        rowData["PayPal Fee"] = decrypted["paypalFee"] || "";
        rowData["PayPal Net Amount"] = decrypted["paypalNetAmount"] || "";
        rowData["PayPal Card Brand"] = decrypted["paypalCardBrand"] || "";
        rowData["PayPal Card Type"] = decrypted["paypalCardType"] || "";
        rowData["PayPal AVS Code"] = decrypted["paypalAvsCode"] || "";
        rowData["PayPal CVV Code"] = decrypted["paypalCvvCode"] || "";
        rowData["PayPal Create Time"] = decrypted["paypalCreateTime"] || "";
        rowData["PayPal Update Time"] = decrypted["paypalUpdateTime"] || "";
        rowData["PayPal Status"] = decrypted["paypalStatus"] || "";
        rowData["PayPal Amount"] = decrypted["paypalAmount"] || "";
        rowData["PayPal Currency"] = decrypted["paypalCurrency"] || "";
        rowData["PayPal Card Last Digits"] =
          decrypted["paypalCardLastDigits"] || "";
        rowData["PayPal Capture Status"] =
          decrypted["paypalCaptureStatus"] || "";
        rowData["PayPal Custom ID"] = decrypted["paypalCustomId"] || "";
        // Add PayPal Refund fields
        rowData["PayPal Refund ID"] = decrypted["paypalRefundId"] || "";
        rowData["PayPal Refund Status"] = decrypted["paypalRefundStatus"] || "";
        rowData["PayPal Refund Amount"] = decrypted["paypalRefundAmount"] || "";
        rowData["PayPal Refund Currency"] =
          decrypted["paypalRefundCurrency"] || "";
        rowData["PayPal Refund Gross Amount"] =
          decrypted["paypalRefundGrossAmount"] || "";
        rowData["PayPal Refund Fee"] = decrypted["paypalRefundFee"] || "";
        rowData["PayPal Refund Net Amount"] =
          decrypted["paypalRefundNetAmount"] || "";
        rowData["PayPal Refund Create Time"] =
          decrypted["paypalRefundCreateTime"] || "";
        rowData["PayPal Refund Update Time"] =
          decrypted["paypalRefundUpdateTime"] || "";
        rowData["PayPal Refund Invoice ID"] =
          decrypted["paypalRefundInvoiceId"] || "";
        rowData["PayPal Refund Custom ID"] =
          decrypted["paypalRefundCustomId"] || "";
        rowData["PayPal Refund Note"] = decrypted["paypalRefundNote"] || "";
      }

      worksheet.addRow(rowData);
    }
    // Write workbook to buffer
    const buffer = await workbook.xlsx.writeBuffer();
    // Upload to S3 with original file name
    const s3Key = `exports/${uploadId}/${fileName}`;
    const s3Url = await s3Service.uploadFile(
      {
        buffer,
        mimetype:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      },
      s3Key
    );
    // Return S3 URL in response
    res.status(200).json({
      status: "success",
      message: "Excel file uploaded to S3",
      url: s3Url,
    });
  } catch (error) {
    console.error("Download Excel error:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to generate Excel file",
      error: error.message,
    });
  }
};

module.exports = {
  upload,
  uploadFile,
  getRowData,
  getStripeRowData,
  getSingleRowData,
  updateSheet,
  getUserFiles,
  deleteFile,
  deleteUploadById,
  getFileHeaders,
  getUploadStatus,
  getUserUploadSessions,
  resumeUpload,
  cleanupFailedUploads,
  downloadExcelByUploadId,
};
