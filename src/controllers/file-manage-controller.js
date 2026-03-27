/**
 * file-manage-controller.js
 * Handles file uploads, upload sessions, and File History for PayPal, Stripe, and QP gateways.
 * Supports unified upload (S3 + processing), retry, delete, archive, and report download.
 *
 * BOOKMARK LIST (landmarks in this file – search for "// MARK:")
 * ------------------------------------
 * Get all upload sessions (File History)
 *   List upload sessions with pagination/search. Optional gateway filter (e.g. paypal,stripe or qp).
 * Charge count per session
 *   QP: count of QPChargeInstance with status SUCCESS or DECLINED for linked file; PayPal/Stripe: count ExcelData/StripeExcelData with "Charge status" Charged.
 * QP session display (total/processed)
 *   For QP sessions, totalRows and processedRows come from linked QPChargeFile; others use session fields.
 */

const mongoose = require("mongoose");
const XLSX = require("xlsx");
const path = require("path");
const fs = require("fs-extra");
const ExcelData = require("../models/ExcelData");
const StripeExcelData = require("../models/StripeExcelData");
const OTA = require("../models/OTA");
const UploadSession = require("../models/UploadSession");
const QPChargeFile = require("../models/QPChargeFile");
const QPChargeInstance = require("../models/QPChargeInstance");
const { upload, s3Service } = require("../config/s3");
const { encryptCardData, decryptCardData } = require("../utils/encryption");
const User = require("../models/User");
const ExcelJS = require("exceljs");
const { importChargeFileFromPath } = require("./qp-charge-controller");

// Generate unique upload ID
function generateUploadId() {
  return `upload_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Optimized batch processing with better error handling
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

    // Use optimized bulkWrite with better options
    try {
      const result = await DataModel.bulkWrite(
        excelDataRecords.map((record) => ({
          insertOne: { document: record },
        })),
        {
          ordered: false, // Continue processing even if some records fail
          writeConcern: { w: 1, j: false }, // Faster writes with journal disabled for bulk ops
        }
      );

      console.log(
        `\x1b[32m✅ Batch ${batchNumber} SUCCESS: Saved ${result.insertedCount}/${excelDataRecords.length} records\x1b[0m`
      );
      return result.insertedCount;
    } catch (bulkWriteError) {
      // Handle bulk write errors gracefully
      if (
        bulkWriteError.code === 11000 ||
        bulkWriteError.name === "BulkWriteError" ||
        bulkWriteError.result
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

// Optimized bulk OTA lookup function
async function bulkOTALookup(otaNames) {
  if (!otaNames || otaNames.length === 0) return {};

  const uniqueOtaNames = [
    ...new Set(otaNames.filter((name) => name && name.trim())),
  ];
  if (uniqueOtaNames.length === 0) return {};

  try {
    const otaRecords = await OTA.find({
      name: { $in: uniqueOtaNames },
      isActive: true,
    }).lean();

    // Create lookup map for O(1) access
    const otaLookupMap = {};
    otaRecords.forEach((record) => {
      otaLookupMap[record.name] = record;
    });

    return otaLookupMap;
  } catch (error) {
    console.error("Bulk OTA lookup error:", error);
    return {};
  }
}

// Background file processing function
async function processFileInBackground(uploadSession, fileBuffer) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    console.log(
      `\x1b[34m🚀 BACKGROUND PROCESSING: File "${uploadSession.fileName}" - ${
        uploadSession.totalRows
      } rows, ${uploadSession.paymentGateway.toUpperCase()} gateway\x1b[0m`
    );

    // Read the Excel file from buffer (no S3 download needed)
    const workbook = XLSX.read(fileBuffer);
    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];

    // Get sheet range
    const range = firstSheet["!ref"];
    const totalRows = XLSX.utils.decode_range(range).e.r + 1;
    const totalCols = XLSX.utils.decode_range(range).e.c + 1;

    // First pass: collect all OTA names for bulk lookup
    const allOtaNames = [];
    for (let row = 1; row < totalRows; row++) {
      const otaCell =
        firstSheet[
          XLSX.utils.encode_cell({
            r: row,
            c: uploadSession.headers.indexOf("OTA"),
          })
        ];
      if (otaCell && otaCell.v) {
        const otaName = otaCell.v.toString().trim();
        if (otaName) allOtaNames.push(otaName);
      }
    }

    // Perform bulk OTA lookup once
    const otaLookupMap = await bulkOTALookup(allOtaNames);
    console.log(
      `\x1b[36m📊 Bulk OTA lookup complete: ${
        Object.keys(otaLookupMap).length
      } OTA records found\x1b[0m`
    );

    // Process data in optimized batches
    const batchSize = 500; // Increased batch size for better performance
    let totalProcessed = 0;
    let batchNumber = 1;

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

        const rowObject = uploadSession.headers.reduce((acc, header, index) => {
          acc[header.trim()] = rowData[index]?.trim() || null;
          return acc;
        }, {});

        if (Object.keys(rowObject).length > 0 && rowObject["Expedia ID"]) {
          // Normalize Card Expire to YYYY-MM format (optimized)
          let cardExpire = normalizeCardExpiry(rowObject["Card Expire"]);

          // Use pre-fetched OTA data (O(1) lookup instead of database query)
          const otaFromExcel = rowObject["OTA"]?.trim();
          const otaRecord = otaFromExcel ? otaLookupMap[otaFromExcel] : null;

          const mappedData = {
            userId: uploadSession.userId,
            uploadId: uploadSession.uploadId,
            fileName: uploadSession.fileName,
            uploadStatus: "processing",
            rowNumber: row,
            "Expedia ID": rowObject["Expedia ID"] || rowObject["Expedia id"],
            Batch: rowObject["Batch"],
            OTA: rowObject["OTA"] || rowObject["Ota"],
            "Posting Type":
              rowObject["Posting Type"] || rowObject["Posting type"],
            Portfolio: rowObject["Portfolio"],
            "Hotel Name": rowObject["Hotel Name"] || rowObject["Hotel name"],
            "Reservation ID":
              rowObject["Reservation ID"] || rowObject["Reservation id"],
            "Hotel Confirmation Code":
              rowObject["Hotel Confirmation Code"] ||
              rowObject["Hotel confirmation code"],
            Name: rowObject["Name"],
            "Check In":
              normalizeDateField(rowObject["Check In"]) ||
              rowObject["Check in"],
            "Check Out":
              normalizeDateField(rowObject["Check Out"]) ||
              rowObject["Check out"],
            Curency: rowObject["Curency"],
            "Amount to charge":
              rowObject["Amount to charge"] || rowObject["Amount to charge"],
            "Charge status":
              rowObject["Charge Status"] || rowObject["Charge status"],
            "Card Number": rowObject["Card Number"] || rowObject["Card number"],
            "Card Expire": cardExpire,
            "Card CVV": rowObject["Card CVV"] || rowObject["Card cvv"],
            "Soft Descriptor":
              rowObject["Soft Descriptor"] || rowObject["BT MAID"],
            "VNP Work ID": rowObject["VNP Work ID"] || rowObject["VNP work id"],
            Status: rowObject["Status"],
            ota: otaRecord?.name || otaFromExcel || null,
            otaId: otaRecord?._id || null,
          };

          // Add Stripe-specific field if needed
          if (uploadSession.paymentGateway === "stripe") {
            mappedData["Connected Account"] =
              rowObject["Connected Account"] ||
              rowObject["Connected account"] ||
              null;
          }

          // Encrypt sensitive data
          const encryptedData = encryptCardData(mappedData);

          // Debug: Log what's being saved to database
          console.log(
            `\x1b[42m💾 DATABASE SAVE DEBUG - Check In:\x1b[0m`,
            encryptedData["Check In"]
          );
          console.log(
            `\x1b[43m💾 DATABASE SAVE DEBUG - Check Out:\x1b[0m`,
            encryptedData["Check Out"]
          );

          excelDataRecords.push(encryptedData);
        }
      }

      // Process batch if there are records
      if (excelDataRecords.length > 0) {
        const processedCount = await processBatch(
          excelDataRecords,
          uploadSession.uploadId,
          batchNumber,
          uploadSession.paymentGateway
        );
        totalProcessed += processedCount;

        // Update session progress
        await UploadSession.findOneAndUpdate(
          { uploadId: uploadSession.uploadId },
          {
            processedRows: totalProcessed,
            status:
              totalProcessed >= uploadSession.totalRows
                ? "completed"
                : "processing",
          },
          { session }
        );
      }

      batchNumber++;

      // Smaller delay for better performance
      if (batchNumber % 20 === 0) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }

    // Mark session as completed
    await UploadSession.findOneAndUpdate(
      { uploadId: uploadSession.uploadId },
      {
        status: "completed",
        completedAt: new Date(),
      },
      { session }
    );

    console.log(
      `\x1b[32m✅ BACKGROUND PROCESSING COMPLETE: File "${uploadSession.fileName}" processed successfully - ${totalProcessed} records saved\x1b[0m`
    );

    // Delete file from S3 after successful processing
    await s3Service.deleteFile(uploadSession.s3Key);

    // Commit transaction
    await session.commitTransaction();
  } catch (error) {
    // Rollback transaction on error
    await session.abortTransaction();

    console.log(
      `\x1b[31m❌ BACKGROUND PROCESSING ERROR: Failed to process file - ${error.message}\x1b[0m`
    );

    // Update session status to failed
    try {
      await UploadSession.findOneAndUpdate(
        { uploadId: uploadSession.uploadId },
        {
          status: "failed",
          errorMessage: error.message,
        }
      );
    } catch (sessionError) {
      console.error("Error updating session status:", sessionError);
    }
  } finally {
    session.endSession();
  }
}

// Simple date normalization function to fix timezone offset issue
function normalizeDateField(dateValue) {
  console.log(
    `\x1b[33m🔍 DATE DEBUG - Input value:\x1b[0m`,
    dateValue,
    `\x1b[33mType:\x1b[0m`,
    typeof dateValue
  );

  if (!dateValue) {
    console.log(
      `\x1b[31m❌ DATE DEBUG - Empty/null value, returning null\x1b[0m`
    );
    return null;
  }

  // If it's a number (Excel date serial), convert to YYYY-MM-DD with timezone fix
  if (!isNaN(dateValue) && dateValue !== "" && dateValue !== null) {
    const serial = Number(dateValue);
    console.log(
      `\x1b[36m📊 DATE DEBUG - Processing as Excel serial number:\x1b[0m`,
      serial
    );

    // Excel's epoch starts at 1899-12-30
    const excelEpoch = new Date(1899, 11, 30);
    console.log(`\x1b[36m📊 DATE DEBUG - Excel epoch:\x1b[0m`, excelEpoch);

    const date = new Date(excelEpoch.getTime() + serial * 24 * 60 * 60 * 1000);
    console.log(`\x1b[36m📊 DATE DEBUG - Calculated date object:\x1b[0m`, date);

    // Get the date in local timezone to avoid the "one day ahead" issue
    const year = date.getFullYear();
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    const day = date.getDate().toString().padStart(2, "0");
    const result = `${year}-${month}-${day}`;

    console.log(
      `\x1b[32m✅ DATE DEBUG - Final result (from serial):\x1b[0m`,
      result
    );
    return result;
  }

  // For string dates, just return as-is (they were working fine)
  const stringResult = dateValue.toString().trim();
  console.log(
    `\x1b[35m📝 DATE DEBUG - Processing as string, result:\x1b[0m`,
    stringResult
  );
  return stringResult;
}

// Optimized card expiry normalization function
function normalizeCardExpiry(cardExpire) {
  if (!cardExpire) return null;

  // If it's a number (Excel date serial), convert to YYYY-MM
  if (!isNaN(cardExpire) && cardExpire !== "" && cardExpire !== null) {
    const serial = Number(cardExpire);
    const excelEpoch = new Date(1899, 11, 30);
    const date = new Date(excelEpoch.getTime() + serial * 24 * 60 * 60 * 1000);
    const year = date.getFullYear();
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    return `${year}-${month}`;
  }

  // Parse date string patterns
  let match = null;
  if (/^\d{4}-\d{2}$/.test(cardExpire)) {
    return cardExpire; // Already correct format
  } else if ((match = cardExpire.match(/^(\d{1,2})[\/-](\d{4})$/))) {
    return `${match[2]}-${match[1].padStart(2, "0")}`;
  } else if ((match = cardExpire.match(/^(\d{4})[\/-](\d{1,2})$/))) {
    return `${match[1]}-${match[2].padStart(2, "0")}`;
  } else if ((match = cardExpire.match(/^(\d{1,2})[\/-](\d{2})$/))) {
    return `20${match[2]}-${match[1].padStart(2, "0")}`;
  } else if ((match = cardExpire.match(/^(\d{2})[\/-](\d{2})[\/-](\d{4})$/))) {
    return `${match[3]}-${match[2]}`;
  }

  // Fallback: try Date.parse
  const d = new Date(cardExpire);
  if (!isNaN(d)) {
    const year = d.getFullYear();
    const month = (d.getMonth() + 1).toString().padStart(2, "0");
    return `${year}-${month}`;
  }

  return cardExpire;
}

// Detect if file is QuantumPay charge format (columns typical of QP charge sheets)
function isQPChargeFormat(headers) {
  const lower = headers.map((h) => (h || "").toLowerCase());
  const hasOtaOrVnp =
    lower.some((h) => h.includes("ota")) ||
    lower.some((h) => h.includes("vnp work id") || h === "vnp work id");
  const hasAmount = lower.some(
    (h) => h.includes("amount to charge") || h === "amount to charge",
  );
  const hasCard = lower.some(
    (h) => h.includes("card number") || h === "card number",
  );
  return !!(hasOtaOrVnp && hasAmount && hasCard);
}

// Optimized upload file API - returns immediately and processes in background
const uploadFile = async (req, res) => {
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
      .slice(0, -5);
    const fileExtension = originalFileName.substring(
      originalFileName.lastIndexOf(".")
    );
    const fileNameWithoutExt = originalFileName.substring(
      0,
      originalFileName.lastIndexOf(".")
    );
    const fileName = `${fileNameWithoutExt}_${timestamp}${fileExtension}`;

    // Check for existing upload session
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

    // Download file from S3 for processing
    const fileBuffer = await s3Service.downloadFile(req.file.key);

    const workbook = XLSX.read(fileBuffer);
    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
    const range = firstSheet["!ref"];
    if (!range) {
      await s3Service.deleteFile(req.file.key);
      return res.status(400).json({
        status: "error",
        message: "Invalid or empty sheet",
      });
    }
    const totalRows = XLSX.utils.decode_range(range).e.r + 1;
    const totalCols = XLSX.utils.decode_range(range).e.c + 1;

    // Get headers for format detection
    const headers = [];
    for (let col = 0; col < totalCols; col++) {
      const cell = firstSheet[XLSX.utils.encode_cell({ r: 0, c: col })];
      if (cell) {
        const headerName = cell.v.toString().trim();
        headers.push(headerName);
      }
    }

    // Auto-detect payment gateway: Stripe -> QP -> PayPal
    let paymentGateway;
    if (headers.includes("Connected Account")) {
      paymentGateway = "stripe";
    } else if (isQPChargeFormat(headers)) {
      paymentGateway = "qp";
    } else {
      paymentGateway = "paypal";
    }

    // QP path: run QP import and create a linked UploadSession
    if (paymentGateway === "qp") {
      const tempDir = path.join(__dirname, "..", "public", "temp");
      await fs.ensureDir(tempDir);
      const tempPath = path.join(
        tempDir,
        `qp_${Date.now()}_${path.basename(originalFileName)}`,
      );
      try {
        await fs.writeFile(tempPath, fileBuffer);
        const {
          chargeFile,
          skipped_duplicate_reservation_rows,
          duplicate_reservation_ids,
        } = await importChargeFileFromPath(
          tempPath,
          userId,
          originalFileName,
        );
        const uploadId = generateUploadId();
        const uploadSession = new UploadSession({
          uploadId,
          userId,
          fileName,
          originalFileName,
          s3Key: req.file.key,
          totalRows: chargeFile.total_rows,
          processedRows: chargeFile.processed_rows,
          status: "completed",
          completedAt: new Date(),
          vnpWorkId: vnpWorkId || null,
          headers,
          batchSize: 500,
          paymentGateway: "qp",
          linkedQpChargeFileId: chargeFile._id,
          ota: null,
          otaId: null,
        });
        await uploadSession.save();
        console.log(
          `\x1b[34m🚀 QP UPLOAD: File "${fileName}" -> QPChargeFile ${chargeFile._id}, ${chargeFile.total_rows} rows\x1b[0m`,
        );
        return res.status(202).json({
          status: "success",
          message:
            "QP charge file imported successfully. View and process in QP Payment.",
          data: {
            uploadId,
            fileName,
            totalRows: chargeFile.total_rows,
            paymentGateway: "qp",
            linkedQpChargeFileId: chargeFile._id,
            skipped_duplicate_reservation_rows,
            duplicate_reservation_ids,
          },
        });
      } finally {
        await fs.remove(tempPath).catch(() => {});
      }
    }

    // PayPal/Stripe: require Expedia ID in A1
    const cellA1 = firstSheet["A1"] ? firstSheet["A1"].v : null;
    if (cellA1 !== "Expedia ID") {
      await s3Service.deleteFile(req.file.key);
      return res.status(400).json({
        status: "error",
        message: 'Invalid VNP Work file. Cell A1 must contain "Expedia ID"',
      });
    }

    // Generate unique upload ID
    const uploadId = generateUploadId();

    console.log(
      `\x1b[34m🚀 UPLOAD INITIATED: File "${fileName}" - ${
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
      batchSize: 500,
      paymentGateway: paymentGateway,
      ota: null,
      otaId: null,
    });

    await uploadSession.save();

    // Start background processing (don't await - process asynchronously)
    processFileInBackground(uploadSession, fileBuffer).catch((error) => {
      console.error("Background processing error:", error);
    });

    // Return immediate response
    res.status(202).json({
      status: "success",
      message: "File upload initiated successfully. Processing in background.",
      data: {
        uploadId: uploadId,
        fileName: fileName,
        totalRows: totalRows - 1,
        totalColumns: totalCols,
        headers: headers,
        status: "processing",
        message:
          "File is being processed. Use the upload status endpoint to track progress.",
      },
    });
  } catch (error) {
    console.log(
      `\x1b[31m❌ UPLOAD ERROR: Failed to initiate upload - ${error.message}\x1b[0m`
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
        // Clean up S3 file on error
        await s3Service.deleteFile(req.file.key);
      } catch (cleanupError) {
        console.error("Error during cleanup:", cleanupError);
      }
    }

    res.status(500).json({
      status: "error",
      message: "Error initiating file upload",
      error: error.message,
    });
  }
};


const getUploadFileSummaries = async (req, res) => {
  try {
    const { search } = req.query;
    const query = {};

    if (search && search.trim() !== "") {
      query.fileName = { $regex: search.trim(), $options: "i" };
    }

    const sessions = await UploadSession.find(query, "uploadId fileName")
      .sort({ createdAt: -1 })
      .lean();

    res.status(200).json({
      status: "success",
      data: sessions.map((session) => ({
        _id: session._id,
        uploadId: session.uploadId,
        fileName: session.fileName,
      })),
    });
  } catch (error) {
    console.error("Error getting upload file summaries:", error);
    res.status(500).json({
      status: "error",
      message: "Error getting upload file summaries",
      error: error.message,
    });
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
      uploadId,
    } = req.query;
    const userId = req.user.userId;

    const skip = (parseInt(page) - 1) * parseInt(limit);

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Build query object - show data from all users
    const query = {};

    // Filter out archived rows - only show rows where archive is false or doesn't exist
    query.archive = { $ne: true };

    // Add filter for Charge status if provided (skip if "All" is selected)
    if (chargeStatus && chargeStatus.trim() !== "" && chargeStatus !== "All") {
      query["Charge status"] = chargeStatus;
    }

    if (uploadId && uploadId.trim() !== "") {
      query.uploadId = uploadId.trim();
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
        { fileName: searchRegex },
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

          // Debug: Log what's being retrieved from database and sent to UI
          

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
          uploadId: uploadId || null,
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

    // Filter out archived rows - only show rows where archive is false or doesn't exist
    query.archive = { $ne: true };

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
      "Check In": normalizeDateField(updateData["Check In"]),
      "Check Out": normalizeDateField(updateData["Check Out"]),
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

    const paymentGateway = uploadSession.paymentGateway || "paypal";
    let recordCount = 0;
    let excelDataDeleteResult = { deletedCount: 0 };

    if (paymentGateway === "qp" && uploadSession.linkedQpChargeFileId) {
      // QP session: soft-delete the linked QP charge file and its instances
      const qpFile = await QPChargeFile.findById(
        uploadSession.linkedQpChargeFileId,
      ).session(session);
      if (qpFile) {
        recordCount = qpFile.total_rows || 0;
        qpFile.deleted_at = new Date();
        qpFile.deleted_by = req.user?.userId;
        await qpFile.save({ session });
        await QPChargeInstance.updateMany(
          { charge_file_id: qpFile._id },
          {
            $set: {
              deleted_at: new Date(),
              deleted_by: req.user?.userId,
            },
          },
          { session },
        );
        excelDataDeleteResult = { deletedCount: recordCount };
      }
      console.log(
        `\x1b[36m🗑️  Deleting QP upload: Upload ID ${uploadId}, linked QPChargeFile ${uploadSession.linkedQpChargeFileId}\x1b[0m`,
      );
    } else {
      const DataModel =
        paymentGateway === "stripe" ? StripeExcelData : ExcelData;
      recordCount = await DataModel.countDocuments({
        uploadId: uploadId,
      });
      console.log(
        `\x1b[36m🗑️  Deleting upload: Upload ID ${uploadId} with ${recordCount} records (${paymentGateway.toUpperCase()})\x1b[0m`,
      );
      excelDataDeleteResult = await DataModel.deleteMany(
        { uploadId: uploadId },
        { session },
      );
    }

    console.log(
      `\x1b[32m✅ DELETE SUCCESS: Removed ${excelDataDeleteResult.deletedCount}/${recordCount} records from database\x1b[0m`,
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
        paymentGateway: uploadSession.paymentGateway || "paypal",
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

// MARK: Get all upload sessions (File History)
// List with optional gateway filter (main: paypal,stripe; qpvt: qp), pagination, search.
const getUserUploadSessions = async (req, res) => {
  try {
    const userId = req.user.userId;
    const { status, limit = 20, page = 1, search, gateway } = req.query;

    const query = {}; // Show sessions from all users
    if (status) {
      query.status = status;
    }

    // Filter by payment gateway (e.g. main branch: paypal,stripe; qpvt branch: qp)
    if (gateway && typeof gateway === "string") {
      const gateways = gateway
        .split(",")
        .map((g) => g.trim().toLowerCase())
        .filter(Boolean);
      if (gateways.length) {
        query.paymentGateway = { $in: gateways };
      }
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

    // MARK: Charge count per session – QP: SUCCESS+DECLINED instances; PayPal/Stripe: Charged count
    const chargedCounts = await Promise.all(
      sessions.map(async (session) => {
        const paymentGateway = session.paymentGateway || "paypal";
        if (paymentGateway === "qp" && session.linkedQpChargeFileId) {
          // Count from instances so it's correct for both bulk runs and single-instance charges
          return await QPChargeInstance.countDocuments({
            charge_file_id: session.linkedQpChargeFileId,
            status: { $in: ["SUCCESS", "DECLINED"] },
            deleted_at: null,
          });
        }
        const DataModel =
          paymentGateway === "stripe" ? StripeExcelData : ExcelData;
        return await DataModel.countDocuments({
          uploadId: session.uploadId,
          "Charge status": "Charged",
        });
      }),
    );

    // MARK: QP session display – totalRows/processedRows from linked QPChargeFile
    const sessionsWithQP = await Promise.all(
      sessions.map(async (session, idx) => {
        const paymentGateway = session.paymentGateway || "paypal";
        let totalRows = session.totalRows;
        let processedRows = session.processedRows;
        if (paymentGateway === "qp" && session.linkedQpChargeFileId) {
          const qpFile = await QPChargeFile.findById(
            session.linkedQpChargeFileId,
          ).lean();
          if (qpFile) {
            totalRows = qpFile.total_rows || 0;
            processedRows = qpFile.processed_rows || 0;
          }
        }
        return {
          ...session.toObject(),
          _totalRows: totalRows,
          _processedRows: processedRows,
          _chargedCount: chargedCounts[idx],
        };
      }),
    );

    res.status(200).json({
      status: "success",
      data: {
        sessions: sessionsWithQP.map((s) => ({
          uploadId: s.uploadId,
          fileName: s.fileName,
          status: s.status,
          totalRows: s._totalRows,
          processedRows: s._processedRows,
          progress:
            s.status === "completed"
              ? 100
              : s._totalRows > 0
                ? Math.round((s._processedRows / s._totalRows) * 100)
                : 0,
          startedAt: s.startedAt,
          completedAt: s.completedAt,
          chargedCount: s._chargedCount,
          paymentGateway: s.paymentGateway || "paypal",
          linkedQpChargeFileId: s.linkedQpChargeFileId || null,
          archive: s.archive || false,
          uploadedBy: {
            userId: s.userId._id,
            email: s.userId.email,
            name:
              s.userId.name || s.userId.username || "Unknown User",
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
            "Check In": normalizeDateField(rowObject["Check In"]),
            "Check Out": normalizeDateField(rowObject["Check Out"]),
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

// Get transaction history with combined PayPal and Stripe data
const getTransactionHistory = async (req, res) => {
  try {
    const {
      limit = 10,
      page = 1,
      search = "",
      filter = "All", // 'All', 'PayPal', 'Stripe'
    } = req.query;

    const skip = (parseInt(page) - 1) * parseInt(limit);

    // Build query for charged transactions only
    const baseQuery = { "Charge status": "Charged" };

    // Add search functionality if provided
    let searchQuery = {};
    if (search && search.trim() !== "") {
      const searchRegex = { $regex: search.trim(), $options: "i" };
      searchQuery = {
        $or: [
          { "Expedia ID": searchRegex },
          { Batch: searchRegex },
          { OTA: searchRegex },
          { "Hotel Name": searchRegex },
          { "Reservation ID": searchRegex },
          { "Hotel Confirmation Code": searchRegex },
          { Name: searchRegex },
          { "VNP Work ID": searchRegex },
        ],
      };
    }

    let paypalData = [];
    let stripeData = [];

    // Fetch data based on filter
    if (filter === "All" || filter === "PayPal" || !filter) {
      // Get PayPal data
      const paypalQuery = { ...baseQuery, ...searchQuery };
      paypalData = await ExcelData.find(paypalQuery)
        .populate("userId", "name email")
        .populate("otaId", "name")
        .sort({ updatedAt: -1 });
    }

    if (filter === "All" || filter === "Stripe" || !filter) {
      // Get Stripe data
      const stripeQuery = { ...baseQuery, ...searchQuery };
      stripeData = await StripeExcelData.find(stripeQuery)
        .populate("userId", "name email")
        .populate("otaId", "name")
        .sort({ updatedAt: -1 });
    }

    // Add payment gateway identifier and decrypt card data for each record
    const paypalDataWithGateway = paypalData.map((record) => {
      // Remove MongoDB internal fields and extract data
      const { _id, userId, __v, createdAt, updatedAt, ...excelData } =
        record.toObject();

      // Decrypt sensitive card data before returning
      const decryptedData = decryptCardData(excelData);

      return {
        id: _id,
        ...decryptedData,
        paymentGateway: "PayPal",
        createdAt: createdAt,
        // Use paypalUpdateTime if available, otherwise use updatedAt
        sortTimestamp: record.paypalUpdateTime
          ? new Date(record.paypalUpdateTime)
          : record.updatedAt,
      };
    });

    const stripeDataWithGateway = stripeData.map((record) => {
      // Remove MongoDB internal fields and extract data
      const { _id, userId, __v, createdAt, updatedAt, ...excelData } =
        record.toObject();

      // Decrypt sensitive card data before returning
      const decryptedData = decryptCardData(excelData);

      return {
        id: _id,
        ...decryptedData,
        paymentGateway: "Stripe",
        createdAt: createdAt,
        // Use stripeCreatedAt if available, otherwise use updatedAt
        sortTimestamp: record.stripeCreatedAt
          ? new Date(record.stripeCreatedAt)
          : record.updatedAt,
      };
    });

    // Combine and sort all data by timestamp (newer first)
    const combinedData = [
      ...paypalDataWithGateway,
      ...stripeDataWithGateway,
    ].sort((a, b) => new Date(b.sortTimestamp) - new Date(a.sortTimestamp));

    // Apply pagination to combined data
    const totalRecords = combinedData.length;
    const paginatedData = combinedData.slice(skip, skip + parseInt(limit));

    // Calculate pagination info
    const totalPages = Math.ceil(totalRecords / parseInt(limit));
    const hasNext = parseInt(page) < totalPages;
    const hasPrev = parseInt(page) > 1;

    res.status(200).json({
      status: "success",
      message: "Transaction history retrieved successfully",
      data: paginatedData,
      pagination: {
        currentPage: parseInt(page),
        totalPages,
        totalRecords,
        hasNext,
        hasPrev,
        limit: parseInt(limit),
      },
      filter: filter || "All",
      search: search || "",
    });
  } catch (error) {
    console.error("Transaction history error:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to retrieve transaction history",
      error: error.message,
    });
  }
};

// Archive all rows for a specific upload session
const archiveFile = async (req, res) => {
  try {
    const { uploadId } = req.params;
    const userId = req.user.userId;

    if (!uploadId) {
      return res.status(400).json({
        status: "error",
        message: "Upload ID is required",
      });
    }

    // Find the upload session to determine payment gateway
    const uploadSession = await UploadSession.findOne({
      uploadId: uploadId,
    });

    if (!uploadSession) {
      return res.status(404).json({
        status: "error",
        message: "Upload session not found",
      });
    }

    // Choose the appropriate model based on payment gateway
    const paymentGateway = uploadSession.paymentGateway || "paypal";
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Update all rows for this uploadId to set archive: true
    const updateResult = await DataModel.updateMany(
      { uploadId: uploadId },
      { $set: { archive: true } }
    );

    // Update the upload session to set archive: true
    await UploadSession.findOneAndUpdate(
      { uploadId: uploadId },
      { $set: { archive: true } }
    );

    console.log(
      `\x1b[36m📦 ARCHIVED: Upload ID ${uploadId} - ${updateResult.modifiedCount} rows archived (${paymentGateway.toUpperCase()})\x1b[0m`
    );

    res.status(200).json({
      status: "success",
      message: "File archived successfully",
      data: {
        uploadId: uploadId,
        fileName: uploadSession.fileName,
        archivedRows: updateResult.modifiedCount,
        matchedRows: updateResult.matchedCount,
      },
    });
  } catch (error) {
    console.error("Error archiving file:", error);
    res.status(500).json({
      status: "error",
      message: "Error archiving file",
      error: error.message,
    });
  }
};

// Unarchive all rows for a specific upload session
const unarchiveFile = async (req, res) => {
  try {
    const { uploadId } = req.params;
    const userId = req.user.userId;

    if (!uploadId) {
      return res.status(400).json({
        status: "error",
        message: "Upload ID is required",
      });
    }

    // Find the upload session to determine payment gateway
    const uploadSession = await UploadSession.findOne({
      uploadId: uploadId,
    });

    if (!uploadSession) {
      return res.status(404).json({
        status: "error",
        message: "Upload session not found",
      });
    }

    // Choose the appropriate model based on payment gateway
    const paymentGateway = uploadSession.paymentGateway || "paypal";
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Update all rows for this uploadId to set archive: false
    const updateResult = await DataModel.updateMany(
      { uploadId: uploadId },
      { $set: { archive: false } }
    );

    // Update the upload session to set archive: false
    await UploadSession.findOneAndUpdate(
      { uploadId: uploadId },
      { $set: { archive: false } }
    );

    console.log(
      `\x1b[36m📂 UNARCHIVED: Upload ID ${uploadId} - ${updateResult.modifiedCount} rows unarchived (${paymentGateway.toUpperCase()})\x1b[0m`
    );

    res.status(200).json({
      status: "success",
      message: "File unarchived successfully",
      data: {
        uploadId: uploadId,
        fileName: uploadSession.fileName,
        unarchivedRows: updateResult.modifiedCount,
        matchedRows: updateResult.matchedCount,
      },
    });
  } catch (error) {
    console.error("Error unarchiving file:", error);
    res.status(500).json({
      status: "error",
      message: "Error unarchiving file",
      error: error.message,
    });
  }
};

// Create a new ExcelData entry manually
const createExcelData = async (req, res) => {
  try {
    const userId = req.user.userId;
    const { paymentGateway = "paypal" } = req.query;
    const data = req.body;

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Generate unique uploadId for manual entry
    const uploadId = `manual_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const fileName = `Manual Entry - ${new Date().toISOString().split('T')[0]}`;

    // Normalize Card Expire to YYYY-MM format
    let cardExpire = normalizeCardExpiry(data["Card Expire"]);

    // Process OTA field if provided
    let otaRecord = null;
    const otaFromExcel = data["OTA"]?.trim();

    if (otaFromExcel) {
      try {
        otaRecord = await OTA.findOne({
          name: otaFromExcel,
          isActive: true,
        });
      } catch (otaError) {
        console.error("Error finding OTA record:", otaError);
      }
    }

    // Map the data
    const mappedData = {
      userId: userId,
      uploadId: uploadId,
      fileName: fileName,
      uploadStatus: "completed",
      rowNumber: 1,
      "Expedia ID": data["Expedia ID"] || null,
      Batch: data["Batch"] || null,
      OTA: data["OTA"] || null,
      "Posting Type": data["Posting Type"] || null,
      Portfolio: data["Portfolio"] || null,
      "Hotel Name": data["Hotel Name"] || null,
      "Reservation ID": data["Reservation ID"] || null,
      "Hotel Confirmation Code": data["Hotel Confirmation Code"] || null,
      Name: data["Name"] || null,
      "Check In": normalizeDateField(data["Check In"]) || null,
      "Check Out": normalizeDateField(data["Check Out"]) || null,
      Curency: data["Curency"] || null,
      "Amount to charge": data["Amount to charge"] || null,
      "Charge status": data["Charge status"] || null,
      "Card Number": data["Card Number"] || null,
      "Card Expire": cardExpire,
      "Card CVV": data["Card CVV"] || null,
      "Soft Descriptor": data["Soft Descriptor"] || data["BT MAID"] || null,
      "VNP Work ID": data["VNP Work ID"] || null,
      Status: data["Status"] || null,
      ota: otaRecord?.name || otaFromExcel || null,
      otaId: otaRecord?._id || null,
      archive: false, // Manual entries are not archived by default
    };

    // Add Stripe-specific field if payment gateway is stripe
    if (paymentGateway === "stripe") {
      mappedData["Connected Account"] = data["Connected Account"] || null;
    }

    // Encrypt sensitive card data
    const encryptedData = encryptCardData(mappedData);

    // Create the record
    const newRecord = new DataModel(encryptedData);
    await newRecord.save();

    // Populate OTA data
    await newRecord.populate("otaId", "name displayName customer billingAddress isActive");

    console.log(
      `\x1b[32m✅ MANUAL ENTRY CREATED: Record ID ${newRecord._id} (${paymentGateway.toUpperCase()})\x1b[0m`
    );

    // Prepare response data
    const { _id, userId: userIdField, __v, createdAt, updatedAt, ...excelData } =
      newRecord.toObject();

    // Decrypt sensitive card data before returning
    const decryptedData = decryptCardData(excelData);

    res.status(201).json({
      status: "success",
      message: "Excel data created successfully",
      data: {
        id: _id,
        ...decryptedData,
        createdAt: createdAt,
      },
    });
  } catch (error) {
    console.error("Error creating Excel data:", error);
    res.status(500).json({
      status: "error",
      message: "Error creating Excel data",
      error: error.message,
    });
  }
};

// Get all manually created ExcelData entries
const getManualExcelData = async (req, res) => {
  try {
    const { paymentGateway = "paypal" } = req.query;

    // Choose the appropriate model based on payment gateway
    const DataModel = paymentGateway === "stripe" ? StripeExcelData : ExcelData;

    // Build query object - filter for manual entries only (across all users)
    const query = {
      uploadId: { $regex: "^manual_", $options: "i" }, // Match uploadId starting with "manual_"
    };

    // Get all manual entries
    const rowData = await DataModel.find(query)
      .populate("otaId", "name displayName customer billingAddress isActive") // Populate OTA data
      .sort({ createdAt: -1 });

    res.status(200).json({
      status: "success",
      message: "Manual ExcelData entries retrieved successfully",
      data: rowData.map((row) => {
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
    });
  } catch (error) {
    console.error("Get manual ExcelData error:", error);
    res.status(500).json({
      status: "error",
      message: "Error retrieving manual ExcelData entries",
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
  getUploadFileSummaries,
  getUserUploadSessions,
  resumeUpload,
  cleanupFailedUploads,
  downloadExcelByUploadId,
  getTransactionHistory,
  archiveFile,
  unarchiveFile,
  createExcelData,
  getManualExcelData,
};
