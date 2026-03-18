/**
 * qp-charge-controller.js
 * QP (Quantum Pay) charge file and instance lifecycle: import XLSX/CSV, CRUD charge files/instances,
 * single/bulk charge processing, export/report download. Export matches template column order + Status Reason, Provider Txn ID, Processed At.
 *
 * BOOKMARK LIST (landmarks in this file – search for "// MARK:")
 * ------------------------------------
 * Map Row to Instance
 *   mapRowToInstance: maps template row to QPChargeInstance (hotel_id, hotel_name, reservation_id, amount, card, billing, status). Handles Hotel Name, flexible column names.
 * Import Charging Sheet (shared logic from path)
 *   importChargeFileFromPath: reads workbook, creates QPChargeFile, inserts QPChargeInstance per row.
 * Import Charging Sheet (HTTP handler)
 *   API handler that accepts multipart file and calls importChargeFileFromPath.
 * Get Charge Files / Get Single Charge File
 *   List and fetch QPChargeFile with optional filters.
 * Get Charge Instances with filters, search, pagination, and aggregate stats
 *   Paginated instances for a file with status aggregates.
 * Delete Charge File (Soft) / Update Charge File / Get Charge File Progress
 *   Soft delete, patch metadata, progress (success_count, declined_count, etc.).
 * Export Raw Pre-processing Instances
 *   Export instances before charging (template-style).
 * Get Single Charge Instance / Update Charge Instance / Delete Charge Instance
 *   CRUD for one instance with validation.
 * Helper – instance to export row (template column order + Status Reason, Provider Txn ID, Processed At)
 *   instanceToExportRow: template headers (Hotel ID*, Portfolio*, Hotel Name*, ... Charge Status) then Status Reason, Provider Txn ID, Processed At. Decrypts card/CVV.
 * Export Filtered Charge Instances
 *   XLSX download of instances (filter by file, status, ids) using instanceToExportRow.
 * Process Single Instance / Process Multiple by ID list / Create and process single (manual entry)
 *   chargeInstance helper; API handlers for one or many charges.
 * Process Bulk Run for a Charge File
 *   processBulkRunAsync: run all PENDING instances for a file, update file success_count/declined_count.
 * Download Compiled Excel
 *   Download compiled results (RowNumber, HotelID, Status, etc.) for a charge file.
 * Download Report (parent file style with charge status column)
 *   Report using instanceToExportRow (template order + extras) for a charge file.
 */

const crypto = require("crypto");
const xlsx = require("xlsx");
const path = require("path");
const fs = require("fs-extra");
const mongoose = require("mongoose");

const QPChargeFile = require("../models/QPChargeFile");
const QPChargeInstance = require("../models/QPChargeInstance");
const UploadSession = require("../models/UploadSession");
const User = require("../models/User");
const { encrypt, decrypt } = require("../utils/encryption");
const {
  TraceLogger,
  generateRequestId,
  generateRunId,
} = require("../utils/logger");

const catchAsync = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// MARK: Map Row to Instance
const mapRowToInstance = (row, chargeFileId, rowNumber, fileName) => {
  const normalize = (str) =>
    String(str || "")
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");

  const getVal = (...keys) => {
    // direct match first
    for (let k of keys) {
      if (row[k] !== undefined && row[k] !== null) return String(row[k]).trim();
    }

    // fallback: try to find a column whose normalized form contains the
    // normalized lookup key, or vice versa.  This handles things like
    // "ReservationID" (no space) or truncated headings such as "Amount to cl".
    const normRowKeys = Object.keys(row).map((k) => ({
      orig: k,
      norm: normalize(k),
    }));

    for (let k of keys) {
      const target = normalize(k);
      for (let { orig, norm } of normRowKeys) {
        if (norm.includes(target) || target.includes(norm)) {
          const v = row[orig];
          if (v !== undefined && v !== null) return String(v).trim();
        }
      }
    }

    return "";
  };

  const instance = new QPChargeInstance({
    charge_file_id: chargeFileId,
    parent_file_name: fileName,
    row_number: rowNumber,

    ota: getVal("OTA Name", "OTA", "ota", "Ota"),
    vnp_work_id: getVal(
      "VNP Work ID",
      "VNP World ID",
      "vnp_work_id",
      "VNP_Work_ID",
      "Work ID",
    ),
    portfolio: getVal("Portfolio", "portfolio"),

    hotel_id: getVal("Hotel ID", "OTA ID", "Expedia ID", "Hotel_ID", "hotel_id"),
    hotel_name: getVal("Hotel Name", "Hotel Name*", "hotel_name"),
    reservation_id: getVal(
      "ReservationID",
      "Reservation ID",
      "reservation_id",
      "Order ID",
    ),
    amount_numeric:
      parseFloat(getVal("Amount to charge", "amount", "Amount")) || null,
    currency: getVal("Currency", "Curency", "currency") || "USD",
    user_id: getVal(
      "OTA Billing Name",
      "QP Username",
      "Name",
      "Username",
      "User ID",
      "user_id",
      "Name ",
    ),

    billing_address: {
      address_1: getVal("Address", "Address 1", "address_1"),
      address_2: getVal("Address 2", "address_2"),
      city: getVal("City", "city"),
      state: getVal("State", "state"),
      postal_code: getVal("Zip Code", "Zip", "Postal Code", "zip", "postal_code"),
      country_code: getVal("Country", "country", "Country Code") || "US",
    },

    status: "PENDING",
  });

  // Handle Card
  const fullPan = getVal("Card Number", "card_number", "PAN");
  if (fullPan) {
    instance.card_number = encrypt(fullPan);
    instance.card_last4 = fullPan.slice(-4);
  }

  function excelDateToJSDate(excelDate) {
    const daysSinceEpoch = excelDate - 25569;
    const milliseconds = daysSinceEpoch * 86400 * 1000;
    const jsDate = new Date(Math.round(milliseconds));

    return jsDate;
  }

  let excelExpireDate = getVal(
    "Expire",
    "Card Expire",
    "Card Expire MM/YY",
    "Exp",
  );
  const expire =
    excelDateToJSDate(excelExpireDate).getTime() > 0
      ? excelDateToJSDate(excelExpireDate)
      : excelExpireDate; // Try Excel date first, fallback to string parsing
  if (expire) {
    const parseExpiryDate = (expiryStr) => {
      if (!expiryStr) return { month: null, year: null };

      const result = { month: null, year: null };

      // If we already have a JS Date (e.g., converted from Excel), use it directly.
      if (expiryStr instanceof Date && !isNaN(expiryStr.getTime())) {
        result.month = expiryStr.getUTCMonth() + 1;
        result.year = expiryStr.getUTCFullYear() % 100; // store 2-digit year
        return result;
      }

      const str = String(expiryStr).trim();

      // ISO date-like string: YYYY-MM-...
      const isoMatch = str.match(/^(\d{4})-(\d{2})/);
      if (isoMatch) {
        const year4 = parseInt(isoMatch[1], 10);
        const month2 = parseInt(isoMatch[2], 10);
        if (!isNaN(month2) && month2 >= 1 && month2 <= 12)
          result.month = month2;
        if (!isNaN(year4) && year4 >= 2000 && year4 <= 2099)
          result.year = year4 % 100;
        return result;
      }

      // Split by delimiter (/ or -)
      let parts = [];
      if (str.includes("/")) {
        parts = str.split("/");
      } else if (str.includes("-")) {
        parts = str.split("-");
      } else {
        parts = [str]; // Single value, no split
      }

      // Extract last two parts and parse as integers
      if (parts.length >= 2) {
        // Take second-to-last as month, last as year
        const monthPart = parseInt(parts[parts.length - 2], 10);
        const yearPart = parseInt(parts[parts.length - 1], 10);

        if (!isNaN(monthPart) && monthPart >= 1 && monthPart <= 12) {
          result.month = monthPart;
        }

        // Normalize year: if 2-digit (0-99), assume 20xx but store as 2 digits
        if (!isNaN(yearPart)) {
          if (yearPart >= 0 && yearPart <= 99) {
            result.year = yearPart; // Keep as 2 digits
          } else if (yearPart >= 2000 && yearPart <= 2099) {
            result.year = yearPart % 100; // Convert 4-digit to 2-digit
          }
        }
      }

      return result;
    };

    const { month, year } = parseExpiryDate(expire);
    if (month) instance.expiry_month = month;
    if (year) instance.expiry_year = year;
  }

  const cvv = getVal("Card CVV", "CVV", "cvv", "Cvv");
  if (cvv) {
    instance.cvv = encrypt(cvv);
  }

  // Idempotency key (uses QP username for lookup; same user + reservation + amount + card = same charge)
  const keyUser = (instance.user_id || instance.hotel_id || "").toString().trim();
  instance.charge_key = crypto
    .createHash("sha256")
    .update(
      `${keyUser}-${instance.reservation_id}-${instance.amount_numeric}-${instance.card_last4}`,
    )
    .digest("hex");

  // Partial file upload: read status from file if present (e.g. re-upload of half-processed file)
  const statusFromFile = getVal(
    "Charge Status",
    "Status",
    "charge status",
    "Charge status",
  );
  const allowedStatuses = [
    "PENDING",
    "PROCESSING",
    "SUCCESS",
    "DECLINED",
    "ERROR",
    "INVALID",
    "SKIPPED",
  ];
  if (statusFromFile) {
    const normalized = String(statusFromFile).trim().toUpperCase();
    if (allowedStatuses.includes(normalized)) {
      instance.status = normalized;
    }
  }

  // Basic Validation (QP username required for terminal key lookup)
  if (
    !instance.user_id ||
    !instance.reservation_id ||
    !instance.amount_numeric ||
    !fullPan
  ) {
    instance.status = "INVALID";
    instance.status_reason =
      "Missing required field (QP username, reservation, amount, or card).";
  }

  return instance;
};

// MARK: 1. Import Charging Sheet (shared logic from path)
// Import QP charge file from a local file path. Used by both the API handler and the unified upload flow.
async function importChargeFileFromPath(filePath, userId, originalFileName) {
  const isXlsx = originalFileName.toLowerCase().endsWith(".xlsx");

  const chargeFile = await QPChargeFile.create({
    file_name: originalFileName,
    file_type: isXlsx ? "XLSX" : "CSV",
    storage_path: filePath,
    created_by: userId,
  });

  const workbook = xlsx.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const rows = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName], {
    defval: "",
  });

  let totalRows = rows.length;
  let validRows = 0;
  let invalidRows = 0;

  const instancesToInsert = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const instance = mapRowToInstance(
      row,
      chargeFile._id,
      i + 1,
      originalFileName,
    );
    instance.created_by = userId;

    if (instance.status === "INVALID") invalidRows++;
    else validRows++;

    instancesToInsert.push(instance);
  }

  const keyMap = new Set();
  const keyToIndices = new Map();

  instancesToInsert.forEach((inst, idx) => {
    if (inst.status !== "INVALID") {
      if (!keyToIndices.has(inst.charge_key)) {
        keyToIndices.set(inst.charge_key, []);
      }
      keyToIndices.get(inst.charge_key).push(idx);

      if (keyMap.has(inst.charge_key)) {
        inst.is_duplicate = true;
      }
      keyMap.add(inst.charge_key);
    }
  });

  for (let [chargeKey, indices] of keyToIndices.entries()) {
    const existingSuccess = await QPChargeInstance.findOne({
      charge_key: chargeKey,
      status: "SUCCESS",
      deleted_at: null,
      charge_file_id: { $ne: chargeFile._id },
    });

    if (existingSuccess) {
      indices.forEach((idx) => {
        const inst = instancesToInsert[idx];
        inst.status = "SKIPPED";
        inst.status_reason = `Already charged in previous run (${existingSuccess.charge_file_id}). Charge Key: ${chargeKey}`;
        inst.is_duplicate = true;
        validRows--;
        invalidRows++;
      });
    }
  }

  if (instancesToInsert.length > 0) {
    await QPChargeInstance.insertMany(instancesToInsert);
  }

  chargeFile.total_rows = totalRows;
  chargeFile.valid_rows = validRows;
  chargeFile.invalid_rows = invalidRows;
  chargeFile.status = "IMPORTED";
  await chargeFile.save();

  return chargeFile;
}

// MARK: 1b. Import Charging Sheet (HTTP handler)
exports.importChargeFile = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId || req.userData?._id;

  if (!req.file) {
    return res
      .status(400)
      .json({ status: "error", message: "File is required" });
  }

  const chargeFile = await importChargeFileFromPath(
    req.file.path,
    userId,
    req.file.originalname,
  );

  TraceLogger.info(
    "CHARGE_FILE_IMPORT",
    `Imported charge file ${req.file.originalname} with ${chargeFile.total_rows} rows`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_type: "QPChargeFile",
      entity_id: chargeFile._id,
      metadata: {
        total_rows: chargeFile.total_rows,
        valid_rows: chargeFile.valid_rows,
        invalid_rows: chargeFile.invalid_rows,
      },
    },
  );

  res.status(201).json({
    status: "success",
    data: {
      id: chargeFile._id,
      total_rows: chargeFile.total_rows,
      valid_rows: chargeFile.valid_rows,
      invalid_rows: chargeFile.invalid_rows,
    },
  });
});

exports.importChargeFileFromPath = importChargeFileFromPath;

// MARK: 2. Get Charge Files
// 2. Get Charge Files with filters (excludes files from archived upload sessions)
exports.getChargeFiles = catchAsync(async (req, res, next) => {
  const { status, created_by, date_from, date_to } = req.query;
  const match = { deleted_at: null };

  const archivedSessions = await UploadSession.find(
    { paymentGateway: "qp", archive: true, linkedQpChargeFileId: { $ne: null } },
    { linkedQpChargeFileId: 1 },
  ).lean();
  const archivedChargeFileIds = archivedSessions
    .map((s) => s.linkedQpChargeFileId)
    .filter(Boolean);
  if (archivedChargeFileIds.length) {
    match._id = { $nin: archivedChargeFileIds };
  }

  if (status) match.status = status;
  if (created_by) match.created_by = mongoose.Types.ObjectId(created_by);

  // Date range filtering
  if (date_from || date_to) {
    match.createdAt = {};
    if (date_from) match.createdAt.$gte = new Date(date_from);
    if (date_to) match.createdAt.$lte = new Date(date_to);
  }

  const files = await QPChargeFile.find(match)
    .sort({ createdAt: -1 })
    .populate("created_by", "name email");

  res.status(200).json({ status: "success", data: files });
});

// MARK: 3. Get Single Charge File
// 3. Get Single Charge File
exports.getChargeFileById = catchAsync(async (req, res, next) => {
  const file = await QPChargeFile.findById(req.params.id).populate(
    "created_by",
    "name email",
  );

  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  res.status(200).json({ status: "success", data: file });
});

// MARK: 4. Get Charge Instances with filters, search, pagination, and aggregate stats
exports.getChargeInstances = catchAsync(async (req, res, next) => {
  const {
    charge_file_id,
    chargeFileId, // frontend sends camelCase
    hotel_id,
    reservation_id,
    status,
    ota,
    vnp_work_id,
    portfolio,
    date_from,
    date_to,
    search,
    page = 1,
    limit = 20,
  } = req.query;

  const match = { deleted_at: null };

  // Exclude charge instances from archived files (File History archive = true)
  const archivedSessions = await UploadSession.find(
    { paymentGateway: "qp", archive: true, linkedQpChargeFileId: { $ne: null } },
    { linkedQpChargeFileId: 1 },
  ).lean();
  const archivedChargeFileIds = archivedSessions
    .map((s) => s.linkedQpChargeFileId)
    .filter(Boolean);

  const fileId = charge_file_id || chargeFileId;
  if (fileId) {
    match.charge_file_id = fileId;
    if (archivedChargeFileIds.length) {
      // Exclude instances from this file if the file is archived
      match.$and = [
        { charge_file_id: fileId },
        { charge_file_id: { $nin: archivedChargeFileIds } },
      ];
      delete match.charge_file_id;
    }
  } else if (archivedChargeFileIds.length) {
    match.charge_file_id = { $nin: archivedChargeFileIds };
  }
  if (reservation_id) match.reservation_id = reservation_id;
  // Normalize status to uppercase to match schema enum (e.g. DECLINED, PENDING)
  if (status) match.status = String(status).toUpperCase();
  if (ota) match.ota = ota;
  if (vnp_work_id) match.vnp_work_id = vnp_work_id;
  if (portfolio) match.portfolio = portfolio;

  if (date_from || date_to) {
    match.createdAt = {};
    if (date_from) match.createdAt.$gte = new Date(date_from);
    if (date_to) match.createdAt.$lte = new Date(date_to);
  }

  // Text search across hotel_id, reservation_id, user_id (case-insensitive)
  if (search && String(search).trim()) {
    const term = String(search).trim().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(term, "i");
    match.$or = [
      { hotel_id: re },
      { reservation_id: re },
      { user_id: re },
    ];
  }

  const pageNum = Math.max(1, parseInt(String(page), 10) || 1);
  const limitNum = Math.min(100, Math.max(1, parseInt(String(limit), 10) || 20));

  // Stats aggregation (same filter, no pagination)
  const statsAgg = await QPChargeInstance.aggregate([
    { $match: match },
    {
      $group: {
        _id: null,
        totalCount: { $sum: 1 },
        totalAmount: { $sum: { $ifNull: ["$amount_numeric", 0] } },
        hotelIds: { $addToSet: "$hotel_id" },
      },
    },
    {
      $project: {
        _id: 0,
        totalCount: 1,
        totalAmount: 1,
        uniqueHotels: { $size: "$hotelIds" },
      },
    },
  ]);

  const stats = statsAgg[0] || {
    totalCount: 0,
    totalAmount: 0,
    uniqueHotels: 0,
  };
  const totalCount = stats.totalCount;
  const totalPages = Math.max(1, Math.ceil(totalCount / limitNum));

  // Paginated rows with find (consistent with same match)
  const rows = await QPChargeInstance.find(match)
    .select("-card_number -cvv")
    .sort({ row_number: 1 })
    .skip((pageNum - 1) * limitNum)
    .limit(limitNum)
    .lean();

  res.status(200).json({
    status: "success",
    data: {
      rows,
      pagination: {
        currentPage: pageNum,
        totalPages,
        totalCount,
        limit: limitNum,
      },
      stats: {
        uniqueHotels: stats.uniqueHotels,
        totalAmount: stats.totalAmount,
      },
    },
  });
});

// MARK: 5. Delete Charge File (Soft)
// 5. Delete Charge File (Soft)
exports.deleteChargeFile = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId || req.userData?._id;

  const file = await QPChargeFile.findById(req.params.id);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  file.deleted_at = new Date();
  file.deleted_by = userId;
  await file.save();

  // Also soft delete instances
  await QPChargeInstance.updateMany(
    { charge_file_id: file._id },
    { $set: { deleted_at: new Date(), deleted_by: userId } },
  );

  TraceLogger.info("CHARGE_FILE_DELETE", `Deleted charge file ${file._id}`, {
    request_id: reqId,
    actor_user_id: userId,
    entity_type: "QPChargeFile",
    entity_id: file._id,
  });

  res.status(200).json({ status: "success", message: "Deleted successfully" });
});

// MARK: 5a. Update Charge File (Patch)
// 5a. Update Charge File (Patch)
exports.updateChargeFile = catchAsync(async (req, res, next) => {
  const file = await QPChargeFile.findById(req.params.id);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  if (req.body.file_name) {
    file.file_name = req.body.file_name;
  }

  file.updated_by = req.user?.userId;
  await file.save();

  res.status(200).json({ status: "success", data: file });
});

// MARK: 5b. Get Charge File Progress
// 5b. Get Charge File Progress
exports.getChargeFileProgress = catchAsync(async (req, res, next) => {
  const file = await QPChargeFile.findById(req.params.id);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  res.status(200).json({
    status: "success",
    data: {
      total_rows: file.total_rows,
      valid_rows: file.valid_rows,
      processed_rows: file.processed_rows,
      success_count: file.success_count,
      declined_count: file.declined_count,
      error_count: file.error_count,
      skipped_count: file.skipped_count,
      status: file.status,
    },
  });
});

// MARK: Export Raw Pre-processing Instances
// Export raw pre-processing instances
exports.exportRawInstances = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;

  const instances = await QPChargeInstance.find({
    charge_file_id: req.params.id,
    deleted_at: null,
  }).sort({ row_number: 1 });

  const mapped = instances.map((i) => ({
    RowNumber: i.row_number,
    HotelID: i.hotel_id,
    ReservationID: i.reservation_id,
    Amount: i.amount_numeric,
    Currency: i.currency,
    CardLast4: i.card_last4,
    Status: i.status,
    StatusReason: i.status_reason,
    IsDuplicate: i.is_duplicate,
    ChargeKey: i.charge_key,
  }));

  const ws = xlsx.utils.json_to_sheet(mapped);
  const wb = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(wb, ws, "Raw Instances");
  const buffer = xlsx.write(wb, { type: "buffer", bookType: "csv" });

  TraceLogger.info(
    "CHARGE_FILE_EXPORT_RAW",
    `Exported raw instances for file ${req.params.id}`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_id: req.params.id,
    },
  );

  res.setHeader(
    "Content-Disposition",
    'attachment; filename="raw_instances.csv"',
  );
  res.setHeader("Content-Type", "text/csv");
  res.send(buffer);
});

// MARK: 5c. Get Single Charge Instance
// 5c. Get Single Charge Instance
exports.getChargeInstanceById = catchAsync(async (req, res, next) => {
  const instance = await QPChargeInstance.findById(req.params.id).select(
    "-card_number -cvv",
  );
  if (!instance || instance.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }
  res.status(200).json({ status: "success", data: instance });
});

// MARK: 5d. Update Charge Instance (Patch) with validation
// 5d. Update Charge Instance (Patch) with complete validation
exports.updateChargeInstance = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const instance = await QPChargeInstance.findById(req.params.id);

  if (!instance || instance.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  const allowedStatuses = [
    "PENDING",
    "PROCESSING",
    "SUCCESS",
    "DECLINED",
    "ERROR",
    "INVALID",
    "SKIPPED",
  ];
  const canEditPayload =
    ["PENDING", "INVALID", "DECLINED", "ERROR"].includes(instance.status);

  const {
    billing_address,
    amount_numeric,
    currency,
    expiry_month,
    expiry_year,
    status: bodyStatus,
    status_reason: bodyStatusReason,
  } = req.body;

  // Status and status_reason: always allowed (for state correction)
  if (bodyStatus !== undefined) {
    const s = String(bodyStatus).toUpperCase();
    if (!allowedStatuses.includes(s)) {
      return res.status(400).json({
        status: "error",
        message: `status must be one of: ${allowedStatuses.join(", ")}`,
      });
    }
    instance.status = s;
  }
  if (bodyStatusReason !== undefined) {
    instance.status_reason =
      typeof bodyStatusReason === "string"
        ? bodyStatusReason
        : String(bodyStatusReason);
  }

  // Amount, address, expiry: only apply when instance is in editable state
  if (canEditPayload) {
  // Validate amount_numeric if provided
  if (amount_numeric !== undefined) {
    if (typeof amount_numeric !== "number" || amount_numeric <= 0) {
      return res.status(400).json({
        status: "error",
        message: "amount_numeric must be a positive number",
      });
    }
    instance.amount_numeric = amount_numeric;
  }

  // Validate currency if provided (ISO 4217 3-letter code)
  if (currency !== undefined) {
    if (!/^[A-Z]{3}$/.test(currency)) {
      return res.status(400).json({
        status: "error",
        message: "currency must be a 3-letter ISO code (e.g., USD, EUR)",
      });
    }
    instance.currency = currency;
  }

  // Validate expiry dates if provided
  if (expiry_month !== undefined) {
    const month = parseInt(expiry_month, 10);
    if (isNaN(month) || month < 1 || month > 12) {
      return res
        .status(400)
        .json({ status: "error", message: "expiry_month must be 1-12" });
    }
    instance.expiry_month = month;
  }

  if (expiry_year !== undefined) {
    const year = parseInt(expiry_year, 10);
    const currentYear = new Date().getFullYear();
    if (isNaN(year) || year < currentYear) {
      return res.status(400).json({
        status: "error",
        message: `expiry_year must be ${currentYear} or later`,
      });
    }
    instance.expiry_year = year;
  }

  // Validate and sanitize billing address if provided
  if (billing_address) {
    // Basic XSS prevention: ensure no HTML/script content
    const sanitizeString = (str) => {
      if (typeof str !== "string") return str;
      return str.replace(/[<>\"']/g, ""); // Remove potential HTML chars
    };

    const sanitizedAddr = {};
    if (billing_address.address_1)
      sanitizedAddr.address_1 = sanitizeString(billing_address.address_1);
    if (billing_address.address_2)
      sanitizedAddr.address_2 = sanitizeString(billing_address.address_2);
    if (billing_address.city)
      sanitizedAddr.city = sanitizeString(billing_address.city);
    if (billing_address.state)
      sanitizedAddr.state = sanitizeString(billing_address.state);
    if (billing_address.postal_code)
      sanitizedAddr.postal_code = sanitizeString(billing_address.postal_code);
    if (billing_address.country_code) {
      // Validate 2-letter country code
      if (!/^[A-Z]{2}$/.test(billing_address.country_code)) {
        return res.status(400).json({
          status: "error",
          message: "country_code must be a 2-letter ISO code",
        });
      }
      sanitizedAddr.country_code = billing_address.country_code;
    }

    instance.billing_address = {
      ...instance.billing_address,
      ...sanitizedAddr,
    };
  }
  }

  instance.updated_by = userId;
  await instance.save();

  TraceLogger.info(
    "CHARGE_INSTANCE_UPDATE",
    `Updated charge instance ${instance._id}`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_id: instance._id,
      entity_type: "QPChargeInstance",
      metadata: { updated_fields: Object.keys(req.body) },
    },
  );

  res.status(200).json({ status: "success", data: instance });
});

// MARK: 5e. Delete Charge Instance (Soft Delete)
// 5e. Delete Charge Instance (Soft Delete)
exports.deleteChargeInstance = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const instance = await QPChargeInstance.findById(req.params.id);

  if (!instance || instance.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  instance.deleted_at = new Date();
  instance.deleted_by = userId;
  await instance.save();

  TraceLogger.info(
    "CHARGE_INSTANCE_DELETE",
    `Deleted charge instance ${instance._id}`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_id: instance._id,
    },
  );

  res.status(200).json({ status: "success", message: "Deleted successfully" });
});

// MARK: Helper – instance to export row (template column order + Status Reason, Provider Txn ID, Processed At)
// Headers match template exactly: Hotel ID*, Portfolio*, ... VNP Work ID, Charge Status, then extra columns.
function instanceToExportRow(i) {
  let cardNumber = "";
  if (i.card_number) {
    try {
      cardNumber = decrypt(i.card_number);
    } catch {
      cardNumber = "***";
    }
  }
  const cardExpire =
    i.expiry_month && i.expiry_year != null
      ? `${String(i.expiry_month).padStart(2, "0")}/${String(i.expiry_year).slice(-2).padStart(2, "0")}`
      : "";
  let cardCvv = "";
  if (i.cvv) {
    try {
      cardCvv = decrypt(i.cvv);
    } catch {
      cardCvv = "";
    }
  }
  const processedAt =
    i.completed_at instanceof Date
      ? i.completed_at.toISOString()
      : i.completed_at
        ? new Date(i.completed_at).toISOString()
        : "";
  return {
    "Hotel ID*": i.hotel_id ?? "",
    "Portfolio*": i.portfolio ?? "",
    "Hotel Name*": i.hotel_name ?? "",
    "ReservationID*": i.reservation_id ?? "",
    "Currency*": i.currency ?? "",
    "Amount to charge*": i.amount_numeric ?? "",
    "Card Number*": cardNumber,
    "Expire*": cardExpire,
    "Card CVV*": cardCvv,
    "OTA Billing Name*": i.user_id ?? "",
    "Address*": i.billing_address?.address_1 ?? "",
    "City*": i.billing_address?.city ?? "",
    "State*": i.billing_address?.state ?? "",
    "Zip Code*": i.billing_address?.postal_code ?? "",
    "QP Username*": i.user_id ?? "",
    "OTA Name*": i.ota ?? "",
    "VNP Work ID": i.vnp_work_id ?? "",
    "Charge Status": i.status ?? "",
    "Status Reason": i.status_reason ?? "",
    "Provider Txn ID": i.provider_transaction_id ?? "",
    "Processed At": processedAt,
  };
}

// MARK: 5f. Export Filtered Charge Instances (XLSX, parent-file style, full card)
exports.exportChargeInstances = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const { charge_file_id, status, ids } = req.query;

  const match = { deleted_at: null };
  if (charge_file_id) match.charge_file_id = charge_file_id;
  if (status) match.status = status;
  if (ids) {
    const idList = (typeof ids === "string" ? ids.split(",") : Array.isArray(ids) ? ids : [])
      .map((id) => String(id).trim())
      .filter(Boolean);
    if (idList.length > 0) match._id = { $in: idList };
  }

  const instances = await QPChargeInstance.find(match)
    .sort({ row_number: 1 })
    .lean();

  const mapped = instances.map((i) => instanceToExportRow(i));

  const ws = xlsx.utils.json_to_sheet(mapped);
  const wb = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(wb, ws, "Charge Instances");
  const buffer = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });

  TraceLogger.info("CHARGE_INSTANCE_EXPORT", `Exported filtered instances`, {
    request_id: reqId,
    actor_user_id: userId,
  });

  const dateStr = new Date().toISOString().slice(0, 10);
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="qp_instances_export_${dateStr}.xlsx"`,
  );
  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.send(buffer);
});

const { processCharge } = require("../services/quantumpay-service");
const TerminalCredential = require("../models/TerminalCredential");
// { generateRunId } is already imported on line 10 as generateRequestId, need to add it there instead

// helper used by both single and multiple processors
async function chargeInstance(instance, userId, runId = null) {
  if (!instance || instance.deleted_at) {
    throw new Error("Charge instance not found");
  }
  if (["SUCCESS", "INVALID", "ERROR"].includes(instance.status)) {
    throw new Error("Cannot process instance in current status");
  }

  // Lookup terminal credentials by QP username (same as Terminal Keys page)
  const qpUsername = (instance.user_id || "").toString().trim();
  const creds = await TerminalCredential.findOne({
    username: qpUsername,
    deleted_at: null,
  });
  if (!creds) {
    instance.status = "ERROR";
    instance.status_reason = qpUsername
      ? `Missing Terminal Credentials for QP Username: ${qpUsername}`
      : "Missing QP Username (required for terminal key lookup)";
    await instance.save();
    throw new Error(instance.status_reason);
  }

  const terminalKey = decrypt(creds.terminal_key);

  instance.status = "PROCESSING";
  instance.requested_at = new Date();
  if (runId) instance.last_run_id = runId;
  await instance.save();

  try {
    const { providerResult, responseBody, reqId } = await processCharge(
      instance,
      terminalKey,
      runId,
      userId,
    );

    instance.status = providerResult;
    instance.status_reason =
      responseBody?.message || responseBody?.reason || providerResult;
    instance.provider_transaction_id =
      responseBody?.transaction_id || responseBody?.id || null;
    instance.provider_code = responseBody?.code;
    instance.last_request_id = reqId;
    instance.last_response_payload = responseBody;
  } catch (err) {
    instance.status = "ERROR";
    instance.status_reason = "System processing error: " + err.message;
    instance.last_response_payload = { error_message: err.message };
  }

  instance.completed_at = new Date();
  // Set VNP Work ID to logged-in user's email after charge
  const user = await User.findById(userId).select("email").lean();
  instance.vnp_work_id = user?.email ?? "";
  await instance.save();
  return instance;
}

// MARK: 6. Process Single Instance
// 6. Process Single Instance
exports.processChargeInstance = catchAsync(async (req, res, next) => {
  const userId = req.user?.userId;
  const instanceId = req.params.id;

  const instance = await QPChargeInstance.findById(instanceId);
  if (!instance || instance.deleted_at) {
    return res
      .status(404)
      .json({ status: "error", message: "Charge instance not found" });
  }

  try {
    const processed = await chargeInstance(instance, userId);
    res.status(200).json({ status: "success", data: processed });
  } catch (e) {
    res.status(400).json({ status: "error", message: e.message });
  }
});

// Random delay 5-15s between bulk charges (human-like)
const delayBetweenCharges = () =>
  new Promise((resolve) =>
    setTimeout(resolve, 5000 + Math.random() * 10000),
  );

// MARK: 6a. Process Multiple Instances by ID list
// 6a. Process Multiple Instances by ID list
exports.processMultipleInstances = catchAsync(async (req, res, next) => {
  const userId = req.user?.userId;
  const { ids, run_id } = req.body;

  if (!Array.isArray(ids) || ids.length === 0) {
    return res
      .status(400)
      .json({ status: "error", message: "ids must be a non-empty array" });
  }

  const results = [];
  for (let i = 0; i < ids.length; i++) {
    const id = ids[i];
    try {
      const inst = await QPChargeInstance.findById(id);
      const done = await chargeInstance(inst, userId, run_id);
      results.push({ id, status: done.status });
    } catch (e) {
      results.push({ id, error: e.message });
    }
    if (i < ids.length - 1) {
      await delayBetweenCharges();
    }
  }

  res.status(200).json({
    status: "success",
    data: results,
  });
});

// MARK: 6b. Create and process single (manual entry)
const MANUAL_FILE_NAME = "Manual entry";
const MANUAL_STORAGE_PATH = "internal://manual";

async function getOrCreateManualChargeFile(userId) {
  let file = await QPChargeFile.findOne({
    file_name: MANUAL_FILE_NAME,
    deleted_at: null,
  });
  if (!file) {
    file = await QPChargeFile.create({
      file_name: MANUAL_FILE_NAME,
      file_type: "XLSX",
      storage_path: MANUAL_STORAGE_PATH,
      status: "IMPORTED",
      created_by: userId,
    });
  }
  return file;
}

function parseExpiryFromBody(body) {
  let month = body.expiry_month;
  let year = body.expiry_year;
  if ((month != null && year != null) || (body.expiry_month !== undefined || body.expiry_year !== undefined)) {
    month = month != null ? parseInt(month, 10) : null;
    year = year != null ? parseInt(year, 10) : null;
    if (!isNaN(month) && month >= 1 && month <= 12) {
      if (!isNaN(year) && year >= 0) {
        const fullYear = year < 100 ? 2000 + year : year;
        return { month, year: fullYear % 100 };
      }
    }
  }
  const cardExpire = body.card_expire || body.cardExpire;
  if (!cardExpire) return { month: null, year: null };
  const str = String(cardExpire).trim();
  const parts = str.split(/[/-]/);
  if (parts.length >= 2) {
    const m = parseInt(parts[0], 10);
    const y = parseInt(parts[1], 10);
    if (!isNaN(m) && m >= 1 && m <= 12 && !isNaN(y)) {
      return { month: m, year: y < 100 ? y : y % 100 };
    }
  }
  return { month: null, year: null };
}

exports.createAndProcessSingle = catchAsync(async (req, res, next) => {
  const userId = req.user?.userId;
  const body = req.body || {};

  const hotel_id = (body.hotel_id || body.hotelId || "").toString().trim();
  const reservation_id = (body.reservation_id || body.reservationId || "").toString().trim();
  const amount_numeric = parseFloat(body.amount_numeric ?? body.amount ?? 0);
  const currency = (body.currency || "USD").toString().toUpperCase().slice(0, 3);
  const card_number = (body.card_number || body.cardNumber || "").toString().trim().replace(/\s/g, "");
  const cvv = (body.cvv || "").toString().trim();
  const billing_address = body.billing_address || body.billingAddress || {};
  const ota = (body.ota || "").toString().trim();
  const vnp_work_id = (body.vnp_work_id || body.vnpWorkId || "").toString().trim();
  const portfolio = (body.portfolio || "").toString().trim();
  const user_id = (body.user_id || body.userId || body.qp_username || "").toString().trim();

  if (!user_id || !reservation_id || !amount_numeric || amount_numeric <= 0) {
    return res.status(400).json({
      status: "error",
      message: "user_id (QP username), reservation_id, and positive amount_numeric are required",
    });
  }
  if (!card_number || card_number.length < 13) {
    return res.status(400).json({
      status: "error",
      message: "Valid card_number is required",
    });
  }
  if (!cvv || !/^\d{3,4}$/.test(cvv)) {
    return res.status(400).json({
      status: "error",
      message: "cvv must be 3 or 4 digits",
    });
  }

  const manualFile = await getOrCreateManualChargeFile(userId);
  const nextRow =
    (await QPChargeInstance.countDocuments({
      charge_file_id: manualFile._id,
      deleted_at: null,
    })) + 1;

  const { month: expiry_month, year: expiry_year } = parseExpiryFromBody(body);
  if (!expiry_month || expiry_year == null) {
    return res.status(400).json({
      status: "error",
      message: "card_expire (MM/YY) or expiry_month and expiry_year are required",
    });
  }

  const card_last4 = card_number.slice(-4);
  const charge_key = crypto
    .createHash("sha256")
    .update(`${user_id}-${reservation_id}-${amount_numeric}-${card_last4}`)
    .digest("hex");

  const instance = new QPChargeInstance({
    charge_file_id: manualFile._id,
    parent_file_name: MANUAL_FILE_NAME,
    row_number: nextRow,
    hotel_id: hotel_id || undefined,
    reservation_id,
    amount_numeric,
    currency,
    user_id,
    ota: ota || undefined,
    vnp_work_id: vnp_work_id || undefined,
    portfolio: portfolio || undefined,
    billing_address: {
      address_1: (billing_address.address_1 || "").toString().trim(),
      address_2: (billing_address.address_2 || "").toString().trim(),
      city: (billing_address.city || "").toString().trim(),
      state: (billing_address.state || "").toString().trim(),
      postal_code: (billing_address.postal_code || "").toString().trim(),
      country_code: (billing_address.country_code || "US").toString().toUpperCase().slice(0, 2),
    },
    card_number: encrypt(card_number),
    card_last4,
    cvv: encrypt(cvv),
    expiry_month,
    expiry_year,
    charge_key,
    status: "PENDING",
    created_by: userId,
  });

  await instance.save();

  try {
    const processed = await chargeInstance(instance, userId);
    res.status(200).json({ status: "success", data: processed });
  } catch (e) {
    res.status(400).json({ status: "error", message: e.message });
  }
});

// MARK: 7. Process Bulk Run for a Charge File
// 7. Process Bulk Run for a Charge File
exports.processChargeFile = catchAsync(async (req, res, next) => {
  const userId = req.user?.userId;
  const fileId = req.params.id;

  const file = await QPChargeFile.findById(fileId);
  if (!file)
    return res.status(404).json({ status: "error", message: "File not found" });

  file.status = "PROCESSING";
  const runId = generateRunId();
  file.last_run_id = runId;
  await file.save();

  TraceLogger.info(
    "CHARGE_FILE_PROCESS_START",
    `Started bulk charge run ${runId} for file ${fileId}`,
    {
      run_id: runId,
      actor_user_id: userId,
      entity_id: fileId,
    },
  );

  // Execute processing asynchronously in the background so response is immediate
  processBulkRunAsync(file, runId, userId).catch((err) =>
    console.error("Bulk run error", err),
  );

  res.status(202).json({
    status: "success",
    message: "Processing started in background",
    data: { run_id: runId },
  });
});

async function processBulkRunAsync(file, runId, userId) {
  // Fetch pending instances in order
  const instances = await QPChargeInstance.find({
    charge_file_id: file._id,
    status: "PENDING",
    deleted_at: null,
  }).sort({ row_number: 1 });

  let success = 0,
    declined = 0,
    errCount = 0;

  for (const instance of instances) {
    instance.status = "PROCESSING";
    instance.last_run_id = runId;
    instance.requested_at = new Date();
    await instance.save();

    const qpUsername = (instance.user_id || "").toString().trim();
    const creds = await TerminalCredential.findOne({
      username: qpUsername,
      deleted_at: null,
    });

    if (!creds) {
      instance.status = "ERROR";
      instance.status_reason = qpUsername
        ? `Missing Terminal Credentials for QP Username: ${qpUsername}`
        : "Missing QP Username (required for terminal key lookup)";
      instance.completed_at = new Date();
      await instance.save();
      errCount++;
      continue;
    }

    const unencryptedTerminalKey = decrypt(creds.terminal_key);

    try {
      const { providerResult, responseBody, reqId } = await processCharge(
        instance,
        unencryptedTerminalKey,
        runId,
        userId,
      );

      instance.status = providerResult;
      instance.status_reason =
        responseBody?.message || responseBody?.reason || providerResult;
      instance.provider_transaction_id =
        responseBody?.transaction_id || responseBody?.id || null;
      instance.provider_code = responseBody?.code;
      instance.last_request_id = reqId;
      instance.last_response_payload = responseBody;

      if (providerResult === "SUCCESS") success++;
      else if (providerResult === "DECLINED") declined++;
      else errCount++;
    } catch (e) {
      instance.status = "ERROR";
      instance.status_reason = "Exception: " + e.message;
      instance.last_response_payload = { error_message: e.message };
      errCount++;
    }

    instance.completed_at = new Date();
    await instance.save();

    // Delay between charges (5-15s) for human-like spacing
    const idx = instances.indexOf(instance);
    if (idx >= 0 && idx < instances.length - 1) {
      await delayBetweenCharges();
    }
  }

  file.status = errCount > 0 ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
  file.success_count += success;
  file.declined_count += declined;
  file.error_count += errCount;
  file.processed_rows += instances.length;

  // Generate and store compiled file after processing completes
  const allInstances = await QPChargeInstance.find({
    charge_file_id: file._id,
    deleted_at: null,
  }).sort({ row_number: 1 });

  const compiledData = allInstances.map((i) => ({
    RowNumber: i.row_number,
    HotelID: i.hotel_id,
    ReservationID: i.reservation_id,
    Amount: i.amount_numeric,
    Currency: i.currency,
    Last4: i.card_last4,
    Status: i.status,
    StatusReason: i.status_reason,
    ProviderTxnID: i.provider_transaction_id,
    ProcessedAt: i.completed_at,
    RequestID: i.last_request_id,
    RunID: i.last_run_id,
  }));

  try {
    const ws = xlsx.utils.json_to_sheet(compiledData);
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, "Compiled Results");

    // Generate file path for compiled results
    const compiledFileName = `compiled_${file._id}_${runId}.xlsx`;
    const compiledFilePath = path.join(
      __dirname,
      "../../public/files",
      compiledFileName,
    );

    // Ensure files directory exists
    fs.ensureDirSync(path.dirname(compiledFilePath));

    // Write file
    xlsx.writeFile(wb, compiledFilePath);

    file.compiled_storage_path = `/files/${compiledFileName}`;
  } catch (compileErr) {
    console.error("Error generating compiled file:", compileErr);
    // Don't fail the bulk run if compilation fails, just log it
    TraceLogger.error(
      "CHARGE_FILE_COMPILE_ERROR",
      `Failed to generate compiled file`,
      compileErr,
      {
        run_id: runId,
        entity_id: file._id,
        entity_type: "QPChargeFile",
      },
    );
  }

  await file.save();

  TraceLogger.info(
    "CHARGE_FILE_PROCESS_END",
    `Finished bulk charge run ${runId}`,
    {
      run_id: runId,
      actor_user_id: userId,
      entity_id: file._id,
      metadata: {
        success,
        declined,
        error_count: errCount,
        processed_rows: instances.length,
      },
    },
  );
}

// MARK: 8. Download Compiled Excel
// 8. Download Compiled Excel
exports.downloadCompiledFile = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const fileId = req.params.id;

  const file = await QPChargeFile.findById(fileId);
  const instances = await QPChargeInstance.find({
    charge_file_id: fileId,
  }).sort({ row_number: 1 });

  const mapped = instances.map((i) => ({
    RowNumber: i.row_number,
    HotelID: i.hotel_id,
    ReservationID: i.reservation_id,
    Amount: i.amount_numeric,
    Last4: i.card_last4,
    Status: i.status,
    StatusReason: i.status_reason,
    ProviderTxnID: i.provider_transaction_id,
    ProcessedAt: i.completed_at,
    RequestID: i.last_request_id,
    RunID: i.last_run_id,
  }));

  const ws = xlsx.utils.json_to_sheet(mapped);
  const wb = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(wb, ws, "Compiled Results");
  const buffer = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });

  TraceLogger.info(
    "CHARGE_FILE_DOWNLOAD_COMPILED",
    `Downloaded compiled results`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_id: fileId,
    },
  );

  res.setHeader(
    "Content-Disposition",
    `attachment; filename="compiled_${file.file_name}"`,
  );
  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.send(buffer);
});

// MARK: 8b. Download Report (parent file style with charge status column)
exports.downloadReport = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const fileId = req.params.id;

  const file = await QPChargeFile.findById(fileId);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  const instances = await QPChargeInstance.find({
    charge_file_id: fileId,
    deleted_at: null,
  })
    .sort({ row_number: 1 })
    .lean();

  const mapped = instances.map((i) => instanceToExportRow(i));

  const ws = xlsx.utils.json_to_sheet(mapped);
  const wb = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(wb, ws, "Report");
  const buffer = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });

  TraceLogger.info(
    "CHARGE_FILE_DOWNLOAD_REPORT",
    `Downloaded report for file ${fileId}`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_id: fileId,
    },
  );

  const safeName = (file.file_name || "report").replace(/[^a-zA-Z0-9._-]/g, "_");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="qp_report_${safeName}"`,
  );
  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.send(buffer);
});
