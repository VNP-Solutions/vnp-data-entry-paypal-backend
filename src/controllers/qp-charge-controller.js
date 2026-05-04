/**
 * qp-charge-controller.js
 * QP (Quantum Pay) charge file and instance lifecycle: import XLSX/CSV, CRUD charge files/instances,
 * single/bulk charge processing, export/report download. Export matches template column order + Status Reason, Provider Txn ID, Processed At.
 *
 * BOOKMARK LIST (landmarks in this file – search for "// MARK:")
 * ------------------------------------
 * Map Row to Instance
 *   mapRowToInstance: maps template row to QPChargeInstance (hotel_id, hotel_name, reservation_id, amount, card, billing, status). Requires QP Username and OTA Billing Name columns; flexible column names for other fields.
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
 *   instanceToExportRow: template headers (Hotel ID*, …; Address/City/State/Zip Code without *) then Status Reason, Provider Txn ID, Processed At. Decrypts card/CVV.
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
const QPQueueSettings = require("../models/QPQueueSettings");
const QPChargeInstance = require("../models/QPChargeInstance");
const QPPaymentAttempt = require("../models/QPPaymentAttempt");
const UploadSession = require("../models/UploadSession");
const User = require("../models/User");
const { encrypt, decrypt } = require("../utils/encryption");
const { sendMail } = require("../utils/email");
const {
  TraceLogger,
  generateRequestId,
  generateRunId,
} = require("../utils/logger");

const catchAsync = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

/**
 * Mongoose casts string ObjectIds in find/countDocuments but aggregate $match does not.
 * Without this, filtered stats (totalAmount, uniqueHotels) can be 0 while rows still return.
 */
function cloneMatchForAggregate(match) {
  if (!match || typeof match !== "object") return match;
  const out = {};
  for (const key of Object.keys(match)) {
    if (key === "$or" || key === "$and") {
      out[key] = match[key].map((clause) => cloneMatchForAggregate(clause));
    } else if (key === "charge_file_id") {
      const v = match[key];
      if (typeof v === "string" && mongoose.Types.ObjectId.isValid(v)) {
        out[key] = new mongoose.Types.ObjectId(v);
      } else if (v && typeof v === "object" && Array.isArray(v.$nin)) {
        out[key] = {
          $nin: v.$nin.map((id) =>
            id != null && mongoose.Types.ObjectId.isValid(String(id))
              ? new mongoose.Types.ObjectId(String(id))
              : id,
          ),
        };
      } else {
        out[key] = v;
      }
    } else {
      out[key] = match[key];
    }
  }
  return out;
}

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

    hotel_id: getVal(
      "Hotel ID",
      "OTA ID",
      "Expedia ID",
      "Hotel_ID",
      "hotel_id",
    ),
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
    // QP Username and OTA Billing Name are separate columns; both required. Terminal lookup uses user_id only.
    user_id: getVal("QP Username", "QP Username*"),
    ota_billing_name: getVal("OTA Billing Name", "OTA Billing Name*"),

    billing_address: {
      address_1: getVal("Address*", "Address", "Address 1", "address_1"),
      address_2: getVal("Address 2", "address_2"),
      city: getVal("City*", "City", "city"),
      state: getVal("State*", "State", "state"),
      postal_code: getVal(
        "Zip Code*",
        "Zip Code",
        "Zip",
        "Postal Code",
        "zip",
        "postal_code",
      ),
      country_code: getVal("Country", "country", "Country Code") || "US",
    },

    status: "PENDING",
  });

  // Handle Card – sanitise spaces/dashes etc. (e.g. "5567 1723 5602 3540" -> digits only)
  const rawPan = getVal("Card Number", "card_number", "PAN");
  const fullPan = rawPan ? String(rawPan).replace(/\D/g, "") : "";
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
  const keyUser = (instance.user_id || instance.hotel_id || "")
    .toString()
    .trim();
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

  // Basic validation: QP username + OTA billing name + reservation + amount + card
  const missing = [];
  if (!instance.user_id) missing.push("QP Username");
  if (!instance.ota_billing_name) missing.push("OTA Billing Name");
  if (!instance.reservation_id) missing.push("reservation");
  if (!instance.amount_numeric) missing.push("amount");
  if (!fullPan) missing.push("card");
  if (missing.length) {
    instance.status = "INVALID";
    instance.status_reason = `Missing required field(s): ${missing.join(", ")}.`;
  }

  return instance;
};

/** Normalized header for matching CVV column (same idea as mapRowToInstance getVal). */
function normalizeQpHeader(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

/**
 * Locate the CVV column on the first header row of the sheet.
 * Uses strict names so we do not pick Stripe/PayPal CVV columns.
 */
function findQpCvvColumnIndex(sheet) {
  if (!sheet || !sheet["!ref"]) return -1;
  const range = xlsx.utils.decode_range(sheet["!ref"]);
  const headerRow = range.s.r;
  for (let c = range.s.c; c <= range.e.c; c++) {
    const addr = xlsx.utils.encode_cell({ r: headerRow, c });
    const cell = sheet[addr];
    if (!cell) continue;
    const text = String(
      cell.w != null && String(cell.w).trim() !== ""
        ? cell.w
        : cell.v != null
          ? cell.v
          : "",
    ).trim();
    if (!text) continue;
    const norm = normalizeQpHeader(text);
    if (norm === "cardcvv" || norm === "cvv") return c;
  }
  return -1;
}

function findCvvPropertyKey(row) {
  for (const k of Object.keys(row)) {
    if (k === "__rowNum__") continue;
    const n = normalizeQpHeader(k);
    if (n === "cardcvv" || n === "cvv") return k;
  }
  return null;
}

/**
 * sheet_to_json uses raw numeric cell values, so a displayed CVV like "030" becomes 30.
 * Re-read the same cell with format_cell / w so Excel display format (e.g. "000") or text is kept.
 * If the workbook only stores the number 30 with General format, the true value is already lost.
 */
function overlayQpCvvFromFormattedCell(row, sheet, cvvColIndex) {
  if (cvvColIndex < 0 || row == null) return;
  const rowNum = row.__rowNum__;
  if (rowNum == null) return;
  const addr = xlsx.utils.encode_cell({ r: rowNum, c: cvvColIndex });
  const cell = sheet[addr];
  if (!cell) return;
  const formatted = String(
    xlsx.utils.format_cell(cell, cell.v, {}),
  ).trim();
  if (!formatted) return;
  const digits = formatted.replace(/\D/g, "");
  if (digits.length < 3 || digits.length > 4) return;
  const key = findCvvPropertyKey(row) || "Card CVV";
  row[key] = digits;
}

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

  // CSV/DSV: SheetJS otherwise parses numeric-looking fields as numbers (t:'n'), which drops
  // leading zeros (e.g. 030 → 30). raw:true keeps each field as text (t:'s'), like Excel Text /
  // Power Query typing. Do not use for .xlsx; binary parsing uses opts.raw differently.
  const workbook = isXlsx
    ? xlsx.readFile(filePath)
    : xlsx.readFile(filePath, { raw: true });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const cvvColIndex = findQpCvvColumnIndex(sheet);
  const rows = xlsx.utils.sheet_to_json(sheet, {
    defval: "",
  });

  let totalRows = rows.length;
  let validRows = 0;
  let invalidRows = 0;

  const instancesToInsert = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    overlayQpCvvFromFormattedCell(row, sheet, cvvColIndex);
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

  // Global reservation ID uniqueness: skip rows whose reservation already exists on any
  // successful instance or appears on any successful QP payment attempt.
  const reservationCandidates = [
    ...new Set(
      instancesToInsert
        .filter(
          (inst) => inst.status !== "INVALID" && inst.status !== "SKIPPED",
        )
        .map((inst) => String(inst.reservation_id || "").trim())
        .filter(Boolean),
    ),
  ];

  const globalTakenReservationsMap = new Map();
  if (reservationCandidates.length > 0) {
    const instanceQuery = {
      deleted_at: null,
      reservation_id: { $in: reservationCandidates },
      status: "SUCCESS",
    };

    const [fromInstances, fromAttempts] = await Promise.all([
      QPChargeInstance.find(instanceQuery)
        .select("reservation_id parent_file_name")
        .lean(),
      QPPaymentAttempt.find({
        "request_payload_redacted.order.order_id": {
          $in: reservationCandidates,
        },
        result: "SUCCESS",
      })
        .select("request_payload_redacted.order.order_id createdAt")
        .lean(),
    ]);

    for (const inst of fromInstances) {
      const t = String(inst.reservation_id || "").trim();
      if (t && !globalTakenReservationsMap.has(t)) {
        globalTakenReservationsMap.set(t, {
          type: "instance",
          filename: inst.parent_file_name || "an existing file",
        });
      }
    }
    for (const attempt of fromAttempts) {
      const orderId = attempt.request_payload_redacted?.order?.order_id;
      const t = String(orderId || "").trim();
      if (t && !globalTakenReservationsMap.has(t)) {
        globalTakenReservationsMap.set(t, {
          type: "attempt",
          date: attempt.createdAt,
        });
      }
    }
  }

  const seenReservationInFile = new Set();
  const skippedRowsForReservationEmail = [];

  for (const inst of instancesToInsert) {
    if (inst.status === "INVALID" || inst.status === "SKIPPED") continue;
    const rid = String(inst.reservation_id || "").trim();
    if (!rid) continue;

    if (globalTakenReservationsMap.has(rid)) {
      const reasonInfo = globalTakenReservationsMap.get(rid);
      inst.status = "SKIPPED";

      if (reasonInfo.type === "instance") {
        inst.status_reason = `Reservation ID already exists in file: ${reasonInfo.filename}`;
      } else {
        const dateStr = reasonInfo.date
          ? new Date(reasonInfo.date).toLocaleDateString()
          : "an earlier date";
        inst.status_reason = `Payment made on ${dateStr} using reservation ID`;
      }

      inst.is_duplicate = true;
      validRows--;
      invalidRows++;
      skippedRowsForReservationEmail.push({ id: rid, info: reasonInfo });
      continue;
    }
    if (seenReservationInFile.has(rid)) {
      inst.status = "SKIPPED";
      inst.status_reason = "Duplicate reservation ID in this file.";
      inst.is_duplicate = true;
      validRows--;
      invalidRows++;
      skippedRowsForReservationEmail.push({
        id: rid,
        info: { type: "current_file" },
      });
      continue;
    }
    seenReservationInFile.add(rid);
  }

  if (instancesToInsert.length > 0) {
    await QPChargeInstance.insertMany(instancesToInsert);
  }

  chargeFile.total_rows = totalRows;
  chargeFile.valid_rows = validRows;
  chargeFile.invalid_rows = invalidRows;
  chargeFile.status = "IMPORTED";
  await chargeFile.save();

  if (skippedRowsForReservationEmail.length > 0 && userId) {
    try {
      const user = await User.findById(userId).select("email").lean();
      const to = user?.email;
      if (to) {
        // Deduplicate the array by ID
        const uniqueSkipped = [];
        const seenIds = new Set();
        for (const item of skippedRowsForReservationEmail) {
          if (!seenIds.has(item.id)) {
            seenIds.add(item.id);
            uniqueSkipped.push(item);
          }
        }
        const n = skippedRowsForReservationEmail.length;

        // Group by category
        const groups = {
          instance: [],
          attempt: [],
          current_file: [],
        };
        for (const item of uniqueSkipped) {
          groups[item.info.type].push(item);
        }

        let listsHtml = "";

        if (groups.instance.length > 0) {
          listsHtml += `<p style="color: #333; font-size: 15px; font-weight: bold; margin-bottom: 4px;">Reservation ID from existing charge file:</p><ul style="color: #333; font-size: 14px; margin-top: 0;">`;
          for (const item of groups.instance) {
            listsHtml += `<li><strong>${escapeHtml(item.id)}</strong> (File: ${escapeHtml(item.info.filename)})</li>`;
          }
          listsHtml += `</ul>`;
        }

        if (groups.attempt.length > 0) {
          listsHtml += `<p style="color: #333; font-size: 15px; font-weight: bold; margin-bottom: 4px;">Payment made previously using reservation ID:</p><ul style="color: #333; font-size: 14px; margin-top: 0;">`;
          for (const item of groups.attempt) {
            const dateStr = item.info.date
              ? new Date(item.info.date).toLocaleDateString()
              : "earlier date";
            listsHtml += `<li><strong>${escapeHtml(item.id)}</strong> (Date: ${escapeHtml(dateStr)})</li>`;
          }
          listsHtml += `</ul>`;
        }

        if (groups.current_file.length > 0) {
          listsHtml += `<p style="color: #333; font-size: 15px; font-weight: bold; margin-bottom: 4px;">Duplicate reservation ID within this uploaded file itself:</p><ul style="color: #333; font-size: 14px; margin-top: 0;">`;
          for (const item of groups.current_file) {
            listsHtml += `<li><strong>${escapeHtml(item.id)}</strong></li>`;
          }
          listsHtml += `</ul>`;
        }

        const subject = "QP charge upload: duplicate reservation IDs skipped";
        const html = `
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
      <p style="color: #333; font-size: 16px; line-height: 1.5;">You uploaded charge file <strong>${escapeHtml(originalFileName)}</strong>.</p>
      <p style="color: #333; font-size: 16px; line-height: 1.5;"><strong>${n}</strong> row${n === 1 ? " was" : "s were"} not imported due to duplicate reservation IDs.</p>
      ${listsHtml}
      <p style="color: #666; font-size: 12px; margin-top: 24px;">This is an automated message, please do not reply.</p>
    </div>`;
        await sendMail({ to, subject, html });
      }
    } catch (err) {
      TraceLogger.warn(
        "QP_IMPORT_RESERVATION_EMAIL_FAILED",
        err?.message || String(err),
        { charge_file_id: chargeFile._id, actor_user_id: userId },
      );
    }
  }

  return {
    chargeFile,
    skipped_duplicate_reservation_rows: skippedRowsForReservationEmail.length,
    duplicate_reservation_ids: [
      ...new Set(skippedRowsForReservationEmail.map((item) => item.id)),
    ].sort(),
  };
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

  const {
    chargeFile,
    skipped_duplicate_reservation_rows,
    duplicate_reservation_ids,
  } = await importChargeFileFromPath(
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
        skipped_duplicate_reservation_rows,
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
      skipped_duplicate_reservation_rows,
      duplicate_reservation_ids,
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
    {
      paymentGateway: "qp",
      archive: true,
      linkedQpChargeFileId: { $ne: null },
    },
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
    {
      paymentGateway: "qp",
      archive: true,
      linkedQpChargeFileId: { $ne: null },
    },
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

  // Text search across hotel_id, reservation_id, user_id, ota_billing_name (case-insensitive)
  if (search && String(search).trim()) {
    const term = String(search)
      .trim()
      .replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(term, "i");
    match.$or = [
      { hotel_id: re },
      { reservation_id: re },
      { user_id: re },
      { ota_billing_name: re },
    ];
  }

  const pageNum = Math.max(1, parseInt(String(page), 10) || 1);
  const limitNum = Math.min(
    100,
    Math.max(1, parseInt(String(limit), 10) || 20),
  );

  // Total count from same match as find() so pagination is always correct
  const totalCount = await QPChargeInstance.countDocuments(match);
  const totalPages = Math.max(1, Math.ceil(totalCount / limitNum));

  // Stats aggregation for amount and uniqueHotels only (totalCount from countDocuments above)
  const statsAgg = await QPChargeInstance.aggregate([
    { $match: cloneMatchForAggregate(match) },
    {
      $group: {
        _id: null,
        totalAmount: { $sum: { $ifNull: ["$amount_numeric", 0] } },
        hotelIds: { $addToSet: "$hotel_id" },
      },
    },
    {
      $project: {
        _id: 0,
        totalAmount: 1,
        uniqueHotels: { $size: "$hotelIds" },
      },
    },
  ]);

  const stats = statsAgg[0] || {
    totalAmount: 0,
    uniqueHotels: 0,
  };

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
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const includeSensitive =
    req.query.include_sensitive === "true" ||
    req.query.include_sensitive === "1";

  if (includeSensitive) {
    const instance = await QPChargeInstance.findById(req.params.id);
    if (!instance || instance.deleted_at) {
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    const o = instance.toObject();
    const pan = o.card_number ? decrypt(o.card_number) : null;
    const cvvPlain = o.cvv ? decrypt(o.cvv) : null;
    delete o.card_number;
    delete o.cvv;
    o.card_number = pan;
    o.cvv = cvvPlain;
    TraceLogger.info(
      "QP_CHARGE_VIEW_SENSITIVE",
      "Decrypted card fields returned for QP charge instance",
      {
        request_id: reqId,
        actor_user_id: userId,
        entity_id: String(req.params.id),
      },
    );
    return res.status(200).json({ status: "success", data: o });
  }

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
  const canEditPayload = ["PENDING", "INVALID", "DECLINED", "ERROR"].includes(
    instance.status,
  );

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
    // Stored billing name only — never fall back to QP Username (user_id).
    "OTA Billing Name*": i.ota_billing_name != null ? String(i.ota_billing_name) : "",
    Address: i.billing_address?.address_1 ?? "",
    City: i.billing_address?.city ?? "",
    State: i.billing_address?.state ?? "",
    "Zip Code": i.billing_address?.postal_code ?? "",
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
    const idList = (
      typeof ids === "string" ? ids.split(",") : Array.isArray(ids) ? ids : []
    )
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

const QP_QUEUE_SETTINGS_ID = "global";
/** Default staleness window for recover-stalled without ?force=true */
const BULK_STALL_MS = 15 * 60 * 1000;

async function getGloballyPaused() {
  const doc = await QPQueueSettings.findById(QP_QUEUE_SETTINGS_ID).lean();
  return !!(doc && doc.globally_paused);
}

async function setGloballyPaused(value) {
  await QPQueueSettings.findByIdAndUpdate(
    QP_QUEUE_SETTINGS_ID,
    { $set: { globally_paused: !!value, updated_at: new Date() } },
    { upsert: true },
  );
}

async function touchBulkActivity(fileId) {
  await QPChargeFile.findByIdAndUpdate(fileId, {
    bulk_last_activity_at: new Date(),
  });
}

/** Increment file counters after one row finishes charging (avoids end-of-run batch double-count). */
async function incrementFileChargeOutcome(fileId, terminalStatus) {
  const inc = { processed_rows: 1 };
  if (terminalStatus === "SUCCESS") inc.success_count = 1;
  else if (terminalStatus === "DECLINED") inc.declined_count = 1;
  else inc.error_count = 1;
  await QPChargeFile.findByIdAndUpdate(fileId, { $inc: inc });
}

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
    instance.last_response_payload = {
      source: "pre_api_validation",
      reason: instance.status_reason,
    };
    instance.completed_at = new Date();
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

// Random delay 5-10s between bulk charges (avoid rate limits)
const delayBetweenCharges = () =>
  new Promise((resolve) => setTimeout(resolve, 5000 + Math.random() * 5000));

// Build and send QP bulk process completion email (success or failure)
async function sendQPProcessCompletionEmail({
  to,
  success,
  count,
  fileName,
  chargeFileId,
  successCount = 0,
  declinedCount = 0,
  errorCount = 0,
  skippedCount = 0,
}) {
  const baseUrl = process.env.FRONTEND_URL || "http://localhost:3000";
  const viewUrl = `${baseUrl}/dashboard/qp-payment?chargeFileId=${chargeFileId}`;
  const subject = success
    ? "QP Payment: bulk process finished"
    : "QP Payment: your process encountered an error";

  let message;
  if (success) {
    const parts = [];
    if (successCount > 0)
      parts.push(`<strong>${successCount}</strong> succeeded`);
    if (declinedCount > 0)
      parts.push(`<strong>${declinedCount}</strong> declined`);
    if (errorCount > 0) parts.push(`<strong>${errorCount}</strong> failed`);
    if (skippedCount > 0)
      parts.push(`<strong>${skippedCount}</strong> not processed`);
    const outcomeLine =
      parts.length > 0
        ? `Outcomes: ${parts.join(", ")}.`
        : "No instances were processed.";
    message = `You ran <strong>${count}</strong> instance${count === 1 ? "" : "s"} from file <strong>${escapeHtml(fileName)}</strong>. ${outcomeLine}`;
  } else {
    message = `Your process for <strong>${count}</strong> instances from file <strong>${escapeHtml(fileName)}</strong> encountered an error before finishing. You can check the status using the link below.`;
  }

  const html = `
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
      <p style="color: #333; font-size: 16px; line-height: 1.5;">${message}</p>
      <div style="text-align: center; margin: 28px 0;">
        <a href="${viewUrl}" style="background-color: #007bff; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block; font-weight: bold;">View results</a>
      </div>
      <p style="color: #666; font-size: 12px; text-align: center;">This is an automated message, please do not reply.</p>
    </div>`;
  await sendMail({ to, subject, html });
}

function escapeHtml(s) {
  if (typeof s !== "string") return "";
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// MARK: 6a. Process Multiple Instances by ID list (async: 202 + background + email)
// 6a. Process Multiple Instances by ID list
exports.processMultipleInstances = catchAsync(async (req, res, next) => {
  const userId = req.user?.userId;
  const userEmail = req.userData?.email;
  const { ids, run_id } = req.body;

  if (!Array.isArray(ids) || ids.length === 0) {
    return res
      .status(400)
      .json({ status: "error", message: "ids must be a non-empty array" });
  }

  const firstInstance = await QPChargeInstance.findById(ids[0]);
  if (!firstInstance || firstInstance.deleted_at) {
    return res
      .status(400)
      .json({
        status: "error",
        message: "At least one instance not found or invalid",
      });
  }

  const chargeFileId = firstInstance.charge_file_id;
  const file = await QPChargeFile.findById(chargeFileId);
  const fileName = file?.file_name || "Unknown file";
  const runId = run_id || generateRunId();
  const count = ids.length;

  res.status(202).json({
    status: "accepted",
    message: "Processing started",
    count,
  });

  setImmediate(async () => {
    let successCount = 0;
    let declinedCount = 0;
    let errorCount = 0;
    let skippedCount = 0;

    try {
      for (let i = 0; i < ids.length; i++) {
        const id = ids[i];
        const inst = await QPChargeInstance.findById(id);
        if (!inst || inst.deleted_at) {
          skippedCount++;
          continue;
        }
        const reservationLabel =
          (inst.reservation_id && String(inst.reservation_id).trim()) ||
          `(no reservation id, instance ${id})`;
        try {
          const done = await chargeInstance(inst, userId, runId);
          if (done.status === "SUCCESS") successCount++;
          else if (done.status === "DECLINED") declinedCount++;
          else errorCount++;
        } catch (instanceErr) {
          errorCount++;
          console.error(
            `QP bulk process failed (Reservation ID: ${reservationLabel}):`,
            instanceErr.message,
          );
        }
        if (i < ids.length - 1) {
          await delayBetweenCharges();
        }
      }
      if (userEmail) {
        await sendQPProcessCompletionEmail({
          to: userEmail,
          success: true,
          count,
          fileName,
          chargeFileId: String(chargeFileId),
          successCount,
          declinedCount,
          errorCount,
          skippedCount,
        });
      }
    } catch (err) {
      console.error("QP bulk process background error:", err);
      if (userEmail) {
        try {
          await sendQPProcessCompletionEmail({
            to: userEmail,
            success: false,
            count,
            fileName,
            chargeFileId: String(chargeFileId),
            successCount,
            declinedCount,
            errorCount,
            skippedCount,
          });
        } catch (emailErr) {
          console.error("Failed to send QP process failure email:", emailErr);
        }
      }
    }
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
  if (
    (month != null && year != null) ||
    body.expiry_month !== undefined ||
    body.expiry_year !== undefined
  ) {
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
  const reservation_id = (body.reservation_id || body.reservationId || "")
    .toString()
    .trim();
  const amount_numeric = parseFloat(body.amount_numeric ?? body.amount ?? 0);
  const currency = (body.currency || "USD")
    .toString()
    .toUpperCase()
    .slice(0, 3);
  const card_number = (body.card_number || body.cardNumber || "")
    .toString()
    .trim()
    .replace(/\s/g, "");
  const cvv = (body.cvv || "").toString().trim();
  const billing_address = body.billing_address || body.billingAddress || {};
  const ota = (body.ota || "").toString().trim();
  const vnp_work_id = (body.vnp_work_id || body.vnpWorkId || "")
    .toString()
    .trim();
  const portfolio = (body.portfolio || "").toString().trim();
  const user_id = (body.user_id || body.userId || body.qp_username || "")
    .toString()
    .trim();
  const ota_billing_name = (
    body.ota_billing_name ||
    body.otaBillingName ||
    ""
  )
    .toString()
    .trim();

  if (
    !user_id ||
    !ota_billing_name ||
    !reservation_id ||
    !amount_numeric ||
    amount_numeric <= 0
  ) {
    return res.status(400).json({
      status: "error",
      message:
        "user_id (QP username), ota_billing_name, reservation_id, and positive amount_numeric are required",
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
      message:
        "card_expire (MM/YY) or expiry_month and expiry_year are required",
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
    ota_billing_name,
    ota: ota || undefined,
    vnp_work_id: vnp_work_id || undefined,
    portfolio: portfolio || undefined,
    billing_address: {
      address_1: (billing_address.address_1 || "").toString().trim(),
      address_2: (billing_address.address_2 || "").toString().trim(),
      city: (billing_address.city || "").toString().trim(),
      state: (billing_address.state || "").toString().trim(),
      postal_code: (billing_address.postal_code || "").toString().trim(),
      country_code: (billing_address.country_code || "US")
        .toString()
        .toUpperCase()
        .slice(0, 2),
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
  const userEmail = req.userData?.email;
  const fileId = req.params.id;

  const file = await QPChargeFile.findById(fileId);
  if (!file)
    return res.status(404).json({ status: "error", message: "File not found" });

  // if file is currently processing
  if (file.status === "PROCESSING") {
    return res.status(409).json({
      status: "error",
      message: "File is already being processed",
      data: { run_id: file.last_run_id },
    });
  }

  // if file is already queued, return position back
  if (file.status === "QUEUED") {
    const position = await QPChargeFile.countDocuments({
      status: "QUEUED",
      deleted_at: null,
      queue_order: { $lt: file.queue_order },
    });
    return res.status(200).json({
      status: "success",
      message: "File is already queued",
      data: { position: position + 1 },
    });
  }

  // if file was in a completed or failed state then requeue
  if (
    ["COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED", "CANCELLED"].includes(
      file.status,
    )
  ) {
    file.status = "IMPORTED";
    file.queue_order = null;
    file.queued_at = null;
    await file.save();
  }

  const activeFile = await QPChargeFile.findOne({
    status: "PROCESSING",
    deleted_at: null,
  });

  const runId = generateRunId();

  if (!activeFile) {
    file.status = "PROCESSING";
    file.last_run_id = runId;
    file.queue_order = null;
    file.queued_at = null;
    file.pause_requested = false;
    file.bulk_last_activity_at = new Date();
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

    processBulkRunAsync(file, runId, userId, userEmail).catch((err) =>
      console.error("Bulk run error", err),
    );

    return res.status(202).json({
      status: "success",
      message:
        "Processing started in background. You will receive an email when it is done.",
      data: { run_id: runId, status: "PROCESSING" },
    });
  }

  // Enqueue this file
  const highestQueued = await QPChargeFile.findOne({
    status: "QUEUED",
    deleted_at: null,
  })
    .sort({ queue_order: -1 })
    .select("queue_order")
    .lean();

  const nextQueueOrder =
    highestQueued && highestQueued.queue_order != null
      ? highestQueued.queue_order + 1
      : 1;
  file.status = "QUEUED";
  file.queue_order = nextQueueOrder;
  file.queued_at = new Date();
  file.last_run_id = null;
  await file.save();

  return res.status(202).json({
    status: "success",
    message: "File queued for processing",
    data: { position: nextQueueOrder, status: "QUEUED" },
  });
});

/** PROCESSING first, then PAUSED, then QUEUED; within group by queue_order, queued_at */
function sortQueueFilesForResponse(files) {
  const arr = Array.isArray(files) ? [...files] : [];
  const priority = (s) =>
    s.status === "PROCESSING" ? 0 : s.status === "PAUSED" ? 1 : 2;
  arr.sort((a, b) => {
    const pa = priority(a);
    const pb = priority(b);
    if (pa !== pb) return pa - pb;
    const ao = a.queue_order != null ? a.queue_order : 999999;
    const bo = b.queue_order != null ? b.queue_order : 999999;
    if (ao !== bo) return ao - bo;
    const ta = a.queued_at ? new Date(a.queued_at).getTime() : 0;
    const tb = b.queued_at ? new Date(b.queued_at).getTime() : 0;
    return ta - tb;
  });
  return arr;
}

// Queue management endpoints
exports.getQueue = catchAsync(async (req, res, next) => {
  const [raw, globally_paused] = await Promise.all([
    QPChargeFile.find({
      status: { $in: ["PROCESSING", "PAUSED", "QUEUED"] },
      deleted_at: null,
    }).lean(),
    getGloballyPaused(),
  ]);

  const queue = sortQueueFilesForResponse(raw);

  res.status(200).json({ status: "success", data: queue, globally_paused });
});

exports.updateQueue = catchAsync(async (req, res, next) => {
  const fileId = req.params.id;
  const { action } = req.body;

  const file = await QPChargeFile.findById(fileId);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "File not found" });
  }

  if (file.status === "PROCESSING") {
    return res.status(400).json({
      status: "error",
      message: "Cannot reorder a file that is actively processing",
    });
  }

  if (!["QUEUED", "PAUSED"].includes(file.status)) {
    return res.status(400).json({
      status: "error",
      message: "Only queued or paused files can be reordered",
    });
  }

  const reorderableFiles = await QPChargeFile.find({
    status: { $in: ["QUEUED", "PAUSED"] },
    deleted_at: null,
  })
    .sort({ queue_order: 1, queued_at: 1 })
    .exec();

  const index = reorderableFiles.findIndex((item) => item._id.equals(file._id));
  if (index === -1) {
    return res
      .status(400)
      .json({ status: "error", message: "File not in reorderable queue" });
  }

  if (action === "up" && index > 0) {
    const prev = reorderableFiles[index - 1];
    const tmp = file.queue_order;
    file.queue_order = prev.queue_order;
    prev.queue_order = tmp;
    await file.save();
    await prev.save();
  } else if (action === "down" && index < reorderableFiles.length - 1) {
    const next = reorderableFiles[index + 1];
    const tmp = file.queue_order;
    file.queue_order = next.queue_order;
    next.queue_order = tmp;
    await file.save();
    await next.save();
  } else if (action === "top") {
    file.queue_order = 0;
    await file.save();
    await normalizeQueueOrder();
  } else if (action === "bottom") {
    const maxOrder =
      reorderableFiles[reorderableFiles.length - 1]?.queue_order || 0;
    file.queue_order = maxOrder + 1;
    await file.save();
    await normalizeQueueOrder();
  } else {
    return res.status(400).json({ status: "error", message: "Invalid action" });
  }

  const allRaw = await QPChargeFile.find({
    status: { $in: ["PROCESSING", "PAUSED", "QUEUED"] },
    deleted_at: null,
  }).lean();
  const updated = sortQueueFilesForResponse(allRaw);

  res.status(200).json({ status: "success", data: updated });
});

exports.removeFromQueue = catchAsync(async (req, res, next) => {
  const fileId = req.params.id;
  const file = await QPChargeFile.findById(fileId);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "File not found" });
  }

  if (!["QUEUED", "PAUSED"].includes(file.status)) {
    return res.status(400).json({
      status: "error",
      message: "Only queued or paused files can be removed from the queue",
    });
  }

  file.status = "CANCELLED";
  file.queue_order = null;
  file.queued_at = null;
  await file.save();

  await normalizeQueueOrder();

  res
    .status(200)
    .json({ status: "success", message: "File removed from queue" });
});

/** Cooperative pause: running bulk loop sets status PAUSED on next row boundary. */
exports.pauseChargeFile = catchAsync(async (req, res) => {
  const file = await QPChargeFile.findById(req.params.id);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "File not found" });
  }
  if (file.status === "PAUSED") {
    return res.status(200).json({
      status: "success",
      message: "Already paused",
      data: { status: file.status },
    });
  }
  if (file.status !== "PROCESSING") {
    return res.status(400).json({
      status: "error",
      message: "Only a file that is actively processing can be paused",
    });
  }
  file.pause_requested = true;
  await file.save();
  return res.status(200).json({
    status: "success",
    message: "Pause requested; will stop after the current row finishes",
    data: { pause_requested: true },
  });
});

/** Resume a cooperatively paused file (same rules as POST …/process for PAUSED). */
exports.resumeChargeFile = catchAsync(async (req, res, next) => {
  const file = await QPChargeFile.findById(req.params.id);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "File not found" });
  }
  if (file.status !== "PAUSED") {
    return res.status(400).json({
      status: "error",
      message:
        "Only paused files can be resumed via this endpoint; use Process for other states",
    });
  }
  return exports.processChargeFile(req, res, next);
});

exports.pauseGlobalQueue = catchAsync(async (req, res) => {
  await setGloballyPaused(true);
  res.status(200).json({ status: "success", globally_paused: true });
});

exports.resumeGlobalQueue = catchAsync(async (req, res) => {
  await setGloballyPaused(false);
  res.status(200).json({ status: "success", globally_paused: false });
});

/**
 * Clear wedged PROCESSING after crash. Optional body: { reset_processing_instances: true }
 * resets instance rows stuck in PROCESSING to PENDING (duplicate-charge risk if payment succeeded).
 */
exports.recoverStalledChargeFile = catchAsync(async (req, res) => {
  const file = await QPChargeFile.findById(req.params.id);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "File not found" });
  }
  if (file.status !== "PROCESSING") {
    return res.status(400).json({
      status: "error",
      message: "File is not in PROCESSING state",
    });
  }
  const force = req.query.force === "true";
  const lastActivity = file.bulk_last_activity_at || file.updatedAt;
  const ageMs = lastActivity ? Date.now() - new Date(lastActivity).getTime() : Infinity;
  if (!force && ageMs < BULK_STALL_MS) {
    return res.status(400).json({
      status: "error",
      message:
        "Run does not appear stalled yet; wait or pass ?force=true (and read duplicate-charge risk if resetting rows)",
    });
  }
  file.status = "PAUSED";
  file.pause_requested = false;
  await file.save();

  let resetCount = 0;
  if (req.body?.reset_processing_instances === true) {
    const result = await QPChargeInstance.updateMany(
      {
        charge_file_id: file._id,
        status: "PROCESSING",
        deleted_at: null,
      },
      {
        $set: {
          status: "PENDING",
          status_reason:
            "Reset from PROCESSING after stalled recovery — verify no duplicate charge before re-running",
        },
      },
    );
    resetCount = result.modifiedCount ?? 0;
  }

  res.status(200).json({
    status: "success",
    message:
      "File moved to PAUSED; you can resume or reconcile counts. Resetting PROCESSING rows can duplicate charges if the gateway already captured payment.",
    data: { status: "PAUSED", reset_processing_instances: resetCount },
  });
});

/** Recompute success/declined/error/processed_rows/skipped_count from instances (canonical row truth). */
exports.reconcileChargeFileCounts = catchAsync(async (req, res) => {
  const fileId = req.params.id;
  const file = await QPChargeFile.findById(fileId);
  if (!file || file.deleted_at) {
    return res.status(404).json({ status: "error", message: "File not found" });
  }

  const oid = new mongoose.Types.ObjectId(String(fileId));
  const rows = await QPChargeInstance.aggregate([
    { $match: { charge_file_id: oid, deleted_at: null } },
    { $group: { _id: "$status", n: { $sum: 1 } } },
  ]);
  const by = Object.fromEntries(rows.map((r) => [r._id, r.n]));

  const success_count = by.SUCCESS || 0;
  const declined_count = by.DECLINED || 0;
  const error_count = by.ERROR || 0;
  const skipped_count = by.SKIPPED || 0;
  const processed_rows = success_count + declined_count + error_count;

  await QPChargeFile.findByIdAndUpdate(fileId, {
    $set: {
      success_count,
      declined_count,
      error_count,
      skipped_count,
      processed_rows,
    },
  });

  const updated = await QPChargeFile.findById(fileId).lean();
  res.status(200).json({ status: "success", data: updated });
});

async function normalizeQueueOrder() {
  const queuedFiles = await QPChargeFile.find({
    status: { $in: ["QUEUED", "PAUSED"] },
    deleted_at: null,
  })
    .sort({ queue_order: 1, queued_at: 1 })
    .exec();

  for (let i = 0; i < queuedFiles.length; i++) {
    const q = queuedFiles[i];
    q.queue_order = i + 1;
    await q.save();
  }
}

async function processBulkRunAsync(file, runId, userId, userEmail) {
  const fileId = file._id;
  const fileNameForEmail = file.file_name || "Unknown file";

  async function finishPausedCooperative() {
    await QPChargeFile.findByIdAndUpdate(fileId, {
      $set: { status: "PAUSED", pause_requested: false },
    });
    TraceLogger.info(
      "CHARGE_FILE_PROCESS_PAUSED",
      `Bulk run ${runId} paused cooperatively`,
      {
        run_id: runId,
        actor_user_id: userId,
        entity_id: fileId,
      },
    );
  }

  let success = 0;
  let declined = 0;
  let errCount = 0;
  let rowsThisRun = 0;

  while (true) {
    if (await getGloballyPaused()) {
      await finishPausedCooperative();
      return;
    }

    const fileSnap = await QPChargeFile.findById(fileId);
    if (!fileSnap || fileSnap.deleted_at) {
      return;
    }
    if (fileSnap.pause_requested) {
      await finishPausedCooperative();
      return;
    }

    const instance = await QPChargeInstance.findOne({
      charge_file_id: fileId,
      status: "PENDING",
      deleted_at: null,
    })
      .sort({ row_number: 1 })
      .exec();

    if (!instance) {
      break;
    }

    await touchBulkActivity(fileId);

    if (await getGloballyPaused()) {
      await finishPausedCooperative();
      return;
    }
    const fileBeforeRow = await QPChargeFile.findById(fileId);
    if (!fileBeforeRow || fileBeforeRow.pause_requested) {
      await finishPausedCooperative();
      return;
    }

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
      await incrementFileChargeOutcome(fileId, "ERROR");
      errCount++;
      rowsThisRun++;
    } else {
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

      const terminal =
        instance.status === "SUCCESS"
          ? "SUCCESS"
          : instance.status === "DECLINED"
            ? "DECLINED"
            : "ERROR";
      await incrementFileChargeOutcome(fileId, terminal);
      rowsThisRun++;
    }

    await touchBulkActivity(fileId);

    if (await getGloballyPaused()) {
      await finishPausedCooperative();
      return;
    }
    const fileAfterRow = await QPChargeFile.findById(fileId);
    if (fileAfterRow && fileAfterRow.pause_requested) {
      await finishPausedCooperative();
      return;
    }

    const hasMorePending = await QPChargeInstance.exists({
      charge_file_id: fileId,
      status: "PENDING",
      deleted_at: null,
    });
    if (hasMorePending) {
      await delayBetweenCharges();
    }
  }

  const fileDoc = await QPChargeFile.findById(fileId);
  if (!fileDoc || fileDoc.deleted_at) {
    return;
  }

  fileDoc.status =
    fileDoc.error_count > 0 ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
  fileDoc.pause_requested = false;
  fileDoc.bulk_last_activity_at = new Date();

  if (userEmail && rowsThisRun > 0) {
    try {
      await sendQPProcessCompletionEmail({
        to: userEmail,
        success: true,
        count: rowsThisRun,
        fileName: fileNameForEmail,
        chargeFileId: String(fileId),
        successCount: success,
        declinedCount: declined,
        errorCount: errCount,
        skippedCount: 0,
      });
    } catch (emailErr) {
      console.error("Failed to send file bulk run completion email:", emailErr);
    }
  }

  const allInstances = await QPChargeInstance.find({
    charge_file_id: fileId,
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

    const compiledFileName = `compiled_${fileId}_${runId}.xlsx`;
    const compiledFilePath = path.join(
      __dirname,
      "../../public/files",
      compiledFileName,
    );

    fs.ensureDirSync(path.dirname(compiledFilePath));

    xlsx.writeFile(wb, compiledFilePath);

    fileDoc.compiled_storage_path = `/files/${compiledFileName}`;
  } catch (compileErr) {
    console.error("Error generating compiled file:", compileErr);
    TraceLogger.error(
      "CHARGE_FILE_COMPILE_ERROR",
      `Failed to generate compiled file`,
      compileErr,
      {
        run_id: runId,
        entity_id: fileId,
        entity_type: "QPChargeFile",
      },
    );
  }

  await fileDoc.save();

  TraceLogger.info(
    "CHARGE_FILE_PROCESS_END",
    `Finished bulk charge run ${runId}`,
    {
      run_id: runId,
      actor_user_id: userId,
      entity_id: fileId,
      metadata: {
        success,
        declined,
        error_count: errCount,
        processed_rows_this_run: rowsThisRun,
      },
    },
  );

  try {
    await processNextQueuedFile(userId, userEmail);
  } catch (queueErr) {
    console.error("Failed to start next queued file:", queueErr);
    TraceLogger.error(
      "CHARGE_FILE_QUEUE_FAILURE",
      "Failed to start next queued file",
      queueErr,
      {
        run_id: runId,
        entity_id: fileId,
      },
    );
  }
}

// helper: start next queued file from queue
async function processNextQueuedFile(userId, userEmail) {
  if (await getGloballyPaused()) {
    return;
  }

  const nextFile = await QPChargeFile.findOne({
    status: "QUEUED",
    deleted_at: null,
  })
    .sort({ queue_order: 1, queued_at: 1 })
    .exec();

  if (!nextFile) {
    return;
  }

  const nextRunId = generateRunId();
  nextFile.status = "PROCESSING";
  nextFile.last_run_id = nextRunId;
  nextFile.queue_order = null;
  nextFile.queued_at = null;
  nextFile.pause_requested = false;
  nextFile.bulk_last_activity_at = new Date();
  await nextFile.save();

  TraceLogger.info(
    "CHARGE_FILE_QUEUE_START",
    `Starting queued file ${nextFile._id} as run ${nextRunId}`,
    {
      run_id: nextRunId,
      actor_user_id: userId,
      entity_id: nextFile._id,
    },
  );

  processBulkRunAsync(nextFile, nextRunId, userId, userEmail).catch((err) => {
    console.error("Queued file bulk run error:", err);
    TraceLogger.error(
      "CHARGE_FILE_QUEUE_RUN_ERROR",
      `Queued file run ${nextRunId} encountered error`,
      err,
      {
        run_id: nextRunId,
        entity_id: nextFile._id,
      },
    );
  });
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

  const safeName = (file.file_name || "report").replace(
    /[^a-zA-Z0-9._-]/g,
    "_",
  );
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
