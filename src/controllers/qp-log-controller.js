const xlsx = require("xlsx");
const QPPaymentAttempt = require("../models/QPPaymentAttempt");
const SystemLog = require("../models/SystemLog");

const catchAsync = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

/**
 * Build Mongo match for payment attempts list/export (same filters).
 * @param {Record<string, string | undefined>} query
 * @returns {Promise<Record<string, unknown>>}
 */
async function buildPaymentAttemptsMatch(query) {
  const { charge_instance_id, run_id, result, date_from, date_to, search } =
    query;

  const match = {};

  if (charge_instance_id) match.charge_instance_id = charge_instance_id;
  if (run_id) match.run_id = run_id;
  if (result) match.result = result;

  if (date_from || date_to) {
    match.createdAt = {};
    if (date_from) match.createdAt.$gte = new Date(date_from);
    if (date_to) match.createdAt.$lte = new Date(date_to);
  }

  if (search) {
    const QPChargeInstance = require("../models/QPChargeInstance");
    const matchingInstances = await QPChargeInstance.find({
      $or: [
        { reservation_id: { $regex: search, $options: "i" } },
        { hotel_name: { $regex: search, $options: "i" } },
        { hotel_id: { $regex: search, $options: "i" } },
      ],
    }).select("_id");

    const instanceIds = matchingInstances.map((i) => i._id);

    match.$or = [
      { request_id: { $regex: search, $options: "i" } },
      { run_id: { $regex: search, $options: "i" } },
      { charge_instance_id: { $in: instanceIds } },
    ];
  }

  return match;
}

function mapAttemptToExportRow(attempt) {
  const body = attempt.response_body;
  const rb =
    body && typeof body === "object" && !Array.isArray(body) ? body : {};
  const proc =
    rb.processor && typeof rb.processor === "object" && !Array.isArray(rb.processor)
      ? rb.processor
      : {};
  const inst = attempt.charge_instance_id;
  const instObj =
    inst && typeof inst === "object" && inst !== null && "reservation_id" in inst
      ? inst
      : {};

  const ti = proc.transaction_identifier;
  const tiTrimmed =
    typeof ti === "string"
      ? ti.trim()
      : ti != null && ti !== ""
        ? String(ti).trim()
        : "";

  const createdAt = attempt.createdAt;
  const attemptAt =
    createdAt instanceof Date
      ? createdAt.toISOString()
      : createdAt
        ? new Date(createdAt).toISOString()
        : "";

  return {
    "Attempt At": attemptAt,
    Result: attempt.result ?? "",
    "Http Status":
      attempt.response_status_code != null
        ? attempt.response_status_code
        : "",
    "Request ID": attempt.request_id ?? "",
    "Run ID": attempt.run_id ?? "",
    "Actor Email": attempt.created_by?.email ?? "",
    "Reservation ID": instObj.reservation_id ?? "",
    Amount:
      instObj.amount_numeric != null && instObj.amount_numeric !== ""
        ? instObj.amount_numeric
        : "",
    Currency: instObj.currency ?? "",
    "QP Username": instObj.user_id ?? "",
    "OTA Billing Name": instObj.ota_billing_name ?? "",
    "Card Last 4": instObj.card_last4 ?? "",
    "Processor message": proc.message ?? "",
    "Approval code": proc.approval_code ?? "",
    "Transaction identifier": tiTrimmed,
    "Retrieval reference number": proc.retrieval_reference_number ?? "",
    "Payment ID": rb.payment_id ?? "",
    "Transaction ID": rb.transaction_id ?? rb.id ?? "",
    Action: rb.action ?? "",
    Code: rb.code ?? "",
    "Response message": rb.message ?? "",
    Status: rb.status ?? "",
    "Timestamp UTC": rb.timestamp_utc ?? "",
  };
}

// MARK: 1. Get Payment Attempts with filters
// 1. Get Payment Attempts with date range filtering, pagination and search
exports.getPaymentAttempts = catchAsync(async (req, res, next) => {
  const page = parseInt(req.query.page, 10) || 1;
  const limit = parseInt(req.query.limit, 10) || 10;
  const skip = (page - 1) * limit;

  const match = await buildPaymentAttemptsMatch(req.query);

  const total = await QPPaymentAttempt.countDocuments(match);
  const attempts = await QPPaymentAttempt.find(match)
    .sort({ createdAt: -1 })
    .skip(skip)
    .limit(limit)
    .populate("created_by", "name email")
    .populate(
      "charge_instance_id",
      "reservation_id hotel_name hotel_id amount_numeric currency status",
    );

  res.status(200).json({
    status: "success",
    data: {
      attempts,
      pagination: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
    },
  });
});

// MARK: 1b. Export payment attempts as .xlsx (same filters as list, no pagination)
exports.exportPaymentAttempts = catchAsync(async (req, res, next) => {
  const match = await buildPaymentAttemptsMatch(req.query);

  // No row cap: exports all documents matching filters. Very large sets use more RAM
  // and time (full cursor + in-memory workbook); use date/search filters if needed.
  const attempts = await QPPaymentAttempt.find(match)
    .sort({ createdAt: -1 })
    .populate("created_by", "name email")
    .populate(
      "charge_instance_id",
      "reservation_id hotel_name hotel_id amount_numeric currency status user_id ota_billing_name card_last4",
    )
    .lean();

  const rows = attempts.map((a) => mapAttemptToExportRow(a));
  const ws = xlsx.utils.json_to_sheet(rows);
  const wb = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(wb, ws, "Payment Attempts");
  const buffer = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });

  const pad = (n) => String(n).padStart(2, "0");
  const d = new Date();
  const dateStr = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}`;
  const filename = `qp_payment_attempts_${dateStr}.xlsx`;

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.send(buffer);
});

// MARK: 2. Get Single Payment Attempt
// 2. Get Single Payment Attempt
exports.getPaymentAttemptById = catchAsync(async (req, res, next) => {
  const attempt = await QPPaymentAttempt.findById(req.params.id).populate(
    "created_by",
    "name email",
  );

  if (!attempt) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  res.status(200).json({ status: "success", data: attempt });
});

// MARK: 3. Get System Logs with date range
// 3. Get System Logs with enhanced filtering
exports.getSystemLogs = catchAsync(async (req, res, next) => {
  const {
    request_id,
    run_id,
    action,
    entity_type,
    entity_id,
    level,
    date_from,
    date_to,
  } = req.query;
  const match = {};

  if (request_id) match.request_id = request_id;
  if (run_id) match.run_id = run_id;
  if (action) match.action = action;
  if (entity_type) match.entity_type = entity_type;
  if (entity_id) match.entity_id = entity_id;
  if (level) match.level = level;

  // Date range filtering on timestamp field
  if (date_from || date_to) {
    match.timestamp = {};
    if (date_from) match.timestamp.$gte = new Date(date_from);
    if (date_to) match.timestamp.$lte = new Date(date_to);
  }

  const logs = await SystemLog.find(match)
    .sort({ timestamp: -1 })
    .limit(1000) // Safety limit
    .populate("actor_user_id", "name email");

  res.status(200).json({ status: "success", data: logs });
});
