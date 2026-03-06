const QPPaymentAttempt = require("../models/QPPaymentAttempt");
const SystemLog = require("../models/SystemLog");

const catchAsync = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// MARK: 1. Get Payment Attempts with filters
// 1. Get Payment Attempts with date range filtering
exports.getPaymentAttempts = catchAsync(async (req, res, next) => {
  const { charge_instance_id, run_id, result, date_from, date_to } = req.query;
  const match = {};

  if (charge_instance_id) match.charge_instance_id = charge_instance_id;
  if (run_id) match.run_id = run_id;
  if (result) match.result = result;

  // Date range filtering
  if (date_from || date_to) {
    match.createdAt = {};
    if (date_from) match.createdAt.$gte = new Date(date_from);
    if (date_to) match.createdAt.$lte = new Date(date_to);
  }

  const attempts = await QPPaymentAttempt.find(match)
    .sort({ createdAt: -1 })
    .populate("created_by", "name email");

  res.status(200).json({ status: "success", data: attempts });
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
