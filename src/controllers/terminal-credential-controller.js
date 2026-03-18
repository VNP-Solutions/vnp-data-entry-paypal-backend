const TerminalCredential = require("../models/TerminalCredential");
const { encrypt, decrypt } = require("../utils/encryption");
const { TraceLogger, generateRequestId } = require("../utils/logger");
const xlsx = require("xlsx");

// Utility to catch async errors and forward to next()
const catchAsync = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// MARK: 1. Create single credential (username + terminal_key required; hotel_id optional)
exports.createCredential = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const { hotel_id, username, terminal_key } = req.body;
  const userId = req.user?.userId;

  const usernameTrim = username != null ? String(username).trim() : "";
  const terminalKeyTrim = terminal_key != null ? String(terminal_key).trim() : "";
  if (!usernameTrim || !terminalKeyTrim) {
    return res
      .status(400)
      .json({ status: "error", message: "Username and terminal key are required" });
  }

  const existing = await TerminalCredential.findOne({
    username: usernameTrim,
    deleted_at: null,
  });
  if (existing) {
    return res.status(409).json({
      status: "error",
      message: "Credential for this username already exists",
    });
  }

  const encryptedKey = encrypt(terminalKeyTrim);

  const credential = await TerminalCredential.create({
    hotel_id: hotel_id != null && String(hotel_id).trim() ? String(hotel_id).trim() : null,
    username: usernameTrim,
    terminal_key: encryptedKey,
    created_by: userId,
  });

  TraceLogger.info(
    "TERMINAL_CRED_CREATE",
    `Created terminal credential for username ${usernameTrim}`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_type: "TerminalCredential",
      entity_id: credential._id,
    },
  );

  res.status(201).json({
    status: "success",
    data: {
      id: credential._id,
      hotel_id: credential.hotel_id,
      username: credential.username,
    },
  });
});

// MARK: 2a. Check unique usernames (read-only, non-destructive)
exports.checkUniqueUsernames = catchAsync(async (req, res) => {
  const duplicates = await TerminalCredential.aggregate([
    { $match: { deleted_at: null } },
    { $group: { _id: "$username", count: { $sum: 1 } } },
    { $match: { count: { $gt: 1 } } },
    { $project: { username: "$_id", count: 1, _id: 0 } },
  ]);
  const duplicateUsernames = duplicates.map((d) => d.username).filter(Boolean);
  res.status(200).json({
    status: "success",
    data: {
      allUnique: duplicateUsernames.length === 0,
      duplicateUsernames,
      duplicateCount: duplicateUsernames.length,
    },
  });
});

// MARK: 2. List credentials
// 2. List credentials
exports.getCredentials = catchAsync(async (req, res, next) => {
  const { hotel_id, q, include_deleted, include_key, page, limit } = req.query;

  let query = {};

  if (include_deleted !== "true") {
    query.deleted_at = null;
  }
  if (hotel_id) {
    query.hotel_id = hotel_id;
  }
  if (q) {
    query.$or = [
      { hotel_id: new RegExp(q, "i") },
      { username: new RegExp(q, "i") },
    ];
  }

  const pageNum = Math.max(1, parseInt(page, 10) || 1);
  const limitNum = Math.min(100, Math.max(1, parseInt(limit, 10) || 20));
  const skip = (pageNum - 1) * limitNum;

  const total = await TerminalCredential.countDocuments(query);

  // By default we omit the encrypted key entirely for security.  If the
  // client passes `include_key=true` we will fetch the documents with the
  // key and decrypt it before returning.
  let credsQuery = TerminalCredential.find(query)
    .populate("created_by updated_by", "name email")
    .skip(skip)
    .limit(limitNum)
    .sort({ createdAt: -1 });

  if (include_key === "true") {
    // do not strip the field
  } else {
    credsQuery = credsQuery.select("-terminal_key");
  }

  let credentials = await credsQuery.exec();

  if (include_key === "true") {
    credentials = credentials.map((c) => {
      const obj = c.toObject();
      obj.terminal_key = decrypt(c.terminal_key);
      return obj;
    });
  }

  const pages = Math.ceil(total / limitNum) || 1;

  res.status(200).json({
    status: "success",
    data: credentials,
    pagination: { total, page: pageNum, limit: limitNum, pages },
  });
});

// MARK: 3. Get Single
// 3. Get Single
exports.getCredentialById = catchAsync(async (req, res, next) => {
  const { include_key } = req.query;

  let query = TerminalCredential.findById(req.params.id);
  if (include_key !== "true") {
    query = query.select("-terminal_key");
  }

  const credential = await query.exec();
  if (!credential || credential.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  let result = credential;
  if (include_key === "true") {
    result = credential.toObject();
    result.terminal_key = decrypt(credential.terminal_key);
  }

  res.status(200).json({ status: "success", data: result });
});

// MARK: 4. Update Credential
// 4. Update Credential
exports.updateCredential = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const { username, terminal_key } = req.body;

  const credential = await TerminalCredential.findById(req.params.id);
  if (!credential || credential.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  if (username) credential.username = username;
  if (terminal_key) credential.terminal_key = encrypt(terminal_key);

  credential.updated_by = userId;
  await credential.save();

  TraceLogger.info(
    "TERMINAL_CRED_UPDATE",
    `Updated terminal credential for hotel ${credential.hotel_id}`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_type: "TerminalCredential",
      entity_id: credential._id,
    },
  );

  res.status(200).json({ status: "success", message: "Updated successfully" });
});

// MARK: 5. Delete Credential (Soft Delete)
// 5. Delete Credential (Soft Delete)
exports.deleteCredential = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;

  const credential = await TerminalCredential.findById(req.params.id);
  if (!credential || credential.deleted_at) {
    return res.status(404).json({ status: "error", message: "Not found" });
  }

  credential.deleted_at = new Date();
  credential.deleted_by = userId;
  await credential.save();

  TraceLogger.info(
    "TERMINAL_CRED_DELETE",
    `Deleted terminal credential for hotel ${credential.hotel_id}`,
    {
      request_id: reqId,
      actor_user_id: userId,
      entity_type: "TerminalCredential",
      entity_id: credential._id,
    },
  );

  res.status(200).json({ status: "success", message: "Deleted successfully" });
});

// MARK: 6. Bulk Import
// 6. Bulk Import
exports.importCredentials = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;

  if (!req.file) {
    return res
      .status(400)
      .json({ status: "error", message: "Upload an excel or csv file" });
  }

  const workbook = xlsx.readFile(req.file.path);
  if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
    return res
      .status(400)
      .json({ status: "error", message: "Uploaded file contains no sheets" });
  }
  const sheetName = workbook.SheetNames[0];
  const rows = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);

  let successes = 0;
  let skipped = 0;

  for (let row of rows) {
    // Username and terminal_key required; hotel_id optional
    const hotel_id = row["hotel_id"] || row["Hotel ID"] || row["Hotel_ID"];
    const username = row["username"] || row["Username"];
    const terminal_key =
      row["terminal_key"] || row["Terminal Key"] || row["Terminal_Key"];

    const usernameStr = username != null ? String(username).trim() : "";
    const terminalKeyStr = terminal_key != null ? String(terminal_key).trim() : "";
    if (!usernameStr || !terminalKeyStr) {
      skipped++;
      continue;
    }

    const encryptedKey = encrypt(terminalKeyStr);
    const hotelIdVal =
      hotel_id != null && String(hotel_id).trim()
        ? String(hotel_id).trim()
        : null;

    const existing = await TerminalCredential.findOne({ username: usernameStr });
    if (existing) {
      existing.terminal_key = encryptedKey;
      existing.updated_by = userId;
      existing.hotel_id = hotelIdVal;
      existing.deleted_at = null;
      existing.deleted_by = undefined;
      await existing.save();
    } else {
      await TerminalCredential.create({
        username: usernameStr,
        terminal_key: encryptedKey,
        hotel_id: hotelIdVal,
        created_by: userId,
      });
    }
    successes++;
  }

  TraceLogger.info(
    "TERMINAL_CRED_IMPORT",
    `Imported ${successes} credentials, skipped ${skipped}`,
    {
      request_id: reqId,
      actor_user_id: userId,
    },
  );

  res.status(200).json({
    status: "success",
    data: { successes, skipped },
  });
});

// MARK: 7. Export Credentials
// 7. Export Credentials
exports.exportCredentials = catchAsync(async (req, res, next) => {
  const reqId = generateRequestId();
  const userId = req.user?.userId;
  const { mask_terminal_key = "true", format = "xlsx", ids } = req.query;

  if (!["xlsx", "csv"].includes(format.toLowerCase())) {
    return res.status(400).json({
      status: "error",
      message: "Invalid format. Use 'xlsx' or 'csv'",
    });
  }

  const query = { deleted_at: null };
  if (ids) {
    const idList = Array.isArray(ids)
      ? ids
      : String(ids)
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
    if (idList.length) query._id = { $in: idList };
  }

  const credentials = await TerminalCredential.find(query);

  const mapped = credentials.map((c) => {
    let tk =
      mask_terminal_key === "true" ? "********" : decrypt(c.terminal_key);
    return {
      hotel_id: c.hotel_id,
      username: c.username,
      terminal_key: tk,
      created_at: c.createdAt,
    };
  });

  let buffer;
  let contentType;
  let filename;
  let disposition;

  if (format.toLowerCase() === "csv") {
    // For CSV, use xlsx to generate CSV directly
    buffer = xlsx.write(mapped, { type: "buffer", bookType: "csv" });
    contentType = "text/csv";
    filename = "terminal_credentials.csv";
    disposition = `attachment; filename="${filename}"`;
  } else {
    // XLSX
    const ws = xlsx.utils.json_to_sheet(mapped);
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, "Credentials");
    buffer = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });
    contentType =
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    filename = "terminal_credentials.xlsx";
    disposition = `attachment; filename="${filename}"`;
  }

  TraceLogger.info(
    "TERMINAL_CRED_EXPORT",
    `Exported credentials vault as ${format}`,
    {
      request_id: reqId,
      actor_user_id: userId,
    },
  );

  res.setHeader("Content-Disposition", disposition);
  res.setHeader("Content-Type", contentType);
  res.send(buffer);
});
