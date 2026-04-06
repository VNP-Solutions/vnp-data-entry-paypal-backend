#!/usr/bin/env node
/**
 * Decrypt and print CVV diagnostics for a QPChargeInstance by MongoDB _id.
 * Usage: ./scripts/decrypt-qp-cvv.js <objectId>
 * Requires .env with MONGODB_URI and ENCRYPTION_KEY (same as the API).
 */

const path = require("path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const mongoose = require("mongoose");
const { decrypt } = require("../src/utils/encryption");
const QPChargeInstance = require("../src/models/QPChargeInstance");

const id = process.argv[2];

if (!id || !/^[a-f0-9]{24}$/i.test(id)) {
  console.error("Usage: decrypt-qp-cvv.js <24-char MongoDB ObjectId>");
  process.exit(1);
}

(async () => {
  const uri = process.env.MONGODB_URI || "mongodb://localhost:27017/paypal-app";
  await mongoose.connect(uri);

  const doc = await QPChargeInstance.findById(id).lean();
  if (!doc) {
    console.error("QPChargeInstance not found:", id);
    await mongoose.disconnect();
    process.exit(1);
  }

  const cvv = doc.cvv ? decrypt(doc.cvv) : null;

  console.log("reservation_id:", doc.reservation_id);
  console.log("card_last4:", doc.card_last4);
  console.log("decrypted:", JSON.stringify(cvv));
  console.log("length:", cvv == null ? null : cvv.length);
  console.log("valid /^\\d{3,4}$/:",
    cvv != null && /^\d{3,4}$/.test(cvv));

  await mongoose.disconnect();
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
