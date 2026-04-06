const mongoose = require("mongoose");

/**
 * Singleton-style settings for QP bulk queue (global pause).
 * Use fixed _id "global" via getOrCreate().
 */
const qpQueueSettingsSchema = new mongoose.Schema(
  {
    _id: { type: String, default: "global" },
    globally_paused: { type: Boolean, default: false },
    updated_at: { type: Date, default: Date.now },
  },
  { collection: "qpqueuesettings" },
);

module.exports = mongoose.model("QPQueueSettings", qpQueueSettingsSchema);
