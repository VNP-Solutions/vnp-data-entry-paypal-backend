const crypto = require('crypto');
const SystemLog = require('../models/SystemLog');

// MARK: Generate Request ID
/**
 * Creates a unique Request ID for correlation
 * @returns {string} 
 */
function generateRequestId() {
  return `req_${crypto.randomUUID()}`;
}

// MARK: Generate Run ID
/**
 * Creates a unique Run ID for bulk batch operation correlation
 * @returns {string}
 */
function generateRunId() {
  return `run_${crypto.randomUUID()}`;
}

// MARK: TraceLogger Class
/**
 * Structured Logging implementation to record system events into the database.
 * Does not block execution (fire and forget).
 */
class TraceLogger {
  
  /**
   * Internal common logger function
   */
  static _log(level, action, message, params = {}) {
    const {
      request_id,
      run_id,
      actor_user_id,
      entity_type,
      entity_id,
      metadata
    } = params;

    const logEntry = new SystemLog({
      level,
      action,
      message,
      request_id,
      run_id,
      actor_user_id,
      entity_type,
      entity_id,
      metadata,
      timestamp: new Date()
    });

    // Fire and forget, catch errors internally so app doesn't crash on logging fail
    logEntry.save().catch(err => {
      console.error(`[TraceLogger Error] Could not save log for action ${action}:`, err);
    });

    if (process.env.NODE_ENV === 'development') {
      const color = level === 'ERROR' ? '\x1b[31m' : level === 'WARN' ? '\x1b[33m' : '\x1b[36m';
      console.log(`${color}[${level}] ${action}\x1b[0m - ${message}`);
    }
  }

  static info(action, message, params = {}) {
    this._log('INFO', action, message, params);
  }

  static warn(action, message, params = {}) {
    this._log('WARN', action, message, params);
  }

  static error(action, message, errorObj, params = {}) {
    // Inject error message into metadata if not present
    let metadata = params.metadata || {};
    if (errorObj) {
      metadata.error_message = errorObj.message;
      if (errorObj.code) metadata.error_code = errorObj.code;
    }

    this._log('ERROR', action, message, { ...params, metadata });
  }
}

module.exports = {
  TraceLogger,
  generateRequestId,
  generateRunId
};
