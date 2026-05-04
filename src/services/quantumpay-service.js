const fetch = globalThis.fetch || require("node-fetch");
const { encrypt, decrypt } = require("../utils/encryption");
const {
  TraceLogger,
  generateRequestId,
  generateRunId,
} = require("../utils/logger");
const QPPaymentAttempt = require("../models/QPPaymentAttempt");

const QP_API_URL =
  process.env.QUANTUMPAY_API_URL || "https://payments.quantumepay.com";
const QP_IDENTITY_URL =
  process.env.QUANTUMPAY_IDENTITY_URL || "https://identity.quantumepay.com";

let cachedToken = null;
let tokenExpiresAt = null;

// MARK: Fetch OAuth Token
/**
 * Fetches and caches the OAuth2 Bearer Token using Client Credentials
 */
async function getBearerToken() {
  if (cachedToken && tokenExpiresAt && Date.now() < tokenExpiresAt) {
    return cachedToken;
  }

  const clientId = process.env.QUANTUMPAY_CLIENT_ID;
  const clientSecret = process.env.QUANTUMPAY_CLIENT_SECRET;

  if (!clientId || !clientSecret) {
    // Fallback to static token if explicitly provided, else throw
    if (process.env.QUANTUMPAY_BEARER_TOKEN) {
      return process.env.QUANTUMPAY_BEARER_TOKEN;
    }
    throw new Error(
      "Missing QuantumPay OAuth credentials (QUANTUMPAY_CLIENT_ID / QUANTUMPAY_CLIENT_SECRET)",
    );
  }

  const params = new URLSearchParams();
  params.append("grant_type", "client_credentials");
  params.append("client_id", clientId);
  params.append("client_secret", clientSecret);

  const res = await fetch(`${QP_IDENTITY_URL}/connect/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: params.toString(),
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(
      `Failed to fetch QuantumPay Token: ${res.status} ${res.statusText} - ${errText}`,
    );
  }

  const data = await res.json();
  cachedToken = data.access_token;

  // Safely cache it, expiring 60 seconds early to avoid edge cases
  const expiresIn = data.expires_in || 3600;
  tokenExpiresAt = Date.now() + (expiresIn - 60) * 1000;

  TraceLogger.info(
    "PROVIDER_AUTH_SUCCESS",
    "Successfully generated new QuantumPay Bearer token",
  );

  return cachedToken;
}

// MARK: Payload Builder
/**
 * Creates the QuantumPay specific JSON payload
 */
function buildPayload(instance, unencryptedPan, unencryptedCvv) {
  // Strip non-numeric safely
  const month = instance.expiry_month || 1;
  const year = instance.expiry_year
    ? parseInt(instance.expiry_year.toString().slice(-2), 10)
    : 25;

  const billingDisplayName = String(instance.ota_billing_name || "").trim();
  const payload = {
    account: {
      // OTA billing name from the sheet (legal/merchant billing identity), not the short OTA Name column.
      first_name: billingDisplayName || "Expedia Group",
      last_name: "",
      card_security_code: unencryptedCvv || "",
      expiry_month: month,
      expiry_year: year,
      card_number: unencryptedPan,
      billing_address: {
        address_1: instance.billing_address?.address_1 || "",
        address_2: instance.billing_address?.address_2 || "",
        city: instance.billing_address?.city || "",
        state: instance.billing_address?.state || "",
        postal_code: instance.billing_address?.postal_code || "",
        country_code: instance.billing_address?.country_code || "US",
      },
    },
    additional_amounts: {},
    amount: instance.amount_numeric,
    credential_on_file: null,
    currency: instance.currency || "USD",
    order: {
      order_id: instance.reservation_id,
    },
    shipping: {},
    user_id: instance.user_id || instance.vnp_work_id || "System",
  };

  return payload;
}

// MARK: Payload Redaction
/**
 * Strips out CVV and full PAN for safe storage
 */
function redactPayload(payload) {
  const safe = JSON.parse(JSON.stringify(payload));
  if (safe.account) {
    if (safe.account.card_number) {
      safe.account.card_number = `****-****-****-${safe.account.card_number.slice(-4)}`;
    }
    delete safe.account.card_security_code;
  }
  return safe;
}

// MARK: Process Charge Core
/**
 * Process a single charge instance against QuantumPay
 */
exports.processCharge = async (
  instance,
  terminalKey,
  runId = null,
  actorUserId = null,
) => {
  const reqId = generateRequestId();
  const startTime = Date.now();

  // Decrypt critical card info at the last possible second
  const unencryptedPan = decrypt(instance.card_number);
  const unencryptedCvv = instance.cvv ? decrypt(instance.cvv) : null;

  if (!unencryptedPan) {
    throw new Error("Failed to decrypt card PAN");
  }

  // Validate CVV is present and valid (3-4 digits)
  if (!unencryptedCvv || !/^\d{3,4}$/.test(unencryptedCvv)) {
    throw new Error("Invalid or missing CVV (must be 3-4 digits)");
  }

  const payload = buildPayload(instance, unencryptedPan, unencryptedCvv);
  const redacted = redactPayload(payload);

  TraceLogger.info(
    "PROVIDER_CALL_START",
    `Initiating call to QuantumPay for res ${instance.reservation_id}`,
    {
      request_id: reqId,
      run_id: runId,
      actor_user_id: actorUserId,
      entity_type: "QPChargeInstance",
      entity_id: instance._id,
    },
  );

  let responseStatusCode = null;
  let responseBody = null;
  let providerResult = "ERROR";

  try {
    const bearerToken = await getBearerToken();

    const url = `${QP_API_URL}/creditcard/sale`;
    const requestHeaders = {
      "X-TERMINAL-KEY": terminalKey,
      Authorization: `Bearer ${bearerToken}`,
      "Content-Type": "application/json",
      "Idempotency-Key": reqId, // Add idempotency to prevent duplicate charges on retry
    };

    // Console diagnostics for single-instance processing (avoid leaking secrets)
    console.log("[QuantumPay] request", {
      request_id: reqId,
      url,
      headers: {
        ...requestHeaders,
        Authorization: "Bearer [REDACTED]",
        "X-TERMINAL-KEY":
          typeof terminalKey === "string" && terminalKey.length > 8
            ? `${terminalKey.slice(0, 4)}…${terminalKey.slice(-4)}`
            : "[REDACTED]",
      },
      payload: redacted,
    });

    const rawRes = await fetch(url, {
      method: "POST",
      headers: requestHeaders,
      body: JSON.stringify(payload),
    });

    responseStatusCode = rawRes.status;
    const responseText = await rawRes.text();
    try {
      responseBody = responseText ? JSON.parse(responseText) : null;
    } catch {
      responseBody = responseText;
    }

    console.log("[QuantumPay] response", {
      request_id: reqId,
      status: rawRes.status,
      statusText: rawRes.statusText,
      body: responseBody,
    });

    // Map HTTP status code + response body to standard result
    // First check response body status field (QuantumPay specific)
    if (responseStatusCode >= 200 && responseStatusCode < 300) {
      // 2xx = Successful request, check body status
      const bodyStatus =
        responseBody?.status?.toLowerCase() ||
        responseBody?.result?.toLowerCase();
      if (
        bodyStatus === "approved" ||
        bodyStatus === "success" ||
        bodyStatus === "completed"
      ) {
        providerResult = "SUCCESS";
      } else if (
        bodyStatus === "declined" ||
        bodyStatus === "failed" ||
        bodyStatus === "rejected"
      ) {
        providerResult = "DECLINED";
      } else {
        // Default to SUCCESS for 2xx (assume approved if status field not present)
        providerResult = "SUCCESS";
      }
    } else if (responseStatusCode === 402 || responseStatusCode === 422) {
      // 402 Payment Required, 422 Unprocessable Entity = likely card decline
      providerResult = "DECLINED";
    } else if (responseStatusCode >= 400 && responseStatusCode < 500) {
      // 4xx Client error = DECLINED or ERROR depending on code
      if (responseStatusCode === 401 || responseStatusCode === 403) {
        providerResult = "ERROR"; // Auth/permission issue
      } else {
        providerResult = "DECLINED"; // Bad request / processing error
      }
    } else if (responseStatusCode >= 500) {
      // 5xx Server error = Treat as ERROR, NOT declined
      providerResult = "ERROR";
    } else {
      providerResult = "ERROR";
    }
  } catch (err) {
    responseBody = { error_message: err.message, stack: err.stack };
    providerResult = "ERROR";
  }

  const durationMs = Date.now() - startTime;

  // Store raw diagnostic trace safely
  await QPPaymentAttempt.create({
    charge_instance_id: instance._id,
    request_id: reqId,
    run_id: runId,
    request_payload_redacted: redacted,
    response_status_code: responseStatusCode,
    response_body: responseBody,
    result: providerResult,
    created_by: actorUserId,
  });

  TraceLogger.info(
    "PROVIDER_CALL_END",
    `QuantumPay responded with ${providerResult} in ${durationMs}ms`,
    {
      request_id: reqId,
      run_id: runId,
      actor_user_id: actorUserId,
      entity_type: "QPChargeInstance",
      entity_id: instance._id,
      metadata: {
        duration_ms: durationMs,
        response_status_code: responseStatusCode,
        result: providerResult,
      },
    },
  );

  return {
    providerResult,
    responseBody,
    reqId,
  };
};
