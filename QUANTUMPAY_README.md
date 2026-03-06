# QuantumPay Integration Logic & Architecture

This document provides a high-level overview of the QuantumPay integration into the VNP Data Entry platform. The integration is designed to be a robust, background-capable batch payment processor replacing/augmenting existing integrations (PayPal/Stripe) via file upload methods.

## Core Mechanics
The QuantumPay integration follows a three-stage architectural flow:
1. **Credential Vaulting**: Securely attaching merchant/terminal API credentials to a given `hotel_id`.
2. **Batch Parser & Queuing**: Extracting charge instances from uploaded `.xlsx`/`.csv` files and mapping them exactly to QuantumPay specifications.
3. **Execution & Traceability**: Safely processing transactions against QuantumPay, either individually or sequentially in a bulk run, with detailed trace emission.

---

## 1. Terminal Credentials Vault (`terminal-credential-controller.js`)
QuantumPay transactions require an `X-TERMINAL-KEY`. Since these keys are tied to physical/virtual terminals assigned to specific hotels (`hotel_id`), they are stored in the database.
- **Encryption**: The `terminal_key` is encrypted at rest using AES-256-CBC and is only decrypted in memory microseconds before hitting the QuantumPay API.
- **Import/Export**: Supports bulk importing credentials via spreadsheets and masking keys during export.

## 2. Dynamic OAuth2 Authentication (`quantumpay-service.js`)
QuantumPay requires a short-lived bearer token alongside the terminal key.
- The `quantumpay-service.js` utilizes OAuth 2.0 `client_credentials` flow.
- It dynamically POSTs to the QuantumEpay Identity Server (`/connect/token`) using `QUANTUMPAY_CLIENT_ID` and `QUANTUMPAY_CLIENT_SECRET`.
- The `access_token` is cached securely in memory and auto-renews up to 60 seconds before expiration.

## 3. The Charging Models
Two core Mongoose schemas enforce the file structure:
- **`QPChargeFile`**: Represents the batch container (the uploaded spreadsheet). Maintains aggregate counts (`valid_rows`, `processed_rows`, `success_count`, etc.) and processing state.
- **`QPChargeInstance`**: Represents an individual row (a single card transaction). Contains all mapped billing limits, PAN (encrypted), CVV (encrypted), and tracing data. Contains a `charge_key` used as a highly reliable duplicate deduplication hash.

## 4. Processing Lifecycle (`qp-charge-controller.js`)
1. **Upload & Map `(/api/qp-charge-files/import)`**: Captures the buffer, converts rows into mapped `QPChargeInstance` docs, encrypts strict data, and flags missing/invalid data before writing them as `PENDING`. CVVs and PANs are isolated and stored using AES-256-CBC.
2. **Trigger Bulk Run `(/:id/process)`**: Async generator begins immediately iterating over `PENDING` instances attached to the charge file.
3. **External Service Execution**: The process decrypts PANs locally (`quantumpay-service.js`), maps the API JSON, strips out CVVs entirely during redaction, and POSTs the attempt to the `/creditcard/sale` gateway.
4. **Resolution**: The `QPChargeInstance` model absorbs the QuantumPay `transaction_id` and shifts state (`SUCCESS`, `DECLINED`, or `ERROR`).

## 5. System Trace Logs & Idempotency
Because payment state mutations are dangerous, strict logging was built using `src/utils/logger.js`.
- **Trace Logger**: Every write operation (imports, edits, processing start, and process end) emits an asynchronous log to `SystemLog.js` with structured data, isolating the `actor_user_id`, `request_id`, and `run_id`.
- **Idempotency Hash**: Built securely by combining `HotelID + ReservationID + Amount + CardLast4`. Safe bounds checking applies `is_duplicate` truth tags locally.
- **Redaction Traces**: Individual QuantumPay attempt JSON traces are saved unaltered internally as `QPPaymentAttempt`, allowing debugging and forensic auditing. The `request_payload_redacted` bucket aggressively censors PAN and drops CVV inside traces to prevent data compromise.

## 6. Exporting
Once processing finishes, `/download-compiled` grabs the database records, aligns the outcome of each transaction row next to the original data injected originally via import, and streams a standard `.xlsx` back.

---
**Note on Code Mapping:** The entire QuantumPay module extensively utilizes `// MARK:` formatting. To browse specific segments of the controllers via an IDE plugin, scan the file system for `MARK:`.
