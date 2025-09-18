const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY);
const StripeSetting = require("../models/StripeSetting");
const ExcelData = require("../models/ExcelData");
const { encryptCardData, decryptCardData } = require("../utils/encryption");
const nodemailer = require("nodemailer");
const StripeExcelData = require("../models/StripeExcelData");
const DisputeModel = require("../models/DisputeModel");
const fs = require("fs");
const path = require("path");

// PayPal configuration
const PAYPAL_CLIENT_ID = process.env.PAYPAL_CLIENT_ID;
const PAYPAL_CLIENT_SECRET = process.env.PAYPAL_CLIENT_SECRET;

/**
 * Process direct payment with card information using PayPal v2 API
 * @see https://developer.paypal.com/docs/api/orders/v2/#orders_create
 */
const processDirectPayment = async (paymentData) => {
  const {
    amount,
    currency = "USD",
    description = "Payment for services",
    descriptor,
    documentId,
    cardNumber,
    cardExpiry,
    cardCvv,
    cardholderName,
    billingAddress,
  } = paymentData;

  // Parse card expiry (MUST be in YYYY-MM format, e.g., "2025-12")
  const [year, month] = cardExpiry.split("-");

  // Parse cardholder name (assuming format: "First Last")
  const nameParts = cardholderName.trim().split(" ");
  const firstName = nameParts[0] || "";
  const lastName = nameParts.slice(1).join(" ") || "";

  // Determine card type based on number
  const getCardType = (number) => {
    const cleanNumber = number.replace(/\s/g, "");
    if (/^4/.test(cleanNumber)) return "VISA";
    if (/^5[1-5]/.test(cleanNumber)) return "MASTERCARD";
    if (/^3[47]/.test(cleanNumber)) return "AMEX";
    if (/^6/.test(cleanNumber)) return "DISCOVER";
    return "VISA"; // default
  };

  const requestBody = {
    intent: "CAPTURE",
    purchase_units: [
      {
        amount: {
          currency_code: currency,
          value: amount.toFixed(2),
        },
        description: description,
        custom_id: documentId?.toString(),
        soft_descriptor: descriptor,
        payment_instruction: {
          platform_fees: [
            {
              amount: {
                currency_code: currency,
                value: "0.00",
              },
            },
          ],
        },
      },
    ],
    payment_source: {
      card: {
        number: cardNumber,
        expiry: `${year}-${month}`,
        security_code: cardCvv,
        name: cardholderName,
        billing_address: {
          address_line_1: billingAddress?.address_line_1 || "123 Main St",
          admin_area_2: billingAddress?.admin_area_2 || "Any City",
          admin_area_1: billingAddress?.admin_area_1 || "CA",
          postal_code: billingAddress?.postal_code || "12345",
          country_code: billingAddress?.country_code || "US",
        },
      },
    },
  };

  try {
    // Get access token using the same method as refund function
    const clientId = PAYPAL_CLIENT_ID.trim();
    const clientSecret = PAYPAL_CLIENT_SECRET.trim();
    const auth = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");

    // Use environment-based URL configuration
    const baseURL =
      process.env.NODE_ENV === "production"
        ? "https://api-m.paypal.com" // Production
        : "https://api-m.sandbox.paypal.com"; // Sandbox

    // First get access token
    const tokenResponse = await fetch(`${baseURL}/v1/oauth2/token`, {
      method: "POST",
      headers: {
        Authorization: `Basic ${auth}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: "grant_type=client_credentials",
    });

    if (!tokenResponse.ok) {
      const tokenError = await tokenResponse.text();
      console.error("Token response status:", tokenResponse.status);
      console.error("Token response error:", tokenError);
      throw new Error(
        `Failed to get PayPal access token: ${tokenResponse.status} ${tokenResponse.statusText}`
      );
    }

    const tokenData = await tokenResponse.json();
    const accessToken = tokenData.access_token;

    if (!accessToken) {
      throw new Error("No access token received from PayPal");
    }

    console.info("Successfully obtained PayPal access token");

    // Step 1: Create the order using direct API
    const createOrderEndpoint = `${baseURL}/v2/checkout/orders`;
    const orderRequestId = `order-${documentId}-${Date.now()}-${Math.random()
      .toString(36)
      .substr(2, 9)}`;

    const createResponse = await fetch(createOrderEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
        "PayPal-Request-Id": orderRequestId,
        Prefer: "return=representation",
      },
      body: JSON.stringify(requestBody),
    });

    const httpStatusCode = createResponse.status;

    if (!createResponse.ok) {
      const errorText = await createResponse.text();
      console.error("Create Order failed with status:", httpStatusCode);
      console.error("Create Order error response:", errorText);

      let errorData;
      try {
        errorData = JSON.parse(errorText);
      } catch (parseErr) {
        errorData = { error: errorText };
      }

      throw new Error(
        `PayPal order creation failed: ${errorData.message || errorText}`
      );
    }

    const jsonResponse = await createResponse.json();

    return { jsonResponse, httpStatusCode };
  } catch (error) {
    console.error("processDirectPayment error:", error);
    throw error;
  }
};
// Email configuration (you'll need to configure this with your email service)
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.EMAIL_USER || "your-email@gmail.com",
    pass: process.env.EMAIL_PASS || "your-app-password",
  },
});

/**
 * Create a Stripe Connect account
 * @param {Object} accountData - Account creation data
 * @returns {Object} - Stripe account object
 */

const createConnectAccount = async (accountData) => {
  const { country = "US", email, type = "express" } = accountData;

  try {
    const account = await stripe.accounts.create({
      country: country,
      email: email,
      type: "express", // or 'standard'
      // Remove controller configuration if not properly set up
      // controller: {
      //     fees: {
      //         payer: 'application',
      //     },
      //     losses: {
      //         payments: 'application',
      //     },
      //     stripe_dashboard: {
      //         type: type,
      //     },
      // },
    });

    return account;
  } catch (error) {
    console.error("Stripe Account Creation Error:", error);
    throw error;
  }
};

const createConnectAccountLinkAndEmail = async (accountId, accountEmail) => {
  try {
    const accountLink = await stripe.accountLinks.create({
      account: accountId,
      refresh_url:
        process.env.FRONTEND_URL + "onboarding-retry" ||
        "http://localhost:3000/onboarding-retry",
      return_url:
        process.env.FRONTEND_URL + "onboarding-success" ||
        "http://localhost:3000/onboarding-success",
      type: "account_onboarding",
    });

    const mailOptions = {
      from: process.env.EMAIL_USER || "your-email@gmail.com",
      to: accountEmail,
      subject: "Stripe Account Onboarding Link",
      text: `Please click the following link to onboard your Stripe account: ${accountLink.url}. 
      Note: link will be expired in 30min.`,
    };

    await transporter.sendMail(mailOptions);
  } catch (error) {
    console.error("Failed to send email:", error);
    throw error;
  }
};

/**
 * Create a Stripe Connect account with controller configuration
 * Use this after setting up platform profile in Stripe Dashboard
 */
const createManagedAccount = async (accountData) => {
  const { country = "US", email, type = "express" } = accountData;

  try {
    const account = await stripe.accounts.create({
      country: country,
      email: email,
      controller: {
        fees: {
          payer: "application",
        },
        losses: {
          payments: "application",
        },
        stripe_dashboard: {
          type: type,
        },
      },
    });

    return account;
  } catch (error) {
    console.error("Stripe Managed Account Creation Error:", error);
    throw error;
  }
};

/**
 * Create Stripe Connect account endpoint
 * POST /api/stripe/create-account
 */
const createAccount = async (req, res) => {
  try {
    const { country, email, type } = req.body;

    // Validate required fields
    if (!email) {
      return res.status(400).json({
        status: "error",
        message: "Email is required",
      });
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({
        status: "error",
        message: "Please provide a valid email address",
      });
    }

    // Validate country code (if provided)
    if (country && country.length !== 2) {
      return res.status(400).json({
        status: "error",
        message: "Country code must be a valid 2-letter ISO code",
      });
    }

    // Validate dashboard type (if provided)
    const validTypes = ["express", "full"];
    if (type && !validTypes.includes(type)) {
      return res.status(400).json({
        status: "error",
        message: 'Type must be either "express" or "full"',
      });
    }

    const accountData = {
      country: country || "US",
      email: email,
      type: type || "express",
    };

    const account = await createConnectAccount(accountData);
    // Extract important account details
    const accountDetails = {
      id: account.id,
      email: account.email,
      country: account.country,
      type: account.type,
      capabilities: account.capabilities,
      requirements: account.requirements,
      charges_enabled: account.charges_enabled,
      details_submitted: account.details_submitted,
      payouts_enabled: account.payouts_enabled,
      created: account.created,
      default_currency: account.default_currency,
      controller: account.controller,
    };

    await createConnectAccountLinkAndEmail(account.id, account.email);

    // TODO: Save account information to database if needed
    // This would follow the same pattern as the PayPal controller
    // const savedAccount = await StripeAccount.create(accountDetails);

    res.status(200).json({
      status: "success",
      message: "Stripe Connect account created successfully",
      data: {
        account: accountDetails,
        // databaseSaved: true // Enable when database integration is added
      },
    });
  } catch (error) {
    console.error("Failed to create Stripe account:", error);

    // Handle specific Stripe errors
    if (error.type === "StripeInvalidRequestError") {
      return res.status(400).json({
        status: "error",
        message: "Invalid request to Stripe",
        error: error.message,
      });
    }

    if (error.type === "StripeAPIError") {
      return res.status(502).json({
        status: "error",
        message: "Stripe API error",
        error: error.message,
      });
    }

    if (error.type === "StripeConnectionError") {
      return res.status(503).json({
        status: "error",
        message: "Unable to connect to Stripe",
        error: error.message,
      });
    }

    if (error.type === "StripeAuthenticationError") {
      return res.status(401).json({
        status: "error",
        message: "Stripe authentication failed",
        error: error.message,
      });
    }

    res.status(500).json({
      status: "error",
      message: "Failed to create Stripe account",
      error: error.message,
    });
  }
};

/**
 * List all Stripe Connect accounts with search functionality
 * GET /api/stripe/accounts
 */
const listAccounts = async (req, res) => {
  try {
    const { limit = 10, page = 1, starting_after, search } = req.query;

    // Validate limit parameter
    const numericLimit = parseInt(limit);
    if (isNaN(numericLimit) || numericLimit <= 0 || numericLimit > 100) {
      return res.status(400).json({
        status: "error",
        message: "Limit must be a valid number between 1 and 100",
      });
    }

    // Validate page parameter
    const currentPage = parseInt(page);
    if (isNaN(currentPage) || currentPage < 1) {
      return res.status(400).json({
        status: "error",
        message: "Page must be a valid number starting from 1",
      });
    }

    // Build Stripe list parameters
    const listParams = {
      limit: numericLimit,
    };

    // Add cursor-based pagination if starting_after is provided
    if (starting_after) {
      listParams.starting_after = starting_after;
    }

    // Get accounts from Stripe (fetch more records to handle pagination properly)
    const accounts = await stripe.accounts.list({
      limit: 100, // Get more records to handle pagination and search
    });

    // If search is provided, filter the results by email and business profile name
    let filteredData = accounts.data;
    if (search) {
      const searchTerm = search.toLowerCase();
      filteredData = accounts.data.filter((account) => {
        // Search in email
        const emailMatch =
          account.email && account.email.toLowerCase().includes(searchTerm);

        // Search in business profile name
        const businessNameMatch =
          account.business_profile &&
          account.business_profile.name &&
          account.business_profile.name.toLowerCase().includes(searchTerm);

        return emailMatch || businessNameMatch;
      });
    }

    // Calculate pagination
    const totalFilteredCount = filteredData.length;
    const totalPages =
      totalFilteredCount > 0 ? Math.ceil(totalFilteredCount / numericLimit) : 1;
    const startIndex = (currentPage - 1) * numericLimit;
    const endIndex = startIndex + numericLimit;

    // Check if page number exceeds available data
    if (totalFilteredCount > 0 && currentPage > totalPages) {
      return res.status(400).json({
        status: "error",
        message: `Page ${currentPage} does not exist. Total pages available: ${totalPages}`,
        pagination: {
          current_page: currentPage,
          total_pages: totalPages,
          total_filtered_count: totalFilteredCount,
        },
      });
    }

    // Get the data for current page
    const pageData = filteredData.slice(startIndex, endIndex);

    // Create paginated response
    const filteredAccounts = {
      object: accounts.object,
      url: accounts.url,
      has_more: currentPage < totalPages,
      data: pageData,
    };

    // Transform accounts data for consistent response
    const transformedAccounts = {
      object: filteredAccounts.object,
      url: filteredAccounts.url,
      data: filteredAccounts.data.map((account) => ({
        id: account.id,
        object: account.object,
        business_profile: account.business_profile,
        business_type: account.business_type,
        capabilities: account.capabilities,
        charges_enabled: account.charges_enabled,
        controller: account.controller,
        country: account.country,
        created: account.created,
        default_currency: account.default_currency,
        details_submitted: account.details_submitted,
        email: account.email,
        external_accounts: account.external_accounts,
        future_requirements: account.future_requirements,
        login_links: account.login_links,
        metadata: account.metadata,
        payouts_enabled: account.payouts_enabled,
        requirements: account.requirements,
        settings: account.settings,
        tos_acceptance: account.tos_acceptance,
        type: account.type,
      })),
    };

    res.status(200).json({
      status: "success",
      message: "Stripe accounts retrieved successfully",
      data: {
        ...transformedAccounts,
        pagination: {
          current_page: currentPage,
          limit: numericLimit,
          has_more: filteredAccounts.has_more,
          total_count: filteredAccounts.data.length,
          total_filtered_count: totalFilteredCount,
          total_pages: totalPages,
          next_page: filteredAccounts.has_more ? currentPage + 1 : null,
          previous_page: currentPage > 1 ? currentPage - 1 : null,
          last_id:
            filteredAccounts.data.length > 0
              ? filteredAccounts.data[filteredAccounts.data.length - 1].id
              : null,
        },
        filters: {
          applied: {
            search: search || null,
          },
        },
      },
    });
  } catch (error) {
    console.error("Failed to list Stripe accounts:", error);

    // Handle specific Stripe errors
    if (error.type === "StripeInvalidRequestError") {
      return res.status(400).json({
        status: "error",
        message: "Invalid request parameters",
        error: error.message,
      });
    }

    if (error.type === "StripeAPIError") {
      return res.status(502).json({
        status: "error",
        message: "Stripe API error",
        error: error.message,
      });
    }

    if (error.type === "StripeConnectionError") {
      return res.status(503).json({
        status: "error",
        message: "Unable to connect to Stripe",
        error: error.message,
      });
    }

    if (error.type === "StripeAuthenticationError") {
      return res.status(401).json({
        status: "error",
        message: "Stripe authentication failed",
        error: error.message,
      });
    }

    res.status(500).json({
      status: "error",
      message: "Failed to retrieve Stripe accounts",
      error: error.message,
    });
  }
};

/**
 * Get Stripe Connect account by ID
 * GET /api/stripe/account/:accountId
 */
const getAccountById = async (req, res) => {
  try {
    const { accountId } = req.params;

    // Validate account ID
    if (!accountId) {
      return res.status(400).json({
        status: "error",
        message: "Account ID is required",
      });
    }

    // Validate account ID format (basic check)
    if (!accountId.startsWith("acct_")) {
      return res.status(400).json({
        status: "error",
        message: "Invalid account ID format",
      });
    }

    // Retrieve account from Stripe
    const account = await stripe.accounts.retrieve(accountId);

    // Transform account data for consistent response
    const accountDetails = {
      id: account.id,
      object: account.object,
      business_profile: account.business_profile,
      business_type: account.business_type,
      capabilities: account.capabilities,
      charges_enabled: account.charges_enabled,
      controller: account.controller,
      country: account.country,
      created: account.created,
      default_currency: account.default_currency,
      details_submitted: account.details_submitted,
      email: account.email,
      external_accounts: account.external_accounts,
      future_requirements: account.future_requirements,
      login_links: account.login_links,
      metadata: account.metadata,
      payouts_enabled: account.payouts_enabled,
      requirements: account.requirements,
      settings: account.settings,
      tos_acceptance: account.tos_acceptance,
      type: account.type,
    };

    res.status(200).json({
      status: "success",
      message: "Account details retrieved successfully",
      data: {
        account: accountDetails,
      },
    });
  } catch (error) {
    console.error("Failed to retrieve Stripe account:", error);

    // Handle specific Stripe errors
    if (error.type === "StripeInvalidRequestError") {
      return res.status(400).json({
        status: "error",
        message: "Invalid account ID or account not found",
        error: error.message,
      });
    }

    if (error.type === "StripeAPIError") {
      return res.status(502).json({
        status: "error",
        message: "Stripe API error",
        error: error.message,
      });
    }

    if (error.type === "StripeConnectionError") {
      return res.status(503).json({
        status: "error",
        message: "Unable to connect to Stripe",
        error: error.message,
      });
    }

    if (error.type === "StripeAuthenticationError") {
      return res.status(401).json({
        status: "error",
        message: "Stripe authentication failed",
        error: error.message,
      });
    }

    res.status(500).json({
      status: "error",
      message: "Failed to retrieve account details",
      error: error.message,
    });
  }
};

/**
 * Delete Stripe Connect account
 * DELETE /api/stripe/account/:accountId
 */
const deleteAccount = async (req, res) => {
  try {
    const { accountId } = req.params;

    // Validate account ID
    if (!accountId) {
      return res.status(400).json({
        status: "error",
        message: "Account ID is required",
      });
    }

    // Validate account ID format (basic check)
    if (!accountId.startsWith("acct_")) {
      return res.status(400).json({
        status: "error",
        message: "Invalid account ID format",
      });
    }

    // Delete account from Stripe
    const deleted = await stripe.accounts.del(accountId);

    res.status(200).json({
      status: "success",
      message: "Account deleted successfully",
      data: {
        deleted: deleted.deleted,
        id: deleted.id,
        object: deleted.object,
      },
    });
  } catch (error) {
    console.error("Failed to delete Stripe account:", error);

    // Handle specific Stripe errors
    if (error.type === "StripeInvalidRequestError") {
      return res.status(400).json({
        status: "error",
        message: "Invalid account ID or account not found",
        error: error.message,
      });
    }

    if (error.type === "StripeAPIError") {
      return res.status(502).json({
        status: "error",
        message: "Stripe API error",
        error: error.message,
      });
    }

    if (error.type === "StripeConnectionError") {
      return res.status(503).json({
        status: "error",
        message: "Unable to connect to Stripe",
        error: error.message,
      });
    }

    if (error.type === "StripeAuthenticationError") {
      return res.status(401).json({
        status: "error",
        message: "Stripe authentication failed",
        error: error.message,
      });
    }

    res.status(500).json({
      status: "error",
      message: "Failed to delete account",
      error: error.message,
    });
  }
};

const createSinglePayment = async (req, res) => {
  try {
    const {
      amount,
      totalAmount,
      currency = "USD",
      description = "Payment for services",
      descriptor,
      cardNumber,
      cardExpiry,
      cardCvv,
      cardholderName,
      billingAddress,
      documentId,
      accountId,
      paymentMethodId,
    } = req.body;

    // Determine payment method based on payload structure
    const isStripePayment = !!(accountId && totalAmount && paymentMethodId); ;
    const isPayPalPayment = !!(
      cardNumber &&
      cardExpiry &&
      cardCvv &&
      cardholderName
    );

    if (isStripePayment) {
      return await processStripePayment(req, res);
    } else if (isPayPalPayment) {
      return await processPayPalPayment(req, res);
    } else {
      return res.status(400).json({
        status: "error",
        message:
          "Invalid payment data. Please provide either Stripe payment details (accountId, totalAmount, paymentMethod) or PayPal payment details (cardNumber, cardExpiry, cardCvv, cardholderName, amount).",
      });
    }
  } catch (error) {
    console.error("Failed to process payment:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to process payment",
      error: error.message,
    });
  }
};

// Stripe Payment Processing
const processStripePayment = async (req, res) => {
  try {
    const { totalAmount, currency, paymentMethodId, accountId, documentId } =
      req.body;

    if (!totalAmount || !accountId || !paymentMethodId) {
      return res.status(400).json({
        status: "error",
        message: "totalAmount, accountId, and paymentMethodId are required for Stripe payments",
      });
    }

    const details = await StripeExcelData.findById(documentId);
    
    if (!details) {
      return res.status(404).json({
        status: "error",
        message: "Document not found",
      });
    }

    // Fetch VNP ratio from settings (default to 15%)
    let vnpRatio = 15;
    try {
      const setting = await StripeSetting.findOne();
      if (setting && typeof setting.vnpRatio === "number") {
        vnpRatio = Math.max(0, Math.min(100, Math.round(setting.vnpRatio)));
      }
    } catch (e) {
      // If settings read fails, proceed with default ratio
    }

    // Compute application fee in cents without rounding (truncate fractional cents)
    const totalAmountCents = Number(totalAmount) || 0; // totalAmount is expected in cents
    const rawFeeCents = (totalAmountCents * (Number(vnpRatio) || 0)) / 100;
    const applicationFeeAmount =
      rawFeeCents >= 0 ? Math.trunc(rawFeeCents) : -Math.trunc(-rawFeeCents);

    const paymentIntent = await stripe.paymentIntents.create({
      amount: totalAmount,
      currency: currency || "usd",
      payment_method: paymentMethodId, // Use the secure payment method ID from frontend
      confirm: true,
      application_fee_amount: applicationFeeAmount,
      transfer_data: {
        destination: accountId,
      },
      automatic_payment_methods: {
        enabled: true,
        allow_redirects: "never",
      },
    });

    // Update database record if documentId is provided
    if (documentId) {
      try {
        const StripeExcelData = require("../models/StripeExcelData");

        let chargeStatus, recordStatus;
        if (paymentIntent.status === "succeeded") {
          chargeStatus = "Charged";
          recordStatus = "Payment Processed";
        } else if (paymentIntent.status === "requires_payment_method") {
          chargeStatus = "Failed";
          recordStatus = "Payment Failed";
        } else {
          chargeStatus = paymentIntent.status;
          recordStatus = `Payment ${paymentIntent.status}`;
        }

        // Update the StripeExcelData record with payment status and details
        const updatedRecord = await StripeExcelData.findByIdAndUpdate(
          documentId,
          {
            "Charge status": chargeStatus,
            Status: recordStatus,
            // Store Stripe payment details
            stripePaymentIntentId: paymentIntent.id,
            stripeLatestChargeId: paymentIntent.latest_charge,
            stripePaymentMethodId: paymentIntent.payment_method,
            stripeTransferDestination: paymentIntent.transfer_data?.destination,
            stripeTransferGroup: paymentIntent.transfer_group,
            stripeApplicationFeeAmount: paymentIntent.application_fee_amount,
            stripeAmount: paymentIntent.amount,
            stripeAmountReceived: paymentIntent.amount_received,
            stripeCurrency: paymentIntent.currency,
            stripeStatus: paymentIntent.status,
            stripeCaptureMethod: paymentIntent.capture_method,
            stripeConfirmationMethod: paymentIntent.confirmation_method,
            stripeCreatedAt: new Date(paymentIntent.created * 1000), // Convert unix timestamp to Date
            stripeClientSecret: paymentIntent.client_secret,
            stripePaymentMethodTypes: paymentIntent.payment_method_types,
            stripeAutomaticPaymentMethods:
              paymentIntent.automatic_payment_methods,
            stripeDescription: paymentIntent.description,
            stripeMetadata: paymentIntent.metadata,
            ...(paymentIntent.status !== "succeeded" && {
              lastFailureDate: new Date().toISOString(),
            }),
          },
          { new: true }
        );

        if (!updatedRecord) {
          console.error(
            `StripeExcelData record not found for documentId: ${documentId}`
          );
        } else {
        }
      } catch (dbError) {
        console.error("Database update error:", dbError);
        // Don't fail the response if DB update fails, just log it
      }
    }

    res.status(200).json({
      status: "success",
      message: "Stripe payment created successfully",
      data: {
        payment: paymentIntent,
        databaseUpdated: documentId ? true : false,
        chargeStatus:
          paymentIntent.status === "succeeded"
            ? "Charged"
            : paymentIntent.status === "requires_payment_method"
            ? "Failed"
            : paymentIntent.status,
      },
    });
  } catch (error) {
    console.error("Failed to create Stripe payment:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to create Stripe payment",
      error: error.message,
    });
  }
};

// PayPal Payment Processing
const processPayPalPayment = async (req, res) => {
  try {
    const {
      amount,
      currency = "USD",
      description = "Payment for services",
      descriptor,
      cardNumber,
      cardExpiry,
      cardCvv,
      cardholderName,
      billingAddress,
      documentId,
    } = req.body;

    // Validation for required fields with specific messages
    if (!amount) {
      return res.status(400).json({
        status: "error",
        message:
          "Payment amount is required. Please enter a valid amount to charge.",
      });
    }

    if (!cardNumber) {
      return res.status(400).json({
        status: "error",
        message:
          "Card number is required. Please enter a valid credit card number.",
      });
    }

    if (!cardExpiry) {
      return res.status(400).json({
        status: "error",
        message:
          "Card expiry date is required. Please enter the expiry date in MM/YY or YYYY-MM format.",
      });
    }

    if (!cardCvv) {
      return res.status(400).json({
        status: "error",
        message:
          "Card security code (CVV) is required. Please enter the 3 or 4 digit code from your card.",
      });
    }

    if (!cardholderName) {
      return res.status(400).json({
        status: "error",
        message:
          "Cardholder name is required. Please enter the name as it appears on your card.",
      });
    }

    // Validate amount is a valid number
    const numericAmount = parseFloat(amount);
    if (isNaN(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        status: "error",
        message: "Amount must be a valid number greater than 0.",
      });
    }

    // Validate and normalize card expiry
    let normalizedExpiry;
    if (cardExpiry.match(/^\d{2}\/\d{2}$/)) {
      // MM/YY format -> convert to YYYY-MM
      const [month, year] = cardExpiry.split("/");
      const fullYear = "20" + year;
      normalizedExpiry = `${fullYear}-${month}`;
    } else if (cardExpiry.match(/^\d{4}-\d{2}$/)) {
      // Already in YYYY-MM format
      normalizedExpiry = cardExpiry;
    } else {
      return res.status(400).json({
        status: "error",
        message: "Card expiry must be in MM/YY or YYYY-MM format.",
      });
    }

    // Clean and validate card number
    const cleanCardNumber = cardNumber.replace(/\s/g, "");
    if (!/^\d{13,19}$/.test(cleanCardNumber)) {
      return res.status(400).json({
        status: "error",
        message: "Card number must contain 13-19 digits.",
      });
    }

    // Process PayPal payment
    const paymentData = {
      amount: numericAmount,
      currency: currency,
      description: description,
      descriptor: descriptor,
      documentId: documentId,
      cardNumber: cleanCardNumber,
      cardExpiry: normalizedExpiry,
      cardCvv: cardCvv,
      cardholderName: cardholderName,
      billingAddress: billingAddress || {},
    };

    const { jsonResponse, httpStatusCode } = await processDirectPayment(
      paymentData
    );

    // Extract payment details from PayPal response
    const paymentDetails = {
      orderId: jsonResponse.id,
      captureId: jsonResponse.purchase_units[0]?.payments?.captures[0]?.id,
      networkTransactionId:
        jsonResponse.purchase_units[0]?.payments?.captures[0]
          ?.network_transaction_reference?.id,
      status: jsonResponse.status,
      amount: jsonResponse.purchase_units[0]?.amount?.value,
      currency: jsonResponse.purchase_units[0]?.amount?.currency_code,
      paypalFee:
        jsonResponse.purchase_units[0]?.payments?.captures[0]
          ?.seller_receivable_breakdown?.paypal_fee?.value,
      netAmount:
        jsonResponse.purchase_units[0]?.payments?.captures[0]
          ?.seller_receivable_breakdown?.net_amount?.value,
      cardLastDigits: jsonResponse.payment_source?.card?.last_digits,
      cardBrand: jsonResponse.payment_source?.card?.brand,
      cardType: jsonResponse.payment_source?.card?.type,
      avsCode:
        jsonResponse.purchase_units[0]?.payments?.captures[0]
          ?.processor_response?.avs_code,
      cvvCode:
        jsonResponse.purchase_units[0]?.payments?.captures[0]
          ?.processor_response?.cvv_code,
      createTime: jsonResponse.create_time,
      updateTime: jsonResponse.update_time,
      captureStatus:
        jsonResponse.purchase_units[0]?.payments?.captures[0]?.status,
      customId:
        jsonResponse.purchase_units[0]?.payments?.captures[0]?.custom_id,
    };

    // Check if payment was successful
    const captureStatus =
      jsonResponse.purchase_units[0]?.payments?.captures[0]?.status;

    if (captureStatus === "COMPLETED") {
      // Payment successful - update status to "Charged"
      if (documentId) {
        try {
          // Encrypt card data before storing
          const cardDataToEncrypt = {
            "Card Number": cleanCardNumber,
            "Card Expire": normalizedExpiry,
            "Card CVV": cardCvv,
          };
          const encryptedCardData = encryptCardData(cardDataToEncrypt);

          // Update the record with successful payment status
          const updatedRecord = await ExcelData.findByIdAndUpdate(
            documentId,
            {
              "Charge status": "Charged",
              "Card Number": encryptedCardData["Card Number"],
              "Card Expire": encryptedCardData["Card Expire"],
              "Card CVV": encryptedCardData["Card CVV"],
              "Soft Descriptor": descriptor,
              Status: "Payment Processed",
              // Add payment details
              paypalOrderId: paymentDetails.orderId,
              paypalCaptureId: paymentDetails.captureId,
              paypalNetworkTransactionId: paymentDetails.networkTransactionId,
              paypalFee: paymentDetails.paypalFee,
              paypalNetAmount: paymentDetails.netAmount,
              paypalCardBrand: paymentDetails.cardBrand,
              paypalCardType: paymentDetails.cardType,
              paypalAvsCode: paymentDetails.avsCode,
              paypalCvvCode: paymentDetails.cvvCode,
              paypalCreateTime: paymentDetails.createTime,
              paypalUpdateTime: paymentDetails.updateTime,
              paypalStatus: paymentDetails.status,
              paypalAmount: paymentDetails.amount,
              paypalCurrency: paymentDetails.currency,
              paypalCardLastDigits: paymentDetails.cardLastDigits,
              paypalCaptureStatus: paymentDetails.captureStatus,
              paypalCustomId: paymentDetails.customId,
            },
            { new: true }
          );

          if (!updatedRecord) {
            console.error(
              `ExcelData record not found for documentId: ${documentId}`
            );
          } else {
          }
        } catch (dbError) {
          console.error("Database update error:", dbError);
          // Don't fail the response if DB update fails, just log it
        }
      }

      // Return successful payment response
      return res.status(200).json({
        status: "success",
        message: "PayPal payment processed successfully",
        data: {
          paymentDetails: paymentDetails,
          databaseUpdated: documentId ? true : false,
          chargeStatus: "Charged",
        },
      });
    } else {
      // Payment failed or declined
      const statusField = "Payment Failed";
      const chargeStatus = captureStatus === "DECLINED" ? "Declined" : "Failed";

      if (documentId) {
        try {
          await ExcelData.findByIdAndUpdate(
            documentId,
            {
              "Charge status": chargeStatus,
              Status: statusField,
              paypalOrderId: paymentDetails.orderId,
              paypalCaptureId: paymentDetails.captureId,
              paypalCaptureStatus: paymentDetails.captureStatus,
              paypalStatus: paymentDetails.status,
              paypalAmount: paymentDetails.amount,
              paypalCurrency: paymentDetails.currency,
              paypalCreateTime: paymentDetails.createTime,
              paypalUpdateTime: paymentDetails.updateTime,
              lastFailureDate: new Date().toISOString(),
            },
            { new: true }
          );
        } catch (dbError) {
          console.error("Database update error for failed payment:", dbError);
        }
      }

      return res.status(400).json({
        status: "error",
        message: `PayPal payment ${chargeStatus.toLowerCase()}`,
        data: {
          paymentDetails: paymentDetails,
          chargeStatus: chargeStatus,
        },
      });
    }
  } catch (error) {
    console.error("Failed to process PayPal payment:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to process PayPal payment",
      error: error.message,
    });
  }
};

// Stripe Refund Processing
const processStripeRefund = async (req, res) => {
  try {
    const {
      paymentIntentId,
      amount, // in cents (optional for partial refund)
      reason, // optional: duplicate, fraudulent, requested_by_customer
      documentId, // optional: our DB record id
    } = req.body;

    let intentId = paymentIntentId;

    // If documentId is provided, try to resolve payment intent from DB
    if (!intentId && documentId) {
      const record = await StripeExcelData.findById(documentId);
      if (!record) {
        return res.status(404).json({
          status: "error",
          message: "Stripe record not found for provided documentId",
        });
      }
      intentId = record.stripePaymentIntentId;
      if (!intentId) {
        return res.status(400).json({
          status: "error",
          message: "No Stripe payment intent id stored for this record",
        });
      }
    }

    if (!intentId) {
      return res.status(400).json({
        status: "error",
        message:
          "paymentIntentId or documentId is required to process a refund",
      });
    }

    // Create refund
    const refundParams = { payment_intent: intentId };
    if (amount && Number(amount) > 0) {
      refundParams.amount = Number(amount);
    }
    if (reason) {
      refundParams.reason = reason;
    }

    const refund = await stripe.refunds.create(refundParams);

    // Determine status fields based on refund amount
    let chargeStatus = "Refunded";
    let recordStatus = "Refund Processed";

    try {
      // If we have a DB record, update it
      if (documentId) {
        // Fetch original record to compare for partial vs full
        const existing = await StripeExcelData.findById(documentId);
        if (existing) {
          const totalCharged = Number(existing.stripeAmount || 0);
          const totalRefunded =
            Number(existing.stripeTotalRefunded || 0) +
            Number(refund.amount || 0);

          if (totalCharged > 0 && totalRefunded < totalCharged) {
            chargeStatus = "Partially refunded";
            recordStatus = "Partial Refund Processed";
          }

          await StripeExcelData.findByIdAndUpdate(
            documentId,
            {
              "Charge status": chargeStatus,
              Status: recordStatus,
              stripeRefundId: refund.id,
              stripeRefundStatus: refund.status,
              stripeRefundAmount: refund.amount || null,
              stripeRefundCurrency: refund.currency || null,
              stripeRefundGrossAmount: refund.amount || null,
              stripeRefundFee: null,
              stripeRefundNetAmount: refund.amount || null,
              stripeTotalRefunded: totalRefunded,
              stripeRefundCreateTime: refund.created
                ? new Date(refund.created * 1000)
                : new Date(),
              stripeRefundUpdateTime: new Date(),
              stripeRefundInvoiceId: refund.charge || null,
              stripeRefundCustomId: refund.metadata?.custom_id || null,
              stripeRefundNote: refund.reason || reason || null,
            },
            { new: true }
          );
        }
      }
    } catch (dbError) {
      console.error("Stripe refund DB update error:", dbError);
      // Continue; don't fail response due to DB write
    }

    return res.status(200).json({
      status: "success",
      message: "Stripe refund created successfully",
      data: {
        refund,
        chargeStatus,
      },
    });
  } catch (error) {
    console.error("Failed to create Stripe refund:", error);
    return res.status(500).json({
      status: "error",
      message: "Failed to create refund",
      error: error.message,
    });
  }
};

// Get Stripe settings (vnpRatio)
const getStripeSettings = async (req, res) => {
  try {
    let setting = await StripeSetting.findOne();
    if (!setting) {
      setting = await StripeSetting.create({ vnpRatio: 15 });
    }
    res.status(200).json({
      status: "success",
      message: "Stripe settings retrieved successfully",
      data: { vnpRatio: setting.vnpRatio },
    });
  } catch (error) {
    console.error("Failed to get Stripe settings:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to get Stripe settings",
      error: error.message,
    });
  }
};

// Update Stripe settings (vnpRatio)
const updateStripeSettings = async (req, res) => {
  try {
    let { vnpRatio } = req.body;
    if (typeof vnpRatio !== "number") {
      return res.status(400).json({
        status: "error",
        message: "vnpRatio must be a number",
      });
    }
    vnpRatio = Math.max(0, Math.min(100, Math.round(vnpRatio)));

    const updated = await StripeSetting.findOneAndUpdate(
      {},
      { vnpRatio },
      { new: true, upsert: true }
    );

    res.status(200).json({
      status: "success",
      message: "Stripe settings updated successfully",
      data: { vnpRatio: updated.vnpRatio },
    });
  } catch (error) {
    console.error("Failed to update Stripe settings:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to update Stripe settings",
      error: error.message,
    });
  }
};

/**
 * Handle Stripe webhook events for disputes
 * POST /api/stripe/webhook
 */
const handleStripeWebhook = async (req, res) => {
  const sig = req.headers["stripe-signature"];
  const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET;

  let event;

  try {
    // Verify webhook signature
    event = stripe.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  try {
    // Handle dispute events
    switch (event.type) {
      case "charge.dispute.created":
        await handleDisputeCreated(event.data.object);
        break;

      case "charge.dispute.updated":
        await handleDisputeUpdated(event.data.object);
        break;

      case "charge.dispute.closed":
        await handleDisputeClosed(event.data.object);
        break;

      default:
    }

    res.json({ received: true });
  } catch (error) {
    console.error(`Error processing webhook: ${error.message}`);
    res.status(500).json({
      status: "error",
      message: "Error processing webhook",
      error: error.message,
    });
  }
};

/**
 * Handle dispute created event
 */
const handleDisputeCreated = async (dispute) => {
  try {
    // Find the original payment record by charge ID
    const paymentRecord = await StripeExcelData.findOne({
      stripeLatestChargeId: dispute.charge,
    });

    if (paymentRecord) {
      // Create new dispute record
      const disputeRecord = await DisputeModel.create({
        stripeExcelDataId: paymentRecord._id,
        userId: paymentRecord.userId,
        stripePaymentIntentId: paymentRecord.stripePaymentIntentId,
        stripeLatestChargeId: dispute.charge,
        stripeDisputeId: dispute.id,
        stripeDisputeStatus: dispute.status,
        stripeDisputeReason: dispute.reason,
        stripeDisputeAmount: dispute.amount,
        stripeDisputeCurrency: dispute.currency,
        stripeDisputeCreatedAt: new Date(dispute.created * 1000),
        stripeDisputeEvidenceDueBy: dispute.evidence_details?.due_by
          ? new Date(dispute.evidence_details.due_by * 1000)
          : null,
        stripeDisputeNetworkReasonCode: dispute.network_reason_code,
        stripeDisputeIsChargeRefundable: dispute.is_charge_refundable,
        stripeDisputeBalanceTransactions:
          dispute.balance_transactions?.map((bt) => bt.id) || [],
        stripeDisputeMetadata: dispute.metadata || {},
        // Copy hotel information for quick access
        hotelName: paymentRecord["Hotel Name"],
        reservationId: paymentRecord["Reservation ID"],
        guestName: paymentRecord.Name,
        checkIn: paymentRecord["Check In"],
        checkOut: paymentRecord["Check Out"],
        connectedAccount: paymentRecord["Connected Account"],
        internalStatus: "new",
      });

      // Update original payment record status
      await StripeExcelData.findByIdAndUpdate(paymentRecord._id, {
        "Charge status": "Disputed",
        Status: "Payment Disputed",
      });

      // Send email notification about dispute
      await sendDisputeNotification(paymentRecord, dispute, "created");
    } else {
    }
  } catch (error) {
    console.error(`Error handling dispute created: ${error.message}`);
    throw error;
  }
};

/**
 * Handle dispute updated event
 */
const handleDisputeUpdated = async (dispute) => {
  try {
    const disputeRecord = await DisputeModel.findOne({
      stripeDisputeId: dispute.id,
    });

    if (disputeRecord) {
      await DisputeModel.findByIdAndUpdate(disputeRecord._id, {
        stripeDisputeStatus: dispute.status,
        stripeDisputeEvidenceDueBy: dispute.evidence_details?.due_by
          ? new Date(dispute.evidence_details.due_by * 1000)
          : null,
        stripeDisputeEvidenceSubmitted:
          dispute.evidence_details?.submission_count > 0,
        stripeDisputeEvidenceDetails: dispute.evidence_details || {},
        stripeDisputeMetadata: dispute.metadata || {},
      });
    } else {
    }
  } catch (error) {
    console.error(`Error handling dispute updated: ${error.message}`);
    throw error;
  }
};

/**
 * Handle dispute closed event
 */
const handleDisputeClosed = async (dispute) => {
  try {
    const disputeRecord = await DisputeModel.findOne({
      stripeDisputeId: dispute.id,
    });

    if (disputeRecord) {
      let chargeStatus = "Disputed";
      let recordStatus = "Payment Disputed";
      let internalStatus = "resolved";

      // Update status based on dispute outcome
      if (dispute.status === "won") {
        chargeStatus = "Dispute Won";
        recordStatus = "Dispute Resolved - Won";
      } else if (dispute.status === "lost") {
        chargeStatus = "Dispute Lost";
        recordStatus = "Dispute Resolved - Lost";
      } else if (dispute.status === "charge_refunded") {
        chargeStatus = "Refunded";
        recordStatus = "Dispute Resolved - Refunded";
      }

      // Update dispute record
      await DisputeModel.findByIdAndUpdate(disputeRecord._id, {
        stripeDisputeStatus: dispute.status,
        internalStatus: internalStatus,
      });

      // Update original payment record
      await StripeExcelData.findByIdAndUpdate(disputeRecord.stripeExcelDataId, {
        "Charge status": chargeStatus,
        Status: recordStatus,
      });

      // Get the original payment record for notification
      const paymentRecord = await StripeExcelData.findById(
        disputeRecord.stripeExcelDataId
      );

      // Send email notification about dispute resolution
      await sendDisputeNotification(paymentRecord, dispute, "closed");
    } else {
    }
  } catch (error) {
    console.error(`Error handling dispute closed: ${error.message}`);
    throw error;
  }
};

/**
 * Send email notification about dispute events
 */
const sendDisputeNotification = async (record, dispute, eventType) => {
  try {
    const subject =
      eventType === "created"
        ? `New Dispute Created - ${dispute.id}`
        : `Dispute Resolved - ${dispute.id}`;

    const emailBody = `
      <h2>Stripe Dispute ${
        eventType === "created" ? "Created" : "Resolved"
      }</h2>
      <p><strong>Dispute ID:</strong> ${dispute.id}</p>
      <p><strong>Status:</strong> ${dispute.status}</p>
      <p><strong>Reason:</strong> ${dispute.reason}</p>
      <p><strong>Amount:</strong> ${(dispute.amount / 100).toFixed(
        2
      )} ${dispute.currency.toUpperCase()}</p>
      <p><strong>Charge ID:</strong> ${dispute.charge}</p>
      <p><strong>Record ID:</strong> ${record._id}</p>
      <p><strong>Hotel Name:</strong> ${record["Hotel Name"] || "N/A"}</p>
      <p><strong>Reservation ID:</strong> ${
        record["Reservation ID"] || "N/A"
      }</p>
      ${
        eventType === "created" && dispute.evidence_details?.due_by
          ? `<p><strong>Evidence Due By:</strong> ${new Date(
              dispute.evidence_details.due_by * 1000
            ).toLocaleDateString()}</p>`
          : ""
      }
      <p>Please review this dispute in your Stripe dashboard and take appropriate action.</p>
    `;

    const mailOptions = {
      from: process.env.EMAIL_USER || "your-email@gmail.com",
      to:
        process.env.DISPUTE_NOTIFICATION_EMAIL ||
        process.env.EMAIL_USER ||
        "your-email@gmail.com",
      subject: subject,
      html: emailBody,
    };

    await transporter.sendMail(mailOptions);
  } catch (error) {
    console.error(`Failed to send dispute notification: ${error.message}`);
  }
};

/**
 * Upload file to use as dispute evidence
 * POST /api/stripe/upload-evidence
 */
const uploadDisputeEvidence = async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({
        status: "error",
        message: "No file uploaded",
      });
    }

    const file = await stripe.files.create({
      purpose: "dispute_evidence",
      file: {
        data: fs.readFileSync(req.file.path),
        name: req.file.originalname,
        type: req.file.mimetype,
      },
    });

    // Clean up uploaded file
    fs.unlinkSync(req.file.path);

    res.status(200).json({
      status: "success",
      message: "Evidence file uploaded successfully",
      data: {
        fileId: file.id,
        filename: file.filename,
        purpose: file.purpose,
        size: file.size,
        type: file.type,
        url: file.url,
      },
    });
  } catch (error) {
    console.error("Failed to upload dispute evidence:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to upload evidence file",
      error: error.message,
    });
  }
};

/**
 * Submit dispute evidence
 * POST /api/stripe/submit-evidence
 */
const submitDisputeEvidence = async (req, res) => {
  try {
    const { disputeId, additionalInfo } = req.body;
    const evidenceFile = req.file; // File uploaded via multer

    if (!disputeId) {
      return res.status(400).json({
        status: "error",
        message: "Dispute ID is required",
      });
    }

    let fileId = null;

    // If a file was uploaded, upload it to Stripe
    if (evidenceFile) {
      try {
        // Read the file from the temporary location
        const fileBuffer = fs.readFileSync(evidenceFile.path);

        // Upload file to Stripe for dispute evidence
        const stripeFile = await stripe.files.create({
          file: {
            data: fileBuffer,
            name: evidenceFile.originalname,
            type: "application/octet-stream",
          },
          purpose: "dispute_evidence",
        });

        fileId = stripeFile.id;

        // Clean up temporary file
        fs.unlinkSync(evidenceFile.path);
      } catch (fileError) {
        console.error("Failed to upload file to Stripe:", fileError);

        // Clean up temporary file even if upload failed
        if (evidenceFile.path && fs.existsSync(evidenceFile.path)) {
          fs.unlinkSync(evidenceFile.path);
        }

        return res.status(500).json({
          status: "error",
          message: "Failed to upload evidence file to Stripe",
          error: fileError.message,
        });
      }
    }

    // Prepare evidence object
    const evidence = {};

    // Add file ID to evidence if file was uploaded
    // Use customer_communication field for uploaded communication documents
    if (fileId) {
      evidence.customer_communication = fileId;
    } else if (additionalInfo && additionalInfo.trim()) {
      // Only add text to customer_communication if no file was uploaded
      evidence.customer_communication = additionalInfo.trim();
    }

    // Add additional text info to a different field if both file and text are provided
    if (fileId && additionalInfo && additionalInfo.trim()) {
      // Use uncategorized_text for additional text information when file is also provided
      evidence.uncategorized_text = additionalInfo.trim();
    }

    // Prepare metadata
    const metadata = {
      uploaded_via_admin: "true",
      upload_timestamp: new Date().toISOString(),
    };

    if (additionalInfo) {
      metadata.additional_info = additionalInfo;
    }

    // Update the dispute with evidence
    const updatedDispute = await stripe.disputes.update(disputeId, {
      evidence,
      metadata,
      submit: true,
    });

    // Update our database record
    const disputeRecord = await DisputeModel.findOne({
      stripeDisputeId: disputeId,
    });

    if (disputeRecord) {
      await DisputeModel.findByIdAndUpdate(disputeRecord._id, {
        stripeDisputeEvidenceSubmitted: true,
        stripeDisputeEvidenceDetails: updatedDispute.evidence_details || {},
        stripeDisputeMetadata: updatedDispute.metadata || {},
        stripeDisputeEvidenceFileId: fileId || null,
        internalStatus: "evidence_submitted",
      });
    }

    res.status(200).json({
      status: "success",
      message: "Dispute evidence submitted successfully",
      data: {
        dispute: updatedDispute,
        uploadedFileId: fileId,
      },
    });
  } catch (error) {
    console.error("Failed to submit dispute evidence:", error);

    // Clean up temporary file if it exists and there was an error
    if (req.file && req.file.path && fs.existsSync(req.file.path)) {
      try {
        fs.unlinkSync(req.file.path);
      } catch (cleanupError) {
        console.error("Failed to clean up temporary file:", cleanupError);
      }
    }

    res.status(500).json({
      status: "error",
      message: "Failed to submit dispute evidence",
      error: error.message,
    });
  }
};

/**
 * Get dispute details
 * GET /api/stripe/dispute/:disputeId
 */
const getDisputeDetails = async (req, res) => {
  try {
    const { disputeId } = req.params;

    if (!disputeId) {
      return res.status(400).json({
        status: "error",
        message: "Dispute ID is required",
      });
    }

    // Get dispute from Stripe
    const dispute = await stripe.disputes.retrieve(disputeId);

    // Get our database record with populated payment data
    const disputeRecord = await DisputeModel.findOne({
      stripeDisputeId: disputeId,
    }).populate("stripeExcelDataId");

    res.status(200).json({
      status: "success",
      message: "Dispute details retrieved successfully",
      data: {
        dispute: dispute,
        disputeRecord: disputeRecord,
        paymentRecord: disputeRecord?.stripeExcelDataId || null,
      },
    });
  } catch (error) {
    console.error("Failed to get dispute details:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to get dispute details",
      error: error.message,
    });
  }
};

/**
 * List all disputes with filtering
 * GET /api/stripe/disputes
 */
const listDisputes = async (req, res) => {
  try {
    const {
      limit = 10,
      page = 1,
      status,
      reason,
      internalStatus,
      hotelName,
      starting_after,
      ending_before,
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);

    // Build Stripe API parameters for filtering
    const stripeParams = {
      limit: limitNum,
    };

    if (starting_after) stripeParams.starting_after = starting_after;
    if (ending_before) stripeParams.ending_before = ending_before;

    // Get ALL disputes from Stripe first (we'll filter client-side)
    const stripeDisputes = await stripe.disputes.list({
      limit: 100, // Get more to allow for filtering
      ...stripeParams,
    });

    // Filter disputes based on frontend parameters
    let filteredDisputes = stripeDisputes.data;

    if (status && status !== "all") {
      filteredDisputes = filteredDisputes.filter(
        (dispute) => dispute.status === status
      );
    }

    if (reason && reason !== "all") {
      filteredDisputes = filteredDisputes.filter(
        (dispute) => dispute.reason === reason
      );
    }

    // Note: hotelName and internalStatus filtering would require database lookup
    // For now, we'll focus on Stripe-native filters (status, reason)

    // Apply pagination to filtered results
    const startIndex = (pageNum - 1) * limitNum;
    const endIndex = startIndex + limitNum;
    const paginatedDisputes = filteredDisputes.slice(startIndex, endIndex);

    // Also get our database records for additional context
    const disputeRecords = await DisputeModel.find({})
      .populate("stripeExcelDataId")
      .sort({ stripeDisputeCreatedAt: -1 })
      .limit(100); // Limit to avoid performance issues

    res.status(200).json({
      status: "success",
      message: "Disputes retrieved successfully",
      data: {
        disputeRecords: disputeRecords,
        stripeDisputes: {
          object: stripeDisputes.object,
          data: paginatedDisputes,
          has_more: endIndex < filteredDisputes.length,
          count: filteredDisputes.length,
          url: stripeDisputes.url,
        },
        pagination: {
          current_page: pageNum,
          limit: limitNum,
          total_count: filteredDisputes.length,
          total_pages: Math.ceil(filteredDisputes.length / limitNum),
          has_more: endIndex < filteredDisputes.length,
        },
        filters: {
          applied: {
            status,
            reason,
            internalStatus,
            hotelName,
          },
        },
      },
    });
  } catch (error) {
    console.error("Failed to list disputes:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to list disputes",
      error: error.message,
    });
  }
};

/**
 * Get dispute statistics
 * GET /api/stripe/disputes/stats
 */
const getDisputeStats = async (req, res) => {
  try {
    const { userId, startDate, endDate } = req.query;

    const matchQuery = {};
    if (userId) matchQuery.userId = userId;
    if (startDate || endDate) {
      matchQuery.stripeDisputeCreatedAt = {};
      if (startDate)
        matchQuery.stripeDisputeCreatedAt.$gte = new Date(startDate);
      if (endDate) matchQuery.stripeDisputeCreatedAt.$lte = new Date(endDate);
    }

    const stats = await DisputeModel.aggregate([
      { $match: matchQuery },
      {
        $group: {
          _id: null,
          totalDisputes: { $sum: 1 },
          totalAmount: { $sum: "$stripeDisputeAmount" },
          statusBreakdown: {
            $push: {
              status: "$stripeDisputeStatus",
              amount: "$stripeDisputeAmount",
            },
          },
          reasonBreakdown: {
            $push: {
              reason: "$stripeDisputeReason",
              amount: "$stripeDisputeAmount",
            },
          },
          internalStatusBreakdown: {
            $push: {
              status: "$internalStatus",
              amount: "$stripeDisputeAmount",
            },
          },
        },
      },
    ]);

    // Process breakdown data
    const result = stats[0] || {
      totalDisputes: 0,
      totalAmount: 0,
      statusBreakdown: [],
      reasonBreakdown: [],
      internalStatusBreakdown: [],
    };

    // Group by status
    const statusStats = {};
    result.statusBreakdown.forEach((item) => {
      if (!statusStats[item.status]) {
        statusStats[item.status] = { count: 0, amount: 0 };
      }
      statusStats[item.status].count++;
      statusStats[item.status].amount += item.amount;
    });

    // Group by reason
    const reasonStats = {};
    result.reasonBreakdown.forEach((item) => {
      if (!reasonStats[item.reason]) {
        reasonStats[item.reason] = { count: 0, amount: 0 };
      }
      reasonStats[item.reason].count++;
      reasonStats[item.reason].amount += item.amount;
    });

    // Group by internal status
    const internalStatusStats = {};
    result.internalStatusBreakdown.forEach((item) => {
      if (!internalStatusStats[item.status]) {
        internalStatusStats[item.status] = { count: 0, amount: 0 };
      }
      internalStatusStats[item.status].count++;
      internalStatusStats[item.status].amount += item.amount;
    });

    res.status(200).json({
      status: "success",
      message: "Dispute statistics retrieved successfully",
      data: {
        summary: {
          totalDisputes: result.totalDisputes,
          totalAmount: result.totalAmount,
          averageAmount:
            result.totalDisputes > 0
              ? result.totalAmount / result.totalDisputes
              : 0,
        },
        breakdowns: {
          byStatus: statusStats,
          byReason: reasonStats,
          byInternalStatus: internalStatusStats,
        },
      },
    });
  } catch (error) {
    console.error("Failed to get dispute statistics:", error);
    res.status(500).json({
      status: "error",
      message: "Failed to get dispute statistics",
      error: error.message,
    });
  }
};

module.exports = {
  createAccount,
  listAccounts,
  getAccountById,
  deleteAccount,
  createSinglePayment,
  getStripeSettings,
  updateStripeSettings,
  processStripeRefund,
  handleStripeWebhook,
  uploadDisputeEvidence,
  submitDisputeEvidence,
  getDisputeDetails,
  listDisputes,
  getDisputeStats,
};
