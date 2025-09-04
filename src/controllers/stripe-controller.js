const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY);
const StripeSetting = require("../models/StripeSetting");
const ExcelData = require("../models/ExcelData");
const { encryptCardData } = require("../utils/encryption");
const nodemailer = require("nodemailer");
const StripeExcelData = require("../models/StripeExcelData");

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

    console.log("Using PayPal base URL:", baseURL);

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

    console.log("Successfully obtained PayPal access token");

    // Step 1: Create the order using direct API
    const createOrderEndpoint = `${baseURL}/v2/checkout/orders`;
    const orderRequestId = `order-${documentId}-${Date.now()}-${Math.random()
      .toString(36)
      .substr(2, 9)}`;

    console.log("Create Order endpoint:", createOrderEndpoint);
    console.log(
      "Create Order request body:",
      JSON.stringify(requestBody, null, 2)
    );

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
    console.log("Order created successfully");
    console.log("Order response:", JSON.stringify(jsonResponse, null, 2));

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
    console.log(account);
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
  console.log("Processing payment via /api/stripe/payment endpoint");
  console.log(req.body);

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
      paymentMethod,
    } = req.body;

    // Determine payment method based on payload structure
    const isStripePayment = !!(accountId && paymentMethod && totalAmount);
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
    const { totalAmount, currency, paymentMethod, accountId, documentId } =
      req.body;

    if (!totalAmount || !accountId) {
      return res.status(400).json({
        status: "error",
        message: "totalAmount and accountId are required for Stripe payments",
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

    // const applicationFeeAmount = Math.round(Number(totalAmount) * (vnpRatio / 100));
    // Compute application fee in cents without rounding (truncate fractional cents)
    const totalAmountCents = Number(totalAmount) || 0; // totalAmount is expected in cents
    const rawFeeCents = (totalAmountCents * (Number(vnpRatio) || 0)) / 100;
    const applicationFeeAmount = rawFeeCents >= 0 ? Math.trunc(rawFeeCents) : -Math.trunc(-rawFeeCents);
    
    const paymentIntent = await stripe.paymentIntents.create({
      amount: totalAmount,
      currency: currency || "usd",
      payment_method: paymentMethod || "pm_card_visa",
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
          console.log(
            `Database updated successfully for documentId ${documentId} with status: ${chargeStatus}`
          );
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
            console.log(
              `Database updated successfully for documentId ${documentId} with status: Charged`
            );
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
          const totalRefunded = Number(existing.stripeTotalRefunded || 0) +
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

module.exports = {
  createAccount,
  listAccounts,
  getAccountById,
  deleteAccount,
  createSinglePayment,
  getStripeSettings,
  updateStripeSettings,
  processStripeRefund,
};
