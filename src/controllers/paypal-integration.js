const fetch = globalThis.fetch || require('node-fetch');
const ExcelData = require('../models/ExcelData');
const OTA = require('../models/OTA');
const { encryptCardData, decryptCardData } = require('../utils/encryption');
const pLimit = require('p-limit');

const {
    PAYPAL_CLIENT_ID,
    PAYPAL_CLIENT_SECRET,
} = process.env;

// PayPal Partner Attribution ID (BN Code) - Replace with your actual BN code
const PAYPAL_BN_CODE = process.env.PAYPAL_BN_CODE || 'VNPSolutionsMOR_SP';

// PayPal API Configuration
console.log('=== PayPal Configuration ===');
console.log('Environment:', process.env.NODE_ENV === 'production' ? 'PRODUCTION' : 'SANDBOX');
console.log('BN Code:', PAYPAL_BN_CODE);
console.log('Client ID (last 10 chars):', PAYPAL_CLIENT_ID ? '...' + PAYPAL_CLIENT_ID.slice(-10) : 'NOT SET');
console.log('Client Secret configured:', !!PAYPAL_CLIENT_SECRET);
console.log('=============================');

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
        billingAddress
    } = paymentData;

    // Validation for required fields and format
    if (!cardNumber) {
        throw new Error('Card number is required');
    }
    if (!cardExpiry) {
        throw new Error('Card expiry is required');
    }
    if (!cardCvv) {
        throw new Error('Card CVV is required');
    }

    // Parse card expiry (MUST be in YYYY-MM format, e.g., "2025-12")
    if (!cardExpiry.match(/^\d{4}-\d{2}$/)) {
        throw new Error('Card expiry must be in YYYY-MM format');
    }
    const [year, month] = cardExpiry.split('-');
    
    // Parse cardholder name (assuming format: "First Last")
    const nameParts = cardholderName.trim().split(' ');
    const firstName = nameParts[0] || '';
    const lastName = nameParts.slice(1).join(' ') || '';

    // Determine card type based on number
    const getCardType = (number) => {
        const cleanNumber = number.replace(/\s/g, '');
        if (/^4/.test(cleanNumber)) return 'VISA';
        if (/^5[1-5]/.test(cleanNumber)) return 'MASTERCARD';
        if (/^3[47]/.test(cleanNumber)) return 'AMEX';
        if (/^6/.test(cleanNumber)) return 'DISCOVER';
        return 'VISA'; // default
    };

    const requestBody = {
        intent: "CAPTURE",
        purchase_units: [
            {
                amount: {
                    currency_code: currency,
                    value: amount.toString(),
                    breakdown: {
                        item_total: {
                            currency_code: currency,
                            value: amount.toString()
                        }
                    }
                },
                description: description,
                custom_id: documentId ? documentId.toString() : undefined,
                items: [
                    {
                        name: description || "Payment for services",
                        description: `Payment processing for ${cardholderName || 'customer'}`,
                        quantity: "1",
                        unit_amount: {
                            currency_code: currency,
                            value: amount.toString()
                        },
                        category: "DIGITAL_GOODS"
                    }
                ],
                ...(descriptor && {
                    soft_descriptor: descriptor
                })
            }
        ]
    };

    try {
        // Get access token using the same method as refund function
        const clientId = PAYPAL_CLIENT_ID.trim();
        const clientSecret = PAYPAL_CLIENT_SECRET.trim();
        const auth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
        
        // Use environment-based URL configuration
        const baseURL = process.env.NODE_ENV === 'production' 
            ? 'https://api-m.paypal.com'           // Production
            : 'https://api-m.sandbox.paypal.com'; // Sandbox
        
        console.log('Using PayPal base URL:', baseURL);
        
        // First get access token
        const tokenResponse = await fetch(`${baseURL}/v1/oauth2/token`, {
            method: 'POST',
            headers: {
                'Authorization': `Basic ${auth}`,
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: 'grant_type=client_credentials'
        });
        
        if (!tokenResponse.ok) {
            const tokenError = await tokenResponse.text();
            console.error('Token response status:', tokenResponse.status);
            console.error('Token response error:', tokenError);
            throw new Error(`Failed to get PayPal access token: ${tokenResponse.status} ${tokenResponse.statusText}`);
        }
        
        const tokenData = await tokenResponse.json();
        const accessToken = tokenData.access_token;
        
        if (!accessToken) {
            throw new Error('No access token received from PayPal');
        }
        
        console.log('Successfully obtained PayPal access token');
        
        // Step 1: Create the order using direct API
        const createOrderEndpoint = `${baseURL}/v2/checkout/orders`;
        const orderRequestId = `order-${documentId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        
        console.log('Create Order endpoint:', createOrderEndpoint);
        console.log('Create Order request body:', JSON.stringify(requestBody, null, 2));

        const orderResponse = await fetch(createOrderEndpoint, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json',
                'PayPal-Partner-Attribution-Id': PAYPAL_BN_CODE,
                'PayPal-Request-Id': orderRequestId,
                'Prefer': 'return=representation'
            },
            body: JSON.stringify(requestBody)
        });

        if (!orderResponse.ok) {
            const orderError = await orderResponse.json();
            console.error('PayPal Create Order API Error Status:', orderResponse.status);
            console.error('PayPal Create Order API Error:', orderError);
            throw new Error(`Failed to create PayPal order: ${orderResponse.status} ${orderResponse.statusText}`);
        }
        
        const orderData = await orderResponse.json();
        console.log('Order created successfully:', orderData.id);
        
        // Step 2: Capture the payment with card details using direct API
        const captureRequest = {
            payment_source: {
                card: {
                    number: cardNumber.replace(/\s/g, ''),
                    expiry: cardExpiry,
                    security_code: cardCvv,
                    name: cardholderName,
                                         billing_address: {
                         address_line_1: billingAddress?.address_line_1 || billingAddress?.addressLine1 || '',
                         address_line_2: billingAddress?.address_line_2 || billingAddress?.addressLine2 || '',
                         admin_area_2: billingAddress?.admin_area_2 || billingAddress?.adminArea2 || billingAddress?.city || '',
                         admin_area_1: billingAddress?.admin_area_1 || billingAddress?.adminArea1 || billingAddress?.state || '',
                         postal_code: billingAddress?.postal_code || billingAddress?.postalCode || billingAddress?.zipCode || '',
                         country_code: billingAddress?.country_code || billingAddress?.countryCode || 'US'
                     }
                }
            }
        };
        
        const captureOrderEndpoint = `${baseURL}/v2/checkout/orders/${orderData.id}/capture`;
        const captureRequestId = `capture-${orderData.id}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        
        console.log('Capture Order endpoint:', captureOrderEndpoint);
        console.log('Capture Order request body:', JSON.stringify(captureRequest, null, 2));
        
        // Debug: Specifically check postal_code
        console.log('Debug - Postal Code Check:');
        console.log('- Billing Address Object:', billingAddress);
        console.log('- Final postal_code being sent:', captureRequest.payment_source.card.billing_address.postal_code);

        const captureResponse = await fetch(captureOrderEndpoint, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json',
                'PayPal-Partner-Attribution-Id': PAYPAL_BN_CODE,
                'PayPal-Request-Id': captureRequestId,
                'Prefer': 'return=representation'
            },
            body: JSON.stringify(captureRequest)
        });

        if (!captureResponse.ok) {
            const captureError = await captureResponse.json();
            console.error('PayPal Capture Order API Error Status:', captureResponse.status);
            console.error('PayPal Capture Order API Error:', captureError);
            throw new Error(`Failed to capture PayPal order: ${captureResponse.status} ${captureResponse.statusText}`);
        }
        
        const captureData = await captureResponse.json();
        console.log('Payment captured successfully:', captureData.id);
        
        return {
            jsonResponse: captureData,
            httpStatusCode: captureResponse.status,
        };
    } catch (error) {
        console.error('PayPal Payment Error:', error.message);
        
        // Handle direct API errors (fetch errors)
        if (error.message && (
            error.message.includes('Failed to create PayPal order') || 
            error.message.includes('Failed to capture PayPal order') ||
            error.message.includes('Failed to get PayPal access token')
        )) {
            
            // Extract field and description from error details
            let errorMessage = 'Payment processing failed';
            
            if (error.result && error.result.details && Array.isArray(error.result.details)) {
                const errorDetails = error.result.details;
                
                // Map common PayPal errors to user-friendly messages
                const userFriendlyErrors = errorDetails.map(detail => {
                    const field = detail.field || '';
                    const description = detail.description || '';
                    const issue = detail.issue || '';
                    
                    // Handle specific common errors
                    if (issue === 'CARD_EXPIRED' || description.toLowerCase().includes('expired')) {
                        return 'Your card has expired. Please use a different card.';
                    }
                    
                    if (issue === 'INVALID_CARD_NUMBER' || field.includes('card') && description.toLowerCase().includes('invalid')) {
                        return 'The card number you entered is invalid. Please check and try again.';
                    }
                    
                    if (issue === 'CARD_TYPE_NOT_SUPPORTED' || description.toLowerCase().includes('not supported')) {
                        return 'This card type is not supported. Please try a different card.';
                    }
                    
                    if (issue === 'INSUFFICIENT_FUNDS' || description.toLowerCase().includes('insufficient')) {
                        return 'Your card has insufficient funds. Please use a different card or contact your bank.';
                    }
                    
                    if (issue === 'CARD_DECLINED' || description.toLowerCase().includes('declined')) {
                        return 'Your card was declined by the bank. Please contact your bank or try a different card.';
                    }
                    
                    if (field.includes('billing_address') || description.toLowerCase().includes('address')) {
                        return 'There is an issue with the billing address. Please verify your address information.';
                    }
                    
                    if (field.includes('security_code') || field.includes('cvv') || description.toLowerCase().includes('security')) {
                        return 'The security code (CVV) is incorrect. Please check the 3 or 4 digit code on your card.';
                    }
                    
                    if (field.includes('expiry') || description.toLowerCase().includes('expiry') || description.toLowerCase().includes('expiration')) {
                        return 'The card expiry date format is invalid. Please check your card\'s expiration date.';
                    }
                    
                    if (description.toLowerCase().includes('format') || description.toLowerCase().includes('syntax')) {
                        return 'Invalid card information format. Please check your card details and try again.';
                    }
                    
                    if (description.toLowerCase().includes('amount')) {
                        return 'There is an issue with the payment amount. Please verify and try again.';
                    }
                    
                    if (description.toLowerCase().includes('currency')) {
                        return 'The currency is not supported or invalid for this transaction.';
                    }
                    
                    if (description.toLowerCase().includes('duplicate')) {
                        return 'This appears to be a duplicate transaction. Please wait a moment before trying again.';
                    }
                    
                    if (description.toLowerCase().includes('limit')) {
                        return 'This transaction exceeds your card limit. Please contact your bank or try a smaller amount.';
                    }
                    
                    if (description.toLowerCase().includes('merchant')) {
                        return 'There is a temporary issue with payment processing. Please try again later.';
                    }
                    
                    // Generic fallback for unhandled errors
                    if (description) {
                        return `Payment error: ${description}`;
                    }
                    
                    return 'Payment processing encountered an error. Please try again.';
                });
                
                // Use the first user-friendly error, or combine multiple unique errors
                const uniqueErrors = [...new Set(userFriendlyErrors)];
                errorMessage = uniqueErrors.length === 1 ? uniqueErrors[0] : uniqueErrors.join(' ');
                
            } else if (error.message) {
                // Handle general PayPal errors
                const message = error.message.toLowerCase();
                
                if (message.includes('invalid') && message.includes('card')) {
                    errorMessage = 'Invalid card information. Please check your card details and try again.';
                } else if (message.includes('declined')) {
                    errorMessage = 'Your payment was declined. Please contact your bank or try a different card.';
                } else if (message.includes('expired')) {
                    errorMessage = 'Your card has expired. Please use a different card.';
                } else if (message.includes('insufficient')) {
                    errorMessage = 'Insufficient funds on your card. Please use a different card or contact your bank.';
                } else if (message.includes('network') || message.includes('connection')) {
                    errorMessage = 'Network connection error. Please check your internet connection and try again.';
                } else if (message.includes('timeout')) {
                    errorMessage = 'Payment processing timed out. Please try again in a few moments.';
                } else {
                    errorMessage = `Payment processing failed: ${error.message}`;
                }
            }
            
            throw new Error(errorMessage);
        }
        
        // Handle network and other errors
        if (error.message) {
            const message = error.message.toLowerCase();
            
            if (message.includes('network') || message.includes('fetch') || message.includes('connection')) {
                throw new Error('Unable to connect to payment processor. Please check your internet connection and try again.');
            }
            
            if (message.includes('timeout')) {
                throw new Error('Payment processing timed out. Please try again in a few moments.');
            }
            
            if (message.includes('certificate') || message.includes('ssl') || message.includes('tls')) {
                throw new Error('Secure connection error. Please try again or contact support if the problem persists.');
            }
        }
        
        console.error('General Payment Error:', error);
        throw new Error('An unexpected error occurred during payment processing. Please try again or contact support if the problem persists.');
    }
};

/**
 * Process direct payment with card information
 * POST /api/paypal/process-payment
 */
const processPayment = async (req, res) => {
    console.log('Processing payment');
    console.log(req.body);
    try {
        const {
            amount,
            currency = "USD",
            description,
            descriptor,
            cardNumber,
            cardExpiry,
            cardCvv,
            cardholderName,
            billingAddress,
            documentId
        } = req.body;

        // Validation for required fields with specific messages
        if (!amount) {
            return res.status(400).json({
                status: 'error',
                message: 'Payment amount is required. Please enter a valid amount to charge.'
            });
        }

        if (!cardNumber) {
            return res.status(400).json({
                status: 'error',
                message: 'Card number is required. Please enter a valid credit card number.'
            });
        }

        if (!cardExpiry) {
            return res.status(400).json({
                status: 'error',
                message: 'Card expiry date is required. Please enter the expiry date in MM/YY or YYYY-MM format.'
            });
        }

        if (!cardCvv) {
            return res.status(400).json({
                status: 'error',
                message: 'Card security code (CVV) is required. Please enter the 3 or 4 digit code from your card.'
            });
        }

        if (!cardholderName) {
            return res.status(400).json({
                status: 'error',
                message: 'Cardholder name is required. Please enter the name as it appears on your card.'
            });
        }

        // Validate amount is a valid number
        const numericAmount = parseFloat(amount);
        if (isNaN(numericAmount)) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid payment amount format. Please enter a valid number (e.g., 25.50).'
            });
        }

        if (numericAmount <= 0) {
            return res.status(400).json({
                status: 'error',
                message: 'Payment amount must be greater than zero. Please enter a positive amount.'
            });
        }

        if (numericAmount > 10000) {
            return res.status(400).json({
                status: 'error',
                message: 'Payment amount exceeds maximum limit of $10,000. Please contact support for larger transactions.'
            });
        }

        // Validate card number (enhanced validation)
        const cleanCardNumber = cardNumber.replace(/\s/g, '');
        if (!/^\d+$/.test(cleanCardNumber)) {
            return res.status(400).json({
                status: 'error',
                message: 'Card number can only contain digits. Please remove any spaces or special characters.'
            });
        }

        if (cleanCardNumber.length < 13) {
            return res.status(400).json({
                status: 'error',
                message: 'Card number is too short. Please enter a complete card number (13-19 digits).'
            });
        }

        if (cleanCardNumber.length > 19) {
            return res.status(400).json({
                status: 'error',
                message: 'Card number is too long. Please check and enter a valid card number (13-19 digits).'
            });
        }

        // Basic Luhn algorithm check for card number
        const luhnCheck = (num) => {
            let sum = 0;
            let isEven = false;
            for (let i = num.length - 1; i >= 0; i--) {
                let digit = parseInt(num.charAt(i), 10);
                if (isEven) {
                    digit *= 2;
                    if (digit > 9) {
                        digit -= 9;
                    }
                }
                sum += digit;
                isEven = !isEven;
            }
            return sum % 10 === 0;
        };

        if (!luhnCheck(cleanCardNumber)) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid card number. Please check your card number and try again.'
            });
        }

        // Validate card expiry format and date
        const expiryRegex = /^(\d{4})-(\d{2})$|^(\d{2})\/(\d{2})$/;
        if (!expiryRegex.test(cardExpiry)) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid expiry date format. Please use MM/YY or YYYY-MM format (e.g., 12/25 or 2025-12).'
            });
        }

        // Parse and validate expiry date
        let month, year;
        if (cardExpiry.includes('-')) {
            [year, month] = cardExpiry.split('-');
        } else {
            [month, year] = cardExpiry.split('/');
            year = year.length === 2 ? `20${year}` : year;
        }

        const expiryMonth = parseInt(month, 10);
        const expiryYear = parseInt(year, 10);
        const currentDate = new Date();
        const currentYear = currentDate.getFullYear();
        const currentMonth = currentDate.getMonth() + 1;

        if (expiryMonth < 1 || expiryMonth > 12) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid expiry month. Please enter a month between 01 and 12.'
            });
        }

        if (expiryYear < currentYear || (expiryYear === currentYear && expiryMonth < currentMonth)) {
            return res.status(400).json({
                status: 'error',
                message: 'Card has expired. Please use a different card or check the expiry date.'
            });
        }

        if (expiryYear > currentYear + 20) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid expiry year. Please check the expiry date on your card.'
            });
        }

        // Validate CVV
        if (!/^\d+$/.test(cardCvv)) {
            return res.status(400).json({
                status: 'error',
                message: 'CVV can only contain digits. Please enter the security code from your card.'
            });
        }

        if (cardCvv.length < 3 || cardCvv.length > 4) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid CVV length. Please enter the 3-digit code (or 4-digit for American Express) from your card.'
            });
        }

        // Validate cardholder name
        if (cardholderName.trim().length < 2) {
            return res.status(400).json({
                status: 'error',
                message: 'Cardholder name is too short. Please enter the full name as it appears on your card.'
            });
        }

        if (cardholderName.trim().length > 50) {
            return res.status(400).json({
                status: 'error',
                message: 'Cardholder name is too long. Please enter a name up to 50 characters.'
            });
        }

        if (!/^[a-zA-Z\s\-\.\']+$/.test(cardholderName.trim())) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid characters in cardholder name. Please use only letters, spaces, hyphens, and apostrophes.'
            });
        }

        // Normalize expiry date to YYYY-MM format for PayPal
        const normalizedExpiry = `${year}-${month.toString().padStart(2, '0')}`;

        // Construct billing address object from request body
        // Handle both nested billingAddress object and flat structure
        const constructedBillingAddress = {
            address_line_1: billingAddress?.address_line_1 || billingAddress?.addressLine1 || req.body.addressLine1 || '',
            address_line_2: billingAddress?.address_line_2 || billingAddress?.addressLine2 || req.body.addressLine2 || '',
            admin_area_2: billingAddress?.admin_area_2 || billingAddress?.city || req.body.city || '',
            admin_area_1: billingAddress?.admin_area_1 || billingAddress?.state || req.body.state || '',
            postal_code: billingAddress?.postal_code || billingAddress?.zipCode || req.body.zipCode || req.body.postalCode || '',
            country_code: billingAddress?.country_code || billingAddress?.countryCode || req.body.countryCode || 'US'
        };

        // Debug log to verify billing address construction
        console.log('Billing address construction:');
        console.log('- Input zipCode:', req.body.zipCode);
        console.log('- Input billingAddress:', billingAddress);
        console.log('- Constructed:', constructedBillingAddress);

        const paymentData = {
            amount: numericAmount,
            currency: currency || "USD",
            description: description || "Payment for services",
            descriptor: descriptor,
            documentId: documentId,
            cardNumber: cleanCardNumber,
            cardExpiry: normalizedExpiry, // Use normalized YYYY-MM format
            cardCvv: cardCvv,
            cardholderName: cardholderName,
            billingAddress: constructedBillingAddress
        };



        const { jsonResponse, httpStatusCode } = await processDirectPayment(paymentData);
        
        // Extract important payment details from PayPal response
        const paymentDetails = {
            orderId: jsonResponse.id,
            captureId: jsonResponse.purchase_units[0]?.payments?.captures[0]?.id,
            networkTransactionId: jsonResponse.purchase_units[0]?.payments?.captures[0]?.network_transaction_reference?.id,
            status: jsonResponse.status,
            amount: jsonResponse.purchase_units[0]?.amount?.value,
            currency: jsonResponse.purchase_units[0]?.amount?.currency_code,
            paypalFee: jsonResponse.purchase_units[0]?.payments?.captures[0]?.seller_receivable_breakdown?.paypal_fee?.value,
            netAmount: jsonResponse.purchase_units[0]?.payments?.captures[0]?.seller_receivable_breakdown?.net_amount?.value,
            cardLastDigits: jsonResponse.payment_source?.card?.last_digits,
            cardBrand: jsonResponse.payment_source?.card?.brand,
            cardType: jsonResponse.payment_source?.card?.type,
            avsCode: jsonResponse.purchase_units[0]?.payments?.captures[0]?.processor_response?.avs_code,
            cvvCode: jsonResponse.purchase_units[0]?.payments?.captures[0]?.processor_response?.cvv_code,
            createTime: jsonResponse.create_time,
            updateTime: jsonResponse.update_time,
            captureStatus: jsonResponse.purchase_units[0]?.payments?.captures[0]?.status,
            customId: jsonResponse.purchase_units[0]?.payments?.captures[0]?.custom_id
        };

        // Check if payment was declined
        const captureStatus = jsonResponse.purchase_units[0]?.payments?.captures[0]?.status;
        
        if (captureStatus === 'DECLINED') {
            // Get decline reason from processor response
            const processorResponse = jsonResponse.purchase_units[0]?.payments?.captures[0]?.processor_response;
            const responseCode = processorResponse?.response_code;
            const avsCode = processorResponse?.avs_code;
            const cvvCode = processorResponse?.cvv_code;
            
            // Get clean, specific decline message based on primary issue
            const getDeclineReason = (code, avsCode, cvvCode, detailedStatusType) => {
                // PRIORITY 1: Check response code first (most specific)
                const declineCodes = {
                    // Card-related errors
                    '0500': 'Your card was declined by your bank.',
                    '0501': 'Your card was declined. Please try again or use a different card.',
                    '0502': 'Your card was declined due to suspected fraud.',
                    '0503': 'Your card was declined by the issuer.',
                    '0504': 'Your card was declined and cannot be processed at this time.',
                    '0505': 'Your card was declined due to bank risk policies.',
                    
                    // Invalid card information
                    '0510': 'The card number you entered is invalid.',
                    '0511': 'The card number format is incorrect.',
                    '0512': 'The card number you entered does not exist.',
                    '0513': 'This card type is not accepted.',
                    '0514': 'The card number checksum is invalid.',
                    '0515': 'The card number length is incorrect.',
                    
                    // Expiration date errors
                    '0520': 'The expiration date you entered is invalid.',
                    '0521': 'The expiration month is invalid.',
                    '0522': 'The expiration year is invalid.',
                    '0523': 'The expiration date format is incorrect.',
                    '0524': 'Your card has expired.',
                    
                    // CVV/Security code errors
                    '0530': 'The security code (CVV) you entered is incorrect.',
                    '0531': 'The CVV code is missing.',
                    '0532': 'The CVV code format is invalid.',
                    '0533': 'CVV verification failed.',
                    '0534': 'CVV code mismatch.',
                    
                    // Insufficient funds
                    '0540': 'Your card has insufficient funds for this transaction.',
                    '0541': 'Insufficient available credit.',
                    '0542': 'Your account balance is too low.',
                    '0543': 'Credit limit exceeded.',
                    '0544': 'Available funds exceeded.',
                    
                    // Expired card
                    '0550': 'Your card has expired.',
                    '0551': 'Your card has expired.',
                    '0552': 'Your card has expired.',
                    
                    // Restricted/Blocked card
                    '0560': 'Your card is restricted or blocked.',
                    '0561': 'Your card has been temporarily blocked.',
                    '0562': 'This card is permanently blocked.',
                    '0563': 'Your card is restricted for online transactions.',
                    '0564': 'Your card is restricted for international transactions.',
                    '0565': 'Your card is blocked due to suspicious activity.',
                    
                    // Transaction not permitted
                    '0570': 'This type of transaction is not permitted on your card.',
                    '0571': 'Online transactions are not enabled on your card.',
                    '0572': 'International transactions are not allowed on your card.',
                    '0573': 'Recurring payments are not permitted on this card.',
                    '0574': 'This merchant category is blocked on your card.',
                    '0575': 'Your card does not support this type of payment.',
                    
                    // Processing issues
                    '0580': 'There is a temporary issue with payment processing.',
                    '0581': 'Payment gateway timeout occurred.',
                    '0582': 'Connection error with card issuer.',
                    '0583': 'System temporarily unavailable.',
                    '0584': 'Processing error occurred.',
                    '0585': 'Network error occurred.',
                    
                    // Amount/Limit errors
                    '0590': 'The transaction amount exceeds your card limit.',
                    '0591': 'Transaction amount is too large.',
                    '0592': 'Transaction amount is too small.',
                    '0593': 'Daily spending limit exceeded.',
                    '0594': 'Monthly spending limit exceeded.',
                    '0595': 'Single transaction limit exceeded.',
                    
                    // Verification required
                    '0600': 'Your card issuer requires additional verification.',
                    '0601': '3D Secure authentication required.',
                    '0602': 'Additional authentication needed.',
                    '0603': 'Your bank requires phone verification.',
                    '0604': 'PIN verification required.',
                    '0605': 'Strong customer authentication (SCA) required.',
                    
                    // Security/Fraud
                    '0700': 'The transaction was flagged for security reasons.',
                    '0701': 'Suspected fraudulent transaction.',
                    '0702': 'Transaction blocked for security reasons.',
                    '0703': 'Unusual activity detected.',
                    '0704': 'Risk assessment failed.',
                    '0705': 'Too many attempts detected.',
                    
                    // Account issues
                    '0800': 'Your card account is closed or inactive.',
                    '0801': 'Card account suspended.',
                    '0802': 'Account frozen.',
                    '0803': 'Account under review.',
                    '0804': 'Card reported lost or stolen.',
                    '0805': 'Account temporarily disabled.',
                    
                    // Fraud prevention
                    '0900': 'The transaction was declined due to fraud prevention.',
                    '0901': 'Fraud alert triggered.',
                    '0902': 'Unusual transaction pattern detected.',
                    '0903': 'Merchant blocked due to fraud prevention.',
                    '0904': 'Location-based security block.',
                    '0905': 'Time-based security block.',
                    
                    // Transaction limits
                    '1000': 'Daily transaction limit exceeded.',
                    '1001': 'Weekly transaction limit exceeded.',
                    '1002': 'Monthly transaction limit exceeded.',
                    '1003': 'Annual transaction limit exceeded.',
                    '1004': 'Too many transactions for today.',
                    '1005': 'Too many attempts, please wait.',
                    
                    // Address/Billing errors
                    '1100': 'Billing address verification failed.',
                    '1101': 'ZIP code verification failed.',
                    '1102': 'Address mismatch detected.',
                    '1103': 'Country code mismatch.',
                    '1104': 'Address format invalid.',
                    
                    // Currency/Region errors
                    '1200': 'Currency not supported.',
                    '1201': 'Transaction not allowed in this region.',
                    '1202': 'Card not valid for this country.',
                    '1203': 'Currency conversion failed.',
                    '1204': 'Exchange rate unavailable.',
                    
                    // Merchant/Business errors
                    '1300': 'Merchant account issue.',
                    '1301': 'Merchant not authorized for this card type.',
                    '1302': 'Merchant category not allowed.',
                    '1303': 'Merchant account suspended.',
                    '1304': 'Merchant daily limit exceeded.',
                    
                    // System/Technical errors
                    '1400': 'Technical error occurred.',
                    '1401': 'Database connection error.',
                    '1402': 'Service temporarily unavailable.',
                    '1403': 'System maintenance in progress.',
                    '1404': 'Configuration error.',
                    
                    // Regulatory/Compliance
                    '1500': 'Transaction violates regulatory requirements.',
                    '1501': 'KYC verification required.',
                    '1502': 'AML check failed.',
                    '1503': 'Transaction requires manual review.',
                    '1504': 'Compliance hold on transaction.',
                    
                    // Refund/Chargeback related
                    '1600': 'Refund processing error.',
                    '1601': 'Chargeback protection triggered.',
                    '1602': 'Previous chargeback history detected.',
                    '1603': 'Refund limit exceeded.',
                    
                    // Generic/Fallback errors
                    '9000': 'Payment processor error.',
                    '9001': 'Unknown error occurred.',
                    '9500': 'Payment processing failed due to technical issue.',
                    '9999': 'General payment failure.'
                };

                // PRIORITY 1: If we have a specific message for the response code, use it
                if (code && declineCodes[code]) {
                    return declineCodes[code];
                }

                // PRIORITY 2: Fallback to CVV-specific messages (only if no response code match)
                if (detailedStatusType === 'cvv_error' && cvvCode && cvvCode !== 'M') {
                    switch (cvvCode) {
                        case 'N':
                            return 'The security code (CVV) you entered does not match our records.';
                        case 'P':
                            return 'The security code (CVV) could not be processed by your bank.';
                        case 'S':
                            return 'Your card should have a security code (CVV), but none was provided.';
                        case 'U':
                            return 'Your bank does not support CVV verification for this card.';
                        default:
                            return 'The security code (CVV) verification failed.';
                    }
                }
                
                // PRIORITY 3: Fallback to AVS-specific messages
                if (detailedStatusType === 'avs_error' && avsCode && !['Y', 'X'].includes(avsCode)) {
                    return 'Your billing address does not match the address on file with your bank.';
                }

                // PRIORITY 4: Generic fallback
                return 'Your payment was declined by your card issuer.';
            };

            // Get detailed status type for internal tracking
            const getDetailedStatusType = (code, avsCode, cvvCode) => {
                // PRIORITY 1: Check response code first (most specific)
                const statusMapping = {
                    // Card invalid
                    '0510': 'card_invalid', '0511': 'card_invalid', '0512': 'card_invalid',
                    '0513': 'card_invalid', '0514': 'card_invalid', '0515': 'card_invalid',
                    
                    // Card expired
                    '0520': 'card_expired', '0521': 'card_expired', '0522': 'card_expired',
                    '0523': 'card_expired', '0524': 'card_expired', '0550': 'card_expired',
                    '0551': 'card_expired', '0552': 'card_expired',
                    
                    // CVV errors
                    '0530': 'cvv_error', '0531': 'cvv_error', '0532': 'cvv_error',
                    '0533': 'cvv_error', '0534': 'cvv_error',
                    
                    // Insufficient funds
                    '0540': 'funds_insufficient', '0541': 'funds_insufficient', '0542': 'funds_insufficient',
                    '0543': 'funds_insufficient', '0544': 'funds_insufficient', '0590': 'funds_insufficient',
                    '0591': 'funds_insufficient', '0593': 'funds_insufficient', '0594': 'funds_insufficient',
                    '0595': 'funds_insufficient', '1000': 'funds_insufficient', '1001': 'funds_insufficient',
                    '1002': 'funds_insufficient', '1003': 'funds_insufficient', '1004': 'funds_insufficient',
                    '1005': 'funds_insufficient',
                    
                    // Verification required
                    '0600': 'verification_required', '0601': 'verification_required', '0602': 'verification_required',
                    '0603': 'verification_required', '0604': 'verification_required', '0605': 'verification_required',
                    
                    // Fraud detected
                    '0502': 'fraud_detected', '0505': 'fraud_detected', '0565': 'fraud_detected',
                    '0700': 'fraud_detected', '0701': 'fraud_detected', '0702': 'fraud_detected',
                    '0703': 'fraud_detected', '0704': 'fraud_detected', '0705': 'fraud_detected',
                    '0900': 'fraud_detected', '0901': 'fraud_detected', '0902': 'fraud_detected',
                    '0903': 'fraud_detected', '0904': 'fraud_detected', '0905': 'fraud_detected',
                    
                    // Account issues
                    '0560': 'account_issue', '0561': 'account_issue', '0562': 'account_issue',
                    '0563': 'account_issue', '0564': 'account_issue', '0570': 'account_issue',
                    '0571': 'account_issue', '0572': 'account_issue', '0573': 'account_issue',
                    '0574': 'account_issue', '0575': 'account_issue', '0800': 'account_issue',
                    '0801': 'account_issue', '0802': 'account_issue', '0803': 'account_issue',
                    '0804': 'account_issue', '0805': 'account_issue',
                    
                    // Region blocked
                    '1200': 'region_blocked', '1201': 'region_blocked', '1202': 'region_blocked',
                    '1203': 'region_blocked', '1204': 'region_blocked',
                    
                    // Processing errors
                    '0580': 'processing_error', '0581': 'processing_error', '0582': 'processing_error',
                    '0583': 'processing_error', '0584': 'processing_error', '0585': 'processing_error',
                    '1300': 'processing_error', '1301': 'processing_error', '1302': 'processing_error',
                    '1303': 'processing_error', '1304': 'processing_error', '1400': 'processing_error',
                    '1401': 'processing_error', '1402': 'processing_error', '1403': 'processing_error',
                    '1404': 'processing_error', '9000': 'processing_error', '9500': 'processing_error',
                    
                    // Compliance blocked
                    '1500': 'compliance_blocked', '1501': 'compliance_blocked', '1502': 'compliance_blocked',
                    '1503': 'compliance_blocked', '1504': 'compliance_blocked',
                    
                    // Chargeback blocked
                    '1600': 'chargeback_blocked', '1601': 'chargeback_blocked', '1602': 'chargeback_blocked',
                    '1603': 'chargeback_blocked',
                    
                    // Address/billing errors (map to avs_error)
                    '1100': 'avs_error', '1101': 'avs_error', '1102': 'avs_error',
                    '1103': 'avs_error', '1104': 'avs_error',
                    
                    // Amount errors (map to processing_error for invalid amounts)
                    '0592': 'processing_error',
                    
                    // Unknown error
                    '9001': 'unknown_error', '9999': 'unknown_error'
                };

                // PRIORITY 1: If we have a specific mapping for the response code, use it
                if (code && statusMapping[code]) {
                    return statusMapping[code];
                }

                // PRIORITY 2: Fallback to CVV-specific status (only if no response code match)
                if (cvvCode && cvvCode !== 'M') {
                    return 'cvv_error';
                }
                
                // PRIORITY 3: Fallback to AVS-specific status
                if (avsCode && !['Y', 'X'].includes(avsCode)) {
                    return 'avs_error';
                }

                // PRIORITY 4: Generic fallback
                return 'declined';
            };

            // Map detailed status to frontend status (3 statuses only)
            const getFrontendStatus = (detailedStatus) => {
                const processingErrors = [
                    'processing_error', 'unknown_error'
                ];
                
                if (processingErrors.includes(detailedStatus)) {
                    return 'error';
                }
                
                // All other statuses are payment declines
                return 'declined';
            };
            
            const detailedStatusType = getDetailedStatusType(responseCode, avsCode, cvvCode);
            const frontendStatus = getFrontendStatus(detailedStatusType);
            const declineReason = getDeclineReason(responseCode, avsCode, cvvCode, detailedStatusType);
            
            // Additional helpful information
            let helpfulTips = '';
            
            // Provide clean, actionable tips based on the detailed status type
            switch (detailedStatusType) {
                case 'funds_insufficient':
                    helpfulTips = 'Try: 1) Check your account balance, 2) Contact your bank to increase limits, or 3) Use a different card.';
                    break;
                case 'account_issue':
                    helpfulTips = 'Contact your bank to remove any restrictions on online or international transactions.';
                    break;
                case 'verification_required':
                    helpfulTips = 'Complete your bank\'s authentication process (3D Secure/SMS/call verification) and retry.';
                    break;
                case 'fraud_detected':
                    helpfulTips = 'Call your bank immediately to confirm this transaction is legitimate, then try again.';
                    break;
                case 'card_invalid':
                    helpfulTips = 'Verify your card number, expiry date, and name are entered correctly.';
                    break;
                case 'card_expired':
                    helpfulTips = 'Update your payment method with a current, valid card.';
                    break;
                case 'cvv_error':
                    // Enhanced CVV-specific tips
                    switch (cvvCode) {
                        case 'N':
                            helpfulTips = 'Double-check the 3-digit code on the back of your card (or 4-digit for Amex).';
                            break;
                        case 'P':
                            helpfulTips = 'Your bank couldn\'t process the security code. Try again or contact your bank.';
                            break;
                        case 'S':
                            helpfulTips = 'Please enter the security code (CVV) found on your card.';
                            break;
                        case 'U':
                            helpfulTips = 'Your bank doesn\'t support CVV verification. Try a different card or contact your bank.';
                            break;
                        default:
                            helpfulTips = 'Verify the security code (CVV) on your card is entered correctly.';
                    }
                    break;
                case 'avs_error':
                    helpfulTips = 'Ensure your billing address exactly matches your bank records (including abbreviations and spacing).';
                    break;
                case 'region_blocked':
                    helpfulTips = 'Use a domestic card or contact support if you believe this is an error.';
                    break;
                case 'processing_error':
                    if (responseCode === '9500') {
                        helpfulTips = 'Technical issue occurred during payment processing. Please try again or contact support.';
                    } else {
                        helpfulTips = 'Wait 2-3 minutes and try again. If it persists, contact our support team.';
                    }
                    break;
                case 'compliance_blocked':
                    helpfulTips = 'Additional identity verification is required. Please contact our support team for assistance.';
                    break;
                case 'chargeback_blocked':
                    helpfulTips = 'Previous payment disputes detected. Contact our support team to resolve this issue.';
                    break;
                case 'unknown_error':
                    helpfulTips = 'An unexpected error occurred. Please contact support with transaction details.';
                    break;
                default:
                    helpfulTips = 'Try again in a few minutes or contact your bank to ensure your card works for online purchases.';
            }
            
            console.log(`Payment ${frontendStatus} (${detailedStatusType}) for documentId ${documentId}: ${declineReason}`);
            
            // Create a user-friendly decline message
            const userMessage = helpfulTips ? `${declineReason} ${helpfulTips}` : declineReason;
            
            // Update database with failed/declined status
            let databaseUpdated = false;
            try {
                // Determine charge status based on frontend status
                let chargeStatus;
                let statusField;
                
                if (frontendStatus === 'declined') {
                    chargeStatus = 'Declined';
                    statusField = 'Payment Declined';
                } else if (frontendStatus === 'error') {
                    chargeStatus = 'Failed';
                    statusField = 'Payment Failed';
                } else {
                    chargeStatus = 'Failed';
                    statusField = 'Payment Failed';
                }

                // Update the ExcelData record with failure/decline information
                const updatedRecord = await ExcelData.findByIdAndUpdate(
                    documentId,
                    {
                        'Charge status': chargeStatus,
                        'Status': statusField,
                        // Add failure details
                        paypalOrderId: paymentDetails.orderId,
                        paypalCaptureId: paymentDetails.captureId,
                        paypalResponseCode: responseCode,
                        paypalDeclineReason: declineReason,
                        paypalStatusType: detailedStatusType,
                        paypalFrontendStatus: frontendStatus,
                        paypalAvsCode: avsCode,
                        paypalCvvCode: cvvCode,
                        paypalAmount: paymentDetails.amount,
                        paypalCurrency: paymentDetails.currency,
                        paypalCardBrand: paymentDetails.cardBrand,
                        paypalCardType: paymentDetails.cardType,
                        paypalCardLastDigits: paymentDetails.cardLastDigits,
                        paypalCaptureStatus: paymentDetails.captureStatus,
                        paypalCreateTime: paymentDetails.createTime,
                        paypalUpdateTime: paymentDetails.updateTime,
                        paypalCustomId: paymentDetails.customId,
                        lastFailureDate: new Date().toISOString()
                    },
                    { new: true }
                );

                if (updatedRecord) {
                    databaseUpdated = true;
                    console.log(`Database updated for documentId ${documentId} with status: ${chargeStatus}`);
                } else {
                    console.error(`ExcelData record not found for documentId: ${documentId}`);
                }
            } catch (dbError) {
                console.error('Database update error for failed payment:', dbError);
                // Don't fail the response if DB update fails, just log it
            }
            
            // Return decline response with database update status
            return res.status(402).json({
                status: frontendStatus,
                message: userMessage,
                error_type: 'payment_declined',
                data: {
                    paymentDetails: paymentDetails,
                    declineDetails: {
                        reason: declineReason,
                        statusType: detailedStatusType,
                        frontendStatus: frontendStatus,
                        responseCode: responseCode,
                        avsCode: avsCode,
                        cvvCode: cvvCode,
                        helpfulTips: helpfulTips
                    },
                    databaseUpdated: databaseUpdated,
                    // Include minimal response data for debugging (admin only)
                    debugInfo: process.env.NODE_ENV === 'development' ? {
                        paypalOrderId: jsonResponse.id,
                        paypalOrderStatus: jsonResponse.status,
                        paypalCaptureStatus: jsonResponse.purchase_units[0]?.payments?.captures[0]?.status,
                        paypalResponseCode: responseCode,
                        actualDeclineReason: jsonResponse.purchase_units[0]?.payments?.captures[0]?.status_details?.reason || 'Unknown'
                    } : null
                }
            });
        }

        // Only proceed with database update if payment was successful (COMPLETED)
        if (captureStatus !== 'COMPLETED') {
            console.log(`Payment status is ${captureStatus}, not updating database`);
            return res.status(400).json({
                status: 'error',
                message: `Payment status is ${captureStatus}. Expected COMPLETED.`,
                data: {
                    ...jsonResponse,
                    paymentDetails: paymentDetails,
                    databaseUpdated: false
                }
            });
        }

        // Update the ExcelData record with payment information (only for successful payments)
        try {
            // Prepare card data for encryption
            const cardDataToEncrypt = {
                'Card Number': cleanCardNumber,
                'Card Expire': normalizedExpiry, // Use normalized YYYY-MM format
                'Card CVV': cardCvv
            };
            
            // Encrypt sensitive card data
            const encryptedCardData = encryptCardData(cardDataToEncrypt);
            
            // First, get the existing record to check for OTA information
            let otaId = null;
            let otaName = null;
            if (documentId) {
                try {
                    const existingRecord = await ExcelData.findById(documentId);
                    if (existingRecord && existingRecord.OTA) {
                        otaName = existingRecord.OTA;
                        console.log(`Found OTA in record: ${otaName}`);
                        const otaRecord = await OTA.findOne({ name: otaName, isActive: true });
                        if (otaRecord) {
                            otaId = otaRecord._id;
                            console.log(`Found OTA record ID: ${otaId}`);
                        } else {
                            console.log(`No OTA record found for: ${otaName}`);
                        }
                    }
                } catch (otaError) {
                    console.error('Error looking up OTA:', otaError);
                }
            }
            
            const updatedRecord = await ExcelData.findByIdAndUpdate(
                documentId,
                {
                    'Charge status': 'Charged',
                    'Card Number': encryptedCardData['Card Number'],
                    'Card Expire': encryptedCardData['Card Expire'],
                    'Card CVV': encryptedCardData['Card CVV'],
                    'Soft Descriptor': descriptor,
                    'Status': 'Payment Processed',
                    // Add payment details as additional fields
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
                    ota: otaName, // Preserve original OTA name from record
                    otaId: otaId
                },
                { new: true }
            );

            if (!updatedRecord) {
                console.error('ExcelData record not found for documentId:', documentId);
                return res.status(404).json({
                    status: 'error',
                    message: 'ExcelData record not found'
                });
            }


            
        } catch (dbError) {
            console.error('Database update error:', dbError);
            // Don't fail the payment response if DB update fails
            // Payment was successful, just log the DB error
        }
        
        res.status(httpStatusCode).json({
            status: 'success',
            message: 'Payment processed successfully',
            data: {
                ...jsonResponse,
                paymentDetails: paymentDetails,
                databaseUpdated: true
            }
        });

    } catch (error) {
        console.error("Failed to process payment:", error);
        
        // Determine appropriate error message and status code
        let statusCode = 500;
        let errorMessage = error.message;
        let errorType = 'processing_error';
        
        // Categorize errors for better user experience
        if (error.message) {
            const message = error.message.toLowerCase();
            
            if (message.includes('card') || message.includes('cvv') || message.includes('expired')) {
                statusCode = 400;
                errorType = 'card_error';
            } else if (message.includes('declined') || message.includes('insufficient')) {
                statusCode = 402;
                errorType = 'payment_declined';
            } else if (message.includes('network') || message.includes('connection') || message.includes('timeout')) {
                statusCode = 503;
                errorType = 'network_error';
                errorMessage = error.message + ' Please try again in a few moments.';
            } else if (message.includes('limit') || message.includes('amount')) {
                statusCode = 400;
                errorType = 'amount_error';
            }
        }
        
        // If we don't have a user-friendly message, provide a generic one
        if (!errorMessage || errorMessage === 'Failed to process payment') {
            errorMessage = 'We encountered an issue processing your payment. Please try again or contact support if the problem persists.';
        }
        
        res.status(statusCode).json({
            status: 'error',
            message: errorMessage,
            error_type: errorType,
            data: {
                timestamp: new Date().toISOString(),
                // Include debug info only in development
                ...(process.env.NODE_ENV === 'development' && { 
                    debugMessage: error.message,
                    stack: error.stack 
                })
            }
        });
    }
};

// Bulk PayPal payment controller (parallel, with address mapping and summary)
const processBulkPayments = async (req, res) => {
    console.log('Processing bulk payments');
    console.log(req.body);
    try {
        const { documentIds } = req.body;
        if (!Array.isArray(documentIds) || documentIds.length === 0) {
            return res.status(400).json({ status: 'error', message: 'documentIds array is required' });
        }
        // Fetch all rows in one query
        const rows = await ExcelData.find({ _id: { $in: documentIds } })
            .populate('otaId', 'name displayName customer billingAddress isActive') // Populate OTA data
            .lean();
        console.log("rows", rows);
        const limit = pLimit(5); // Limit concurrency to 5
        const results = await Promise.all(rows.map(row => limit(async () => {
            try {
                // Create encrypted card data object from individual fields
                const rowEncryptedCardData = {
                    'Card Number': row['Card Number'],
                    'Card Expire': row['Card Expire'], 
                    'Card CVV': row['Card CVV']
                };
                
                // Decrypt card data
                const decrypted = decryptCardData(rowEncryptedCardData);
                console.log(decrypted);
                // Debug log for decrypted card data
                if (!decrypted['Card Number'] || !decrypted['Card Expire'] || !decrypted['Card CVV']) {
                    console.error('Decryption failed for row:', row._id, {
                        cardNumber: decrypted['Card Number'],
                        cardExpiry: decrypted['Card Expire'],
                        cardCvv: decrypted['Card CVV']
                    });
                    throw new Error('Decryption failed for one or more card fields');
                }
                // Map billing address fields - use OTA billing address if available, otherwise use row data
                let billingAddress;
                
                if (row.otaId && row.otaId.billingAddress) {
                    // Use OTA billing address
                    console.log(`Using OTA billing address for ${row.otaId.name}`);
                    billingAddress = {
                        address_line_1: row.otaId.billingAddress.addressLine1 || '',
                        address_line_2: row.otaId.billingAddress.addressLine2 || '',
                        admin_area_2: row.otaId.billingAddress.city || '',
                        admin_area_1: row.otaId.billingAddress.state || '',
                        postal_code: row.otaId.billingAddress.zipCode || '',
                        country_code: row.otaId.billingAddress.countryCode || 'US'
                    };
                } else {
                    // Fallback to row data billing address
                    console.log('No OTA billing address found, using row data');
                    billingAddress = {
                        address_line_1: row['Billing Address Line 1'] || '',
                        address_line_2: row['Billing Address Line 2'] || '',
                        admin_area_2: row['City'] || '',
                        admin_area_1: row['State'] || '',
                        postal_code: row['Postal Code'] || '',
                        country_code: row['Country Code'] || 'US'
                    };
                }

                // Use guest's actual name as cardholder name (not OTA displayName)
                // This is required by PayPal and banks for proper card validation
                let cardholderName = row['Name'];
                console.log(`Using guest Name as cardholder: ${cardholderName}`);

                // Normalize expiry date to YYYY-MM format for PayPal
                let normalizedExpiry = decrypted['Card Expire'];
                
                // Check if expiry needs normalization
                if (normalizedExpiry && !normalizedExpiry.match(/^\d{4}-\d{2}$/)) {
                    console.log(`Normalizing expiry date from: ${normalizedExpiry}`);
                    
                    // Parse different formats and convert to YYYY-MM
                    let month, year;
                    
                    // Handle MM/YYYY format (like "2/2028")
                    if (normalizedExpiry.match(/^(\d{1,2})\/(\d{4})$/)) {
                        const match = normalizedExpiry.match(/^(\d{1,2})\/(\d{4})$/);
                        month = match[1].padStart(2, '0');
                        year = match[2];
                        normalizedExpiry = `${year}-${month}`;
                    }
                    // Handle MM-YYYY format (like "2-2028") 
                    else if (normalizedExpiry.match(/^(\d{1,2})-(\d{4})$/)) {
                        const match = normalizedExpiry.match(/^(\d{1,2})-(\d{4})$/);
                        month = match[1].padStart(2, '0');
                        year = match[2];
                        normalizedExpiry = `${year}-${month}`;
                    }
                    // Handle MM/YY format (like "2/28")
                    else if (normalizedExpiry.match(/^(\d{1,2})\/(\d{2})$/)) {
                        const match = normalizedExpiry.match(/^(\d{1,2})\/(\d{2})$/);
                        month = match[1].padStart(2, '0');
                        year = `20${match[2]}`;
                        normalizedExpiry = `${year}-${month}`;
                    }
                    // Handle other formats - try to parse as date
                    else {
                        try {
                            const date = new Date(normalizedExpiry);
                            if (!isNaN(date.getTime())) {
                                year = date.getFullYear();
                                month = (date.getMonth() + 1).toString().padStart(2, '0');
                                normalizedExpiry = `${year}-${month}`;
                            }
                        } catch (e) {
                            console.error(`Failed to normalize expiry date: ${normalizedExpiry}`);
                            throw new Error(`Invalid card expiry date format: ${normalizedExpiry}`);
                        }
                    }
                    
                    console.log(`Normalized expiry date to: ${normalizedExpiry}`);
                }

                const paymentData = {
                    amount: row['Amount to charge'],
                    currency: row['Curency'] || 'USD',
                    description: 'Bulk payment',
                    descriptor: row['Soft Descriptor'],
                    documentId: row._id,
                    cardNumber: decrypted['Card Number'],
                    cardExpiry: normalizedExpiry, // Use normalized YYYY-MM format
                    cardCvv: decrypted['Card CVV'],
                    cardholderName: cardholderName,
                    billingAddress
                };
                // Call existing payment logic
                const { jsonResponse, httpStatusCode } = await processDirectPayment(paymentData);
                
                // Prepare card data for encryption
                const cardDataToEncrypt = {
                    'Card Number': decrypted['Card Number'],
                    'Card Expire': decrypted['Card Expire'],
                    'Card CVV': decrypted['Card CVV']
                };
                const encryptedCardData = encryptCardData(cardDataToEncrypt);

                // Extract payment details from PayPal response
                const paymentDetails = {
                    orderId: jsonResponse.id,
                    captureId: jsonResponse.purchase_units[0]?.payments?.captures[0]?.id,
                    networkTransactionId: jsonResponse.purchase_units[0]?.payments?.captures[0]?.network_transaction_reference?.id,
                    status: jsonResponse.status,
                    amount: jsonResponse.purchase_units[0]?.amount?.value,
                    currency: jsonResponse.purchase_units[0]?.amount?.currency_code,
                    paypalFee: jsonResponse.purchase_units[0]?.payments?.captures[0]?.seller_receivable_breakdown?.paypal_fee?.value,
                    netAmount: jsonResponse.purchase_units[0]?.payments?.captures[0]?.seller_receivable_breakdown?.net_amount?.value,
                    cardLastDigits: jsonResponse.payment_source?.card?.last_digits,
                    cardBrand: jsonResponse.payment_source?.card?.brand,
                    cardType: jsonResponse.payment_source?.card?.type,
                    avsCode: jsonResponse.purchase_units[0]?.payments?.captures[0]?.processor_response?.avs_code,
                    cvvCode: jsonResponse.purchase_units[0]?.payments?.captures[0]?.processor_response?.cvv_code,
                    createTime: jsonResponse.create_time,
                    updateTime: jsonResponse.update_time,
                    captureStatus: jsonResponse.purchase_units[0]?.payments?.captures[0]?.status,
                    customId: jsonResponse.purchase_units[0]?.payments?.captures[0]?.custom_id
                };

                // Check if payment was declined
                const captureStatus = jsonResponse.purchase_units[0]?.payments?.captures[0]?.status;
                
                if (captureStatus === 'DECLINED') {
                    // Get decline reason from processor response
                    const processorResponse = jsonResponse.purchase_units[0]?.payments?.captures[0]?.processor_response;
                    const responseCode = processorResponse?.response_code;
                    const avsCode = processorResponse?.avs_code;
                    const cvvCode = processorResponse?.cvv_code;
                    
                    // Map response codes to user-friendly messages
                    // Use the same improved decline reason function as single payment
                    const getDeclineReason = (code, avsCode, cvvCode) => {
                        const declineCodes = {
                            '0500': 'Card was declined by the bank. Contact bank or try different card.',
                            '0510': 'Invalid card number. Check card number.',
                            '0520': 'Invalid expiration date. Check card expiry.',
                            '0530': 'Incorrect security code (CVV). Check CVV.',
                            '0540': 'Insufficient funds. Use different card or contact bank.',
                            '0550': 'Card has expired. Use different card.',
                            '0560': 'Card is restricted. Contact bank or try different card.',
                            '0570': 'Transaction not permitted. Contact bank.',
                            '0580': 'Payment processing issue. Try again later.',
                            '0590': 'Amount exceeds card limit. Try smaller amount or contact bank.',
                            '0600': 'Additional verification required. Contact bank.',
                            '0700': 'Flagged for security. Contact bank.',
                            '0800': 'Card account inactive. Use different card.',
                            '0900': 'Declined for fraud prevention. Contact bank.',
                            '1000': 'Daily transaction limit reached. Try tomorrow.'
                        };
                        
                        let baseMessage = declineCodes[code] || 'Payment declined by card issuer. Contact bank or try different card.';
                        
                        if (avsCode && avsCode !== 'Y') {
                            baseMessage += ' Verify billing address.';
                        }
                        if (cvvCode && cvvCode !== 'M' && !baseMessage.toLowerCase().includes('cvv')) {
                            baseMessage += ' Double-check CVV.';
                        }
                        
                        return baseMessage;
                    };
                    
                    const declineReason = getDeclineReason(responseCode, avsCode, cvvCode);
                    
                    console.log(`Bulk payment declined for documentId ${row._id}: ${declineReason}`);
                    
                    // Update database with declined status for bulk payment
                    let databaseUpdated = false;
                    try {
                        const updatedRecord = await ExcelData.findByIdAndUpdate(
                            row._id,
                            {
                                'Charge status': 'Declined',
                                'Status': 'Payment Declined',
                                // Add decline details
                                paypalOrderId: paymentDetails.orderId,
                                paypalCaptureId: paymentDetails.captureId,
                                paypalResponseCode: responseCode,
                                paypalDeclineReason: declineReason,
                                paypalAvsCode: avsCode,
                                paypalCvvCode: cvvCode,
                                paypalAmount: paymentDetails.amount,
                                paypalCurrency: paymentDetails.currency,
                                paypalCardBrand: paymentDetails.cardBrand,
                                paypalCardType: paymentDetails.cardType,
                                paypalCardLastDigits: paymentDetails.cardLastDigits,
                                paypalCaptureStatus: paymentDetails.captureStatus,
                                paypalCreateTime: paymentDetails.createTime,
                                paypalUpdateTime: paymentDetails.updateTime,
                                paypalCustomId: paymentDetails.customId,
                                lastFailureDate: new Date().toISOString()
                            },
                            { new: true }
                        );

                        if (updatedRecord) {
                            databaseUpdated = true;
                            console.log(`Bulk payment database updated for documentId ${row._id} with status: Declined`);
                        }
                    } catch (dbError) {
                        console.error('Database update error for bulk declined payment:', dbError);
                    }
                    
                    // Return decline response with database update status
                    return {
                        documentId: row._id,
                        status: 'declined',
                        message: declineReason,
                        declineDetails: {
                            reason: declineReason,
                            responseCode: responseCode,
                            avsCode: avsCode,
                            cvvCode: cvvCode
                        },
                        databaseUpdated: databaseUpdated
                    };
                }

                // Only proceed with database update if payment was successful (COMPLETED)
                if (captureStatus !== 'COMPLETED') {
                    console.log(`Bulk payment status is ${captureStatus} for documentId ${row._id}, updating database with error status`);
                    
                    // Update database with error status for bulk payment
                    let databaseUpdated = false;
                    try {
                        const updatedRecord = await ExcelData.findByIdAndUpdate(
                            row._id,
                            {
                                'Charge status': 'Failed',
                                'Status': 'Payment Failed',
                                // Add error details
                                paypalOrderId: paymentDetails.orderId,
                                paypalCaptureId: paymentDetails.captureId,
                                paypalAmount: paymentDetails.amount,
                                paypalCurrency: paymentDetails.currency,
                                paypalCardBrand: paymentDetails.cardBrand,
                                paypalCardType: paymentDetails.cardType,
                                paypalCardLastDigits: paymentDetails.cardLastDigits,
                                paypalCaptureStatus: paymentDetails.captureStatus,
                                paypalCreateTime: paymentDetails.createTime,
                                paypalUpdateTime: paymentDetails.updateTime,
                                paypalCustomId: paymentDetails.customId,
                                paypalErrorReason: `Payment status is ${captureStatus}. Expected COMPLETED.`,
                                lastFailureDate: new Date().toISOString()
                            },
                            { new: true }
                        );

                        if (updatedRecord) {
                            databaseUpdated = true;
                            console.log(`Bulk payment database updated for documentId ${row._id} with status: Failed`);
                        }
                    } catch (dbError) {
                        console.error('Database update error for bulk failed payment:', dbError);
                    }
                    
                    return {
                        documentId: row._id,
                        status: 'error',
                        error: `Payment status is ${captureStatus}. Expected COMPLETED.`,
                        response: jsonResponse,
                        databaseUpdated: databaseUpdated
                    };
                }

                // Update the ExcelData record (only for successful payments)
                try {
                    // Prepare card data for encryption
                    const cardDataToEncrypt = {
                        'Card Number': decrypted['Card Number'],
                        'Card Expire': normalizedExpiry, // Use normalized YYYY-MM format
                        'Card CVV': decrypted['Card CVV']
                    };
                    const encryptedCardData = encryptCardData(cardDataToEncrypt);

                    // First, get the existing record to check for OTA information
                    let otaId = null;
                    let otaName = null;
                    if (row.otaId && row.otaId.name) {
                        otaName = row.otaId.name;
                        console.log(`Found OTA in row: ${otaName}`);
                        const otaRecord = await OTA.findOne({ name: otaName, isActive: true });
                        if (otaRecord) {
                            otaId = otaRecord._id;
                            console.log(`Found OTA record ID: ${otaId}`);
                        } else {
                            console.log(`No OTA record found for: ${otaName}`);
                        }
                    }
                    
                    const updatedRecord = await ExcelData.findByIdAndUpdate(
                        row._id,
                        {
                            'Charge status': 'Charged',
                            'Card Number': encryptedCardData['Card Number'],
                            'Card Expire': encryptedCardData['Card Expire'],
                            'Card CVV': encryptedCardData['Card CVV'],
                            'Soft Descriptor': row['Soft Descriptor'],
                            'Status': 'Payment Processed',
                            // Add payment details as additional fields
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
                            ota: otaName, // Preserve original OTA name from record
                            otaId: otaId
                        },
                        { new: true }
                    );

                    if (!updatedRecord) {
                        console.error('ExcelData record not found for documentId:', row._id);
                        return {
                            documentId: row._id,
                            status: 'error',
                            message: 'ExcelData record not found'
                        };
                    }

                } catch (dbError) {
                    console.error('Database update error during bulk payment:', dbError);
                    // Don't fail the bulk payment response if DB update fails
                }

                return {
                    documentId: row._id,
                    status: 'success',
                    httpStatusCode,
                    response: jsonResponse
                };
            } catch (err) {
                // Provide user-friendly error messages for bulk operations
                let errorMessage = err.message;
                let errorType = 'processing_error';
                
                if (err.message) {
                    const message = err.message.toLowerCase();
                    
                    if (message.includes('card') || message.includes('cvv') || message.includes('expired')) {
                        errorType = 'card_error';
                    } else if (message.includes('declined') || message.includes('insufficient')) {
                        errorType = 'payment_declined';
                    } else if (message.includes('network') || message.includes('connection') || message.includes('timeout')) {
                        errorType = 'network_error';
                        errorMessage = 'Network connection issue. Will retry automatically.';
                    } else if (message.includes('limit') || message.includes('amount')) {
                        errorType = 'amount_error';
                    }
                }
                
                // Provide generic message if original is not user-friendly
                if (!errorMessage || errorMessage.length > 200) {
                    errorMessage = 'Payment processing failed for this transaction.';
                }
                
                // Update database with error status for bulk payment
                let databaseUpdated = false;
                try {
                    const updatedRecord = await ExcelData.findByIdAndUpdate(
                        row._id,
                        {
                            'Charge status': 'Failed',
                            'Status': 'Payment Failed',
                            paypalErrorReason: errorMessage,
                            paypalErrorType: errorType,
                            lastFailureDate: new Date().toISOString()
                        },
                        { new: true }
                    );

                    if (updatedRecord) {
                        databaseUpdated = true;
                        console.log(`Bulk payment database updated for documentId ${row._id} with error status: Failed`);
                    }
                } catch (dbError) {
                    console.error('Database update error for bulk payment error:', dbError);
                }
                
                return {
                    documentId: row._id,
                    status: 'error',
                    message: errorMessage,
                    error_type: errorType,
                    timestamp: new Date().toISOString(),
                    databaseUpdated: databaseUpdated,
                    // Include debug info only in development
                    ...(process.env.NODE_ENV === 'development' && { 
                        debugMessage: err.message,
                        stack: err.stack 
                    })
                };
            }
        })));
        // Add comprehensive summary stats
        const successCount = results.filter(r => r.status === 'success').length;
        const errorCount = results.filter(r => r.status === 'error').length;
        const declinedCount = results.filter(r => r.status === 'declined').length;
        const totalCount = results.length;
        
        // Calculate percentages
        const successRate = totalCount > 0 ? Math.round((successCount / totalCount) * 100) : 0;
        const failureRate = totalCount > 0 ? Math.round(((errorCount + declinedCount) / totalCount) * 100) : 0;
        
        // Calculate total amounts
        const successfulAmount = results
            .filter(r => r.status === 'success')
            .reduce((sum, r) => {
                const amount = parseFloat(r.response?.purchase_units?.[0]?.amount?.value || 0);
                return sum + amount;
            }, 0);
            
        const failedAmount = results
            .filter(r => r.status !== 'success')
            .reduce((sum, r) => {
                // Try to get original amount from document if available
                return sum + 0; // We'll need to add this if we want failed amounts
            }, 0);
        
        // Create user-friendly summary
        const summary = {
            // Main counts
            total_transactions: totalCount,
            successful_payments: successCount,
            failed_payments: errorCount + declinedCount,
            
            // Breakdown of failures
            payment_errors: errorCount,
            payment_declines: declinedCount,
            
            // Success rate
            success_rate_percentage: successRate,
            failure_rate_percentage: failureRate,
            
            // Financial summary
            total_amount_processed: successfulAmount.toFixed(2),
            currency: results.find(r => r.status === 'success')?.response?.purchase_units?.[0]?.amount?.currency_code || 'USD',
            
            // User-friendly message
            summary_message: `${successCount} of ${totalCount} payments completed successfully (${successRate}%). ` +
                           `${errorCount + declinedCount} payments failed${declinedCount > 0 ? ` (${declinedCount} declined, ${errorCount} errors)` : ''}.`,
            
            // Processing details
            processing_time: new Date().toISOString(),
            
            // Legacy format for backwards compatibility
            total: totalCount,
            success: successCount,
            error: errorCount,
            declined: declinedCount
        };
        
        // Determine overall response status
        const overallStatus = successCount === totalCount ? 'all_success' : 
                            successCount === 0 ? 'all_failed' : 'partial_success';
        
        res.status(200).json({ 
            status: overallStatus,
            message: summary.summary_message,
            summary, 
            results 
        });
    } catch (error) {
        console.error('Bulk PayPal payment error:', error);
        
        // Determine appropriate error message for bulk operation
        let errorMessage = 'Bulk payment processing failed';
        let statusCode = 500;
        
        if (error.message) {
            const message = error.message.toLowerCase();
            
            if (message.includes('documentids') || message.includes('array')) {
                statusCode = 400;
                errorMessage = 'Invalid document IDs provided. Please check the request format.';
            } else if (message.includes('network') || message.includes('connection') || message.includes('timeout')) {
                statusCode = 503;
                errorMessage = 'Network connection issue during bulk processing. Please try again.';
            } else if (message.includes('database') || message.includes('mongo')) {
                statusCode = 503;
                errorMessage = 'Database connection issue. Please try again later.';
            } else if (error.message.length < 200) {
                errorMessage = `Bulk payment failed: ${error.message}`;
            }
        }
        
        res.status(statusCode).json({ 
            status: 'error', 
            message: errorMessage,
            error_type: 'bulk_processing_error',
            data: {
                timestamp: new Date().toISOString(),
                // Include debug info only in development
                ...(process.env.NODE_ENV === 'development' && { 
                    debugMessage: error.message,
                    stack: error.stack 
                })
            }
        });
    }
};

/**
 * Process direct refund with PayPal v2 API
 * @param {Object} refundData - The refund data
 * @returns {Object} - The refund response
 */
const processDirectRefund = async (refundData) => {
    const { 
        captureId, 
        amount, 
        currency = "USD", 
        invoiceId, 
        customId,
        noteToPayer = "Refund processed",
        documentId
    } = refundData;

    // Validation for required fields
    if (!captureId) {
        throw new Error('Capture ID is required for refund');
    }

    // Validate PayPal credentials
    if (!PAYPAL_CLIENT_ID || !PAYPAL_CLIENT_SECRET) {
        throw new Error('PayPal credentials are not configured properly');
    }

    const requestBody = {};

    // Add amount if partial refund
    if (amount) {
        requestBody.amount = {
            currency_code: currency,
            value: amount.toString()
        };
    }

    // Add optional fields
    if (invoiceId) {
        requestBody.invoice_id = invoiceId;
    }
    
    if (customId) {
        requestBody.custom_id = customId;
    }
    
    if (noteToPayer) {
        requestBody.note_to_payer = noteToPayer;
    }

    try {
        // Clean credentials (remove any whitespace)
        const clientId = PAYPAL_CLIENT_ID.trim();
        const clientSecret = PAYPAL_CLIENT_SECRET.trim();
        
        console.log('PayPal Client ID length:', clientId.length);
        console.log('PayPal Client Secret length:', clientSecret.length);
        
        // Get access token using the client's authentication
        const auth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
        
        // Use environment-based URL configuration
        const baseURL = process.env.NODE_ENV === 'production' 
            ? 'https://api-m.paypal.com'           // Production
            : 'https://api-m.sandbox.paypal.com'; // Sandbox
        
        console.log('Using PayPal base URL:', baseURL);
        
        // First get access token
        const tokenResponse = await fetch(`${baseURL}/v1/oauth2/token`, {
            method: 'POST',
            headers: {
                'Authorization': `Basic ${auth}`,
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: 'grant_type=client_credentials'
        });
        
        if (!tokenResponse.ok) {
            const tokenError = await tokenResponse.text();
            console.error('Token response status:', tokenResponse.status);
            console.error('Token response error:', tokenError);
            throw new Error(`Failed to get PayPal access token: ${tokenResponse.status} ${tokenResponse.statusText}`);
        }
        
        const tokenData = await tokenResponse.json();
        const accessToken = tokenData.access_token;
        
        if (!accessToken) {
            throw new Error('No access token received from PayPal');
        }
        
        console.log('Successfully obtained PayPal access token');
        
        // Now make the refund request
        const refundEndpoint = `${baseURL}/v2/payments/captures/${captureId}/refund`;
        console.log('Refund endpoint:', refundEndpoint);
        console.log('Refund request body:', JSON.stringify(requestBody, null, 2));

        const response = await fetch(refundEndpoint, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json',
                'Prefer': 'return=representation',
                'PayPal-Partner-Attribution-Id': PAYPAL_BN_CODE,
                'PayPal-Request-Id': `refund-${captureId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
            },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            const errorData = await response.json();
            console.error('PayPal Refund API Error Status:', response.status);
            console.error('PayPal Refund API Error:', errorData);
            
            let errorMessage = 'PayPal Refund Error';
            
            if (errorData.details && Array.isArray(errorData.details)) {
                // Map specific refund errors to user-friendly messages
                const userFriendlyErrors = errorData.details.map(detail => {
                    const issue = detail.issue || '';
                    const description = detail.description || '';
                    
                    // Handle specific refund errors
                    if (issue === 'CAPTURE_FULLY_REFUNDED') {
                        return 'This payment has already been fully refunded. No further refunds can be processed.';
                    }
                    
                    if (issue === 'CAPTURE_NOT_FOUND') {
                        return 'Payment capture not found. Please verify the transaction ID.';
                    }
                    
                    if (issue === 'REFUND_AMOUNT_EXCEEDS_CAPTURE_AMOUNT') {
                        return 'Refund amount exceeds the original payment amount. Please check the refund amount.';
                    }
                    
                    if (issue === 'REFUND_TIME_LIMIT_EXCEEDED') {
                        return 'The time limit for refunding this payment has expired.';
                    }
                    
                    if (issue === 'INVALID_REFUND_AMOUNT') {
                        return 'Invalid refund amount. Please enter a valid positive amount.';
                    }
                    
                    if (issue === 'CAPTURE_NOT_REFUNDABLE') {
                        return 'This payment cannot be refunded. It may have been disputed or have other restrictions.';
                    }
                    
                    if (issue === 'CURRENCY_MISMATCH') {
                        return 'Refund currency does not match the original payment currency.';
                    }
                    
                    if (description.toLowerCase().includes('business validation')) {
                        return 'Refund failed business validation. Please contact support for assistance.';
                    }
                    
                    // Fallback to description or generic message
                    if (description) {
                        return `Refund error: ${description}`;
                    }
                    
                    return 'Unable to process refund. Please contact support.';
                });
                
                // Use the first user-friendly error, or combine unique errors
                const uniqueErrors = [...new Set(userFriendlyErrors)];
                errorMessage = uniqueErrors.length === 1 ? uniqueErrors[0] : uniqueErrors.join(' ');
                
            } else {
                errorMessage = `PayPal Refund Error: ${errorData.message || response.statusText}`;
            }
            
            throw new Error(errorMessage);
        }
        
        const refundResponse = await response.json();
        console.log('Refund successful:', refundResponse.id);
        
        return {
            jsonResponse: refundResponse,
            httpStatusCode: response.status,
        };
    } catch (error) {
        console.error('General Refund Error:', error);
        throw error;
    }
};

/**
 * Process single refund
 * POST /api/paypal/process-refund
 */
const processRefund = async (req, res) => {
    try {
        const { 
            documentId,
            captureId,
            amount,
            currency,
            invoiceId,
            customId,
            noteToPayer,
            refundType = 'full' // 'full' or 'partial'
        } = req.body;

        // Validate required fields
        if (!documentId && !captureId) {
            return res.status(400).json({
                status: 'error',
                message: 'Either documentId or captureId is required'
            });
        }

        let actualCaptureId = captureId;
        let record = null;

        // If documentId provided, fetch the record and get captureId
        if (documentId) {
            record = await ExcelData.findById(documentId)
                .populate('otaId', 'name displayName customer billingAddress isActive'); // Populate OTA data
            if (!record) {
                return res.status(404).json({
                    status: 'error',
                    message: 'ExcelData record not found'
                });
            }

            if (!record.paypalCaptureId) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No PayPal capture ID found for this record. Cannot process refund.'
                });
            }

            actualCaptureId = record.paypalCaptureId;
        }

        // Validate refund amount for partial refunds
        if (refundType === 'partial' && (!amount || isNaN(parseFloat(amount)) || parseFloat(amount) <= 0)) {
            return res.status(400).json({
                status: 'error',
                message: 'Valid amount is required for partial refunds'
            });
        }

        const refundData = {
            captureId: actualCaptureId,
            amount: refundType === 'partial' ? parseFloat(amount) : null,
            currency: currency || record?.paypalCurrency || "USD",
            invoiceId: invoiceId || `REFUND-${documentId || actualCaptureId}-${Date.now()}`,
            customId: customId || `REFUND-CUSTOM-${Date.now()}`,
            noteToPayer: noteToPayer || "Refund processed",
            documentId: documentId
        };

        const { jsonResponse, httpStatusCode } = await processDirectRefund(refundData);
        
        // Extract refund details from PayPal response
        const refundDetails = {
            refundId: jsonResponse.id,
            status: jsonResponse.status,
            amount: jsonResponse.amount?.value,
            currency: jsonResponse.amount?.currency_code,
            invoiceId: jsonResponse.invoice_id,
            customId: jsonResponse.custom_id,
            noteToPayer: jsonResponse.note_to_payer,
            grossAmount: jsonResponse.seller_payable_breakdown?.gross_amount?.value,
            paypalFee: jsonResponse.seller_payable_breakdown?.paypal_fee?.value,
            netAmount: jsonResponse.seller_payable_breakdown?.net_amount?.value,
            totalRefunded: jsonResponse.seller_payable_breakdown?.total_refunded_amount?.value,
            createTime: jsonResponse.create_time,
            updateTime: jsonResponse.update_time
        };

        // Update the ExcelData record with refund information
        if (documentId && record) {
            try {
                const updateFields = {
                    'Charge status': refundType === 'full' ? 'Refunded' : 'Partially Refunded',
                    'Status': `Refund ${refundDetails.status}`,
                    // Add refund details
                    paypalRefundId: refundDetails.refundId,
                    paypalRefundStatus: refundDetails.status,
                    paypalRefundAmount: refundDetails.amount,
                    paypalRefundCurrency: refundDetails.currency,
                    paypalRefundGrossAmount: refundDetails.grossAmount,
                    paypalRefundFee: refundDetails.paypalFee,
                    paypalRefundNetAmount: refundDetails.netAmount,
                    paypalTotalRefunded: refundDetails.totalRefunded,
                    paypalRefundCreateTime: refundDetails.createTime,
                    paypalRefundUpdateTime: refundDetails.updateTime,
                    paypalRefundInvoiceId: refundDetails.invoiceId,
                    paypalRefundCustomId: refundDetails.customId,
                    paypalRefundNote: refundDetails.noteToPayer
                };

                await ExcelData.findByIdAndUpdate(documentId, updateFields, { new: true });
            } catch (dbError) {
                console.error('Database update error during refund:', dbError);
                // Don't fail the refund response if DB update fails
            }
        }
        
        res.status(httpStatusCode).json({
            status: 'success',
            message: 'Refund processed successfully',
            data: {
                ...jsonResponse,
                refundDetails: refundDetails,
                databaseUpdated: documentId ? true : false
            }
        });

    } catch (error) {
        console.error("Failed to process refund:", error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to process refund',
            error: error.message
        });
    }
};

/**
 * Process bulk refunds
 * POST /api/paypal/process-bulk-refunds
 */
const processBulkRefunds = async (req, res) => {
    console.log('Processing bulk refunds');
    try {
        const { documentIds, refundType = 'full', amount, currency, noteToPayer } = req.body;
        
        if (!Array.isArray(documentIds) || documentIds.length === 0) {
            return res.status(400).json({ 
                status: 'error', 
                message: 'documentIds array is required' 
            });
        }

        // Validate partial refund parameters
        if (refundType === 'partial' && (!amount || isNaN(parseFloat(amount)) || parseFloat(amount) <= 0)) {
            return res.status(400).json({
                status: 'error',
                message: 'Valid amount is required for bulk partial refunds'
            });
        }

        // Fetch all rows in one query
        const rows = await ExcelData.find({ _id: { $in: documentIds } })
            .populate('otaId', 'name displayName customer billingAddress isActive') // Populate OTA data
            .lean();
        
        if (rows.length === 0) {
            return res.status(404).json({
                status: 'error',
                message: 'No records found for the provided document IDs'
            });
        }

        const limit = pLimit(3); // Limit concurrency to 3 for refunds (more conservative)
        const results = await Promise.all(rows.map(row => limit(async () => {
            try {
                if (!row.paypalCaptureId) {
                    throw new Error('No PayPal capture ID found for this record');
                }

                const refundData = {
                    captureId: row.paypalCaptureId,
                    amount: refundType === 'partial' ? parseFloat(amount) : null,
                    currency: currency || row.paypalCurrency || "USD",
                    invoiceId: `BULK-REFUND-${row._id}-${Date.now()}`,
                    customId: `BULK-REFUND-CUSTOM-${Date.now()}`,
                    noteToPayer: noteToPayer || "Bulk refund processed",
                    documentId: row._id
                };

                const { jsonResponse, httpStatusCode } = await processDirectRefund(refundData);
                
                // Extract refund details
                const refundDetails = {
                    refundId: jsonResponse.id,
                    status: jsonResponse.status,
                    amount: jsonResponse.amount?.value,
                    currency: jsonResponse.amount?.currency_code,
                    grossAmount: jsonResponse.seller_payable_breakdown?.gross_amount?.value,
                    paypalFee: jsonResponse.seller_payable_breakdown?.paypal_fee?.value,
                    netAmount: jsonResponse.seller_payable_breakdown?.net_amount?.value,
                    totalRefunded: jsonResponse.seller_payable_breakdown?.total_refunded_amount?.value,
                    createTime: jsonResponse.create_time,
                    updateTime: jsonResponse.update_time
                };

                // Update the ExcelData record
                await ExcelData.findByIdAndUpdate(
                    row._id,
                    {
                        'Charge status': refundType === 'full' ? 'Refunded' : 'Partially Refunded',
                        'Status': `Refund ${refundDetails.status}`,
                        paypalRefundId: refundDetails.refundId,
                        paypalRefundStatus: refundDetails.status,
                        paypalRefundAmount: refundDetails.amount,
                        paypalRefundCurrency: refundDetails.currency,
                        paypalRefundGrossAmount: refundDetails.grossAmount,
                        paypalRefundFee: refundDetails.paypalFee,
                        paypalRefundNetAmount: refundDetails.netAmount,
                        paypalTotalRefunded: refundDetails.totalRefunded,
                        paypalRefundCreateTime: refundDetails.createTime,
                        paypalRefundUpdateTime: refundDetails.updateTime,
                        paypalRefundInvoiceId: refundData.invoiceId,
                        paypalRefundCustomId: refundData.customId,
                        paypalRefundNote: refundData.noteToPayer
                    },
                    { new: true }
                );

                return {
                    documentId: row._id,
                    status: 'success',
                    httpStatusCode,
                    refund: refundDetails,
                    response: jsonResponse
                };
            } catch (err) {
                // Provide user-friendly error messages for bulk refunds
                let errorMessage = err.message;
                let errorType = 'refund_error';
                
                if (err.message) {
                    const message = err.message.toLowerCase();
                    
                    if (message.includes('already been fully refunded')) {
                        errorType = 'already_refunded';
                        errorMessage = 'This payment has already been fully refunded.';
                    } else if (message.includes('capture not found')) {
                        errorType = 'capture_not_found';
                        errorMessage = 'Payment capture not found for refund.';
                    } else if (message.includes('refund amount exceeds')) {
                        errorType = 'amount_exceeds';
                        errorMessage = 'Refund amount exceeds original payment amount.';
                    } else if (message.includes('time limit exceeded')) {
                        errorType = 'time_limit_exceeded';
                        errorMessage = 'Refund time limit has expired.';
                    } else if (message.includes('network') || message.includes('connection') || message.includes('timeout')) {
                        errorType = 'network_error';
                        errorMessage = 'Network connection issue during refund.';
                    }
                }
                
                // Provide generic message if original is not user-friendly
                if (!errorMessage || errorMessage.length > 200) {
                    errorMessage = 'Refund processing failed for this transaction.';
                }
                
                return {
                    documentId: row._id,
                    status: 'error',
                    message: errorMessage,
                    error_type: errorType,
                    timestamp: new Date().toISOString(),
                    // Include debug info only in development
                    ...(process.env.NODE_ENV === 'development' && { 
                        debugMessage: err.message,
                        stack: err.stack 
                    })
                };
            }
        })));

        // Add comprehensive summary stats for refunds
        const successCount = results.filter(r => r.status === 'success').length;
        const errorCount = results.filter(r => r.status === 'error').length;
        const totalCount = results.length;
        
        // Calculate percentages
        const successRate = totalCount > 0 ? Math.round((successCount / totalCount) * 100) : 0;
        const failureRate = totalCount > 0 ? Math.round((errorCount / totalCount) * 100) : 0;
        
        // Calculate total refund amounts
        const totalRefundAmount = results
            .filter(r => r.status === 'success')
            .reduce((sum, r) => sum + parseFloat(r.refund?.amount || 0), 0);
            
        // Create user-friendly summary
        const summary = {
            // Main counts
            total_refund_requests: totalCount,
            successful_refunds: successCount,
            failed_refunds: errorCount,
            
            // Success rate
            success_rate_percentage: successRate,
            failure_rate_percentage: failureRate,
            
            // Refund details
            refund_type: refundType,
            total_refunded_amount: totalRefundAmount.toFixed(2),
            currency: results.find(r => r.status === 'success')?.refund?.currency || 'USD',
            
            // User-friendly message
            summary_message: `${successCount} of ${totalCount} refunds completed successfully (${successRate}%). ` +
                           `${errorCount} refunds failed. Total refunded: $${totalRefundAmount.toFixed(2)}.`,
            
            // Processing details
            processing_time: new Date().toISOString(),
            
            // Legacy format for backwards compatibility
            total: totalCount,
            success: successCount,
            error: errorCount,
            totalRefundAmount: totalRefundAmount
        };
        
        // Determine overall response status
        const overallStatus = successCount === totalCount ? 'all_success' : 
                            successCount === 0 ? 'all_failed' : 'partial_success';

        res.status(200).json({ 
            status: overallStatus,
            message: summary.summary_message,
            summary, 
            results 
        });
    } catch (error) {
        console.error('Bulk PayPal refund error:', error);
        res.status(500).json({ 
            status: 'error', 
            message: 'Bulk refund failed', 
            error: error.message 
        });
    }
};

/**
 * Get refund details by refund ID
 * GET /api/paypal/refund/:refundId
 */
const getRefundDetails = async (req, res) => {
    try {
        const { refundId } = req.params;

        if (!refundId) {
            return res.status(400).json({
                status: 'error',
                message: 'Refund ID is required'
            });
        }

        // Use PayPal SDK to get refund details (if available in SDK)
        // For now, we'll search in our database
        const record = await ExcelData.findOne({ paypalRefundId: refundId });

        if (!record) {
            return res.status(404).json({
                status: 'error',
                message: 'Refund record not found'
            });
        }

        const refundInfo = {
            refundId: record.paypalRefundId,
            documentId: record._id,
            status: record.paypalRefundStatus,
            amount: record.paypalRefundAmount,
            currency: record.paypalRefundCurrency,
            grossAmount: record.paypalRefundGrossAmount,
            paypalFee: record.paypalRefundFee,
            netAmount: record.paypalRefundNetAmount,
            totalRefunded: record.paypalTotalRefunded,
            createTime: record.paypalRefundCreateTime,
            updateTime: record.paypalRefundUpdateTime,
            invoiceId: record.paypalRefundInvoiceId,
            customId: record.paypalRefundCustomId,
            note: record.paypalRefundNote,
            originalCaptureId: record.paypalCaptureId,
            originalOrderId: record.paypalOrderId,
            chargeStatus: record['Charge status']
        };

        res.status(200).json({
            status: 'success',
            message: 'Refund details retrieved successfully',
            data: refundInfo
        });

    } catch (error) {
        console.error("Failed to get refund details:", error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to get refund details',
            error: error.message
        });
    }
};

/**
 * Get payment details for return screen
 * GET /api/paypal/payment-details/:documentId
 */
const getPaymentDetails = async (req, res) => {
    try {
        const { documentId } = req.params;

        if (!documentId) {
            return res.status(400).json({
                status: 'error',
                message: 'Document ID is required'
            });
        }

        const record = await ExcelData.findById(documentId)
            .populate('otaId', 'name displayName customer billingAddress isActive'); // Populate OTA data

        if (!record) {
            return res.status(404).json({
                status: 'error',
                message: 'Payment record not found'
            });
        }

        const paymentInfo = {
            documentId: record._id,
            paypalOrderId: record.paypalOrderId,
            paypalCaptureId: record.paypalCaptureId,
            status: record.paypalStatus,
            amount: record.paypalAmount,
            currency: record.paypalCurrency,
            chargeStatus: record['Charge status'],
            customerName: record['Name'],
            hotelName: record['Hotel Name'],
            reservationId: record['Reservation ID'],
            createTime: record.paypalCreateTime,
            updateTime: record.paypalUpdateTime,
            cardLastDigits: record.paypalCardLastDigits,
            cardBrand: record.paypalCardBrand,
            // Refund information if available
            refundId: record.paypalRefundId,
            refundStatus: record.paypalRefundStatus,
            refundAmount: record.paypalRefundAmount,
            refundCreateTime: record.paypalRefundCreateTime
        };

        res.status(200).json({
            status: 'success',
            message: 'Payment details retrieved successfully',
            data: paymentInfo
        });

    } catch (error) {
        console.error("Failed to get payment details:", error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to get payment details',
            error: error.message
        });
    }
};

const getAdminExcelData = async (req, res) => {
    try {
        // Get authorized admin emails from environment variables
        const adminEmails = process.env.ADMIN_EMAILS ? 
            process.env.ADMIN_EMAILS.split(',').map(email => email.trim().toLowerCase()) : [];
        
        // Check if current user email is in authorized list
        const userEmail = req.user.email.toLowerCase();
        
        if (!adminEmails.includes(userEmail)) {
            return res.status(403).json({
                status: 'error',
                message: 'You are not authorized'
            });
        }
        
        // Extract query parameters
        const {
            page = 1,
            limit = 10,
            sort = 'createdAt',
            order = 'desc',
            search = '',
            status = '',
            portfolio = '',
            batch = '',
            hotel = '',
        } = req.query;

        // Validate pagination parameters
        const pageNum = Math.max(1, parseInt(page));
        const limitNum = Math.min(100, Math.max(1, parseInt(limit))); // Max 100 records per page
        const skip = (pageNum - 1) * limitNum;

        // Build base query (only show processed payment statuses)
        let query = {
            'Charge status': { $in: ['Charged', 'Failed', 'Declined'] }
        };

        // Add search functionality
        if (search) {
            const searchRegex = new RegExp(search, 'i'); // Case-insensitive search
            query.$or = [
                { 'Hotel Name': searchRegex },
                { 'Name': searchRegex },
                { 'Reservation ID': searchRegex },
                { 'Hotel Confirmation Code': searchRegex },
                { 'Expedia ID': searchRegex },
                { 'VNP Work ID': searchRegex },
                { 'Batch': searchRegex },
                { 'Portfolio': searchRegex }
            ];
        }

        // Add filters
        if (status) {
            // Only allow filtering by approved statuses
            const allowedStatuses = ['Charged', 'Failed', 'Declined'];
            if (allowedStatuses.includes(status)) {
            query['Charge status'] = status;
            }
            // If invalid status provided, keep the base filter (show all approved statuses)
        }
        
        if (portfolio) {
            query['Portfolio'] = new RegExp(portfolio, 'i');
        }
        
        if (batch) {
            query['Batch'] = new RegExp(batch, 'i');
        }
        
        if (hotel) {
            query['Hotel Name'] = new RegExp(hotel, 'i');
        }

        // Build sort object
        const sortOrder = order.toLowerCase() === 'desc' ? -1 : 1;
        const sortObj = {};
        sortObj[sort] = sortOrder;

        // Get paginated data with filters
        const excelData = await ExcelData.find(query)
            .populate('otaId', 'name displayName customer billingAddress isActive') // Populate OTA data
            .sort(sortObj)
            .skip(skip)
            .limit(limitNum)
            .lean(); // Use lean() for better performance

        // Calculate pagination info
        const totalCount = await ExcelData.countDocuments(query);
        const totalPages = Math.ceil(totalCount / limitNum);
        const hasNextPage = pageNum < totalPages;
        const hasPrevPage = pageNum > 1;

        // Get unique values for filters (helpful for frontend dropdowns)
        const [statusList, portfolioList, batchList] = await Promise.all([
            ExcelData.distinct('Charge status', { 'Charge status': { $in: ['Charged', 'Failed', 'Declined'] } }),
            ExcelData.distinct('Portfolio', { 'Portfolio': { $ne: null, $ne: '' } }),
            ExcelData.distinct('Batch', { 'Batch': { $ne: null, $ne: '' } })
        ]);

        // Decrypt sensitive card data before returning
        const decryptedExcelData = excelData.map(record => {
            return decryptCardData(record);
        });

        res.status(200).json({
            status: 'success',
            message: 'Admin ExcelData retrieved successfully',
            data: {
                data: decryptedExcelData,
                pagination: {
                    currentPage: pageNum,
                    totalPages,
                    totalCount: totalCount,
                    limit: limitNum,
                    hasNextPage,
                    hasPrevPage,
                },
                filters: {
                    applied: {
                        search,
                        status,
                        portfolio,
                        batch,
                        hotel,
                        sort,
                        order
                    },
                    available: {
                        statusOptions: statusList.filter(s => s), // Remove null/empty
                        portfolioOptions: portfolioList.filter(p => p),
                        batchOptions: batchList.filter(b => b)
                    }
                },
                requestedBy: req.user.email,
                timestamp: new Date().toISOString()
            }
        });

    } catch (error) {
        console.error('Get admin ExcelData error:', error);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: error.message
        });
    }
};

module.exports = {
    processPayment,
    getAdminExcelData,
    processBulkPayments,
    processRefund,
    processBulkRefunds,
    getRefundDetails,
    getPaymentDetails
};
