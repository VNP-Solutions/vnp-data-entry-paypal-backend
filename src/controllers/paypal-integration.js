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
            
            // Map response codes to user-friendly messages
            const getDeclineReason = (code, avsCode, cvvCode) => {
                const declineCodes = {
                    '0500': 'Your card was declined by your bank. Please contact your bank or try a different card.',
                    '0510': 'The card number you entered is invalid. Please check your card number and try again.',
                    '0520': 'The expiration date you entered is invalid. Please check your card\'s expiry date.',
                    '0530': 'The security code (CVV) you entered is incorrect. Please check the 3 or 4 digit code on your card.',
                    '0540': 'Your card has insufficient funds for this transaction. Please use a different card or contact your bank.',
                    '0550': 'Your card has expired. Please use a different card.',
                    '0560': 'Your card is restricted or blocked. Please contact your bank or try a different card.',
                    '0570': 'This type of transaction is not permitted on your card. Please contact your bank or try a different card.',
                    '0580': 'There is a temporary issue with payment processing. Please try again later or contact support.',
                    '0590': 'The transaction amount exceeds your card limit. Please try a smaller amount or contact your bank.',
                    '0600': 'Your card issuer requires additional verification. Please contact your bank.',
                    '0700': 'The transaction was flagged for security reasons. Please contact your bank or try again.',
                    '0800': 'Your card account is closed or inactive. Please use a different card.',
                    '0900': 'The transaction was declined due to fraud prevention. Please contact your bank.',
                    '1000': 'Your card has reached its transaction limit for today. Please try again tomorrow or contact your bank.'
                };
                
                let baseMessage = declineCodes[code] || 'Your payment was declined by your card issuer. Please contact your bank or try a different card.';
                
                // Add specific guidance based on AVS/CVV failures
                if (avsCode && avsCode !== 'Y') {
                    baseMessage += ' Additionally, please verify that your billing address matches the address on file with your bank.';
                }
                if (cvvCode && cvvCode !== 'M') {
                    if (!baseMessage.toLowerCase().includes('cvv') && !baseMessage.toLowerCase().includes('security code')) {
                        baseMessage += ' Please also double-check the security code (CVV) on your card.';
                    }
                }
                
                return baseMessage;
            };
            
            const declineReason = getDeclineReason(responseCode, avsCode, cvvCode);
            
            // Additional helpful information
            let helpfulTips = '';
            
            // Provide tips based on the type of decline
            if (responseCode === '0540' || responseCode === '0590') {
                helpfulTips = ' You may want to contact your bank to increase your limits or verify available funds.';
            } else if (responseCode === '0560' || responseCode === '0570') {
                helpfulTips = ' Your bank may have temporarily restricted online or international transactions for security.';
            } else if (responseCode === '0500' || !responseCode) {
                helpfulTips = ' This is often a temporary issue. You can try again in a few minutes or contact your bank to ensure your card is active for online transactions.';
            }
            
            console.log(`Payment declined for documentId ${documentId}: ${declineReason}`);
            
            // Create a user-friendly decline message
            const userMessage = `${declineReason}${helpfulTips}`;
            
            // Return decline response without updating database
            return res.status(402).json({
                status: 'declined',
                message: userMessage,
                error_type: 'payment_declined',
                data: {
                    paymentDetails: paymentDetails,
                    declineDetails: {
                        reason: declineReason,
                        responseCode: responseCode,
                        avsCode: avsCode,
                        cvvCode: cvvCode,
                        helpfulTips: helpfulTips.trim()
                    },
                    databaseUpdated: false,
                    // Include minimal response data for debugging (admin only)
                    debugInfo: process.env.NODE_ENV === 'development' ? {
                        paypalOrderId: jsonResponse.id,
                        paypalStatus: jsonResponse.status
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

                // Determine cardholder name - use OTA displayName if available, otherwise use row Name
                let cardholderName;
                if (row.otaId && row.otaId.displayName) {
                    cardholderName = row.otaId.displayName;
                    console.log(`Using OTA displayName as cardholder: ${cardholderName}`);
                } else {
                    cardholderName = row['Name'];
                    console.log(`Using row Name as cardholder: ${cardholderName}`);
                }

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
                    
                    // Return decline response without updating database
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
                        databaseUpdated: false
                    };
                }

                // Only proceed with database update if payment was successful (COMPLETED)
                if (captureStatus !== 'COMPLETED') {
                    console.log(`Bulk payment status is ${captureStatus} for documentId ${row._id}, not updating database`);
                    return {
                        documentId: row._id,
                        status: 'error',
                        error: `Payment status is ${captureStatus}. Expected COMPLETED.`,
                        response: jsonResponse,
                        databaseUpdated: false
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

        // Build base query (always exclude "Ready to charge" and "Partially charged")
        let query = {
            'Charge status': { $nin: ['Ready to charge', 'Partially charged'] }
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
            query['Charge status'] = status;
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
            ExcelData.distinct('Charge status', { 'Charge status': { $nin: ['Ready to charge', 'Partially charged'] } }),
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
