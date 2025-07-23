const {
    ApiError,
    Client,
    Environment,
    LogLevel,
    OrdersController,
} = require("@paypal/paypal-server-sdk");
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

// Initialize PayPal client
const client = new Client({
    clientCredentialsAuthCredentials: {
        oAuthClientId: PAYPAL_CLIENT_ID,
        oAuthClientSecret: PAYPAL_CLIENT_SECRET,
    },
    timeout: 0,
    environment: Environment.Production, // Change to Environment.Production for production
    logging: {
        logLevel: LogLevel.Info,
        logRequest: { logBody: true },
        logResponse: { logHeaders: true },
    },
});

const ordersController = new OrdersController(client);

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

    // Parse card expiry (format: "2025-12" -> month: "12", year: "2025")
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
        purchaseUnits: [
            {
                amount: {
                    currencyCode: currency,
                    value: amount.toString(),
                    breakdown: {
                        itemTotal: {
                            currencyCode: currency,
                            value: amount.toString()
                        }
                    }
                },
                description: description,
                customId: documentId ? documentId.toString() : undefined,
                items: [
                    {
                        name: description || "Payment for services",
                        description: `Payment processing for ${cardholderName || 'customer'}`,
                        quantity: "1",
                        unitAmount: {
                            currencyCode: currency,
                            value: amount.toString()
                        },
                        category: "DIGITAL_GOODS"
                    }
                ],
                ...(descriptor && {
                    softDescriptor: descriptor
                })
            }
        ]
    };

    try {
        // Create the order without payment source first
        const { body: orderBody, ...orderHttpResponse } = await ordersController.createOrder({
            body: requestBody,
            prefer: "return=representation",
            payPalPartnerAttributionId: PAYPAL_BN_CODE,
            payPalRequestId: `order-${documentId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
        });
        
        const orderResponse = JSON.parse(orderBody);
        
        // Now capture the payment with card details
        const captureRequest = {
            paymentSource: {
                card: {
                    number: cardNumber.replace(/\s/g, ''),
                    expiry: cardExpiry,
                    securityCode: cardCvv,
                    name: cardholderName,
                    billingAddress: {
                        addressLine1: billingAddress?.address_line_1 || billingAddress?.addressLine1 || '',
                        addressLine2: billingAddress?.address_line_2 || billingAddress?.addressLine2 || '',
                        adminArea2: billingAddress?.admin_area_2 || billingAddress?.adminArea2 || billingAddress?.city || '',
                        adminArea1: billingAddress?.admin_area_1 || billingAddress?.adminArea1 || billingAddress?.state || '',
                        postalCode: billingAddress?.postal_code || billingAddress?.postalCode || billingAddress?.zipCode || '',
                        countryCode: billingAddress?.country_code || billingAddress?.countryCode || 'US'
                    }
                }
            }
        };
        
        const { body: captureBody, ...captureHttpResponse } = await ordersController.captureOrder({
            id: orderResponse.id,
            body: captureRequest,
            prefer: "return=representation",
            payPalPartnerAttributionId: PAYPAL_BN_CODE,
            payPalRequestId: `capture-${orderResponse.id}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
        });
        
        const captureResponse = JSON.parse(captureBody);
        
        return {
            jsonResponse: captureResponse,
            httpStatusCode: captureHttpResponse.statusCode,
        };
    } catch (error) {
        if (error instanceof ApiError) {
            console.error('PayPal API Error:', error.message);
            console.error('PayPal Error Details:', error.result);
            
            // Extract field and description from error details
            let errorMessage = 'PayPal Payment Error';
            
            if (error.result && error.result.details && Array.isArray(error.result.details)) {
                const errorDetails = error.result.details.map(detail => ({
                    field: detail.field || 'Unknown field',
                    description: detail.description || 'Unknown error'
                }));
                
                // Format error message with field and description
                const formattedErrors = errorDetails.map(detail => 
                    `${detail.field}: ${detail.description}`
                ).join('; ');
                
                errorMessage = `PayPal Error - ${formattedErrors}`;
            } else {
                errorMessage = `PayPal Error: ${error.message}`;
            }
            
            throw new Error(errorMessage);
        }
        console.error('General Error:', error);
        throw error;
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

        // Validation for required fields
        if (!amount || !cardNumber || !cardExpiry || !cardCvv || !cardholderName) {
            return res.status(400).json({
                status: 'error',
                message: 'Amount, card details, and cardholder name are required'
            });
        }

        // Validate amount is a valid number
        const numericAmount = parseFloat(amount);
        if (isNaN(numericAmount) || numericAmount <= 0) {
            return res.status(400).json({
                status: 'error',
                message: 'Amount must be a valid positive number'
            });
        }

        // Validate card number (basic validation)
        const cleanCardNumber = cardNumber.replace(/\s/g, '');
        if (cleanCardNumber.length < 13 || cleanCardNumber.length > 19) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid card number'
            });
        }

        // Validate CVV
        if (cardCvv.length < 3 || cardCvv.length > 4) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid CVV'
            });
        }

        const paymentData = {
            amount: numericAmount,
            currency: currency || "USD",
            description: description || "Payment for services",
            descriptor: descriptor,
            documentId: documentId,
            cardNumber: cleanCardNumber,
            cardExpiry: cardExpiry,
            cardCvv: cardCvv,
            cardholderName: cardholderName,
            billingAddress: billingAddress
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
            const getDeclineReason = (code) => {
                const declineCodes = {
                    '0500': 'Transaction declined by issuing bank',
                    '0510': 'Invalid card number',
                    '0520': 'Invalid expiration date',
                    '0530': 'Invalid CVV code',
                    '0540': 'Insufficient funds',
                    '0550': 'Card expired',
                    '0560': 'Card restricted',
                    '0570': 'Transaction not permitted',
                    '0580': 'Invalid merchant',
                    '0590': 'Transaction amount exceeds limit'
                };
                return declineCodes[code] || 'Payment declined by card issuer';
            };
            
            const declineReason = getDeclineReason(responseCode);
            
            // Additional context based on AVS/CVV codes
            let additionalInfo = '';
            if (avsCode && avsCode !== 'Y') {
                additionalInfo += ' Address verification failed.';
            }
            if (cvvCode && cvvCode !== 'M') {
                additionalInfo += ' CVV verification failed.';
            }
            
            console.log(`Payment declined for documentId ${documentId}: ${declineReason}`);
            
            // Return decline response without updating database
            return res.status(400).json({
                status: 'declined',
                message: 'Payment was declined',
                data: {
                    ...jsonResponse,
                    paymentDetails: paymentDetails,
                    declineReason: declineReason + additionalInfo,
                    responseCode: responseCode,
                    avsCode: avsCode,
                    cvvCode: cvvCode,
                    databaseUpdated: false
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
                'Card Expire': cardExpiry,
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
        res.status(500).json({
            status: 'error',
            message: 'Failed to process payment',
            error: error.message
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

                const paymentData = {
                    amount: row['Amount to charge'],
                    currency: row['Curency'] || 'USD',
                    description: 'Bulk payment',
                    descriptor: row['Soft Descriptor'],
                    documentId: row._id,
                    cardNumber: decrypted['Card Number'],
                    cardExpiry: decrypted['Card Expire'],
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
                    const getDeclineReason = (code) => {
                        const declineCodes = {
                            '0500': 'Transaction declined by issuing bank',
                            '0510': 'Invalid card number',
                            '0520': 'Invalid expiration date',
                            '0530': 'Invalid CVV code',
                            '0540': 'Insufficient funds',
                            '0550': 'Card expired',
                            '0560': 'Card restricted',
                            '0570': 'Transaction not permitted',
                            '0580': 'Invalid merchant',
                            '0590': 'Transaction amount exceeds limit'
                        };
                        return declineCodes[code] || 'Payment declined by card issuer';
                    };
                    
                    const declineReason = getDeclineReason(responseCode);
                    
                    // Additional context based on AVS/CVV codes
                    let additionalInfo = '';
                    if (avsCode && avsCode !== 'Y') {
                        additionalInfo += ' Address verification failed.';
                    }
                    if (cvvCode && cvvCode !== 'M') {
                        additionalInfo += ' CVV verification failed.';
                    }
                    
                    console.log(`Bulk payment declined for documentId ${row._id}: ${declineReason}`);
                    
                    // Return decline response without updating database
                    return {
                        documentId: row._id,
                        status: 'declined',
                        declineReason: declineReason + additionalInfo,
                        responseCode: responseCode,
                        avsCode: avsCode,
                        cvvCode: cvvCode,
                        response: jsonResponse,
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
                        'Card Expire': decrypted['Card Expire'],
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
                return {
                    documentId: row._id,
                    status: 'error',
                    error: err.message,
                    stack: err.stack
                };
            }
        })));
        // Add summary stats
        const summary = {
            total: results.length,
            success: results.filter(r => r.status === 'success').length,
            error: results.filter(r => r.status === 'error').length,
            declined: results.filter(r => r.status === 'declined').length
        };
        res.status(200).json({ status: 'success', summary, results });
    } catch (error) {
        console.error('Bulk PayPal payment error:', error);
        res.status(500).json({ status: 'error', message: 'Bulk payment failed', error: error.message });
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
                const errorDetails = errorData.details.map(detail => ({
                    field: detail.field || 'Unknown field',
                    description: detail.description || 'Unknown error'
                }));
                
                const formattedErrors = errorDetails.map(detail => 
                    `${detail.field}: ${detail.description}`
                ).join('; ');
                
                errorMessage = `PayPal Refund Error - ${formattedErrors}`;
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
                return {
                    documentId: row._id,
                    status: 'error',
                    error: err.message,
                    stack: err.stack
                };
            }
        })));

        // Add summary stats
        const summary = {
            total: results.length,
            success: results.filter(r => r.status === 'success').length,
            error: results.filter(r => r.status === 'error').length,
            refundType: refundType,
            totalRefundAmount: results
                .filter(r => r.status === 'success')
                .reduce((sum, r) => sum + parseFloat(r.refund?.amount || 0), 0)
        };

        res.status(200).json({ 
            status: 'success', 
            message: 'Bulk refunds processed',
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
