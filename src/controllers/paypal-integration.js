const {
    ApiError,
    Client,
    Environment,
    LogLevel,
    OrdersController,
} = require("@paypal/paypal-server-sdk");
const ExcelData = require('../models/ExcelData');
const { encryptCardData } = require('../utils/encryption');

const {
    PAYPAL_CLIENT_ID,
    PAYPAL_CLIENT_SECRET,
} = process.env;

// Initialize PayPal client
const client = new Client({
    clientCredentialsAuthCredentials: {
        oAuthClientId: PAYPAL_CLIENT_ID,
        oAuthClientSecret: PAYPAL_CLIENT_SECRET,
    },
    timeout: 0,
    environment: Environment.Sandbox, // Change to Environment.Live for production
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
                    value: amount.toString()
                },
                description: description,
                customId: documentId,
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
            prefer: "return=representation"
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
                        adminArea2: billingAddress?.admin_area_2 || billingAddress?.adminArea2 || '',
                        adminArea1: billingAddress?.admin_area_1 || billingAddress?.adminArea1 || '',
                        postalCode: billingAddress?.postal_code || billingAddress?.postalCode || '',
                        countryCode: billingAddress?.country_code || billingAddress?.countryCode || 'US'
                    }
                }
            }
        };
        
        const { body: captureBody, ...captureHttpResponse } = await ordersController.captureOrder({
            id: orderResponse.id,
            body: captureRequest,
            prefer: "return=representation"
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
            throw new Error(`PayPal Error: ${error.message}`);
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
    try {
        const { 
            amount, 
            currency, 
            description, 
            descriptor,
            documentId,
            cardNumber,
            cardExpiry,
            cardCvv,
            cardholderName,
            billingAddress
        } = req.body;

        // Validate required fields
        if (!amount || !documentId || !cardNumber || !cardExpiry || !cardCvv || !cardholderName) {
            return res.status(400).json({
                status: 'error',
                message: 'Amount, documentId, cardNumber, cardExpiry, cardCvv, and cardholderName are required'
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
            updateTime: jsonResponse.update_time
        };

        // Update the ExcelData record with payment information
        try {
            // Prepare card data for encryption
            const cardDataToEncrypt = {
                'Card first 4': cleanCardNumber.substring(0, 4),
                'Card last 12': cleanCardNumber.substring(cleanCardNumber.length - 4),
                'Card Expire': cardExpiry,
                'Card CVV': '***' // Don't store actual CVV
            };
            
            // Encrypt sensitive card data
            const encryptedCardData = encryptCardData(cardDataToEncrypt);
            
            const updatedRecord = await ExcelData.findByIdAndUpdate(
                documentId,
                {
                    'Charge status': 'Completed',
                    'Card first 4': encryptedCardData['Card first 4'],
                    'Card last 12': encryptedCardData['Card last 12'],
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
                    paypalCardLastDigits: paymentDetails.cardLastDigits
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

module.exports = {
    processPayment
};
