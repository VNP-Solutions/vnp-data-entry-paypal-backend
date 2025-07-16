const {
    ApiError,
    Client,
    Environment,
    LogLevel,
    OrdersController,
} = require("@paypal/paypal-server-sdk");
const ExcelData = require('../models/ExcelData');
const { encryptCardData, decryptCardData } = require('../utils/encryption');

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
                    'Charge status': 'Charged',
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

        // Execute query with pagination
        const [excelData, totalCount] = await Promise.all([
            ExcelData.find(query)
                .sort(sortObj)
                .skip(skip)
                .limit(limitNum)
                .lean(), // Use lean() for better performance
            ExcelData.countDocuments(query)
        ]);

        // Calculate pagination info
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
    getAdminExcelData
};
