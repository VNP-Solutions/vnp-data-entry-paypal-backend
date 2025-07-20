const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

/**
 * Create a Stripe Connect account
 * @param {Object} accountData - Account creation data
 * @returns {Object} - Stripe account object
 */
const createConnectAccount = async (accountData) => {
    const { 
        country = 'US', 
        email, 
        type = 'express' 
    } = accountData;

    try {
        const account = await stripe.accounts.create({
            country: country,
            email: email,
            type: 'express', // or 'standard'
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
        console.error('Stripe Account Creation Error:', error);
        throw error;
    }
};

/**
 * Create a Stripe Connect account with controller configuration
 * Use this after setting up platform profile in Stripe Dashboard
 */
const createManagedAccount = async (accountData) => {
    const { 
        country = 'US', 
        email, 
        type = 'express' 
    } = accountData;

    try {
        const account = await stripe.accounts.create({
            country: country,
            email: email,
            controller: {
                fees: {
                    payer: 'application',
                },
                losses: {
                    payments: 'application',
                },
                stripe_dashboard: {
                    type: type,
                },
            },
        });

        return account;
    } catch (error) {
        console.error('Stripe Managed Account Creation Error:', error);
        throw error;
    }
};

/**
 * Create Stripe Connect account endpoint
 * POST /api/stripe/create-account
 */
const createAccount = async (req, res) => {
    try {
        const { 
            country, 
            email, 
            type 
        } = req.body;

        // Validate required fields
        if (!email) {
            return res.status(400).json({
                status: 'error',
                message: 'Email is required'
            });
        }

        // Validate email format
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(email)) {
            return res.status(400).json({
                status: 'error',
                message: 'Please provide a valid email address'
            });
        }

        // Validate country code (if provided)
        if (country && country.length !== 2) {
            return res.status(400).json({
                status: 'error',
                message: 'Country code must be a valid 2-letter ISO code'
            });
        }

        // Validate dashboard type (if provided)
        const validTypes = ['express', 'full'];
        if (type && !validTypes.includes(type)) {
            return res.status(400).json({
                status: 'error',
                message: 'Type must be either "express" or "full"'
            });
        }

        const accountData = {
            country: country || 'US',
            email: email,
            type: type || 'express'
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
            controller: account.controller
        };

        // TODO: Save account information to database if needed
        // This would follow the same pattern as the PayPal controller
        // const savedAccount = await StripeAccount.create(accountDetails);

        res.status(200).json({
            status: 'success',
            message: 'Stripe Connect account created successfully',
            data: {
                account: accountDetails,
                // databaseSaved: true // Enable when database integration is added
            }
        });

    } catch (error) {
        console.error("Failed to create Stripe account:", error);
        
        // Handle specific Stripe errors
        if (error.type === 'StripeInvalidRequestError') {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid request to Stripe',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAPIError') {
            return res.status(502).json({
                status: 'error',
                message: 'Stripe API error',
                error: error.message
            });
        }
        
        if (error.type === 'StripeConnectionError') {
            return res.status(503).json({
                status: 'error',
                message: 'Unable to connect to Stripe',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAuthenticationError') {
            return res.status(401).json({
                status: 'error',
                message: 'Stripe authentication failed',
                error: error.message
            });
        }

        res.status(500).json({
            status: 'error',
            message: 'Failed to create Stripe account',
            error: error.message
        });
    }
};

/**
 * List all Stripe Connect accounts with search functionality
 * GET /api/stripe/accounts
 */
const listAccounts = async (req, res) => {
    try {
        const { 
            limit = 10, 
            page = 1,
            starting_after,
            search 
        } = req.query;

        // Validate limit parameter
        const numericLimit = parseInt(limit);
        if (isNaN(numericLimit) || numericLimit <= 0 || numericLimit > 100) {
            return res.status(400).json({
                status: 'error',
                message: 'Limit must be a valid number between 1 and 100'
            });
        }

        // Validate page parameter
        const currentPage = parseInt(page);
        if (isNaN(currentPage) || currentPage < 1) {
            return res.status(400).json({
                status: 'error',
                message: 'Page must be a valid number starting from 1'
            });
        }

        // Build Stripe list parameters
        const listParams = {
            limit: numericLimit
        };

        // Add cursor-based pagination if starting_after is provided
        if (starting_after) {
            listParams.starting_after = starting_after;
        }


        // Get accounts from Stripe (fetch more records to handle pagination properly)
        const accounts = await stripe.accounts.list({
            limit: 100 // Get more records to handle pagination and search
        });

        // If search is provided, filter the results by email and business profile name
        let filteredData = accounts.data;
        if (search) {
            const searchTerm = search.toLowerCase();
            filteredData = accounts.data.filter(account => {
                // Search in email
                const emailMatch = account.email && account.email.toLowerCase().includes(searchTerm);
                
                // Search in business profile name
                const businessNameMatch = account.business_profile && 
                                        account.business_profile.name && 
                                        account.business_profile.name.toLowerCase().includes(searchTerm);
                
                return emailMatch || businessNameMatch;
            });
        }

        // Calculate pagination
        const totalFilteredCount = filteredData.length;
        const totalPages = totalFilteredCount > 0 ? Math.ceil(totalFilteredCount / numericLimit) : 1;
        const startIndex = (currentPage - 1) * numericLimit;
        const endIndex = startIndex + numericLimit;
        
        // Check if page number exceeds available data
        if (totalFilteredCount > 0 && currentPage > totalPages) {
            return res.status(400).json({
                status: 'error',
                message: `Page ${currentPage} does not exist. Total pages available: ${totalPages}`,
                pagination: {
                    current_page: currentPage,
                    total_pages: totalPages,
                    total_filtered_count: totalFilteredCount
                }
            });
        }
        
        // Get the data for current page
        const pageData = filteredData.slice(startIndex, endIndex);
        
        // Create paginated response
        const filteredAccounts = {
            object: accounts.object,
            url: accounts.url,
            has_more: currentPage < totalPages,
            data: pageData
        };

        // Transform accounts data for consistent response
        const transformedAccounts = {
            object: filteredAccounts.object,
            url: filteredAccounts.url,
            data: filteredAccounts.data.map(account => ({
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
                type: account.type
            }))
        };

        res.status(200).json({
            status: 'success',
            message: 'Stripe accounts retrieved successfully',
            data: transformedAccounts,
            pagination: {
                current_page: currentPage,
                limit: numericLimit,
                has_more: filteredAccounts.has_more,
                total_count: filteredAccounts.data.length,
                total_filtered_count: totalFilteredCount,
                total_pages: totalPages,
                next_page: filteredAccounts.has_more ? currentPage + 1 : null,
                previous_page: currentPage > 1 ? currentPage - 1 : null,
                last_id: filteredAccounts.data.length > 0 ? filteredAccounts.data[filteredAccounts.data.length - 1].id : null
            },
            filters: {
                applied: {
                    search: search || null
                }
            }
        });

    } catch (error) {
        console.error("Failed to list Stripe accounts:", error);
        
        // Handle specific Stripe errors
        if (error.type === 'StripeInvalidRequestError') {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid request parameters',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAPIError') {
            return res.status(502).json({
                status: 'error',
                message: 'Stripe API error',
                error: error.message
            });
        }
        
        if (error.type === 'StripeConnectionError') {
            return res.status(503).json({
                status: 'error',
                message: 'Unable to connect to Stripe',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAuthenticationError') {
            return res.status(401).json({
                status: 'error',
                message: 'Stripe authentication failed',
                error: error.message
            });
        }

        res.status(500).json({
            status: 'error',
            message: 'Failed to retrieve Stripe accounts',
            error: error.message
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
                status: 'error',
                message: 'Account ID is required'
            });
        }

        // Validate account ID format (basic check)
        if (!accountId.startsWith('acct_')) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid account ID format'
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
            type: account.type
        };

        res.status(200).json({
            status: 'success',
            message: 'Account details retrieved successfully',
            data: {
                account: accountDetails
            }
        });

    } catch (error) {
        console.error("Failed to retrieve Stripe account:", error);
        
        // Handle specific Stripe errors
        if (error.type === 'StripeInvalidRequestError') {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid account ID or account not found',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAPIError') {
            return res.status(502).json({
                status: 'error',
                message: 'Stripe API error',
                error: error.message
            });
        }
        
        if (error.type === 'StripeConnectionError') {
            return res.status(503).json({
                status: 'error',
                message: 'Unable to connect to Stripe',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAuthenticationError') {
            return res.status(401).json({
                status: 'error',
                message: 'Stripe authentication failed',
                error: error.message
            });
        }

        res.status(500).json({
            status: 'error',
            message: 'Failed to retrieve account details',
            error: error.message
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
                status: 'error',
                message: 'Account ID is required'
            });
        }

        // Validate account ID format (basic check)
        if (!accountId.startsWith('acct_')) {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid account ID format'
            });
        }

        // Delete account from Stripe
        const deleted = await stripe.accounts.del(accountId);

        res.status(200).json({
            status: 'success',
            message: 'Account deleted successfully',
            data: {
                deleted: deleted.deleted,
                id: deleted.id,
                object: deleted.object
            }
        });

    } catch (error) {
        console.error("Failed to delete Stripe account:", error);
        
        // Handle specific Stripe errors
        if (error.type === 'StripeInvalidRequestError') {
            return res.status(400).json({
                status: 'error',
                message: 'Invalid account ID or account not found',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAPIError') {
            return res.status(502).json({
                status: 'error',
                message: 'Stripe API error',
                error: error.message
            });
        }
        
        if (error.type === 'StripeConnectionError') {
            return res.status(503).json({
                status: 'error',
                message: 'Unable to connect to Stripe',
                error: error.message
            });
        }
        
        if (error.type === 'StripeAuthenticationError') {
            return res.status(401).json({
                status: 'error',
                message: 'Stripe authentication failed',
                error: error.message
            });
        }

        res.status(500).json({
            status: 'error',
            message: 'Failed to delete account',
            error: error.message
        });
    }
};

module.exports = {
    createAccount,
    listAccounts,
    getAccountById,
    deleteAccount
}; 