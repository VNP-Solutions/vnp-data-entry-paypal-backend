const OTA = require('../models/OTA');

/**
 * Get all OTAs
 * GET /api/ota
 */
const getAllOTAs = async (req, res) => {
    try {
        const { isActive } = req.query;
        
        let filter = {};
        if (isActive !== undefined) {
            filter.isActive = isActive === 'true';
        }

        const otas = await OTA.find(filter).sort({ name: 1 });

        res.status(200).json({
            status: 'success',
            message: 'OTAs retrieved successfully',
            data: otas,
            count: otas.length
        });
    } catch (error) {
        console.error('Error fetching OTAs:', error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to fetch OTAs',
            error: error.message
        });
    }
};

/**
 * Get OTA by ID
 * GET /api/ota/:id
 */
const getOTAById = async (req, res) => {
    try {
        const { id } = req.params;

        const ota = await OTA.findById(id);

        if (!ota) {
            return res.status(404).json({
                status: 'error',
                message: 'OTA not found'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'OTA retrieved successfully',
            data: ota
        });
    } catch (error) {
        console.error('Error fetching OTA:', error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to fetch OTA',
            error: error.message
        });
    }
};

/**
 * Get OTA by name
 * GET /api/ota/name/:name
 */
const getOTAByName = async (req, res) => {
    try {
        const { name } = req.params;

        const ota = await OTA.findOne({ name: name, isActive: true });

        if (!ota) {
            return res.status(404).json({
                status: 'error',
                message: 'OTA not found'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'OTA retrieved successfully',
            data: ota
        });
    } catch (error) {
        console.error('Error fetching OTA by name:', error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to fetch OTA',
            error: error.message
        });
    }
};

/**
 * Create new OTA
 * POST /api/ota
 */
const createOTA = async (req, res) => {
    try {
        const {
            name,
            displayName,
            customer,
            billingAddress,
            isActive = true
        } = req.body;

        // Validation
        if (!name || !displayName || !customer || !billingAddress) {
            return res.status(400).json({
                status: 'error',
                message: 'Name, displayName, customer, and billingAddress are required'
            });
        }

        if (!billingAddress.zipCode || !billingAddress.countryCode) {
            return res.status(400).json({
                status: 'error',
                message: 'Billing address must include zipCode and countryCode'
            });
        }

        // Check if OTA already exists
        const existingOTA = await OTA.findOne({ name });
        if (existingOTA) {
            return res.status(400).json({
                status: 'error',
                message: 'OTA with this name already exists'
            });
        }

        const newOTA = new OTA({
            name,
            displayName,
            customer,
            billingAddress,
            isActive
        });

        const savedOTA = await newOTA.save();

        res.status(201).json({
            status: 'success',
            message: 'OTA created successfully',
            data: savedOTA
        });
    } catch (error) {
        console.error('Error creating OTA:', error);
        
        if (error.code === 11000) {
            return res.status(400).json({
                status: 'error',
                message: 'OTA with this name already exists'
            });
        }

        res.status(500).json({
            status: 'error',
            message: 'Failed to create OTA',
            error: error.message
        });
    }
};

/**
 * Update OTA
 * PUT /api/ota/:id
 */
const updateOTA = async (req, res) => {
    try {
        const { id } = req.params;
        const updateData = req.body;

        // Remove fields that shouldn't be updated directly
        delete updateData._id;
        delete updateData.createdAt;

        const updatedOTA = await OTA.findByIdAndUpdate(
            id,
            { ...updateData, updatedAt: Date.now() },
            { new: true, runValidators: true }
        );

        if (!updatedOTA) {
            return res.status(404).json({
                status: 'error',
                message: 'OTA not found'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'OTA updated successfully',
            data: updatedOTA
        });
    } catch (error) {
        console.error('Error updating OTA:', error);

        if (error.code === 11000) {
            return res.status(400).json({
                status: 'error',
                message: 'OTA with this name already exists'
            });
        }

        res.status(500).json({
            status: 'error',
            message: 'Failed to update OTA',
            error: error.message
        });
    }
};

/**
 * Delete OTA (soft delete by setting isActive to false)
 * DELETE /api/ota/:id
 */
const deleteOTA = async (req, res) => {
    try {
        const { id } = req.params;
        const { permanent = false } = req.query;

        if (permanent === 'true') {
            // Hard delete
            const deletedOTA = await OTA.findByIdAndDelete(id);
            
            if (!deletedOTA) {
                return res.status(404).json({
                    status: 'error',
                    message: 'OTA not found'
                });
            }

            res.status(200).json({
                status: 'success',
                message: 'OTA permanently deleted'
            });
        } else {
            // Soft delete
            const updatedOTA = await OTA.findByIdAndUpdate(
                id,
                { isActive: false, updatedAt: Date.now() },
                { new: true }
            );

            if (!updatedOTA) {
                return res.status(404).json({
                    status: 'error',
                    message: 'OTA not found'
                });
            }

            res.status(200).json({
                status: 'success',
                message: 'OTA deactivated successfully',
                data: updatedOTA
            });
        }
    } catch (error) {
        console.error('Error deleting OTA:', error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to delete OTA',
            error: error.message
        });
    }
};

/**
 * Restore OTA (set isActive to true)
 * PATCH /api/ota/:id/restore
 */
const restoreOTA = async (req, res) => {
    try {
        const { id } = req.params;

        const restoredOTA = await OTA.findByIdAndUpdate(
            id,
            { isActive: true, updatedAt: Date.now() },
            { new: true }
        );

        if (!restoredOTA) {
            return res.status(404).json({
                status: 'error',
                message: 'OTA not found'
            });
        }

        res.status(200).json({
            status: 'success',
            message: 'OTA restored successfully',
            data: restoredOTA
        });
    } catch (error) {
        console.error('Error restoring OTA:', error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to restore OTA',
            error: error.message
        });
    }
};

/**
 * Seed initial OTA data
 * POST /api/ota/seed
 */
const seedOTAData = async (req, res) => {
    try {
        // Check if data already exists
        const existingCount = await OTA.countDocuments();
        if (existingCount > 0) {
            return res.status(400).json({
                status: 'error',
                message: 'OTA data already exists. Clear existing data first if you want to reseed.'
            });
        }

        const seedData = [
            {
                name: 'Agoda',
                displayName: 'Agoda',
                customer: 'Agoda Company Pte Ltd.',
                billingAddress: {
                    zipCode: '80525',
                    countryCode: 'US',
                    addressLine1: '',
                    addressLine2: '',
                    city: '',
                    state: ''
                }
            },
            {
                name: 'Expedia',
                displayName: 'Expedia',
                customer: 'Expedia Group',
                billingAddress: {
                    zipCode: '98119',
                    countryCode: 'US',
                    addressLine1: '',
                    addressLine2: '',
                    city: '',
                    state: ''
                }
            },
            {
                name: 'Booking.com',
                displayName: 'Booking.com',
                customer: 'Booking.com',
                billingAddress: {
                    zipCode: '10118',
                    countryCode: 'US',
                    addressLine1: '',
                    addressLine2: '',
                    city: '',
                    state: ''
                }
            }
        ];

        const createdOTAs = await OTA.insertMany(seedData);

        res.status(201).json({
            status: 'success',
            message: 'OTA seed data created successfully',
            data: createdOTAs,
            count: createdOTAs.length
        });
    } catch (error) {
        console.error('Error seeding OTA data:', error);
        res.status(500).json({
            status: 'error',
            message: 'Failed to seed OTA data',
            error: error.message
        });
    }
};

module.exports = {
    getAllOTAs,
    getOTAById,
    getOTAByName,
    createOTA,
    updateOTA,
    deleteOTA,
    restoreOTA,
    seedOTAData
}; 