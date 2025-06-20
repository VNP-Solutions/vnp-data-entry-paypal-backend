const mongoose = require('mongoose');
const ExcelData = require('../models/ExcelData');

/**
 * Migration script to handle existing duplicate records when transitioning to global duplicate prevention
 * This script identifies potential duplicate Portfolio + Reservation ID combinations across different users
 * and provides options for handling them.
 */

async function findExistingGlobalDuplicates() {
    try {
        
        // Aggregate to find Portfolio + Reservation ID combinations that exist multiple times
        const duplicates = await ExcelData.aggregate([
            {
                $match: {
                    'Portfolio': { $ne: null },
                    'Reservation ID': { $ne: null }
                }
            },
            {
                $group: {
                    _id: {
                        portfolio: '$Portfolio',
                        reservationId: '$Reservation ID'
                    },
                    count: { $sum: 1 },
                    records: {
                        $push: {
                            _id: '$_id',
                            userId: '$userId',
                            fileName: '$fileName',
                            uploadId: '$uploadId',
                            createdAt: '$createdAt',
                            expediaId: '$Expedia ID'
                        }
                    }
                }
            },
            {
                $match: {
                    count: { $gt: 1 }
                }
            },
            {
                $sort: { count: -1 }
            }
        ]);

        if (duplicates.length === 0) {
            return { duplicates: [], totalAffected: 0 };
        }
        
        let totalAffectedRecords = 0;
        const detailedDuplicates = duplicates.map(dup => {
            totalAffectedRecords += dup.count;
            return {
                portfolio: dup._id.portfolio,
                reservationId: dup._id.reservationId,
                totalCount: dup.count,
                records: dup.records.map(record => ({
                    id: record._id,
                    userId: record.userId,
                    fileName: record.fileName,
                    uploadId: record.uploadId,
                    createdAt: record.createdAt,
                    expediaId: record.expediaId
                }))
            };
        });

        return { duplicates: detailedDuplicates, totalAffected: totalAffectedRecords };
        
    } catch (error) {
        console.error('Error analyzing duplicates:', error);
        throw error;
    }
}

async function handleDuplicateStrategy(duplicates, strategy = 'keep_oldest') {
    if (duplicates.length === 0) {
        return;
    }

    let totalRemoved = 0;
    
    for (const duplicate of duplicates) {
        let recordsToKeep = [];
        let recordsToRemove = [];
        
        switch (strategy) {
            case 'keep_oldest':
                // Sort by creation date and keep the oldest
                const sortedByDate = duplicate.records.sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
                recordsToKeep = [sortedByDate[0]];
                recordsToRemove = sortedByDate.slice(1);
                break;
                
            case 'keep_newest':
                // Sort by creation date and keep the newest
                const sortedByDateDesc = duplicate.records.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
                recordsToKeep = [sortedByDateDesc[0]];
                recordsToRemove = sortedByDateDesc.slice(1);
                break;
                
            case 'manual_review':
                continue; // Skip automatic handling for manual review
                
            default:
                return;
        }
        
        if (recordsToRemove.length > 0) {
            // Remove duplicate records
            const idsToRemove = recordsToRemove.map(r => r.id);
            await ExcelData.deleteMany({ _id: { $in: idsToRemove } });
            
            totalRemoved += recordsToRemove.length;
        }
    }
    
}

// Main migration function
async function runMigration(strategy = 'keep_oldest') {
    try {
        const analysis = await findExistingGlobalDuplicates();
        
        if (analysis.totalAffected === 0) {
            return;
        }
        
        if (strategy !== 'manual_review') {
            await handleDuplicateStrategy(analysis.duplicates, strategy);
        }
        
    } catch (error) {
        console.error('❌ Migration failed:', error);
        throw error;
    }
}

module.exports = {
    findExistingGlobalDuplicates,
    handleDuplicateStrategy,
    runMigration
};

// If running directly
if (require.main === module) {
    const strategy = process.argv[2] || 'manual_review';
    
    mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/paypal')
    .then(() => {
        console.log('MongoDB connected for migration');
        return runMigration(strategy);
    })
    .then(() => {
        console.log('Migration completed successfully');
        process.exit(0);
    })
    .catch(error => {
        console.error('Migration failed:', error);
        process.exit(1);
    });
} 