const crypto = require('crypto');

// Encryption configuration
const algorithm = 'aes-256-cbc';
const secretKey = process.env.ENCRYPTION_KEY; // 32 characters
const ivLength = 16; // For AES, this is always 16

// Ensure we have a proper 32-byte key
const getKey = () => {
    if (!secretKey) {
        throw new Error('ENCRYPTION_KEY environment variable is required');
    }
    
    // If the key is exactly 32 bytes, use it directly
    if (secretKey.length === 32) {
        return Buffer.from(secretKey, 'utf8');
    }
    
    // Otherwise, derive a 32-byte key using PBKDF2
    return crypto.pbkdf2Sync(secretKey, 'salt', 10000, 32, 'sha256');
};

// Decrypt using old method (for backward compatibility)
const decryptLegacy = (text) => {
    try {
        const textParts = text.split(':');
        const encryptedText = textParts.join(':');
        const decipher = crypto.createDecipher(algorithm, secretKey);
        let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        return decrypted;
    } catch (error) {
        return null;
    }
};

// Decrypt using new method
const decryptModern = (text) => {
    try {
        const key = getKey();
        const textParts = text.split(':');
        
        if (textParts.length !== 2) {
            return null;
        }
        
        const iv = Buffer.from(textParts[0], 'hex');
        const encryptedText = textParts[1];
        
        const decipher = crypto.createDecipheriv(algorithm, key, iv);
        let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        
        return decrypted;
    } catch (error) {
        return null;
    }
};

// Encrypt sensitive data (always uses new method)
const encrypt = (text) => {
    if (!text) return null;
    
    try {
        const key = getKey();
        const iv = crypto.randomBytes(ivLength);
        const cipher = crypto.createCipheriv(algorithm, key, iv);
        
        let encrypted = cipher.update(text, 'utf8', 'hex');
        encrypted += cipher.final('hex');
        
        // Return IV and encrypted data separated by colon
        return iv.toString('hex') + ':' + encrypted;
    } catch (error) {
        console.error('Encryption error:', error);
        return null;
    }
};

// Backward-compatible decrypt function
const decrypt = (text) => {
    if (!text) return null;
    
    // Try modern decryption first
    let decrypted = decryptModern(text);
    if (decrypted !== null) {
        return decrypted;
    }
    
    // If modern decryption fails, try legacy decryption
    decrypted = decryptLegacy(text);
    if (decrypted !== null) {
        // Successfully decrypted with legacy method
        // Optionally, you could re-encrypt with modern method here
        return decrypted;
    }
    
    console.error('Decryption failed with both methods for text:', text.substring(0, 20) + '...');
    return null;
};

// Encrypt card-related fields
const encryptCardData = (data) => {
    const encryptedData = { ...data };
    
    // Encrypt sensitive card fields
    const cardFields = [
        'Card Number',
        'Card Expire',
        'Card CVV'
    ];
    
    cardFields.forEach(field => {
        if (encryptedData[field]) {
            encryptedData[field] = encrypt(encryptedData[field]);
        }
    });
    
    return encryptedData;
};

// Decrypt card-related fields
const decryptCardData = (data) => {
    const decryptedData = { ...data };
    
    // Decrypt sensitive card fields
    const cardFields = [
        'Card Number',
        'Card Expire',
        'Card CVV'
    ];
    
    cardFields.forEach(field => {
        if (decryptedData[field]) {
            decryptedData[field] = decrypt(decryptedData[field]);
        }
    });
    
    return decryptedData;
};

module.exports = {
    encrypt,
    decrypt,
    encryptCardData,
    decryptCardData
}; 