require('dotenv').config();

const config = {
  shopify: {
    shopName: process.env.SHOPIFY_SHOP_NAME,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    apiVersion: '2024-01',
    rateLimit: parseInt(process.env.SHOPIFY_RATE_LIMIT || '2', 10),
    batchSize: parseInt(process.env.SHOPIFY_BATCH_SIZE || '250', 10),
    maxRetries: parseInt(process.env.MAX_RETRIES || '3', 10),
    timeout: parseInt(process.env.API_TIMEOUT || '30000', 10),
  },
  apis: {
    dataUrl: process.env.DATA_API_URL,
    inventoryUrl: process.env.INVENTORY_API_URL,
  },
  sync: {
    mode: process.env.SYNC_MODE || 'shopify_first',
    type: process.env.SYNC_TYPE || 'both',
    locationId: process.env.LOCATION_ID,
  },
  discounts: {
    csvPath: process.env.DISCOUNT_CSV_PATH || 'discounts.csv',
  },
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    directory: process.env.LOG_DIR || 'logs',
    maxSize: parseInt(process.env.LOG_MAX_SIZE || '100', 10) * 1024 * 1024, // Convert MB to bytes
    maxFiles: parseInt(process.env.LOG_MAX_FILES || '5', 10),
  },
  cache: {
    ttl: parseInt(process.env.CACHE_TTL || '3600', 10), // 1 hour default
  }
};

// Validation
const requiredEnvVars = ['SHOPIFY_SHOP_NAME', 'SHOPIFY_ACCESS_TOKEN', 'DATA_API_URL', 'INVENTORY_API_URL'];
const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);

if (missingVars.length > 0) {
  throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
}

module.exports = config; 