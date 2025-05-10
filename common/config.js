require('dotenv').config();
module.exports = {
  SHOPIFY_SHOP_NAME: process.env.SHOPIFY_SHOP_NAME,
  SHOPIFY_ACCESS_TOKEN: process.env.SHOPIFY_ACCESS_TOKEN,
  DATA_API_URL:      process.env.DATA_API_URL,
  INVENTORY_API_URL: process.env.INVENTORY_API_URL,
  DISCOUNT_CSV_PATH: process.env.DISCOUNT_CSV_PATH || 'discounts.csv',
  API_TIMEOUT: parseInt(process.env.API_TIMEOUT || '60000', 10),
  SHOPIFY_RATE_LIMIT: parseInt(process.env.SHOPIFY_RATE_LIMIT || '2', 10),
  SHOPIFY_API_VERSION: process.env.SHOPIFY_API_VERSION || '2024-10',
};
