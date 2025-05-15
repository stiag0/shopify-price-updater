require('dotenv').config();
const path = require('path');

module.exports = {
    SHOPIFY_SHOP_NAME: process.env.SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN: process.env.SHOPIFY_ACCESS_TOKEN,
    DATA_API_URL: process.env.DATA_API_URL, // Para productos locales
    INVENTORY_API_URL: process.env.INVENTORY_API_URL, // Para inventario local
    DISCOUNT_CSV_PATH: process.env.DISCOUNT_CSV_PATH, // URL pública del CSV de descuentos  Ejemplo en .env: DISCOUNT_CSV_PATH="https://docs.google.com/spreadsheets/d/e/YOUR_SHEET_ID/pub?gid=YOUR_GID&single=true&output=csv"

    LOG_FILE_PATH: process.env.LOG_FILE_PATH || path.join(__dirname, '..', 'logs', 'shopify-sync.log'), // Ajusta la ruta si 'common' está en un subdirectorio
    LOG_MAX_SIZE_MB: parseInt(process.env.LOG_MAX_SIZE_MB || '50', 10), // Default 50MB
    
    API_TIMEOUT: parseInt(process.env.API_TIMEOUT || '60000', 10),
    SHOPIFY_RATE_LIMIT: parseInt(process.env.SHOPIFY_RATE_LIMIT || '2', 10),
    SHOPIFY_API_VERSION: process.env.SHOPIFY_API_VERSION || '2024-10', // Versión de API estable

    // SYNC_MODE ya no se usa directamente aquí, se puede pasar como argumento al script principal si es necesario
    // SYNC_TYPE también se puede manejar en el script principal
};