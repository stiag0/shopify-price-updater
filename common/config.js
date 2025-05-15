require('dotenv').config();
const path = require('path');

/**
 * @fileoverview Archivo de configuración centralizado.
 * Carga variables de entorno y proporciona valores por defecto.
 * Para usar descuentos desde Google Drive, la variable de entorno DISCOUNT_CSV_PATH
 * debe contener la URL pública del archivo CSV publicado desde Google Drive.
 */

module.exports = {
    // Credenciales y URLs de Shopify
    SHOPIFY_SHOP_NAME: process.env.SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN: process.env.SHOPIFY_ACCESS_TOKEN,
    SHOPIFY_API_VERSION: process.env.SHOPIFY_API_VERSION || '2024-10', // Versión de API estable

    // URLs de APIs locales o de terceros para datos de productos e inventario
    DATA_API_URL: process.env.DATA_API_URL, // Para productos locales (precios)
    INVENTORY_API_URL: process.env.INVENTORY_API_URL, // Para inventario local

    /**
     * Ruta al archivo CSV de descuentos.
     * Puede ser una ruta local (ej: './discounts.csv') o una URL pública.
     * Para Google Drive: usa la URL generada al "Publicar en la web" como CSV.
     * Ejemplo en .env: DISCOUNT_CSV_PATH="https://docs.google.com/spreadsheets/d/e/YOUR_SHEET_ID/pub?gid=YOUR_GID&single=true&output=csv"
     */
    DISCOUNT_CSV_PATH: process.env.DISCOUNT_CSV_PATH,

    // Configuración de Logging
    LOG_FILE_PATH: process.env.LOG_FILE_PATH || path.join(__dirname, '..', 'logs', 'shopify-sync.log'), // Ajusta la ruta si 'common' está en un subdirectorio
    LOG_MAX_SIZE_MB: parseInt(process.env.LOG_MAX_SIZE_MB || '50', 10), // Default 50MB

    // Configuración de API y Red
    API_TIMEOUT: parseInt(process.env.API_TIMEOUT || '60000', 10), // Timeout para las solicitudes API en milisegundos
    SHOPIFY_RATE_LIMIT: parseInt(process.env.SHOPIFY_RATE_LIMIT || '2', 10), // Tokens por segundo para el limitador de tasa de Shopify
    MAX_RETRIES: parseInt(process.env.MAX_RETRIES || '3', 10), // Número máximo de reintentos para fetchWithRetry

    // SYNC_MODE y SYNC_TYPE (si se usan, deben ser manejados en el script principal o pasados como argumentos)
    // Ejemplo: SYNC_TYPE: process.env.SYNC_TYPE || 'both'
};
