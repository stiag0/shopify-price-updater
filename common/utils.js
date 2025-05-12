const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse'); // Usar 'parse' de csv-parse
const { RateLimiter } = require('limiter');
const Logger = require('./logger'); 

const {
    SHOPIFY_RATE_LIMIT,
    API_TIMEOUT,
    MAX_RETRIES = 3, // Proporcionar un valor por defecto si no está en config
} = require('./config');

const shopifyLimiter = new RateLimiter({ tokensPerInterval: SHOPIFY_RATE_LIMIT, interval: 'second' });

function cleanSku(input) {
    if (input == null) return null;
    const s = String(input).trim().replace(/[^0-9]/g, '').replace(/^0+/, '');
    return s.length > 0 ? s : null;
}

async function fetchWithRetry(config, useShopifyLimiter = false, retries = MAX_RETRIES) {
    if (useShopifyLimiter) {
        try {
            await shopifyLimiter.removeTokens(1);
        } catch (limitError) {
            Logger.error("Error removing token from rate limiter", limitError);
            throw limitError;
        }
    }
    try {
        const response = await axios({ ...config, timeout: API_TIMEOUT });
        if (useShopifyLimiter && response.data && response.data.errors) {
            Logger.warn(`GraphQL errors in response from ${config.url}: ${JSON.stringify(response.data.errors)}`);
        }
        return response.data; // Devuelve solo response.data para consistencia con la versión anterior
    } catch (error) {
        const attempt = MAX_RETRIES - retries + 1;
        const statusCode = error.response?.status;
        const responseData = error.response?.data;
        const errorMessage = error.message || 'Unknown error';
        Logger.warn(`API Request failed (Attempt ${attempt}/${MAX_RETRIES}): ${config.method} ${config.url} - Status: ${statusCode || 'N/A'} - Message: ${errorMessage}`);
        if (responseData) { Logger.warn(`Response Data: ${JSON.stringify(responseData)}`); }

        if (retries > 0 && (statusCode === 429 || statusCode >= 500 || error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.code === 'ENOTFOUND' || error.code === 'ECONNRESET')) {
            const delay = Math.pow(2, MAX_RETRIES - retries) * 1000 + Math.random() * 1000;
            Logger.log(`Retrying in ${(delay / 1000).toFixed(1)}s... (${retries} retries left)`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return fetchWithRetry(config, useShopifyLimiter, retries - 1);
        } else {
            Logger.error(`API Request failed permanently after ${attempt} attempts: ${config.method} ${config.url}`, error);
            throw error;
        }
    }
}

async function loadDiscounts(csvPath) {
    return new Promise(async (resolve, reject) => {
        const discounts = new Map();
        let inputStream;
        const isUrl = /^https?:\/\//i.test(csvPath);

        if (isUrl) {
            try {
                Logger.log(`🌐 Descargando CSV de descuentos desde URL: ${csvPath}`);
                const resp = await axios.get(csvPath, { responseType: 'stream' });
                inputStream = resp.data;
            } catch (err) {
                Logger.error(`Error descargando CSV desde ${csvPath}`, err);
                return resolve(discounts); // Resuelve con mapa vacío en caso de error de descarga
            }
        } else {
            if (!fs.existsSync(csvPath)) {
                Logger.warn(`Archivo de descuentos no encontrado en ${csvPath}. Continuando sin descuentos.`);
                return resolve(discounts); // Resuelve con mapa vacío si el archivo local no existe
            }
            inputStream = fs.createReadStream(csvPath);
        }

        const parser = parse({
            columns: true, // Asume que la primera línea son los encabezados
            skip_empty_lines: true,
            trim: true,
        });

        inputStream.pipe(parser)
            .on('data', (row) => {
                // Asume que las columnas se llaman 'sku' y 'discount' en el CSV
                const skuValue = row.sku || row.SKU; // Intenta con 'sku' o 'SKU'
                const discountValue = row.discount || row.DISCOUNT || row.Discount; // Varias opciones para el nombre de la columna de descuento

                const cleanedSku = cleanSku(skuValue);
                const pct = parseFloat(discountValue);

                if (cleanedSku && !isNaN(pct) && pct >= 0 && pct <= 100) {
                    if (discounts.has(cleanedSku)) {
                        Logger.warn(`SKU duplicado en archivo de descuentos: ${cleanedSku}. Usando la última entrada.`);
                    }
                    discounts.set(cleanedSku, pct);
                } else {
                    Logger.warn(`Saltando fila inválida en archivo de descuentos: SKU='${skuValue}', Descuento='${discountValue}'`);
                }
            })
            .on('end', () => {
                Logger.log(`🗒️ Cargados ${discounts.size} descuentos válidos desde ${isUrl ? csvPath : path.basename(csvPath)}`);
                resolve(discounts);
            })
            .on('error', (err) => {
                Logger.error(`Error leyendo o parseando archivo de descuentos ${isUrl ? csvPath : path.basename(csvPath)}`, err);
                resolve(discounts); // Resuelve con mapa vacío en caso de error de parseo
            });
    });
}


module.exports = {
    cleanSku,
    fetchWithRetry,
    loadDiscounts,
    // shopifyLimiter // Ya no se exporta, se usa internamente en fetchWithRetry
};