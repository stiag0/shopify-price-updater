const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse'); // Usar 'parse' de csv-parse
const { RateLimiter } = require('limiter');
const Logger = require('./logger'); // Asumiendo que logger.js está en el mismo directorio 'common'

// Cargar configuración. Asegúrate que la ruta a config.js sea correcta
// Si utils.js está en 'common', y config.js también, sería './config'
const {
    SHOPIFY_RATE_LIMIT,
    API_TIMEOUT,
    MAX_RETRIES, // Asegúrate que MAX_RETRIES esté definido en config.js
} = require('./config');

// Inicializar el limitador de tasa para las APIs de Shopify
const shopifyLimiter = new RateLimiter({ tokensPerInterval: SHOPIFY_RATE_LIMIT, interval: 'second' });

/**
 * Limpia un SKU eliminando caracteres no numéricos y ceros a la izquierda.
 * @param {*} input El SKU a limpiar.
 * @returns {string|null} El SKU limpio o null si es inválido.
 */
function cleanSku(input) {
    if (input == null) return null;
    // Convertir a string, quitar espacios, caracteres no numéricos y ceros iniciales
    const s = String(input).trim().replace(/[^0-9]/g, '').replace(/^0+/, '');
    return s.length > 0 ? s : null;
}

/**
 * Realiza una solicitud HTTP con reintentos en caso de ciertos errores.
 * @param {object} config Configuración de Axios para la solicitud.
 * @param {boolean} [useShopifyLimiter=false] Si se debe aplicar el limitador de tasa de Shopify.
 * @param {number} [retries=MAX_RETRIES] Número de reintentos restantes.
 * @returns {Promise<object>} La data de la respuesta.
 * @throws {Error} Si la solicitud falla después de todos los reintentos.
 */
async function fetchWithRetry(config, useShopifyLimiter = false, retries = MAX_RETRIES) {
    if (useShopifyLimiter) {
        try {
            await shopifyLimiter.removeTokens(1); // Esperar a que haya un token disponible
        } catch (limitError) {
            Logger.error("Error al consumir token del limitador de tasa de Shopify", limitError);
            // Podrías optar por esperar un poco y reintentar la obtención del token,
            // o simplemente lanzar el error para que el reintento de fetchWithRetry lo maneje.
            throw limitError; // Esto hará que fetchWithRetry lo catalogue como un error y reintente si es aplicable.
        }
    }

    try {
        const response = await axios({ ...config, timeout: API_TIMEOUT });
        // Si hay errores de GraphQL en una respuesta exitosa (status 200), registrarlos.
        if (useShopifyLimiter && response.data && response.data.errors) {
            Logger.warn(`Errores GraphQL en la respuesta de ${config.url}: ${JSON.stringify(response.data.errors)}`);
        }
        return response.data;
    } catch (error) {
        const attempt = (MAX_RETRIES || 3) - retries + 1; // Usar valor por defecto si MAX_RETRIES no está cargado
        const maxRetriesToLog = MAX_RETRIES || 3;
        const statusCode = error.response?.status;
        const responseData = error.response?.data;
        const errorMessage = error.message || 'Error desconocido';

        Logger.warn(`Fallo en solicitud API (Intento ${attempt}/${maxRetriesToLog}): ${config.method} ${config.url} - Estado: ${statusCode || 'N/A'} - Mensaje: ${errorMessage}`);
        if (responseData) { Logger.warn(`Datos de respuesta del error: ${JSON.stringify(responseData)}`); }

        // Condiciones para reintentar
        if (retries > 0 && (statusCode === 429 || statusCode >= 500 || error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.code === 'ENOTFOUND' || error.code === 'ECONNRESET')) {
            const delay = Math.pow(2, (MAX_RETRIES || 3) - retries) * 1000 + Math.random() * 1000; // Backoff exponencial
            Logger.log(`Reintentando en ${(delay / 1000).toFixed(1)}s... (${retries} reintentos restantes)`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return fetchWithRetry(config, useShopifyLimiter, retries - 1);
        } else {
            Logger.error(`Fallo permanente en solicitud API después de ${attempt} intentos: ${config.method} ${config.url}`, error);
            throw error; // Lanzar el error para ser manejado por la función llamante
        }
    }
}

/**
 * Carga los descuentos desde un archivo CSV, ya sea local o desde una URL.
 * @param {string} csvPath Ruta local o URL al archivo CSV.
 * @returns {Promise<Map<string, number>>} Un mapa con SKU como clave y porcentaje de descuento como valor.
 * Retorna un mapa vacío si hay errores o el archivo no se encuentra.
 */
async function loadDiscounts(csvPath) {
    return new Promise(async (resolve, reject) => {
        const discounts = new Map();
        if (!csvPath) {
            Logger.warn("No se proporcionó DISCOUNT_CSV_PATH. Continuando sin descuentos.");
            return resolve(discounts);
        }

        let inputStream;
        const isUrl = /^https?:\/\//i.test(csvPath);

        if (isUrl) {
            try {
                Logger.log(`🌐 Descargando CSV de descuentos desde URL: ${csvPath}`);
                // Configuración para que axios trate la respuesta como un stream
                const response = await axios.get(csvPath, { responseType: 'stream' });
                inputStream = response.data;
            } catch (err) {
                Logger.error(`Error descargando CSV desde ${csvPath}. ${err.message}`, err.response ? { status: err.response.status, data: err.response.data } : '');
                Logger.warn("Continuando sin descuentos debido a error de descarga.");
                return resolve(discounts); // Resuelve con mapa vacío en caso de error de descarga
            }
        } else {
            // Manejo de archivo local
            const absolutePath = path.resolve(csvPath); // Resuelve a ruta absoluta
            if (!fs.existsSync(absolutePath)) {
                Logger.warn(`Archivo de descuentos no encontrado en ${absolutePath}. Continuando sin descuentos.`);
                return resolve(discounts); // Resuelve con mapa vacío si el archivo local no existe
            }
            Logger.log(`📄 Cargando CSV de descuentos desde archivo local: ${absolutePath}`);
            inputStream = fs.createReadStream(absolutePath);
        }

        const parser = parse({
            columns: true, // Asume que la primera línea son los encabezados (ej: sku,discount)
            skip_empty_lines: true,
            trim: true,
            bom: true, // Para manejar Byte Order Mark si existe
        });

        inputStream.pipe(parser)
            .on('data', (row) => {
                // Intentar encontrar las columnas 'sku' y 'discount' (insensible a mayúsculas/minúsculas)
                const skuKey = Object.keys(row).find(k => k.toLowerCase() === 'sku');
                const discountKey = Object.keys(row).find(k => k.toLowerCase() === 'discount');

                const skuValue = skuKey ? row[skuKey] : undefined;
                const discountValue = discountKey ? row[discountKey] : undefined;

                const cleanedSku = cleanSku(skuValue);
                // Permitir descuentos como "10" o "10%"
                const pctString = String(discountValue).replace('%', '').trim();
                const pct = parseFloat(pctString);

                if (cleanedSku && !isNaN(pct) && pct >= 0 && pct <= 100) {
                    if (discounts.has(cleanedSku)) {
                        Logger.warn(`SKU duplicado en archivo de descuentos: ${cleanedSku}. Se usará la última entrada encontrada.`);
                    }
                    discounts.set(cleanedSku, pct);
                } else {
                    if (Object.keys(row).length > 0) { // Solo advertir si la fila no está completamente vacía
                       Logger.warn(`Saltando fila inválida o incompleta en archivo de descuentos: SKU='${skuValue}', Descuento='${discountValue}'`);
                    }
                }
            })
            .on('end', () => {
                Logger.log(`🗒️ Cargados ${discounts.size} descuentos válidos desde ${isUrl ? csvPath : path.basename(csvPath)}`);
                resolve(discounts);
            })
            .on('error', (err) => {
                Logger.error(`Error leyendo o parseando archivo de descuentos ${isUrl ? csvPath : path.basename(csvPath)}: ${err.message}`);
                Logger.warn("Continuando sin descuentos debido a error de parseo.");
                resolve(discounts); // Resuelve con mapa vacío en caso de error de parseo
            });

        // Manejar errores en el inputStream (ej. error de red si es URL, error de lectura si es local)
        inputStream.on('error', (err) => {
            Logger.error(`Error en el stream de entrada para ${isUrl ? csvPath : path.basename(csvPath)}: ${err.message}`);
            Logger.warn("Continuando sin descuentos debido a error en el stream.");
            // Asegurarse de que la promesa se resuelva para no colgar el proceso.
            // El parser también tiene un .on('error'), pero este es para el stream en sí.
            // Si el parser ya resolvió, esto no tendrá efecto. Si no, esto asegura que resuelva.
            if (!parser.destroyed) { // Evitar doble resolución si el parser ya manejó el error.
                 parser.end(); // Terminar el parser para que 'end' se emita si no lo ha hecho.
                 resolve(discounts);
            }
        });
    });
}


module.exports = {
    cleanSku,
    fetchWithRetry,
    loadDiscounts,
};
