// Consolidated Shopify Price and Inventory Updater
// This script consolidates the fixes from prototipo-fixed.js to correctly handle inventory updates using Shopify API 2025-01+
require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { RateLimiter } = require('limiter');

// --- Configuration from Environment Variables ---
const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    DATA_API_URL,
    INVENTORY_API_URL,
    LOCATION_ID, // Optional: For multi-location inventory
} = process.env;

// --- Constants and Defaults ---
const SHOPIFY_API_VERSION = '2025-04'; // Use a current supported API version
const SHOPIFY_GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);
const LOG_FILE_PATH = process.env.LOG_FILE_PATH || path.join('logs', 'shopify-sync.log');
const LOG_MAX_SIZE_MB = parseInt(process.env.LOG_MAX_SIZE || '100', 10);
const LOG_MAX_SIZE_BYTES = LOG_MAX_SIZE_MB * 1024 * 1024;
const SYNC_MODE = process.env.SYNC_MODE || 'shopify_first'; // 'local_first' or 'shopify_first'
const SYNC_TYPE = process.env.SYNC_TYPE || 'both'; // 'price', 'inventory', 'both'
const API_TIMEOUT = parseInt(process.env.API_TIMEOUT || '60000', 10); // 60s timeout
const SHOPIFY_RATE_LIMIT = parseInt(process.env.SHOPIFY_RATE_LIMIT || '2', 10); // Default: 2 requests/sec

// --- Basic Validation ---
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !DATA_API_URL || !INVENTORY_API_URL) {
    console.error("Error: Missing required environment variables (SHOPIFY_SHOP_NAME, SHOPIFY_ACCESS_TOKEN, DATA_API_URL, INVENTORY_API_URL).");
    process.exit(1);
}
if ((SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') && !LOCATION_ID) {
    console.warn("Warning: LOCATION_ID environment variable is not set. Inventory updates will use the default location logic. This is expected for single-location stores.");
}

// --- Shopify API Rate Limiter ---
const shopifyLimiter = new RateLimiter({ tokensPerInterval: SHOPIFY_RATE_LIMIT, interval: 'second' });

// --- Logger Module ---
const Logger = {
    logDir: path.dirname(LOG_FILE_PATH),
    logPath: LOG_FILE_PATH,
    logQueue: [],
    isWriting: false,

    init() {
        try {
            if (!fs.existsSync(this.logDir)) {
                fs.mkdirSync(this.logDir, { recursive: true });
            }
            if (!fs.existsSync(this.logPath)) {
                fs.writeFileSync(this.logPath, `[${new Date().toISOString()}] [INFO] Logger initialized.\n`);
            }
        } catch (error) {
            console.error(`Fatal Error: Could not initialize logger at ${this.logPath}. ${error.message}`);
            process.exit(1);
        }
    },

    async processQueue() {
        if (this.isWriting || this.logQueue.length === 0) {
            return;
        }
        this.isWriting = true;

        await this.checkLogSize();

        const messagesToWrite = this.logQueue.splice(0, this.logQueue.length);
        const logContent = messagesToWrite.join('');

        try {
            await fs.promises.appendFile(this.logPath, logContent, 'utf8');
        } catch (error) {
            console.error(`Error writing to log file ${this.logPath}: ${error.message}`);
        } finally {
            this.isWriting = false;
            if (this.logQueue.length > 0) {
                setImmediate(() => this.processQueue());
            }
        }
    },

    log(message, level = 'INFO') {
        const timestamp = new Date().toISOString();
        let formattedMessage = message;
        if (typeof message === 'object' && message !== null) {
            try {
                formattedMessage = JSON.stringify(message, (key, value) =>
                    typeof value === 'string' && value.length > 500 ? value.substring(0, 500) + '...' : value,
                2);
            } catch (e) {
                formattedMessage = '[Unserializable Object]';
            }
        }

        const logEntry = `[${timestamp}] [${level}] ${formattedMessage}\n`;

        if (level === 'ERROR') {
            console.error(formattedMessage);
        } else if (level !== 'DEBUG') {
            console.log(formattedMessage);
        }

        this.logQueue.push(logEntry);
        setTimeout(() => this.processQueue(), 50);
    },

    debug(message) {
        this.log(message, 'DEBUG');
    },

    warn(message) {
        this.log(message, 'WARN');
    },

    error(message, error = null) {
        let logMessage = message;
        if (error) {
            logMessage += `: ${error.message || JSON.stringify(error)}`;
            if (error.stack) {
                logMessage += `\nStack: ${error.stack}`;
            }
            if (error.response) {
                logMessage += `\nResponse: Status=${error.response.status}, Data=${JSON.stringify(error.response.data)}`;
            }
        }
        this.log(logMessage, 'ERROR');
    },

    async checkLogSize() {
        try {
            if (!fs.existsSync(this.logPath)) {
                return;
            }
            const stats = await fs.promises.stat(this.logPath);
            if (stats.size >= LOG_MAX_SIZE_BYTES) {
                await this.rotateLog();
            }
        } catch (error) {
            console.error(`Error checking log size for ${this.logPath}: ${error.message}`);
        }
    },

    async rotateLog() {
        const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\..+/, '');
        const backupPath = path.join(this.logDir, `shopify-sync.${timestamp}.log`);
        const rotateMessage = `[${new Date().toISOString()}] [INFO] Rotating log file. Previous log: ${path.basename(backupPath)}\n`;

        try {
            console.log(`Log file size limit reached. Rotating ${path.basename(this.logPath)} to ${path.basename(backupPath)}`);
            await fs.promises.rename(this.logPath, backupPath);
            await fs.promises.writeFile(this.logPath, rotateMessage, 'utf8');
            console.log("Log rotation complete.");
            this.cleanOldLogs().catch(err => console.error(`Error during old log cleanup: ${err.message}`));
        } catch (error) {
            console.error(`Error rotating log file ${this.logPath}: ${error.message}`);
            try {
                await fs.promises.appendFile(this.logPath, `[${new Date().toISOString()}] [ERROR] Log rotation failed: ${error.message}\n`, 'utf8');
            } catch (appendError) {
                console.error(`CRITICAL: Failed to write rotation error to log file: ${appendError.message}`);
            }
        }
    },

    async cleanOldLogs() {
        try {
            const files = await fs.promises.readdir(this.logDir);
            const logFiles = files
                .filter(file => file.startsWith('shopify-sync.') && file.endsWith('.log') && file !== path.basename(this.logPath))
                .map(file => {
                    try {
                        return {
                            name: file,
                            path: path.join(this.logDir, file),
                            mtime: fs.statSync(path.join(this.logDir, file)).mtime.getTime()
                        };
                    } catch (statErr) {
                        console.error(`Could not stat file during cleanup: ${file}. Skipping. Error: ${statErr.message}`);
                        return null;
                    }
                })
                .filter(Boolean)
                .sort((a, b) => b.mtime - a.mtime);

            const keepCount = 10;
            const filesToDelete = logFiles.slice(keepCount);

            if (filesToDelete.length > 0) {
                Logger.log(`Cleaning up ${filesToDelete.length} old log files...`);
                for (const file of filesToDelete) {
                    try {
                        await fs.promises.unlink(file.path);
                        Logger.log(`Deleted old log file: ${file.name}`);
                    } catch (unlinkError) {
                        Logger.error(`Failed to delete old log file ${file.name}`, unlinkError);
                    }
                }
            }
        } catch (error) {
            Logger.error("Error cleaning old log files", error);
        }
    }
};

/**
 * Cleans a SKU: Keeps ONLY numbers, removes leading zeros.
 * Returns null if input is invalid or result is empty.
 * @param {string | number | null | undefined} sku - The original SKU.
 * @returns {string | null} - The cleaned numeric SKU string or null.
 */
function cleanSku(sku) {
    if (sku === null || sku === undefined) return null;
    try {
        const cleaned = String(sku).trim().replace(/[^0-9]/g, '').replace(/^0+/, '');
        return cleaned.length > 0 ? cleaned : null;
    } catch (e) {
        Logger.error(`Error cleaning SKU: ${sku}`, e);
        return null;
    }
}

/**
 * Performs an API request (using Axios) with rate limiting and retries.
 * @param {object} config - Axios request configuration.
 * @param {boolean} [useShopifyLimiter=false] - Apply Shopify rate limiter.
 * @param {number} [retries=MAX_RETRIES] - Retries remaining.
 * @returns {Promise<object>} - Response data.
 * @throws {Error} - If request fails after retries.
 */
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
        return response.data;
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

/**
 * Fetches product data from the local API.
 * @returns {Promise<Array<object>>} - Array of local product objects.
 */
async function getLocalProducts() {
    Logger.log("Fetching local product data from API...");
    try {
        const responseData = await fetchWithRetry({ method: 'GET', url: DATA_API_URL, headers: { 'Accept': 'application/json' } });
        const localProducts = responseData?.value || responseData || [];
        if (!Array.isArray(localProducts)) {
            Logger.error(`Invalid data structure received from product API: Expected an array, got ${typeof localProducts}. Response: ${JSON.stringify(responseData)}`);
            throw new Error(`Invalid data structure received from product API.`);
        }
        Logger.log(`Fetched ${localProducts.length} local product records.`);
        return localProducts;
    } catch (error) {
        if (!error.response) { Logger.error("Failed to fetch local product data", error); }
        throw error;
    }
}

/**
 * Fetches and processes inventory data from the local API.
 * @returns {Promise<object>} - Map of inventory data keyed by cleaned SKU.
 */
async function getLocalInventory() {
    if (SYNC_TYPE === 'price') { 
        Logger.log("Skipping local inventory fetch (sync type is 'price')."); 
        return {}; 
    }
    Logger.log("Fetching local inventory data from API...");
    try {
        const responseData = await fetchWithRetry({ method: 'GET', url: INVENTORY_API_URL, headers: { 'Accept': 'application/json' } });
        const inventoryData = responseData?.value || responseData || [];
        if (!Array.isArray(inventoryData)) { 
            Logger.error(`Invalid data structure received from inventory API: Expected an array, got ${typeof inventoryData}. Response: ${JSON.stringify(responseData)}`); 
            throw new Error(`Invalid data structure received from inventory API.`); 
        }
        Logger.log(`Fetched ${inventoryData.length} local inventory records.`);
        const inventoryMap = {};
        const inventoryBySku = {};
        for (const item of inventoryData) {
            if (!item || typeof item !== 'object' || !item.CodigoProducto) { 
                Logger.warn(`Inventory record missing CodigoProducto or invalid format: ${JSON.stringify(item)}`); 
                continue; 
            }
            const sku = cleanSku(item.CodigoProducto);
            if (!sku) { 
                Logger.warn(`Could not clean SKU for inventory record with CodigoProducto ${item.CodigoProducto}. Record: ${JSON.stringify(item)}`); 
                continue; 
            }
            if (!inventoryBySku[sku]) { 
                inventoryBySku[sku] = []; 
            }
            item.parsedDate = item.Fecha ? new Date(item.Fecha) : new Date(0);
            if (isNaN(item.parsedDate)) { 
                Logger.warn(`Invalid date format for SKU ${sku}, Fecha: ${item.Fecha}. Using epoch.`); 
                item.parsedDate = new Date(0); 
            }
            inventoryBySku[sku].push(item);
        }
        for (const [sku, items] of Object.entries(inventoryBySku)) {
            const sortedItems = items.sort((a, b) => b.parsedDate - a.parsedDate);
            const mostRecentItem = sortedItems[0];
            const initial = parseFloat(mostRecentItem.CantidadInicial || 0);
            const received = parseFloat(mostRecentItem.CantidadEntradas || 0);
            const shipped = parseFloat(mostRecentItem.CantidadSalidas || 0);
            if (isNaN(initial) || isNaN(received) || isNaN(shipped)) { 
                Logger.warn(`Invalid numeric values for inventory calculation for SKU ${sku}. Record: ${JSON.stringify(mostRecentItem)}. Skipping inventory update for this SKU.`); 
                continue; 
            }
            const calculatedQuantity = Math.max(0, initial + received - shipped);
            inventoryMap[sku] = { ...mostRecentItem, calculatedQuantity: Math.floor(calculatedQuantity) };
        }
        Logger.log(`Processed inventory for ${Object.keys(inventoryMap).length} unique SKUs.`);
        return inventoryMap;
    } catch (error) {
        if (!error.response) { Logger.error("Failed to fetch or process local inventory data", error); }
        throw error;
    }
}

/**
 * Fetches all product variants from Shopify using GraphQL pagination.
 * Uses quantities field when available (2025-01+) and falls back to available when needed.
 * @returns {Promise<Array<object>>} - Array of Shopify variant objects.
 */
async function getAllShopifyVariants() {
    Logger.log("Fetching all product variants from Shopify...");
    const allVariants = [];
    let hasNextPage = true;
    let cursor = null;
    let pageCount = 0;
    const MAX_PAGES = 500; // Safeguard against infinite loops

    // Updated GraphQL query that works with both 2024 and 2025 API versions
    // For 2025-01+, it retrieves quantities
    // For older versions, it retrieves available
    const queryTemplate = (cursor) => `
      query GetVariants($limit: Int!, ${cursor ? '$cursor: String!' : ''}) {
        productVariants(first: $limit ${cursor ? ', after: $cursor' : ''}) {
          edges {
            cursor
            node {
              id
              sku
              price
              displayName
              inventoryItem {
                id
                tracked
                inventoryLevels(first: 1) {
                  edges {
                    node {
                      available
                      quantities(names: "available") {
                        name
                        quantity
                      }
                      location {
                        id
                      }
                    }
                  }
                }
              }
              product {
                id
                title
              }
            }
          }
          pageInfo {
            hasNextPage
          }
        }
      }
    `;

    while (hasNextPage && pageCount < MAX_PAGES) {
        pageCount++;
        const query = queryTemplate(cursor);
        const variables = { limit: 100 };
        if (cursor) {
            variables.cursor = cursor;
        }

        Logger.log(`Fetching Shopify variants page ${pageCount}${cursor ? ` (cursor: ${cursor})` : ''}...`);

        try {
            const responseData = await fetchWithRetry({
                method: 'POST',
                url: SHOPIFY_GRAPHQL_URL,
                headers: {
                    'Content-Type': 'application/json',
                    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
                },
                data: JSON.stringify({ query, variables }),
            }, true);

            if (responseData.errors) {
                Logger.error(`GraphQL Error fetching variants (Page ${pageCount}): ${JSON.stringify(responseData.errors)}`);
                if (responseData.errors.some(e => e.extensions?.code === 'THROTTLED')) {
                    Logger.warn("Rate limit hit during variant fetch. Continuing after delay...");
                    await new Promise(resolve => setTimeout(resolve, 5000));
                    continue;
                } else {
                    Logger.error("Non-throttling GraphQL error encountered. Stopping variant fetch.");
                    throw new Error(`GraphQL Error fetching variants: ${JSON.stringify(responseData.errors)}`);
                }
            }

            const variantsData = responseData?.data?.productVariants;
            if (!variantsData) {
                Logger.error(`No productVariants data found in response for page ${pageCount}. Response: ${JSON.stringify(responseData)}`);
                break;
            }

            const edges = variantsData.edges || [];
            for (const edge of edges) {
                if (edge?.node?.id && edge.node.inventoryItem?.id) {
                    allVariants.push(edge.node);
                } else {
                    Logger.warn(`Skipping invalid variant structure received: ${JSON.stringify(edge)}`);
                }
            }

            hasNextPage = variantsData.pageInfo?.hasNextPage ?? false;
            if (hasNextPage && edges.length > 0) {
                cursor = edges[edges.length - 1].cursor;
            } else {
                hasNextPage = false;
            }

            Logger.log(`Fetched ${edges.length} variants on page ${pageCount}. Total fetched: ${allVariants.length}. Has next page: ${hasNextPage}`);

            if (hasNextPage) {
                await new Promise(resolve => setTimeout(resolve, 250));
            }

        } catch (error) {
            Logger.error(`Failed to fetch or process Shopify variants page ${pageCount}. Stopping variant fetch.`);
            throw new Error("Failed to fetch all Shopify variants.");
        }
    }
    if (pageCount >= MAX_PAGES) {
        Logger.warn(`Reached maximum page limit (${MAX_PAGES}) for fetching Shopify variants. Process may be incomplete.`);
    }

    Logger.log(`Successfully fetched a total of ${allVariants.length} Shopify variants after ${pageCount} page(s).`);
    return allVariants;
}

/**
 * Updates price and/or inventory for a specific variant in Shopify.
 * Handles both 2025-01+ API (quantities) and earlier API versions (available).
 * 
 * @param {object} variant - The Shopify variant object.
 * @param {string|null} newPrice - The new price or null to skip update.
 * @param {number|null} newInventory - The absolute inventory quantity or null to skip update.
 * @returns {Promise<object>} - Result object with success status and update info.
 */
async function updateVariantInShopify(variant, newPrice, newInventory) {
    // Input Validation
    if (!variant || !variant.id || !variant.inventoryItem?.id) {
        Logger.error("Invalid variant object passed to updateVariantInShopify", variant);
        return { success: false, updatedPrice: false, updatedInventory: false, message: "Invalid variant data received", error: "Invalid variant data" };
    }

    const variantId = variant.id;
    const inventoryItemId = variant.inventoryItem.id;
    const currentPrice = variant.price;

    // --- Determine Current Inventory ---
    let currentInventory = null;
    const inventoryLevelEdge = variant.inventoryItem.inventoryLevels?.edges?.[0]?.node;
    
    // Try to get quantity from quantities field first (2025-01+)
    if (inventoryLevelEdge?.quantities?.length > 0) {
        const availableObj = inventoryLevelEdge.quantities.find(q => q.name === 'available');
        currentInventory = availableObj?.quantity;
        Logger.debug(`Found inventory using quantities field: ${currentInventory}`);
    } 
    // Fall back to available field if quantities is not available
    else if (inventoryLevelEdge?.available !== undefined) {
        currentInventory = inventoryLevelEdge.available;
        Logger.debug(`Found inventory using available field: ${currentInventory}`);
    }

    const productName = variant.product?.title || 'Unknown Product';
    const sku = variant.sku || 'No SKU';
    let updatedPrice = false;
    let updatedInventory = false;
    let messages = [];
    let errors = [];

    // Validate current and new inventory values
    const currentInventoryNum = (currentInventory !== undefined && currentInventory !== null && !isNaN(Number(currentInventory))) ? Number(currentInventory) : null;
    const newInventoryNum = (newInventory !== null && !isNaN(Number(newInventory))) ? Math.floor(Number(newInventory)) : null;

    // --- 1. Price Update ---
    const shouldUpdatePrice = SYNC_TYPE === 'price' || SYNC_TYPE === 'both';
    const newPriceStr = newPrice !== null ? String(newPrice) : null;
    const currentPriceStr = currentPrice !== null ? String(currentPrice) : null;

    if (shouldUpdatePrice && newPriceStr !== null && currentPriceStr !== newPriceStr) {
        Logger.log(`Updating price for SKU ${sku} (${productName}): ${currentPriceStr} -> ${newPriceStr}`);
        const priceMutation = `
          mutation ProductVariantUpdate($input: ProductVariantInput!) {
            productVariantUpdate(input: $input) {
              productVariant {
                id
                price
              }
              userErrors {
                field
                message
              }
            }
          }`;
        const priceVariables = {
            input: {
                id: variantId,
                price: newPriceStr,
            }
        };

        try {
            const result = await fetchWithRetry({
                method: 'POST',
                url: SHOPIFY_GRAPHQL_URL,
                headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN },
                data: JSON.stringify({ query: priceMutation, variables: priceVariables }),
            }, true);

            const updateResult = result?.data?.productVariantUpdate;
            const userErrors = updateResult?.userErrors;

            if (userErrors && userErrors.length > 0) {
                const errorMsg = `Price update failed: ${userErrors.map(e => `(${e.field}) ${e.message}`).join(', ')}`;
                Logger.error(`Error updating price for SKU ${sku}: ${JSON.stringify(userErrors)}`);
                messages.push(errorMsg);
                errors.push(errorMsg);
            } else if (updateResult?.productVariant?.id) {
                Logger.log(`✅ Price updated successfully for SKU ${sku}.`, 'SUCCESS');
                updatedPrice = true;
                messages.push(`Price: ${currentPriceStr} -> ${newPriceStr}`);
            } else {
                Logger.warn(`Unknown result structure after price update for SKU ${sku}: ${JSON.stringify(result)}`);
                messages.push("Price update status unknown.");
            }
        } catch (error) {
            const errorMsg = `Price update failed: API error during mutation.`;
            messages.push(errorMsg);
            errors.push(errorMsg);
        }
    } else if (shouldUpdatePrice && newPriceStr !== null) {
        messages.push("Price already up-to-date.");
    }

    // --- 2. Inventory Update ---
    const shouldUpdateInventory = SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both';

    if (shouldUpdateInventory && newInventoryNum !== null && newInventoryNum !== currentInventoryNum) {
        if (!variant.inventoryItem.tracked) {
            Logger.warn(`SKU ${sku} (${productName}) inventory is not tracked by Shopify. Skipping inventory update.`);
            messages.push("Inventory not tracked.");
        } else {
            // Use inventoryAdjustQuantity mutation
            Logger.log(`Updating inventory for SKU ${sku} (${productName}) using Adjust: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum}`);
            const delta = newInventoryNum - (currentInventoryNum ?? 0);

            if (delta === 0) {
                messages.push("Inventory delta is 0, skipping adjust call.");
            } else {
                // Updated mutation to support both API versions by requesting both quantities and available
                const inventoryMutation = `
                  mutation InventoryAdjustQuantity($input: InventoryAdjustQuantityInput!) {
                    inventoryAdjustQuantity(input: $input) {
                      inventoryLevel {
                        available
                        quantities(names: "available") {
                          name
                          quantity
                        }
                      }
                      userErrors {
                        field
                        message
                      }
                    }
                  }`;
                const inventoryVariables = { 
                    input: { 
                        inventoryItemId: inventoryItemId, 
                        availableDelta: delta 
                    } 
                };
                
                try {
                    const result = await fetchWithRetry({
                        method: 'POST',
                        url: SHOPIFY_GRAPHQL_URL,
                        headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN },
                        data: JSON.stringify({ query: inventoryMutation, variables: inventoryVariables }),
                    }, true);

                    const adjustResult = result?.data?.inventoryAdjustQuantity;
                    const userErrors = adjustResult?.userErrors;

                    if (userErrors && userErrors.length > 0) {
                        const errorMsg = `Inventory update failed: ${userErrors.map(e => `(${e.field}) ${e.message}`).join(', ')}`;
                        Logger.error(`Error adjusting inventory for SKU ${sku}: ${JSON.stringify(userErrors)}`);
                        messages.push(errorMsg);
                        errors.push(errorMsg);
                    } else if (adjustResult?.inventoryLevel) {
                        // Get final quantity from either quantities or available field
                        let finalAvailable = null;
                        const quantities = adjustResult.inventoryLevel.quantities;
                        
                        if (quantities && quantities.length > 0) {
                            const availableObj = quantities.find(q => q.name === 'available');
                            finalAvailable = availableObj?.quantity;
                        }
                        
                        // Fall back to available field if quantities is not available
                        if (finalAvailable === null && adjustResult.inventoryLevel.available !== undefined) {
                            finalAvailable = adjustResult.inventoryLevel.available;
                        }

                        Logger.log(`✅ Inventory adjusted successfully for SKU ${sku}. New available: ${finalAvailable}`, 'SUCCESS');
                        updatedInventory = true;
                        messages.push(`Inventory: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum} (Delta: ${delta}, Final: ${finalAvailable})`);
                        
                        if (finalAvailable !== newInventoryNum) {
                            Logger.warn(`SKU ${sku}: Final inventory level (${finalAvailable}) differs from expected (${newInventoryNum}) after adjustment.`);
                        }
                    } else {
                        Logger.warn(`Unknown result structure after inventory adjust for SKU ${sku}: ${JSON.stringify(result)}`);
                        messages.push("Inventory update status unknown.");
                    }
                } catch (error) {
                    const errorMsg = `Inventory update failed: API error during mutation.`;
                    messages.push(errorMsg);
                    errors.push(errorMsg);
                }
            }
        }
    } else if (shouldUpdateInventory && newInventoryNum !== null) {
        messages.push("Inventory already up-to-date.");
    } else if (shouldUpdateInventory && newInventoryNum === null) {
        messages.push("Invalid new inventory value (null).");
    }

    // --- 3. Final Result ---
    const success = errors.length === 0;
    const updated = updatedPrice || updatedInventory;
    let finalMessage = `SKU ${sku} (${productName}): ${messages.join(' | ')}`;

    if (updated && success) { 
        Logger.log(`✅ ${finalMessage}`, 'SUCCESS'); 
    } else if (!success) { 
        Logger.error(`❌ ${finalMessage}`); 
    } else { 
        Logger.log(`ℹ️ SKU ${sku} (${productName}) - No changes needed.`); 
        finalMessage = `SKU ${sku} (${productName}): No changes needed.`; 
    }

    return { 
        success, 
        updated, 
        updatedPrice, 
        updatedInventory, 
        message: finalMessage, 
        error: success ? null : errors.join('; ') 
    };
}

/**
 * Main synchronization function.
 */
async function syncShopifyData() {
    Logger.log(`🚀 Starting Shopify Sync (Mode: ${SYNC_MODE.toUpperCase()}, Type: ${SYNC_TYPE.toUpperCase()})`);
    const startTime = Date.now();
    try {
        Logger.log("Fetching data from Local APIs and Shopify...");
        const [localProducts, localInventoryMap, shopifyVariants] = await Promise.all([
            getLocalProducts(), 
            getLocalInventory(), 
            getAllShopifyVariants()
        ]).catch(fetchError => { 
            Logger.error("💥 FATAL ERROR during initial data fetching. Aborting sync.", fetchError); 
            throw fetchError; 
        });
        Logger.log("Data fetching complete.");

        const shopifyVariantMap = new Map();
        for (const variant of shopifyVariants) {
            if (!variant || !variant.sku) { 
                Logger.warn(`Shopify variant missing SKU. ID: ${variant?.id || 'N/A'}. Skipping.`); 
                continue; 
            }
            const cleanedSku = cleanSku(variant.sku);
            if (cleanedSku) {
                if (shopifyVariantMap.has(cleanedSku)) { 
                    const existingVariant = shopifyVariantMap.get(cleanedSku); 
                    Logger.warn(`Duplicate SKU found in Shopify: ${cleanedSku}. Variant ID: ${variant.id} (${variant.displayName}). Previous: ${existingVariant.id} (${existingVariant.displayName}). Using the last one found.`); 
                }
                shopifyVariantMap.set(cleanedSku, variant);
            } else { 
                Logger.warn(`Shopify variant ID ${variant.id} (${variant.displayName}) has invalid SKU '${variant.sku}'. Skipping.`); 
            }
        }

        const localProductMap = new Map();
        for (const product of localProducts) {
            if (!product || !product.CodigoProducto) { 
                Logger.warn(`Local product record missing CodigoProducto or invalid format: ${JSON.stringify(product)}`); 
                continue; 
            }
            const cleanedSku = cleanSku(product.CodigoProducto);
            if (cleanedSku) {
                if (localProductMap.has(cleanedSku)) { 
                    Logger.warn(`Duplicate SKU found in Local Products: ${cleanedSku}. Using the last one found.`); 
                }
                localProductMap.set(cleanedSku, product);
            } else { 
                Logger.warn(`Could not clean SKU for local product with CodigoProducto '${product.CodigoProducto}'. Record: ${JSON.stringify(product)}`); 
            }
        }
        Logger.log(`Prepared maps: ${shopifyVariantMap.size} unique Shopify SKUs, ${localProductMap.size} unique Local SKUs.`);

        let stats = { 
            processed: 0, 
            priceUpdates: 0, 
            inventoryUpdates: 0, 
            bothUpdates: 0, 
            skippedNoChange: 0, 
            notFoundLocal: 0, 
            notFoundShopify: 0, 
            errors: 0, 
            invalidDataLocal: 0 
        };
        
        const itemsToProcess = SYNC_MODE === 'shopify_first' 
            ? Array.from(shopifyVariantMap.entries()) 
            : Array.from(localProductMap.entries());
            
        Logger.log(`Starting update process for ${itemsToProcess.length} items based on ${SYNC_MODE} mode...`);

        const updatePromises = itemsToProcess.map(async ([sku, item], index) => {
            let variant = null; 
            let localProduct = null;
            
            if (index > 0 && index % 100 === 0) { 
                Logger.log(`Processing item ${index + 1} / ${itemsToProcess.length} (SKU: ${sku})...`); 
            }
            
            if (SYNC_MODE === 'shopify_first') {
                variant = item; 
                localProduct = localProductMap.get(sku);
                if (!localProduct) { 
                    Logger.log(`⚠️ SKU ${sku} (${variant.displayName}) found in Shopify but not in local data. Skipping.`, 'WARN'); 
                    return { status: 'skipped', reason: 'notFoundLocal', sku: sku }; 
                }
            } else {
                localProduct = item; 
                variant = shopifyVariantMap.get(sku);
                if (!variant) { 
                    Logger.log(`⚠️ SKU ${sku} found in local data but not in Shopify. Skipping.`, 'WARN'); 
                    return { status: 'skipped', reason: 'notFoundShopify', sku: sku }; 
                }
            }

            let newPrice = null; 
            let newInventory = null; 
            let dataValid = true;
            
            if (SYNC_TYPE === 'price' || SYNC_TYPE === 'both') {
                if (localProduct && localProduct.Venta1 !== undefined && localProduct.Venta1 !== null) {
                    const priceValue = parseFloat(localProduct.Venta1);
                    if (!isNaN(priceValue)) { 
                        newPrice = priceValue.toFixed(2); 
                    } else { 
                        Logger.warn(`SKU ${sku}: Invalid non-numeric price (Venta1: '${localProduct.Venta1}') in local data. Skipping price update.`); 
                        dataValid = false; 
                    }
                } else { 
                    Logger.warn(`SKU ${sku}: Missing price (Venta1) in local data. Skipping price update.`); 
                    dataValid = false; 
                }
            }
            
            if (SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') {
                const inventoryData = localInventoryMap[sku];
                if (inventoryData && inventoryData.calculatedQuantity !== undefined && inventoryData.calculatedQuantity !== null) {
                    const invValue = Number(inventoryData.calculatedQuantity);
                    if(!isNaN(invValue)){ 
                        newInventory = Math.floor(invValue); 
                    } else { 
                        Logger.warn(`SKU ${sku}: Invalid non-numeric calculated inventory ('${inventoryData.calculatedQuantity}') found. Skipping inventory update.`); 
                        dataValid = false; 
                    }
                } else { 
                    Logger.warn(`SKU ${sku}: Missing calculated inventory in local data map. Skipping inventory update.`); 
                    dataValid = false; 
                }
            }
            
            if (!dataValid) { 
                return { status: 'skipped', reason: 'invalidDataLocal', sku: sku }; 
            }

            Logger.debug(`SKU ${sku}: Attempting update. Target Variant ID: ${variant.id}. Local Price: ${localProduct?.Venta1}. Local Inv Record: ${JSON.stringify(localInventoryMap[sku])}. Calculated New Price: ${newPrice}. Calculated New Inventory: ${newInventory}.`);
            
            try {
                const result = await updateVariantInShopify(variant, newPrice, newInventory);
                return { status: 'processed', result: result, sku: sku };
            } catch (error) { 
                Logger.error(`Critical error processing SKU ${sku} during updateVariantInShopify call.`, error); 
                return { status: 'error', error: error.message || 'Unknown critical error', sku: sku }; 
            }
        });

        const results = await Promise.allSettled(updatePromises);
        results.forEach(outcome => {
            if (outcome.status === 'fulfilled') {
                const data = outcome.value; 
                stats.processed++;
                
                if (data.status === 'skipped') { 
                    if (data.reason === 'notFoundLocal') stats.notFoundLocal++; 
                    else if (data.reason === 'notFoundShopify') stats.notFoundShopify++; 
                    else if (data.reason === 'invalidDataLocal') stats.invalidDataLocal++; 
                } else if (data.status === 'processed') {
                    const result = data.result;
                    if (!result.success) { 
                        stats.errors++; 
                        Logger.error(`Update failed for SKU ${data.sku}: ${result.error}`); 
                    } else if (result.updated) { 
                        if (result.updatedPrice && result.updatedInventory) { 
                            stats.bothUpdates++; 
                        } else if (result.updatedPrice) { 
                            stats.priceUpdates++; 
                        } else if (result.updatedInventory) { 
                            stats.inventoryUpdates++; 
                        }
                    } else { 
                        stats.skippedNoChange++; 
                    }
                } else if (data.status === 'error') { 
                    stats.errors++; 
                    Logger.error(`Critical error was recorded for SKU ${data.sku}`); 
                }
            } else { 
                stats.errors++; 
                Logger.error(`Unhandled promise rejection during processing: ${outcome.reason}`); 
            }
        });

        const endTime = Date.now(); 
        const duration = ((endTime - startTime) / 1000).toFixed(2);
        
        Logger.log("\n📊 ===== SYNC SUMMARY =====");
        Logger.log(`Mode: ${SYNC_MODE.toUpperCase()}, Type: ${SYNC_TYPE.toUpperCase()}`);
        Logger.log(`Duration: ${duration} seconds`);
        Logger.log(`---------------------------`);
        Logger.log(`Total Items Considered: ${itemsToProcess.length}`);
        Logger.log(`Successfully Processed: ${stats.processed - stats.notFoundLocal - stats.notFoundShopify - stats.invalidDataLocal - stats.errors}`);
        Logger.log(`Successful Updates:`);
        Logger.log(`  - Price Only: ${stats.priceUpdates}`);
        Logger.log(`  - Inventory Only: ${stats.inventoryUpdates}`);
        Logger.log(`  - Both Price & Inventory: ${stats.bothUpdates}`);
        Logger.log(`  - Total Updated Variants: ${stats.priceUpdates + stats.inventoryUpdates + stats.bothUpdates}`);
        Logger.log(`Skipped (No Change Needed): ${stats.skippedNoChange}`);
        Logger.log(`Skipped (Not Found Locally): ${stats.notFoundLocal}`);
        Logger.log(`Skipped (Not Found in Shopify): ${stats.notFoundShopify}`);
        Logger.log(`Skipped (Invalid Local Data): ${stats.invalidDataLocal}`);
        Logger.log(`Errors during update process: ${stats.errors}`);
        Logger.log("==========================\n");

        if (stats.errors > 0 || stats.invalidDataLocal > 0) { 
            Logger.error("Sync completed with errors or data issues. Please review the logs."); 
        } else if (stats.priceUpdates + stats.inventoryUpdates + stats.bothUpdates === 0 && stats.skippedNoChange > 0) { 
            Logger.log("Sync completed. No updates were necessary for processed items."); 
        } else if (stats.priceUpdates + stats.inventoryUpdates + stats.bothUpdates > 0) { 
            Logger.log("✅ Sync completed successfully with updates applied."); 
        } else { 
            Logger.log("Sync completed. No updates applied."); 
        }

    } catch (error) { 
        process.exitCode = 1; 
    } finally { 
        await Logger.processQueue(); 
        Logger.log(`🏁 Sync process finished at ${new Date().toLocaleString()}`); 
        await new Promise(resolve => setTimeout(resolve, 200)); 
    }
}

// --- Main Execution ---
(async () => {
    Logger.init();
    Logger.log(`Script started at ${new Date().toLocaleString()}`);
    await syncShopifyData();
})();

// Graceful shutdown handlers
process.on('SIGINT', async () => { 
    Logger.log('Received SIGINT. Shutting down gracefully...'); 
    await Logger.processQueue(); 
    process.exit(0); 
});

process.on('SIGTERM', async () => { 
    Logger.log('Received SIGTERM. Shutting down gracefully...'); 
    await Logger.processQueue(); 
    process.exit(0); 
});