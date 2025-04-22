// Import necessary modules
require('dotenv').config(); // Load environment variables from .env file
const axios = require('axios'); // HTTP client for making API requests
const fs = require('fs'); // File system module for interacting with files
const path = require('path'); // Module for handling file paths
const readline = require('readline'); // Module for reading files line by line
const { RateLimiter } = require('limiter'); // Library for rate limiting API calls

// --- Configuration from Environment Variables ---
// Ensure these are set in your .env file
// SHOPIFY_SHOP_NAME=your-shop-name
// SHOPIFY_ACCESS_TOKEN=your-access-token
// DATA_API_URL=http://your-local-api/products
// INVENTORY_API_URL=http://your-local-api/inventory
// MAX_RETRIES=3 (Optional, default: 3)
// LOG_FILE_PATH=logs/shopify-sync.log (Optional, default: logs/shopify-sync.log)
// LOG_MAX_SIZE=100 (Optional, max log size in MB, default: 100)
// SYNC_MODE=shopify_first (Optional, 'local_first' or 'shopify_first', default: 'shopify_first')
// SYNC_TYPE=both (Optional, 'price', 'inventory', 'both', default: 'both')
// API_TIMEOUT=30000 (Optional, timeout for API requests in ms, default: 30000)
// SHOPIFY_RATE_LIMIT=2 (Optional, requests per second to Shopify, default: 2)
// LOCATION_ID=your_shopify_location_id (Optional, required if using multi-location inventory)

const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    DATA_API_URL,
    INVENTORY_API_URL,
    LOCATION_ID, // Optional: For multi-location inventory
} = process.env;

// --- Constants and Defaults ---
const SHOPIFY_API_VERSION = '2025-04'; // Use a recent, stable API version
const SHOPIFY_GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);
const LOG_FILE_PATH = process.env.LOG_FILE_PATH || path.join('logs', 'shopify-sync.log');
const LOG_MAX_SIZE_MB = parseInt(process.env.LOG_MAX_SIZE || '100', 10);
const LOG_MAX_SIZE_BYTES = LOG_MAX_SIZE_MB * 1024 * 1024;
const SYNC_MODE = process.env.SYNC_MODE || 'shopify_first'; // 'local_first' or 'shopify_first'
const SYNC_TYPE = process.env.SYNC_TYPE || 'both'; // 'price', 'inventory', 'both'
const API_TIMEOUT = parseInt(process.env.API_TIMEOUT || '30000', 10); // 30 seconds default timeout
const SHOPIFY_RATE_LIMIT = parseInt(process.env.SHOPIFY_RATE_LIMIT || '2', 10); // Default: 2 requests/sec

// --- Basic Validation ---
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !DATA_API_URL || !INVENTORY_API_URL) {
    console.error("Error: Missing required environment variables (SHOPIFY_SHOP_NAME, SHOPIFY_ACCESS_TOKEN, DATA_API_URL, INVENTORY_API_URL).");
    process.exit(1); // Exit if essential config is missing
}
if ((SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') && !LOCATION_ID) {
     console.warn("Warning: LOCATION_ID environment variable is not set. Inventory updates might not work correctly for multi-location stores. Assuming a single default location.");
     // Consider making LOCATION_ID mandatory if inventory sync is needed for multi-location stores.
}


// --- Shopify API Rate Limiter ---
// Limits requests to Shopify API to avoid hitting rate limits (429 errors)
// Adjust tokensPerInterval and interval based on Shopify's limits (typically 2/sec for GraphQL)
const shopifyLimiter = new RateLimiter({ tokensPerInterval: SHOPIFY_RATE_LIMIT, interval: 'second' });

/**
 * Optimized Logger Module
 * - Uses streams for efficient file writing/rotation.
 * - Handles log rotation based on size.
 * - Cleans up old log files.
 * - Provides paginated log reading.
 */
const Logger = {
    logDir: path.dirname(LOG_FILE_PATH),
    logPath: LOG_FILE_PATH,
    logQueue: [], // Queue for log messages
    isWriting: false, // Flag to prevent concurrent writes

    /**
     * Initializes the logger: creates directory and initial log file if needed.
     */
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

    /**
     * Asynchronously writes queued log messages to the file.
     */
    async processQueue() {
        if (this.isWriting || this.logQueue.length === 0) {
            return;
        }
        this.isWriting = true;

        // Check size before writing the batch
        await this.checkLogSize();

        const messagesToWrite = this.logQueue.splice(0, this.logQueue.length); // Get all queued messages
        const logContent = messagesToWrite.join('');

        try {
            // Use appendFile for simplicity with async/await
            await fs.promises.appendFile(this.logPath, logContent, 'utf8');
        } catch (error) {
            console.error(`Error writing to log file ${this.logPath}: ${error.message}`);
            // Optional: Retry or handle error appropriately
        } finally {
            this.isWriting = false;
            // If more messages arrived while writing, process them
            if (this.logQueue.length > 0) {
                // Use setImmediate to avoid potential stack overflow with recursive calls
                setImmediate(() => this.processQueue());
            }
        }
    },

    /**
     * Logs a message to the console and queues it for file writing.
     * @param {String} message - The message to log.
     * @param {String} [level='INFO'] - Log level (e.g., INFO, WARN, ERROR, SUCCESS).
     */
    log(message, level = 'INFO') {
        const timestamp = new Date().toISOString();
        // Simple check to avoid logging excessively long objects directly
        let formattedMessage = message;
        if (typeof message === 'object' && message !== null) {
            try {
                // Limit depth and length to prevent huge log entries
                formattedMessage = JSON.stringify(message, (key, value) =>
                    typeof value === 'string' && value.length > 500 ? value.substring(0, 500) + '...' : value,
                2);
            } catch (e) {
                formattedMessage = '[Unserializable Object]';
            }
        }

        const logEntry = `[${timestamp}] [${level}] ${formattedMessage}\n`;

        // Log to console immediately
        if (level === 'ERROR') {
            console.error(formattedMessage);
        } else if (level !== 'DEBUG') { // Avoid flooding console with DEBUG logs by default
             console.log(formattedMessage);
        }

        // Add to queue and trigger processing
        this.logQueue.push(logEntry);
        // Debounce processing slightly to batch writes
        setTimeout(() => this.processQueue(), 50);
    },

     /**
     * Logs a debug message (less verbose than info).
     * @param {String} message - The debug message.
     */
    debug(message) {
        // Control debug logging via an environment variable if needed
        // if (process.env.DEBUG_MODE === 'true') {
             this.log(message, 'DEBUG');
        // }
    },


    /**
     * Alias for log with WARN level
     * @param {String} message - The warning message
     */
    warn(message) {
        this.log(message, 'WARN');
    },

    /**
     * Logs an error message.
     * @param {String} message - The error description.
     * @param {Error|Object|null} [error=null] - Optional error object or details.
     */
    error(message, error = null) {
        let logMessage = message;
        if (error) {
            logMessage += `: ${error.message || JSON.stringify(error)}`;
            // Include stack trace if available
            if (error.stack) {
                logMessage += `\nStack: ${error.stack}`;
            }
            // Include Axios response details if present
             if (error.response) {
                logMessage += `\nResponse: Status=${error.response.status}, Data=${JSON.stringify(error.response.data)}`;
            }
        }
        this.log(logMessage, 'ERROR');
    },

    /**
     * Checks log file size and initiates rotation if needed.
     */
    async checkLogSize() {
        try {
            if (!fs.existsSync(this.logPath)) {
                return; // File doesn't exist, nothing to check
            }
            const stats = await fs.promises.stat(this.logPath);
            if (stats.size >= LOG_MAX_SIZE_BYTES) {
                await this.rotateLog();
            }
        } catch (error) {
            console.error(`Error checking log size for ${this.logPath}: ${error.message}`);
            // Don't log this error using Logger.error to avoid potential infinite loop
        }
    },

    /**
     * Rotates the log file by renaming the current one and creating a new one.
     */
    async rotateLog() {
        const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\..+/, ''); // Clean timestamp
        const backupPath = path.join(this.logDir, `shopify-sync.${timestamp}.log`);
        const rotateMessage = `[${new Date().toISOString()}] [INFO] Rotating log file. Previous log: ${path.basename(backupPath)}\n`;

        try {
            console.log(`Log file size limit reached. Rotating ${path.basename(this.logPath)} to ${path.basename(backupPath)}`);
            // Rename current log file
            await fs.promises.rename(this.logPath, backupPath);
            // Create a new empty log file with rotation message
            await fs.promises.writeFile(this.logPath, rotateMessage, 'utf8');
            console.log("Log rotation complete.");
            // Clean up old logs asynchronously (don't block)
            this.cleanOldLogs().catch(err => console.error(`Error during old log cleanup: ${err.message}`));
        } catch (error) {
            console.error(`Error rotating log file ${this.logPath}: ${error.message}`);
            // Attempt to continue logging to the original file if rename failed
            try {
                await fs.promises.appendFile(this.logPath, `[${new Date().toISOString()}] [ERROR] Log rotation failed: ${error.message}\n`, 'utf8');
            } catch (appendError) {
                console.error(`CRITICAL: Failed to write rotation error to log file: ${appendError.message}`);
            }
        }
    },

    /**
     * Deletes older log files, keeping a configured number of recent ones.
     */
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
                         return null; // Skip files that can't be stated
                    }
                })
                .filter(Boolean) // Remove null entries
                .sort((a, b) => b.mtime - a.mtime); // Sort newest first

            // Keep a reasonable number of logs (e.g., last 10)
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
    },

    /**
     * Reads log content paginated (useful for large logs).
     * @param {string|null} [filePath=null] - Path to the log file (defaults to current).
     * @param {number} [page=1] - Page number (1-based).
     * @param {number} [pageSize=1000] - Lines per page.
     * @returns {Promise<object>} - { content: string[], page, totalPages, totalLines, hasMore }
     */
    async readLogPaginated(filePath = null, page = 1, pageSize = 1000) {
        const targetPath = filePath || this.logPath;
        const result = { content: [], page: 1, totalPages: 0, totalLines: 0, hasMore: false };

        try {
            if (!fs.existsSync(targetPath)) {
                Logger.warn(`Log file not found: ${targetPath}`);
                return result;
            }

            page = Math.max(1, page);
            pageSize = Math.max(10, Math.min(5000, pageSize)); // Clamp page size

            let lineCount = 0;
            // Use a stream to count lines efficiently without loading the whole file
            const countStream = fs.createReadStream(targetPath);
            const rlCount = readline.createInterface({ input: countStream, crlfDelay: Infinity });

            for await (const _ of rlCount) {
                lineCount++;
            }
             // Ensure stream is closed
             countStream.close();


            result.totalLines = lineCount;
            result.totalPages = Math.ceil(lineCount / pageSize);

            // Adjust page if out of bounds
            if (page > result.totalPages && result.totalPages > 0) {
                page = result.totalPages;
            }
            result.page = page;

            const startLine = (page - 1) * pageSize;
            const endLine = startLine + pageSize;
            let currentLineNum = 0;

            // Re-read the file to get the specific page content
            const contentStream = fs.createReadStream(targetPath);
            const contentRl = readline.createInterface({
                input: contentStream,
                crlfDelay: Infinity
            });

            for await (const line of contentRl) {
                currentLineNum++;
                if (currentLineNum > startLine && currentLineNum <= endLine) {
                    result.content.push(line);
                }
                if (currentLineNum >= endLine) {
                    contentRl.close(); // Stop reading once page is filled
                    contentStream.destroy(); // Explicitly destroy stream
                    break;
                }
            }
             // Ensure stream is closed if loop finishes before break
             if (!contentStream.destroyed) {
                contentStream.close();
             }


            result.hasMore = page < result.totalPages;
            return result;

        } catch (error) {
            Logger.error(`Error reading log file ${targetPath} paginated`, error);
            // Return default result structure on error
            result.page = page; // Keep requested page
            return result;
        }
    },

     /**
     * Gets metadata for available log files.
     * @returns {Promise<Array<object>>} - Array of log file info objects.
     */
    async getLogFiles() {
        try {
            const files = await fs.promises.readdir(this.logDir);
            const logFilePromises = files
                .filter(file => file.startsWith('shopify-sync.') && file.endsWith('.log'))
                .map(async (file) => {
                    const filePath = path.join(this.logDir, file);
                    try {
                        const stats = await fs.promises.stat(filePath);
                        return {
                            name: file,
                            path: filePath,
                            size: stats.size,
                            sizeFormatted: this.formatFileSize(stats.size),
                            createdAt: stats.birthtime,
                            modifiedAt: stats.mtime,
                            isCurrentLog: filePath === this.logPath
                        };
                    } catch (statError) {
                        Logger.error(`Could not get stats for log file ${file}`, statError);
                        return null; // Skip files with errors
                    }
                });

            const logFiles = (await Promise.all(logFilePromises))
                .filter(Boolean) // Remove nulls from failed stats
                .sort((a, b) => b.modifiedAt.getTime() - a.modifiedAt.getTime()); // Sort newest first

            return logFiles;
        } catch (error) {
            Logger.error("Error getting log file list", error);
            return [];
        }
    },

    /**
     * Formats file size in bytes to a human-readable string.
     * @param {number} bytes - Size in bytes.
     * @returns {string} - Formatted size (B, KB, MB, GB).
     */
    formatFileSize(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        // Handle potential non-numeric or negative input
        if (isNaN(bytes) || bytes < 0) return 'Invalid Size';
        const i = Math.floor(Math.log(bytes) / Math.log(k));
         // Ensure index is within bounds
         const index = Math.max(0, Math.min(i, sizes.length - 1));
        return parseFloat((bytes / Math.pow(k, index)).toFixed(2)) + ' ' + sizes[index];
    }
};

/**
 * Cleans a SKU: trims, removes non-alphanumeric characters (adjust regex if needed), converts to uppercase.
 * Handles null or empty SKUs.
 * @param {string | number | null | undefined} sku - The original SKU.
 * @returns {string | null} - The cleaned SKU or null if input is invalid.
 */
function cleanSku(sku) {
    if (sku === null || sku === undefined) return null; // Devuelve null para inválidos
    try {
        // Limpia conservando SOLO números y quitando ceros a la izquierda
        const cleaned = String(sku).trim().replace(/[^0-9]/g, '').replace(/^0+/, '');
        // Devuelve null si queda vacío, o el número limpio como string
        return cleaned.length > 0 ? cleaned : null;
    } catch (e) {
        Logger.error(`Error cleaning SKU: ${sku}`, e);
        return null;
    }
}

/**
 * Performs an API request (using Axios) with rate limiting and retries.
 * @param {object} config - Axios request configuration (url, method, headers, data, etc.).
 * @param {boolean} [useShopifyLimiter=false] - Whether to apply the Shopify rate limiter.
 * @param {number} [retries=MAX_RETRIES] - Number of retries remaining.
 * @returns {Promise<object>} - The data part of the Axios response.
 * @throws {Error} - Throws error if request fails after all retries.
 */
async function fetchWithRetry(config, useShopifyLimiter = false, retries = MAX_RETRIES) {
    if (useShopifyLimiter) {
        // Wait if rate limit is exceeded
        try {
            await shopifyLimiter.removeTokens(1);
        } catch (limitError) {
             // Handle potential errors from the limiter itself
             Logger.error("Error removing token from rate limiter", limitError);
             throw limitError; // Rethrow or handle as appropriate
        }
    }

    try {
        // Set a reasonable timeout for the request
        const response = await axios({ ...config, timeout: API_TIMEOUT });
        // Add basic check for expected data structure if possible (e.g., Shopify GraphQL)
        if (useShopifyLimiter && response.data && response.data.errors) {
             // Log GraphQL errors even if the HTTP request succeeded
             Logger.warn(`GraphQL errors in response from ${config.url}: ${JSON.stringify(response.data.errors)}`);
        }
        return response.data; // Return only the data part
    } catch (error) {
        const attempt = MAX_RETRIES - retries + 1;
        const statusCode = error.response?.status;
        const responseData = error.response?.data;
        const errorMessage = error.message || 'Unknown error';

        Logger.warn(`API Request failed (Attempt ${attempt}/${MAX_RETRIES}): ${config.method} ${config.url} - Status: ${statusCode || 'N/A'} - Message: ${errorMessage}`);
        if (responseData) {
            Logger.warn(`Response Data: ${JSON.stringify(responseData)}`); // Log response body on error
        }


        // Check if retry is possible and worthwhile
        if (retries > 0 && (statusCode === 429 || statusCode >= 500 || error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.code === 'ENOTFOUND' || error.code === 'ECONNRESET')) {
            // Exponential backoff for retries (e.g., 1s, 2s, 4s) + jitter
            const delay = Math.pow(2, MAX_RETRIES - retries) * 1000 + Math.random() * 1000;
            Logger.log(`Retrying in ${(delay / 1000).toFixed(1)}s... (${retries} retries left)`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return fetchWithRetry(config, useShopifyLimiter, retries - 1); // Retry
        } else {
            // No more retries or non-retryable error
            Logger.error(`API Request failed permanently after ${attempt} attempts: ${config.method} ${config.url}`, error);
            // Re-throw the original error for upstream handling
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
        const responseData = await fetchWithRetry({
            method: 'GET',
            url: DATA_API_URL,
            headers: { 'Accept': 'application/json' } // Ensure correct Accept header
        });
        // Adjust based on actual API response structure
        const localProducts = responseData?.value || responseData || [];
        if (!Array.isArray(localProducts)) {
             // Log the actual response for debugging
             Logger.error(`Invalid data structure received from product API: Expected an array, got ${typeof localProducts}. Response: ${JSON.stringify(responseData)}`);
             throw new Error(`Invalid data structure received from product API.`);
        }
        Logger.log(`Fetched ${localProducts.length} local product records.`);
        return localProducts;
    } catch (error) {
        // Error is already logged by fetchWithRetry if it's an API error
        if (!error.response) { // Log if it's not an Axios error already logged
             Logger.error("Failed to fetch local product data", error);
        }
        throw error; // Propagate error to stop the sync if needed
    }
}

/**
 * Fetches and processes inventory data from the local API.
 * @returns {Promise<object>} - Map of inventory data keyed by cleaned SKU.
 */
async function getLocalInventory() {
    if (SYNC_TYPE === 'price') {
        Logger.log("Skipping local inventory fetch (sync type is 'price').");
        return {}; // Return empty map if only syncing price
    }

    Logger.log("Fetching local inventory data from API...");
    try {
        const responseData = await fetchWithRetry({
            method: 'GET',
            url: INVENTORY_API_URL,
            headers: { 'Accept': 'application/json' }
        });
        // Adjust based on actual API response structure
        const inventoryData = responseData?.value || responseData || [];
         if (!Array.isArray(inventoryData)) {
              // Log the actual response for debugging
             Logger.error(`Invalid data structure received from inventory API: Expected an array, got ${typeof inventoryData}. Response: ${JSON.stringify(responseData)}`);
             throw new Error(`Invalid data structure received from inventory API.`);
        }
        Logger.log(`Fetched ${inventoryData.length} local inventory records.`);

        // Process inventory: Group by SKU, find most recent, calculate quantity
        const inventoryMap = {};
        const inventoryBySku = {};

        for (const item of inventoryData) {
            // Ensure CodigoProducto exists and is valid before cleaning
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
            // Add date parsing for reliable sorting
            item.parsedDate = item.Fecha ? new Date(item.Fecha) : new Date(0); // Handle missing/invalid dates
             if (isNaN(item.parsedDate)) {
                 Logger.warn(`Invalid date format for SKU ${sku}, Fecha: ${item.Fecha}. Using epoch.`);
                 item.parsedDate = new Date(0);
             }
            inventoryBySku[sku].push(item);
        }

        for (const [sku, items] of Object.entries(inventoryBySku)) {
            // Sort by parsed date, descending (most recent first)
            const sortedItems = items.sort((a, b) => b.parsedDate - a.parsedDate);
            const mostRecentItem = sortedItems[0];

            // Calculate quantity carefully, handling potential null/undefined/non-numeric values
            const initial = parseFloat(mostRecentItem.CantidadInicial || 0);
            const received = parseFloat(mostRecentItem.CantidadEntradas || 0);
            const shipped = parseFloat(mostRecentItem.CantidadSalidas || 0);

            // Ensure calculations result in valid numbers
            if (isNaN(initial) || isNaN(received) || isNaN(shipped)) {
                 Logger.warn(`Invalid numeric values for inventory calculation for SKU ${sku}. Record: ${JSON.stringify(mostRecentItem)}. Skipping inventory update for this SKU.`);
                 continue; // Skip this SKU if data is bad
            }

            // Calculate current available quantity
            const calculatedQuantity = Math.max(0, initial + received - shipped);

            inventoryMap[sku] = {
                ...mostRecentItem, // Include original data
                calculatedQuantity: Math.floor(calculatedQuantity) // Use floor, ensure integer
            };
        }

        Logger.log(`Processed inventory for ${Object.keys(inventoryMap).length} unique SKUs.`);
        return inventoryMap;
    } catch (error) {
         if (!error.response) { // Log if it's not an Axios error already logged
            Logger.error("Failed to fetch or process local inventory data", error);
         }
        throw error;
    }
}

/**
 * Fetches all product variants from Shopify using GraphQL pagination.
 * @returns {Promise<Array<object>>} - Array of Shopify variant objects.
 */
async function getAllShopifyVariants() {
    Logger.log("Fetching all product variants from Shopify...");
    const allVariants = [];
    let hasNextPage = true;
    let cursor = null;
    let pageCount = 0;
    const MAX_PAGES = 500; // Add a safeguard against infinite loops

    // Define the GraphQL query, including inventory levels conditionally
    const inventoryLevelQuery = LOCATION_ID ? `
        inventoryLevels(first: 5) { # Adjust 'first' if more locations per item
          edges {
            node {
              available
              location {
                id # Location GID (e.g., gid://shopify/Location/11223)
              }
            }
          }
        }` : '';

    const queryTemplate = (cursor) => `
      query GetVariants($limit: Int!, ${cursor ? '$cursor: String!' : ''}) {
        productVariants(first: $limit ${cursor ? ', after: $cursor' : ''}) {
          edges {
            cursor
            node {
              id # Variant GID (e.g., gid://shopify/ProductVariant/12345)
              sku
              price
              displayName # Helpful for logging
              # inventoryQuantity # Legacy field, avoid using for multi-location
              inventoryItem {
                id # InventoryItem GID (e.g., gid://shopify/InventoryItem/67890)
                tracked # Whether inventory is tracked
                ${inventoryLevelQuery} # Include inventory levels query part
              }
              product {
                 id # Product GID
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
        const variables = { limit: 100 }; // Fetch 100 variants per page
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
                data: JSON.stringify({ query, variables }), // Send variables separately
            }, true); // Use Shopify rate limiter

            // Check for top-level errors first
            if (responseData.errors) {
                // Log specific GraphQL errors
                Logger.error(`GraphQL Error fetching variants (Page ${pageCount}): ${JSON.stringify(responseData.errors)}`);
                // Decide whether to stop or continue based on error type
                if (responseData.errors.some(e => e.extensions?.code === 'THROTTLED')) {
                    Logger.warn("Rate limit hit during variant fetch. Consider reducing SHOPIFY_RATE_LIMIT. Continuing after delay...");
                    await new Promise(resolve => setTimeout(resolve, 5000)); // 5s delay before next attempt
                    continue; // Retry the same page after delay
                } else {
                     // For other errors, decide if fatal or skippable
                     Logger.error("Non-throttling GraphQL error encountered. Stopping variant fetch.");
                     throw new Error(`GraphQL Error fetching variants: ${JSON.stringify(responseData.errors)}`);
                }
            }

            const variantsData = responseData?.data?.productVariants;
            if (!variantsData) {
                 // This case implies a successful HTTP request but unexpected GraphQL response structure
                 Logger.error(`No productVariants data found in response for page ${pageCount}. Response: ${JSON.stringify(responseData)}`);
                 // Potentially break or throw depending on severity
                 break; // Stop if data structure is unexpected
            }

            const edges = variantsData.edges || [];
            for (const edge of edges) {
                 // More robust validation of the node structure
                 if (edge?.node?.id && edge.node.inventoryItem?.id) {
                    // Ensure inventoryLevels exist if requested (and handle cases where it might be null)
                    if (LOCATION_ID && !edge.node.inventoryItem.inventoryLevels) {
                        Logger.warn(`Variant ${edge.node.id} (SKU: ${edge.node.sku}) requested inventoryLevels but received none. Check permissions or product setup.`);
                        // Decide how to handle: skip, use fallback, etc. Here we push it anyway but inventory might be wrong.
                    }
                    allVariants.push(edge.node);
                 } else {
                     Logger.warn(`Skipping invalid variant structure received: ${JSON.stringify(edge)}`);
                 }
            }

            hasNextPage = variantsData.pageInfo?.hasNextPage ?? false;
            if (hasNextPage && edges.length > 0) {
                cursor = edges[edges.length - 1].cursor;
            } else {
                 hasNextPage = false; // Ensure loop terminates if edges are empty or no next page
            }

            Logger.log(`Fetched ${edges.length} variants on page ${pageCount}. Total fetched: ${allVariants.length}. Has next page: ${hasNextPage}`);

             // Add a small delay between pages to be kind to the API
             if (hasNextPage) {
                 await new Promise(resolve => setTimeout(resolve, 250)); // 250ms delay
             }


        } catch (error) {
            // Error should have been logged by fetchWithRetry or the GraphQL error handler above
            Logger.error(`Failed to fetch or process Shopify variants page ${pageCount}. Stopping variant fetch.`);
            // Depending on requirements, you might want to stop the entire sync or proceed with fetched variants
            throw new Error("Failed to fetch all Shopify variants."); // Rethrow to stop sync
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
 * Handles price and inventory updates separately due to GraphQL structure.
 * Uses inventorySetOnHandQuantities for multi-location or inventoryAdjustQuantity for single location.
 *
 * @param {object} variant - The Shopify variant object (from getAllShopifyVariants).
 * @param {string|null} newPrice - The new price (as string) or null to skip price update.
 * @param {number|null} newInventory - The absolute inventory quantity or null to skip inventory update.
 * @returns {Promise<object>} - { success: boolean, updatedPrice: boolean, updatedInventory: boolean, message: string, error?: any }
 */
async function updateVariantInShopify(variant, newPrice, newInventory) {
    // --- Input Validation ---
    if (!variant || !variant.id || !variant.inventoryItem?.id) {
        Logger.error("Invalid variant object passed to updateVariantInShopify", variant);
        return { success: false, updatedPrice: false, updatedInventory: false, message: "Invalid variant data received", error: "Invalid variant data" };
    }

    const variantId = variant.id;
    const inventoryItemId = variant.inventoryItem.id;
    const currentPrice = variant.price;

    // Determine current inventory based on whether LOCATION_ID is set and data is available
    let currentInventory = null;
    if (LOCATION_ID) {
        const level = variant.inventoryItem.inventoryLevels?.edges?.find(edge => edge.node.location.id === LOCATION_ID)?.node;
        currentInventory = level?.available; // Will be undefined if not found
    } else {
        // Fallback: Try to get inventoryQuantity if available (might be inaccurate)
        // Or rely on inventoryLevels even without LOCATION_ID if the structure exists
         const firstLevel = variant.inventoryItem.inventoryLevels?.edges?.[0]?.node;
         currentInventory = firstLevel?.available ?? variant.inventoryQuantity ?? null; // Prioritize levels if present
    }


    const productName = variant.product?.title || 'Unknown Product';
    const sku = variant.sku || 'No SKU';
    let updatedPrice = false;
    let updatedInventory = false;
    let messages = [];
    let errors = []; // Collect specific errors

    // --- 1. Price Update ---
    const shouldUpdatePrice = SYNC_TYPE === 'price' || SYNC_TYPE === 'both';
    // Convert newPrice to string for consistent comparison if it's a number
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
                price: newPriceStr, // Ensure price is sent as a string
            }
        };

        try {
            const result = await fetchWithRetry({
                method: 'POST',
                url: SHOPIFY_GRAPHQL_URL,
                headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN },
                data: JSON.stringify({ query: priceMutation, variables: priceVariables }),
            }, true); // Use limiter

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
                 // Handle cases where the mutation might succeed but return unexpected data
                 Logger.warn(`Unknown result structure after price update for SKU ${sku}: ${JSON.stringify(result)}`);
                 messages.push("Price update status unknown.");
                 // Consider treating this as an error depending on strictness required
                 // errors.push("Price update status unknown.");
            }
        } catch (error) {
            // Error should be logged by fetchWithRetry
            const errorMsg = `Price update failed: API error during mutation.`;
            messages.push(errorMsg);
            errors.push(errorMsg);
        }
    } else if (shouldUpdatePrice && newPriceStr !== null) {
         messages.push("Price already up-to-date.");
    }


    // --- 2. Inventory Update ---
    const shouldUpdateInventory = SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both';
     // Ensure we have valid numbers for comparison and update
    const currentInventoryNum = (currentInventory !== undefined && currentInventory !== null && !isNaN(Number(currentInventory))) ? Number(currentInventory) : null;
    const newInventoryNum = (newInventory !== null && !isNaN(Number(newInventory))) ? Math.floor(Number(newInventory)) : null; // Ensure integer

    // Only proceed if inventory should be updated, the new value is valid, and it's different from the current value
    if (shouldUpdateInventory && newInventoryNum !== null && newInventoryNum !== currentInventoryNum) {
         if (!variant.inventoryItem.tracked) {
             Logger.warn(`SKU ${sku} (${productName}) inventory is not tracked by Shopify. Skipping inventory update.`);
             messages.push("Inventory not tracked.");
         } else if (currentInventoryNum === null && LOCATION_ID) {
              // If multi-location is expected but current inventory couldn't be determined, log a warning
              Logger.warn(`SKU ${sku} (${productName}): Could not determine current inventory level for location ${LOCATION_ID}. Proceeding with update, but comparison is skipped.`);
              // Proceed to update, but acknowledge the missing comparison value
         }

         // Choose mutation based on LOCATION_ID presence
         if (LOCATION_ID) {
             // Multi-location: Use inventorySetOnHandQuantities
             Logger.log(`Updating inventory for SKU ${sku} (${productName}) at Location ${LOCATION_ID}: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum}`);
             const inventoryMutation = `
                mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
                  inventorySetOnHandQuantities(input: $input) {
                    inventoryAdjustmentGroup {
                       id
                       # reason # Can check reason if needed
                       # changes { state } # Can check state if needed
                    }
                    userErrors {
                      field
                      code # Use code for better error handling
                      message
                    }
                  }
                }`;
             const inventoryVariables = {
                 input: {
                     reason: "external_sync", // Or a more descriptive reason
                     setQuantities: [{
                         inventoryItemId: inventoryItemId,
                         locationId: LOCATION_ID,
                         quantity: newInventoryNum, // Use the validated integer
                     }]
                 }
             };
             try {
                 const result = await fetchWithRetry({
                     method: 'POST',
                     url: SHOPIFY_GRAPHQL_URL,
                     headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN },
                     data: JSON.stringify({ query: inventoryMutation, variables: inventoryVariables }),
                 }, true); // Use limiter

                 const setResult = result?.data?.inventorySetOnHandQuantities;
                 const userErrors = setResult?.userErrors;

                 if (userErrors && userErrors.length > 0) {
                     const errorMsg = `Inventory update failed: ${userErrors.map(e => `(${e.field}) ${e.message}`).join(', ')}`;
                     Logger.error(`Error setting inventory for SKU ${sku}: ${JSON.stringify(userErrors)}`);
                     messages.push(errorMsg);
                     errors.push(errorMsg);
                 } else if (setResult?.inventoryAdjustmentGroup?.id) {
                     Logger.log(`✅ Inventory set successfully for SKU ${sku} at ${LOCATION_ID}.`, 'SUCCESS');
                     updatedInventory = true;
                     messages.push(`Inventory: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum}`);
                 } else {
                      Logger.warn(`Unknown result structure after inventory set for SKU ${sku}: ${JSON.stringify(result)}`);
                      messages.push("Inventory update status unknown.");
                      // errors.push("Inventory update status unknown.");
                 }
             } catch (error) {
                 const errorMsg = `Inventory update failed: API error during mutation.`;
                 messages.push(errorMsg);
                 errors.push(errorMsg);
             }

         } else {
             // Single location (or fallback): Use inventoryAdjustQuantity
             Logger.log(`Updating inventory for SKU ${sku} (${productName}) using Adjust: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum}`);
             // Calculate delta carefully, especially if currentInventoryNum is null
             const delta = newInventoryNum - (currentInventoryNum ?? 0);

             // Avoid sending delta 0 if possible, though API might handle it
             if (delta === 0) {
                  messages.push("Inventory delta is 0, skipping adjust call.");
             } else {
                 const inventoryMutation = `
                   mutation InventoryAdjustQuantity($input: InventoryAdjustQuantityInput!) {
                     inventoryAdjustQuantity(input: $input) {
                       inventoryLevel {
                         available
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
                         availableDelta: delta,
                     }
                 };
                 try {
                     const result = await fetchWithRetry({
                         method: 'POST',
                         url: SHOPIFY_GRAPHQL_URL,
                         headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN },
                         data: JSON.stringify({ query: inventoryMutation, variables: inventoryVariables }),
                     }, true); // Use limiter

                     const adjustResult = result?.data?.inventoryAdjustQuantity;
                     const userErrors = adjustResult?.userErrors;

                     if (userErrors && userErrors.length > 0) {
                         const errorMsg = `Inventory update failed: ${userErrors.map(e => `(${e.field}) ${e.message}`).join(', ')}`;
                         Logger.error(`Error adjusting inventory for SKU ${sku}: ${JSON.stringify(userErrors)}`);
                         messages.push(errorMsg);
                         errors.push(errorMsg);
                     } else if (adjustResult?.inventoryLevel) {
                         // Optional: Verify if the new available matches expected, though delays can occur
                         const finalAvailable = adjustResult.inventoryLevel.available;
                         Logger.log(`✅ Inventory adjusted successfully for SKU ${sku}. New available: ${finalAvailable}`, 'SUCCESS');
                         updatedInventory = true;
                         messages.push(`Inventory: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum} (Delta: ${delta}, Final: ${finalAvailable})`);
                         // Add a warning if final quantity doesn't match expected (can happen due to race conditions/other adjustments)
                         if (finalAvailable !== newInventoryNum) {
                             Logger.warn(`SKU ${sku}: Final inventory level (${finalAvailable}) differs from expected (${newInventoryNum}) after adjustment.`);
                         }
                     } else {
                          Logger.warn(`Unknown result structure after inventory adjust for SKU ${sku}: ${JSON.stringify(result)}`);
                          messages.push("Inventory update status unknown.");
                          // errors.push("Inventory update status unknown.");
                     }
                 } catch (error) {
                     const errorMsg = `Inventory update failed: API error during mutation.`;
                     messages.push(errorMsg);
                     errors.push(errorMsg);
                 }
             } // end if delta !== 0
         }
    } else if (shouldUpdateInventory && newInventoryNum !== null) {
         messages.push("Inventory already up-to-date.");
    } else if (shouldUpdateInventory && newInventoryNum === null) {
         messages.push("Invalid new inventory value (null)."); // Log if the new value itself is invalid
    }

    // --- 3. Final Result ---
    const success = errors.length === 0; // Success if no specific errors were collected
    const updated = updatedPrice || updatedInventory;
    let finalMessage = `SKU ${sku} (${productName}): ${messages.join(' | ')}`;

    if (updated && success) {
        Logger.log(`✅ ${finalMessage}`, 'SUCCESS');
    } else if (!success) {
         Logger.error(`❌ ${finalMessage}`); // Log the combined message which includes error details
    } else {
         Logger.log(`ℹ️ SKU ${sku} (${productName}) - No changes needed.`);
         finalMessage = `SKU ${sku} (${productName}): No changes needed.`; // Cleaner message if no updates
    }


    return {
        success,
        updated,
        updatedPrice,
        updatedInventory,
        message: finalMessage,
        error: success ? null : errors.join('; ') // Return joined errors if any occurred
    };
}


/**
 * Main synchronization function.
 */
async function syncShopifyData() {
    Logger.log(`🚀 Starting Shopify Sync (Mode: ${SYNC_MODE.toUpperCase()}, Type: ${SYNC_TYPE.toUpperCase()})`);
    const startTime = Date.now();

    try {
        // Fetch data concurrently
        Logger.log("Fetching data from Local APIs and Shopify...");
        const [localProducts, localInventoryMap, shopifyVariants] = await Promise.all([
            getLocalProducts(),
            getLocalInventory(), // This returns the map directly now
            getAllShopifyVariants()
        ]).catch(fetchError => {
             // Catch errors during initial data fetching phase
             Logger.error("💥 FATAL ERROR during initial data fetching. Aborting sync.", fetchError);
             throw fetchError; // Rethrow to stop execution
        });
        Logger.log("Data fetching complete.");

        // --- Data Preparation ---
        // Map Shopify variants by cleaned SKU for quick lookup
        const shopifyVariantMap = new Map();
        for (const variant of shopifyVariants) {
            // Ensure variant and SKU are valid before cleaning/adding
            if (!variant || !variant.sku) {
                 Logger.warn(`Shopify variant missing SKU. ID: ${variant?.id || 'N/A'}. Skipping.`);
                 continue;
            }
            const cleanedSku = cleanSku(variant.sku);
            if (cleanedSku) {
                 if (shopifyVariantMap.has(cleanedSku)) {
                     // Log details of both variants involved in the duplicate SKU
                     const existingVariant = shopifyVariantMap.get(cleanedSku);
                     Logger.warn(`Duplicate SKU found in Shopify: ${cleanedSku}. Variant ID: ${variant.id} (${variant.displayName}). Previous: ${existingVariant.id} (${existingVariant.displayName}). Using the last one found.`);
                 }
                shopifyVariantMap.set(cleanedSku, variant);
            } else {
                 Logger.warn(`Shopify variant ID ${variant.id} (${variant.displayName}) has invalid SKU '${variant.sku}'. Skipping.`);
            }
        }

        // Map local products by cleaned SKU
        const localProductMap = new Map();
        for (const product of localProducts) {
             // Ensure product and CodigoProducto are valid
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


        // --- Synchronization Logic ---
        let stats = {
            processed: 0,
            priceUpdates: 0,
            inventoryUpdates: 0,
            bothUpdates: 0, // Count variants where both price and inventory were successfully updated
            skippedNoChange: 0,
            notFoundLocal: 0, // Shopify SKU not found in local data
            notFoundShopify: 0, // Local SKU not found in Shopify data
            errors: 0,
            invalidDataLocal: 0, // Count items skipped due to invalid local data (price/inventory)
        };

        const itemsToProcess = SYNC_MODE === 'shopify_first'
            ? Array.from(shopifyVariantMap.entries()) // Process based on Shopify SKUs [sku, variant]
            : Array.from(localProductMap.entries()); // Process based on Local SKUs [sku, product]

        Logger.log(`Starting update process for ${itemsToProcess.length} items based on ${SYNC_MODE} mode...`);

        // Use Promise.allSettled for concurrent processing with error handling per item
        const updatePromises = itemsToProcess.map(async ([sku, item], index) => {
            let variant = null;
            let localProduct = null;

            // Log progress periodically (moved inside map function)
             if (index > 0 && index % 100 === 0) {
                  Logger.log(`Processing item ${index + 1} / ${itemsToProcess.length} (SKU: ${sku})...`);
             }


            if (SYNC_MODE === 'shopify_first') {
                variant = item; // item is the variant object
                localProduct = localProductMap.get(sku);
                if (!localProduct) {
                    Logger.log(`⚠️ SKU ${sku} (${variant.displayName}) found in Shopify but not in local data. Skipping.`, 'WARN');
                    return { status: 'skipped', reason: 'notFoundLocal', sku: sku };
                }
            } else { // local_first
                localProduct = item; // item is the product object
                variant = shopifyVariantMap.get(sku);
                if (!variant) {
                    Logger.log(`⚠️ SKU ${sku} found in local data but not in Shopify. Skipping.`, 'WARN');
                    return { status: 'skipped', reason: 'notFoundShopify', sku: sku };
                }
            }

            // --- Enhanced Data Validation and Logging ---
            let newPrice = null;
            let newInventory = null;
            let dataValid = true;

            // Validate and get price
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
                    dataValid = false; // Mark as invalid if price sync is expected but data missing
                }
            }

            // Validate and get inventory
            if (SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') {
                const inventoryData = localInventoryMap[sku];
                if (inventoryData && inventoryData.calculatedQuantity !== undefined && inventoryData.calculatedQuantity !== null) {
                     const invValue = Number(inventoryData.calculatedQuantity);
                     if(!isNaN(invValue)){
                         newInventory = Math.floor(invValue); // Already calculated and floored in getLocalInventory
                     } else {
                         // This case should be rare if getLocalInventory handles NaN
                         Logger.warn(`SKU ${sku}: Invalid non-numeric calculated inventory ('${inventoryData.calculatedQuantity}') found. Skipping inventory update.`);
                         dataValid = false;
                     }

                } else {
                    Logger.warn(`SKU ${sku}: Missing calculated inventory in local data map. Skipping inventory update.`);
                    dataValid = false; // Mark as invalid if inventory sync is expected but data missing
                }
            }

            if (!dataValid) {
                 // If essential data for the requested SYNC_TYPE is invalid/missing
                 return { status: 'skipped', reason: 'invalidDataLocal', sku: sku };
            }

            // Log the data being sent *before* the API call
            Logger.debug(`SKU ${sku}: Attempting update. Target Variant ID: ${variant.id}. Local Price: ${localProduct?.Venta1}. Local Inv Record: ${JSON.stringify(localInventoryMap[sku])}. Calculated New Price: ${newPrice}. Calculated New Inventory: ${newInventory}.`);


            try {
                // Call the update function
                const result = await updateVariantInShopify(variant, newPrice, newInventory);
                return { status: 'processed', result: result, sku: sku }; // Return the result object

            } catch (error) {
                // Catch unexpected errors *during* the updateVariantInShopify call itself
                // (though most API errors should be caught inside that function)
                Logger.error(`Critical error processing SKU ${sku} during updateVariantInShopify call.`, error);
                return { status: 'error', error: error.message || 'Unknown critical error', sku: sku };
            }
        }); // End of map function

        // Process results after all promises settle
        const results = await Promise.allSettled(updatePromises);

        results.forEach(outcome => {
            if (outcome.status === 'fulfilled') {
                const data = outcome.value; // The object returned from the map function
                stats.processed++; // Count all attempts that reached the processing stage

                if (data.status === 'skipped') {
                    if (data.reason === 'notFoundLocal') stats.notFoundLocal++;
                    else if (data.reason === 'notFoundShopify') stats.notFoundShopify++;
                    else if (data.reason === 'invalidDataLocal') stats.invalidDataLocal++;
                } else if (data.status === 'processed') {
                    // Process the result from updateVariantInShopify
                    const result = data.result;
                    if (!result.success) {
                        stats.errors++;
                        // Log the specific error message from the result
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
                     // Error already logged, maybe log SKU again for clarity
                     Logger.error(`Critical error was recorded for SKU ${data.sku}`);
                }
            } else {
                // Handle rejected promises from the map function (should be rare now)
                stats.errors++;
                Logger.error(`Unhandled promise rejection during processing: ${outcome.reason}`);
            }
        });


        // --- Final Summary ---
        const endTime = Date.now();
        const duration = ((endTime - startTime) / 1000).toFixed(2); // Duration in seconds

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
             Logger.log("Sync completed. No updates applied."); // Catch-all for cases with no updates and no errors
        }

    } catch (error) {
        // Catch errors from initial data fetching or setup (already logged)
        // Logger.error("💥 FATAL ERROR during synchronization process", error); // Already logged if thrown from Promise.all
        process.exitCode = 1; // Indicate failure
    } finally {
         // Ensure all logs are written before exiting
        await Logger.processQueue(); // Process any remaining logs in the queue
        Logger.log(`🏁 Sync process finished at ${new Date().toLocaleString()}`);
         // Wait a tiny bit more for logs to flush, especially if exiting immediately after
         await new Promise(resolve => setTimeout(resolve, 200));
    }
}

// --- Main Execution ---
(async () => {
    Logger.init(); // Initialize the logger first
    Logger.log(`Script started at ${new Date().toLocaleString()}`);
    await syncShopifyData(); // Run the main sync function
})();

// Optional: Handle graceful shutdown
process.on('SIGINT', async () => {
    Logger.log('Received SIGINT. Shutting down gracefully...');
    // Add any cleanup logic here if needed
    await Logger.processQueue(); // Ensure logs are written
    process.exit(0);
});

process.on('SIGTERM', async () => {
    Logger.log('Received SIGTERM. Shutting down gracefully...');
    await Logger.processQueue();
    process.exit(0);
});
