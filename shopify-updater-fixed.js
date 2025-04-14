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
const SHOPIFY_API_VERSION = '2024-04'; // Use a recent, stable API version
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
                this.processQueue();
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
        const logEntry = `[${timestamp}] [${level}] ${message}\n`;

        // Log to console immediately
        if (level === 'ERROR') {
            console.error(message);
        } else {
            console.log(message);
        }

        // Add to queue and trigger processing
        this.logQueue.push(logEntry);
        // Debounce processing slightly to batch writes
        setTimeout(() => this.processQueue(), 50);
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
                .map(file => ({
                    name: file,
                    path: path.join(this.logDir, file),
                    // Get modification time for sorting
                    mtime: fs.statSync(path.join(this.logDir, file)).mtime.getTime()
                }))
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
            let lines = [];
            const fileStream = fs.createReadStream(targetPath);
            const rl = readline.createInterface({
                input: fileStream,
                crlfDelay: Infinity
            });

            // Efficiently count lines first (can be slow for huge files)
            // Consider alternative counting methods if performance is critical
             for await (const line of rl) {
                 lineCount++;
             }

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
                if (currentLineNum > endLine) {
                    contentRl.close(); // Stop reading once page is filled
                    break;
                }
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
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
};

/**
 * Cleans a SKU: trims, removes non-alphanumeric characters (adjust regex if needed), converts to uppercase.
 * Handles null or empty SKUs.
 * @param {string | number | null | undefined} sku - The original SKU.
 * @returns {string | null} - The cleaned SKU or null if input is invalid.
 */
function cleanSku(sku) {
    if (sku === null || sku === undefined) return null;
    // Keep alphanumeric and possibly dashes/underscores if they are part of your SKU format
    // Example: /[^a-zA-Z0-9\-\_]/g to keep letters, numbers, dashes, underscores
    const cleaned = String(sku).trim().toUpperCase().replace(/[^A-Z0-9]/g, ''); // Keep only uppercase letters and numbers
    return cleaned.length > 0 ? cleaned : null; // Return null if cleaning results in empty string
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
        await shopifyLimiter.removeTokens(1);
    }

    try {
        // Set a reasonable timeout for the request
        const response = await axios({ ...config, timeout: API_TIMEOUT });
        return response.data; // Return only the data part
    } catch (error) {
        const attempt = MAX_RETRIES - retries + 1;
        const statusCode = error.response?.status;
        const errorMessage = error.message || 'Unknown error';

        Logger.warn(`API Request failed (Attempt ${attempt}/${MAX_RETRIES}): ${config.method} ${config.url} - Status: ${statusCode || 'N/A'} - Message: ${errorMessage}`);

        // Check if retry is possible and worthwhile
        if (retries > 0 && (statusCode === 429 || statusCode >= 500 || error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT')) {
            // Exponential backoff for retries (e.g., 1s, 2s, 4s)
            const delay = Math.pow(2, MAX_RETRIES - retries) * 1000;
            Logger.log(`Retrying in ${delay / 1000}s... (${retries} retries left)`);
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
             throw new Error(`Invalid data structure received from product API: Expected an array, got ${typeof localProducts}`);
        }
        Logger.log(`Fetched ${localProducts.length} local product records.`);
        return localProducts;
    } catch (error) {
        Logger.error("Failed to fetch local product data", error);
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
             throw new Error(`Invalid data structure received from inventory API: Expected an array, got ${typeof inventoryData}`);
        }
        Logger.log(`Fetched ${inventoryData.length} local inventory records.`);

        // Process inventory: Group by SKU, find most recent, calculate quantity
        const inventoryMap = {};
        const inventoryBySku = {};

        for (const item of inventoryData) {
            // Ensure CodigoProducto exists and is valid before cleaning
            if (!item.CodigoProducto) {
                Logger.warn(`Inventory record missing CodigoProducto: ${JSON.stringify(item)}`);
                continue;
            }
            const sku = cleanSku(item.CodigoProducto);
            if (!sku) {
                 Logger.warn(`Could not clean SKU for inventory record: ${JSON.stringify(item)}`);
                 continue;
            }

            if (!inventoryBySku[sku]) {
                inventoryBySku[sku] = [];
            }
            // Add date parsing for reliable sorting
            item.parsedDate = item.Fecha ? new Date(item.Fecha) : new Date(0); // Handle missing/invalid dates
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
        Logger.error("Failed to fetch or process local inventory data", error);
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
              inventoryQuantity # Legacy field, may not be accurate for multi-location
              inventoryItem {
                id # InventoryItem GID (e.g., gid://shopify/InventoryItem/67890)
                tracked # Whether inventory is tracked
                # Fetch inventory levels if multi-location is possible
                 ${LOCATION_ID ? `
                inventoryLevels(first: 5) { # Adjust 'first' if more locations per item
                  edges {
                    node {
                      available
                      location {
                        id # Location GID (e.g., gid://shopify/Location/11223)
                      }
                    }
                  }
                }` : ''}

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

    while (hasNextPage) {
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

            if (responseData.errors) {
                // Log specific GraphQL errors
                Logger.error(`GraphQL Error fetching variants (Page ${pageCount}): ${JSON.stringify(responseData.errors)}`);
                // Decide whether to stop or continue based on error type
                if (responseData.errors.some(e => e.extensions?.code === 'THROTTLED')) {
                    Logger.warn("Rate limit hit during variant fetch. Consider reducing SHOPIFY_RATE_LIMIT. Continuing...");
                    // Optional: Add a delay here before the next loop
                    await new Promise(resolve => setTimeout(resolve, 5000)); // 5s delay
                } else {
                     throw new Error(`GraphQL Error fetching variants: ${JSON.stringify(responseData.errors)}`);
                }

            }

            const variantsData = responseData?.data?.productVariants;
            if (!variantsData) {
                 Logger.warn(`No productVariants data found in response for page ${pageCount}. Response: ${JSON.stringify(responseData)}`);
                 break; // Stop if data structure is unexpected
            }

            const edges = variantsData.edges || [];
            for (const edge of edges) {
                 // Basic validation of the node structure
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
                 hasNextPage = false; // Ensure loop terminates if edges are empty
            }

            Logger.log(`Fetched ${edges.length} variants on page ${pageCount}. Total fetched: ${allVariants.length}. Has next page: ${hasNextPage}`);

        } catch (error) {
            // Error already logged by fetchWithRetry
            Logger.error(`Failed to fetch Shopify variants page ${pageCount}. Stopping variant fetch.`, error);
            // Depending on requirements, you might want to stop the entire sync or proceed with fetched variants
            throw new Error("Failed to fetch all Shopify variants.");
        }
    }

    Logger.log(`Successfully fetched a total of ${allVariants.length} Shopify variants.`);
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
    const variantId = variant.id;
    const inventoryItemId = variant.inventoryItem.id;
    const currentPrice = variant.price;
    const currentInventory = LOCATION_ID
        ? variant.inventoryItem.inventoryLevels?.edges.find(edge => edge.node.location.id === LOCATION_ID)?.node.available
        : variant.inventoryQuantity; // Fallback for single location or if levels weren't fetched

    const productName = variant.product?.title || 'Unknown Product';
    const sku = variant.sku || 'No SKU';
    let updatedPrice = false;
    let updatedInventory = false;
    let messages = [];

    // --- 1. Price Update ---
    const shouldUpdatePrice = SYNC_TYPE === 'price' || SYNC_TYPE === 'both';
    if (shouldUpdatePrice && newPrice !== null && parseFloat(currentPrice) !== parseFloat(newPrice)) {
        Logger.log(`Updating price for SKU ${sku} (${productName}): ${currentPrice} -> ${newPrice}`);
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
                price: newPrice,
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
            if (updateResult?.userErrors?.length > 0) {
                Logger.error(`Error updating price for SKU ${sku}: ${JSON.stringify(updateResult.userErrors)}`);
                messages.push(`Price update failed: ${updateResult.userErrors[0].message}`);
                // Decide if we should stop or continue with inventory
            } else if (updateResult?.productVariant?.id) {
                Logger.log(`✅ Price updated successfully for SKU ${sku}.`, 'SUCCESS');
                updatedPrice = true;
                messages.push(`Price: ${currentPrice} -> ${newPrice}`);
            } else {
                 Logger.warn(`Unknown result structure after price update for SKU ${sku}: ${JSON.stringify(result)}`);
                 messages.push("Price update status unknown.");
            }
        } catch (error) {
            Logger.error(`Failed API call during price update for SKU ${sku}`, error);
            messages.push(`Price update failed: API error`);
            // Decide if we should stop or continue
        }
    } else if (shouldUpdatePrice && newPrice !== null) {
         messages.push("Price already up-to-date.");
    }


    // --- 2. Inventory Update ---
    const shouldUpdateInventory = SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both';
     // Ensure we have a valid number for comparison and update
    const currentInventoryNum = (currentInventory !== undefined && currentInventory !== null) ? Number(currentInventory) : null;
    const newInventoryNum = (newInventory !== null) ? Number(newInventory) : null;

    if (shouldUpdateInventory && newInventoryNum !== null && newInventoryNum !== currentInventoryNum) {
         if (!variant.inventoryItem.tracked) {
             Logger.warn(`SKU ${sku} (${productName}) inventory is not tracked by Shopify. Skipping inventory update.`);
             messages.push("Inventory not tracked.");
         } else if (LOCATION_ID) {
             // Multi-location: Use inventorySetOnHandQuantities (more robust than adjust)
             Logger.log(`Updating inventory for SKU ${sku} (${productName}) at Location ${LOCATION_ID}: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum}`);
             const inventoryMutation = `
                mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
                  inventorySetOnHandQuantities(input: $input) {
                    inventoryAdjustmentGroup {
                       id
                       reason
                       changes { state } # Check the state of changes
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
                         quantity: newInventoryNum,
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
                 if (setResult?.userErrors?.length > 0) {
                     Logger.error(`Error setting inventory for SKU ${sku}: ${JSON.stringify(setResult.userErrors)}`);
                     messages.push(`Inventory update failed: ${setResult.userErrors[0].message}`);
                 } else if (setResult?.inventoryAdjustmentGroup?.id) {
                     Logger.log(`✅ Inventory set successfully for SKU ${sku} at ${LOCATION_ID}.`, 'SUCCESS');
                     updatedInventory = true;
                     messages.push(`Inventory: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum}`);
                 } else {
                      Logger.warn(`Unknown result structure after inventory set for SKU ${sku}: ${JSON.stringify(result)}`);
                      messages.push("Inventory update status unknown.");
                 }
             } catch (error) {
                 Logger.error(`Failed API call during inventory set for SKU ${sku}`, error);
                 messages.push(`Inventory update failed: API error`);
             }

         } else {
             // Single location (or fallback): Use inventoryAdjustQuantity
             Logger.log(`Updating inventory for SKU ${sku} (${productName}) using Adjust: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum}`);
             const delta = newInventoryNum - (currentInventoryNum ?? 0); // Calculate delta carefully
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
                 if (adjustResult?.userErrors?.length > 0) {
                     Logger.error(`Error adjusting inventory for SKU ${sku}: ${JSON.stringify(adjustResult.userErrors)}`);
                     messages.push(`Inventory update failed: ${adjustResult.userErrors[0].message}`);
                 } else if (adjustResult?.inventoryLevel) {
                     Logger.log(`✅ Inventory adjusted successfully for SKU ${sku}. New available: ${adjustResult.inventoryLevel.available}`, 'SUCCESS');
                     updatedInventory = true;
                     messages.push(`Inventory: ${currentInventoryNum ?? 'N/A'} -> ${newInventoryNum} (Delta: ${delta})`);
                 } else {
                      Logger.warn(`Unknown result structure after inventory adjust for SKU ${sku}: ${JSON.stringify(result)}`);
                      messages.push("Inventory update status unknown.");
                 }
             } catch (error) {
                 Logger.error(`Failed API call during inventory adjust for SKU ${sku}`, error);
                 messages.push(`Inventory update failed: API error`);
             }
         }
    } else if (shouldUpdateInventory && newInventoryNum !== null) {
         messages.push("Inventory already up-to-date.");
    }

    // --- 3. Final Result ---
    const success = !messages.some(msg => msg.includes('failed')); // Success if no failures reported
    const updated = updatedPrice || updatedInventory;
    let finalMessage = `SKU ${sku} (${productName}): ${messages.join(' | ')}`;

    if (updated) {
        Logger.log(`✅ ${finalMessage}`, 'SUCCESS');
    } else if (!success) {
         Logger.error(`❌ ${finalMessage}`);
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
        // Include error details if needed for summary reporting
        error: success ? null : messages.filter(msg => msg.includes('failed')).join('; ')
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
        const [localProducts, localInventory, shopifyVariants] = await Promise.all([
            getLocalProducts(),
            getLocalInventory(),
            getAllShopifyVariants()
        ]);
        Logger.log("Data fetching complete.");

        // --- Data Preparation ---
        // Map Shopify variants by cleaned SKU for quick lookup
        const shopifyVariantMap = new Map();
        for (const variant of shopifyVariants) {
            const cleanedSku = cleanSku(variant.sku);
            if (cleanedSku) {
                 if (shopifyVariantMap.has(cleanedSku)) {
                     Logger.warn(`Duplicate SKU found in Shopify: ${cleanedSku}. Variant ID: ${variant.id}. Previous: ${shopifyVariantMap.get(cleanedSku).id}. Using the last one found.`);
                 }
                shopifyVariantMap.set(cleanedSku, variant);
            } else {
                 Logger.warn(`Shopify variant ID ${variant.id} (${variant.displayName}) has no valid SKU. Skipping.`);
            }
        }

        // Map local products by cleaned SKU
        const localProductMap = new Map();
        for (const product of localProducts) {
             if (!product.CodigoProducto) {
                 Logger.warn(`Local product record missing CodigoProducto: ${JSON.stringify(product)}`);
                 continue;
             }
            const cleanedSku = cleanSku(product.CodigoProducto);
            if (cleanedSku) {
                 if (localProductMap.has(cleanedSku)) {
                     Logger.warn(`Duplicate SKU found in Local Products: ${cleanedSku}. Using the last one found.`);
                 }
                localProductMap.set(cleanedSku, product);
            } else {
                 Logger.warn(`Could not clean SKU for local product: ${JSON.stringify(product)}`);
            }
        }
        Logger.log(`Prepared maps: ${shopifyVariantMap.size} Shopify SKUs, ${localProductMap.size} Local SKUs.`);


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
        };

        const itemsToProcess = SYNC_MODE === 'shopify_first'
            ? Array.from(shopifyVariantMap.entries()) // Process based on Shopify SKUs [sku, variant]
            : Array.from(localProductMap.entries()); // Process based on Local SKUs [sku, product]

        Logger.log(`Starting update process for ${itemsToProcess.length} items based on ${SYNC_MODE} mode...`);

        for (const [index, item] of itemsToProcess.entries()) {
            const sku = item[0];
            let variant = null;
            let localProduct = null;
            stats.processed++;

            if (SYNC_MODE === 'shopify_first') {
                variant = item[1];
                localProduct = localProductMap.get(sku);
                if (!localProduct) {
                    Logger.log(`⚠️ SKU ${sku} (${variant.displayName}) found in Shopify but not in local data. Skipping.`, 'WARN');
                    stats.notFoundLocal++;
                    continue; // Skip if local data is missing for this Shopify variant
                }
            } else { // local_first
                localProduct = item[1];
                variant = shopifyVariantMap.get(sku);
                if (!variant) {
                    Logger.log(`⚠️ SKU ${sku} found in local data but not in Shopify. Skipping.`, 'WARN');
                    stats.notFoundShopify++;
                    continue; // Skip if Shopify variant is missing for this local product
                }
            }

             // Log progress periodically
            if (index > 0 && index % 100 === 0) {
                 Logger.log(`Processed ${index} / ${itemsToProcess.length} items...`);
            }

            try {
                // Determine new price and inventory from local data
                const newPrice = (localProduct && localProduct.Venta1 !== undefined && localProduct.Venta1 !== null)
                    ? parseFloat(localProduct.Venta1).toFixed(2)
                    : null;

                const inventoryData = localInventory[sku]; // Already processed inventory map
                const newInventory = (inventoryData && inventoryData.calculatedQuantity !== undefined)
                    ? inventoryData.calculatedQuantity // Use pre-calculated quantity
                    : null;

                 if (newPrice === null && SYNC_TYPE !== 'inventory') {
                      Logger.warn(`SKU ${sku}: Missing price (Venta1) in local data. Skipping price update.`);
                 }
                 if (newInventory === null && SYNC_TYPE !== 'price') {
                      Logger.warn(`SKU ${sku}: Missing calculated inventory in local data. Skipping inventory update.`);
                 }

                // Call the update function
                const result = await updateVariantInShopify(variant, newPrice, newInventory);

                // Update statistics based on the result
                if (!result.success) {
                    stats.errors++;
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

            } catch (error) {
                Logger.error(`Critical error processing SKU ${sku}`, error);
                stats.errors++;
                // Optional: Decide whether to continue or stop on critical errors
            }
        } // End of loop

        // --- Final Summary ---
        const endTime = Date.now();
        const duration = ((endTime - startTime) / 1000).toFixed(2); // Duration in seconds

        Logger.log("\n📊 ===== SYNC SUMMARY =====");
        Logger.log(`Mode: ${SYNC_MODE.toUpperCase()}, Type: ${SYNC_TYPE.toUpperCase()}`);
        Logger.log(`Duration: ${duration} seconds`);
        Logger.log(`---------------------------`);
        Logger.log(`Total Items Processed: ${stats.processed}`);
        Logger.log(`Successful Updates:`);
        Logger.log(`  - Price Only: ${stats.priceUpdates}`);
        Logger.log(`  - Inventory Only: ${stats.inventoryUpdates}`);
        Logger.log(`  - Both Price & Inventory: ${stats.bothUpdates}`);
        Logger.log(`  - Total Updated Variants: ${stats.priceUpdates + stats.inventoryUpdates + stats.bothUpdates}`);
        Logger.log(`Skipped (No Change Needed): ${stats.skippedNoChange}`);
        Logger.log(`Skipped (Not Found Locally): ${stats.notFoundLocal}`);
        Logger.log(`Skipped (Not Found in Shopify): ${stats.notFoundShopify}`);
        Logger.log(`Errors during update: ${stats.errors}`);
        Logger.log("==========================\n");

        if (stats.errors > 0) {
            Logger.error("Sync completed with errors. Please review the logs.");
        } else if (stats.priceUpdates + stats.inventoryUpdates + stats.bothUpdates === 0 && stats.skippedNoChange > 0) {
            Logger.log("Sync completed. No updates were necessary.");
        } else {
            Logger.log("✅ Sync completed successfully.");
        }

    } catch (error) {
        // Catch errors from initial data fetching or setup
        Logger.error("💥 FATAL ERROR during synchronization process", error);
        process.exitCode = 1; // Indicate failure
    } finally {
         // Ensure all logs are written before exiting
        await Logger.processQueue(); // Process any remaining logs in the queue
        Logger.log(`🏁 Sync process finished at ${new Date().toLocaleString()}`);
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