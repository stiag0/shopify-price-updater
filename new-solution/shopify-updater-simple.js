require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { RateLimiter } = require('limiter');

// --- Configuration ---
const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    INVENTORY_API_URL,
    DATA_API_URL,
    DISCOUNT_CSV_PATH = 'discounts.csv',
    USE_REST_API = 'false', // Default to GraphQL if not specified
    LOCATION_ID, // Optional: For multi-location inventory
    UPDATE_MODE = 'both', // 'price', 'inventory', or 'both'
    LOG_FILE_PATH = path.join('logs', `shopify-sync_${new Date().toISOString().replace(/T/, '_').replace(/:/g, '-').split('.')[0]}.log`),
    LIMIT = 250 // Maximum allowed by Shopify REST API for variants endpoint
} = process.env;

const SHOPIFY_API_VERSION = '2024-01';
const SHOPIFY_GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const SHOPIFY_REST_BASE_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}`;
const MAX_RETRIES = 3;
const RATE_LIMIT = USE_REST_API === 'true' ? 1 : 2; // More conservative rate limit for REST API
const REST_API_LIMIT = 250; // Maximum allowed by Shopify REST API for variants endpoint

// --- Validation ---
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !DATA_API_URL || !INVENTORY_API_URL) {
    console.error("Error: Missing required environment variables");
    process.exit(1);
}

// --- Setup ---
const shopifyLimiter = new RateLimiter({ tokensPerInterval: RATE_LIMIT, interval: 'second' });

// Axios instances for different endpoints
const axiosShopify = axios.create({
    baseURL: USE_REST_API === 'true' ? undefined : SHOPIFY_GRAPHQL_URL, // Only use baseURL for GraphQL
    headers: {
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json',
    }
});

// --- Logger Module ---
const Logger = {
    logDir: path.dirname(LOG_FILE_PATH),
    logPath: LOG_FILE_PATH,
    logQueue: [],
    isWriting: false,

    init() {
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
        const timestamp = new Date().toISOString();
        const header = [
            `=`.repeat(80),
            `Shopify Price Updater Log - Started at ${timestamp}`,
            `Environment: ${process.env.NODE_ENV || 'development'}`,
            `Shop: ${SHOPIFY_SHOP_NAME}`,
            `API Mode: ${USE_REST_API === 'true' ? 'REST' : 'GraphQL'}`,
            `Update Mode: ${UPDATE_MODE}`,
            `Rate Limit: ${RATE_LIMIT} requests per second`,
            `=`.repeat(80),
            ''
        ].join('\n');
        
        fs.writeFileSync(this.logPath, header);
    },

    log(message, level = 'INFO') {
        const timestamp = new Date().toISOString();
        
        // Format for console (with emoji)
        const consoleMessage = this.formatConsoleMessage(message, level);
        
        // Format for log file (clean, with level and timestamp)
        const logMessage = `[${timestamp}] [${level.padEnd(7)}] ${message}\n`;
        
        // Console output
        if (level === 'ERROR') {
            console.error(consoleMessage);
        } else if (level === 'WARN') {
            console.warn(consoleMessage);
        } else {
            console.log(consoleMessage);
        }

        // Queue for file
        this.logQueue.push(logMessage);
        this.processQueue();
    },

    formatConsoleMessage(message, level) {
        const emoji = level === 'ERROR' ? '❌' :
                     level === 'WARN' ? '⚠️' :
                     level === 'SUCCESS' ? '✅' :
                     level === 'DEBUG' ? '🔍' : 'ℹ️';
        
        return `${emoji} ${message}`;
    },

    async processQueue() {
        if (this.isWriting || this.logQueue.length === 0) return;
        
        this.isWriting = true;
        try {
            const content = this.logQueue.join('');
            await fs.promises.appendFile(this.logPath, content, 'utf8');
            this.logQueue = [];
        } catch (error) {
            console.error('Error writing to log file:', error);
        } finally {
            this.isWriting = false;
            if (this.logQueue.length > 0) {
                setImmediate(() => this.processQueue());
            }
        }
    },

    info(message) { this.log(message, 'INFO'); },
    error(message, error = null) {
        let logMessage = message;
        if (error) {
            logMessage += `\nError Details:`;
            if (error.message) {
                logMessage += `\n  Message: ${error.message}`;
            }
            if (error.response?.data) {
                logMessage += `\n  Response Data: ${JSON.stringify(error.response.data, null, 2)}`;
            }
            if (error.stack) {
                logMessage += `\n  Stack Trace:\n    ${error.stack.split('\n').join('\n    ')}`;
            }
        }
        this.log(logMessage, 'ERROR');
    },
    warn(message) { this.log(message, 'WARN'); },
    debug(message) { this.log(message, 'DEBUG'); },
    success(message) { this.log(message, 'SUCCESS'); },

    section(title) {
        const separator = '-'.repeat(40);
        // Add empty line before section
        this.logQueue.push('\n');
        // Add section header
        this.log(`${separator}`);
        this.log(`${title}`);
        this.log(`${separator}`);
        // Add empty line after section
        this.logQueue.push('\n');
    },

    // Helper for price formatting
    formatPrice(price) {
        if (isNaN(price) || price === null || price === undefined) {
            return 'N/A';
        }
        return `$${parseFloat(price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    },

    // Helper for logging price changes
    logPriceChange(sku, productName, oldPrice, newPrice, type = 'regular') {
        const formattedOld = this.formatPrice(oldPrice);
        const formattedNew = this.formatPrice(newPrice);
        const changeMessage = `${type === 'compare' ? 'Compare at Price' : 'Price'}: ${formattedOld} → ${formattedNew}`;
        
        if (type === 'regular') {
            const changePercent = ((newPrice - oldPrice) / oldPrice * 100).toFixed(1);
            if (Math.abs(changePercent) > 50) {
                this.warn(`Large price change detected for ${sku} (${productName}): ${changeMessage} (${changePercent}% change)`);
            }
        }
        
        return changeMessage;
    },

    // Helper for logging inventory changes
    logInventoryChange(sku, productName, oldQty, newQty) {
        return `Inventory: ${oldQty} → ${newQty} units`;
    }
};

// Initialize logger
Logger.init();

// --- Timer Utility ---
const Timer = {
    start: null,
    end: null,
    
    startTimer() {
        this.start = process.hrtime();
    },
    
    endTimer() {
        this.end = process.hrtime(this.start);
        return this.getFormattedDuration();
    },
    
    getFormattedDuration() {
        const durationInSeconds = this.end[0] + this.end[1] / 1e9;
        const hours = Math.floor(durationInSeconds / 3600);
        const minutes = Math.floor((durationInSeconds % 3600) / 60);
        const seconds = Math.floor(durationInSeconds % 60);
        const milliseconds = Math.floor((durationInSeconds % 1) * 1000);
        
        return `${hours}h ${minutes}m ${seconds}s ${milliseconds}ms`;
    }
};

// --- Helper Functions ---
async function fetchWithRetry(config, retries = MAX_RETRIES) {
    try {
        await shopifyLimiter.removeTokens(1);
        return await axios(config);
    } catch (error) {
        if (retries > 0 && (error.response?.status === 429 || error.response?.status >= 500)) {
            const delay = Math.pow(2, MAX_RETRIES - retries) * 1000;
            console.log(`Retrying after ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return fetchWithRetry(config, retries - 1);
        }
        throw error;
    }
}

// --- REST API Functions ---
async function getVariantBySkuRest(sku) {
    try {
        await shopifyLimiter.removeTokens(1);
        const response = await axiosShopify.get(`/variants.json?sku=${encodeURIComponent(sku)}`);
        return response.data.variants[0] || null;
    } catch (error) {
        console.error(`REST API Error fetching variant for SKU ${sku}:`, error.response?.data || error.message);
        throw error;
    }
}

async function updateVariantRest(variantId, updates) {
    try {
        await shopifyLimiter.removeTokens(1);
        // Convert variantId to string and extract numeric ID if it's a Shopify GID
        const variantIdStr = String(variantId);
        const numericId = variantIdStr.includes('/') ? variantIdStr.split('/').pop() : variantIdStr;
        
        const response = await axiosShopify.put(
            `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/variants/${numericId}.json`,
            {
                variant: {
                    id: numericId,
                    ...updates
                }
            }
        );
        return response.data.variant;
    } catch (error) {
        console.error('REST API Error updating variant:', error.response?.data || error.message);
        throw error;
    }
}

// --- GraphQL Functions ---
async function getVariantBySkuGraphQL(sku) {
    const query = `
        query getVariantBySku($query: String!) {
            productVariants(first: 1, query: $query) {
                edges {
                    node {
                        id
                        sku
                        price
                        compareAtPrice
                        inventoryQuantity
                        inventoryItem {
                            id
                        }
                    }
                }
            }
        }
    `;

    try {
        await shopifyLimiter.removeTokens(1);
        const response = await axiosShopify.post('', {
            query,
            variables: { query: `sku:${sku}` }
        });

        if (response.data.errors) {
            throw new Error(JSON.stringify(response.data.errors));
        }

        return response.data.data.productVariants.edges[0]?.node || null;
    } catch (error) {
        console.error(`GraphQL Error fetching variant for SKU ${sku}:`, error.message);
        throw error;
    }
}

async function updateVariantGraphQL(variantId, updates) {
    const mutation = `
        mutation variantUpdate($input: ProductVariantInput!) {
            productVariantUpdate(input: $input) {
                productVariant {
                    id
                    price
                    compareAtPrice
                    inventoryQuantity
                }
                userErrors {
                    field
                    message
                }
            }
        }
    `;

    try {
        await shopifyLimiter.removeTokens(1);
        const response = await axiosShopify.post('', {
            query: mutation,
            variables: {
                input: {
                    id: variantId,
                    ...updates
                }
            }
        });

        if (response.data.errors) {
            throw new Error(JSON.stringify(response.data.errors));
        }

        const result = response.data.data.productVariantUpdate;
        if (result.userErrors.length > 0) {
            throw new Error(JSON.stringify(result.userErrors));
        }

        return result.productVariant;
    } catch (error) {
        console.error('GraphQL Error updating variant:', error.message);
        throw error;
    }
}

async function updateInventoryLevelGraphQL(inventoryItemId, locationId, quantity) {
    const mutation = `
        mutation inventoryAdjustQuantity($input: InventoryAdjustQuantityInput!) {
            inventoryAdjustQuantity(input: $input) {
                inventoryLevel {
                    available
                }
                userErrors {
                    field
                    message
                }
            }
        }
    `;

    try {
        await shopifyLimiter.removeTokens(1);
        const response = await axiosShopify.post('', {
            query: mutation,
            variables: {
                input: {
                    inventoryItemId,
                    locationId,
                    availableDelta: quantity
                }
            }
        });

        if (response.data.errors) {
            throw new Error(JSON.stringify(response.data.errors));
        }

        const result = response.data.data.inventoryAdjustQuantity;
        if (result.userErrors.length > 0) {
            throw new Error(JSON.stringify(result.userErrors));
        }

        return result.inventoryLevel;
    } catch (error) {
        console.error('GraphQL Error updating inventory:', error.message);
        throw error;
    }
}

// --- API Wrapper Functions ---
async function getVariantBySku(sku) {
    return USE_REST_API === 'true' ? 
        getVariantBySkuRest(sku) : 
        getVariantBySkuGraphQL(sku);
}

async function updateVariant(variantId, updates) {
    if (USE_REST_API === 'true') {
        // Convert GraphQL field names to REST API field names
        const restUpdates = {
            ...updates,
            compare_at_price: updates.compareAtPrice,
            inventory_quantity: updates.inventoryQuantity
        };
        delete restUpdates.compareAtPrice;
        delete restUpdates.inventoryQuantity;
        
        return updateVariantRest(variantId, restUpdates);
    } else {
        return updateVariantGraphQL(variantId, updates);
    }
}

async function updateInventoryLevel(inventoryItemId, locationId, quantity) {
    if (USE_REST_API === 'true') {
        // For REST API, inventory updates are handled through the variant update
        return;
    } else {
        return updateInventoryLevelGraphQL(inventoryItemId, locationId, quantity);
    }
}

// --- Main Functions ---
async function getOriginalData() {
    try {
        Logger.info('Fetching data from API...');
        const response = await fetchWithRetry({ url: DATA_API_URL });
        
        const products = response.data.value || [];
        
        if (!Array.isArray(products)) {
            throw new Error(`Invalid API response structure. Expected array in value property`);
        }

        const dataMap = new Map();
        let skippedNonNumeric = 0;
        let skippedZeroPrices = 0;
        let processedSkus = new Set();

        // Debug logging for SKU processing
        Logger.info('Processing SKUs from API data...');

        for (const product of products) {
            const rawSku = (product.Referencia || product.CodigoProducto || '').toString().trim();
            // Convert to string and pad with zeros if needed (up to 5 digits)
            const paddedSku = rawSku.padStart(5, '0');
            const numericSku = rawSku.replace(/^0+/, ''); // Remove leading zeros for comparison
            
            // Log SKU processing
            Logger.debug(`Processing SKU - Raw: ${rawSku}, Padded: ${paddedSku}, Numeric: ${numericSku}`);
            
            // Only process if SKU is purely numeric (1-5 digits)
            if (/^\d{1,5}$/.test(numericSku)) {
                const price = parseFloat(product.Venta1 || 0);
                if (!isNaN(price) && price > 0) {
                    // Store both padded and non-padded versions
                    dataMap.set(paddedSku, {
                        price,
                        inventory: 0,
                        originalSku: rawSku
                    });
                    dataMap.set(numericSku, {
                        price,
                        inventory: 0,
                        originalSku: rawSku
                    });
                    processedSkus.add(rawSku);
                } else {
                    skippedZeroPrices++;
                }
            } else {
                skippedNonNumeric++;
            }
        }
        
        // Now fetch inventory data
        Logger.info('Fetching inventory data...');
        const invResponse = await fetchWithRetry({ url: INVENTORY_API_URL });
        const inventory = invResponse.data.value || [];

        let skippedInvNonNumeric = 0;
        let processedInvSkus = new Set();

        // Update inventory quantities
        for (const item of inventory) {
            const rawSku = (item.Referencia || item.CodigoProducto || '').toString().trim();
            const paddedSku = rawSku.padStart(5, '0');
            const numericSku = rawSku.replace(/^0+/, '');
            
            if (/^\d{1,5}$/.test(numericSku)) {
                const realInventory = parseFloat(item.CantidadInicial || 0) + 
                                    parseFloat(item.CantidadEntradas || 0) - 
                                    parseFloat(item.CantidadSalidas || 0);
                
                // Try both padded and non-padded versions
                if (dataMap.has(paddedSku)) {
                    dataMap.get(paddedSku).inventory = Math.max(0, Math.round(realInventory));
                    processedInvSkus.add(rawSku);
                }
                if (dataMap.has(numericSku)) {
                    dataMap.get(numericSku).inventory = Math.max(0, Math.round(realInventory));
                    processedInvSkus.add(rawSku);
                }
            } else {
                skippedInvNonNumeric++;
            }
        }
        
        Logger.info(`Loaded ${processedSkus.size} numeric-only products from API`);
        Logger.info(`Processed ${processedInvSkus.size} inventory records`);
        Logger.info(`Skipped ${skippedNonNumeric} products with non-numeric SKUs`);
        Logger.info(`Skipped ${skippedZeroPrices} products with zero or invalid prices`);
        Logger.info(`Skipped ${skippedInvNonNumeric} inventory records with non-numeric SKUs`);
        
        return dataMap;
    } catch (error) {
        Logger.error('Error fetching data:', error);
        throw error;
    }
}

async function loadDiscountPrices() {
    const discountPrices = new Map();
    
    try {
        Logger.info('Fetching discount CSV from URL...');
        const response = await axios({
            method: 'get',
            url: DISCOUNT_CSV_PATH,
            responseType: 'stream',
            validateStatus: false // Allow non-200 status codes
        });

        if (response.status !== 200) {
            throw new Error(`Failed to fetch discount CSV: HTTP ${response.status}`);
        }

        return new Promise((resolve, reject) => {
            let rowCount = 0;
            let errorCount = 0;

            response.data
                .pipe(csv())
                .on('data', (row) => {
                    rowCount++;
                    const sku = row.sku?.toString().trim();
                    const price = parseFloat(row.discount);
                    
                    if (!sku) {
                        Logger.warn(`Row ${rowCount}: Empty SKU`);
                        errorCount++;
                        return;
                    }
                    
                    if (isNaN(price) || price <= 0) {
                        Logger.warn(`Row ${rowCount}: Invalid price for SKU ${sku}: ${row.discount}`);
                        errorCount++;
                        return;
                    }
                    
                    discountPrices.set(sku, price);
                    Logger.info(`Found discount for SKU ${sku}: ${price}`);
                })
                .on('end', () => {
                    Logger.info(`Processed ${rowCount} rows from CSV`);
                    Logger.info(`Found ${discountPrices.size} valid discount prices`);
                    if (errorCount > 0) {
                        Logger.warn(`Encountered ${errorCount} errors while processing discount prices`);
                    }
                    resolve(discountPrices);
                })
                .on('error', (error) => {
                    Logger.error('Error reading discount CSV:', error);
                    reject(error);
                });
        });
    } catch (error) {
        Logger.error('Error loading discount prices:', error);
        throw error;
    }
}

async function getAllShopifyVariants() {
    Logger.info('Fetching all product variants from Shopify...');
    const allVariants = new Map();
    let pageCount = 0;
    const MAX_PAGES = 500;

    if (USE_REST_API === 'true') {
        // REST API Implementation using Link header-based pagination
        let nextUrl = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/products.json?limit=${REST_API_LIMIT}&fields=id,title,variants`;  // Complete URL with protocol and domain
        
        while (nextUrl && pageCount < MAX_PAGES) {
            try {
                await shopifyLimiter.removeTokens(1);
                Logger.debug(`Requesting URL: ${nextUrl}`); // Add debug logging
                const response = await axiosShopify.get(nextUrl);
                const products = response.data.products;

                if (!products || products.length === 0) {
                    break;
                }

                // Process variants from each product
                for (const product of products) {
                    if (product.variants && Array.isArray(product.variants)) {
                        product.variants.forEach(variant => {
                            if (variant.sku && /^\d{1,5}$/.test(variant.sku)) {
                                allVariants.set(variant.sku, {
                                    ...variant,
                                    compareAtPrice: variant.compare_at_price, // Normalize field name
                                    inventoryQuantity: variant.inventory_quantity,
                                    inventoryItem: {
                                        id: variant.inventory_item_id,
                                        tracked: true // REST API doesn't provide this info directly
                                    },
                                    product: {
                                        id: product.id,
                                        title: product.title
                                    }
                                });
                            }
                        });
                    }
                }

                // Get the next URL from the Link header
                const linkHeader = response.headers.link;
                nextUrl = null;
                
                if (linkHeader) {
                    const links = linkHeader.split(',');
                    for (const link of links) {
                        if (link.includes('rel="next"')) {
                            // Extract URL from the link - Link header already contains full URL
                            const matches = link.match(/<([^>]+)>/);
                            if (matches) {
                                nextUrl = matches[1]; // Use the full URL from the Link header
                            }
                            break;
                        }
                    }
                }

                pageCount++;
                Logger.info(`Fetched page ${pageCount} of products using REST API (${products.length} products)...`);

            } catch (error) {
                Logger.error('Error fetching variants with REST API:', error);
                throw error;
            }
        }
    } else {
        // GraphQL API Implementation
        let hasNextPage = true;
        let cursor = null;

        const queryTemplate = `
            query getVariants($first: Int!, $after: String) {
                productVariants(first: $first, after: $after) {
                    edges {
                        node {
                            id
                            sku
                            price
                            compareAtPrice
                            inventoryQuantity
                            displayName
                            product {
                                title
                            }
                            inventoryItem {
                                id
                                tracked
                                inventoryLevels(first: 1) {
                                    edges {
                                        node {
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
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        `;

        while (hasNextPage && pageCount < MAX_PAGES) {
            try {
                await shopifyLimiter.removeTokens(1);
                const response = await axiosShopify.post('', {
                    query: queryTemplate,
                    variables: {
                        first: 250,
                        after: cursor
                    }
                });

                if (response.data.errors) {
                    throw new Error(JSON.stringify(response.data.errors));
                }

                const variants = response.data.data.productVariants;
                
                variants.edges.forEach(({ node }) => {
                    const rawSku = node.sku?.trim() || '';
                    const paddedSku = rawSku.padStart(5, '0');
                    const numericSku = rawSku.replace(/^0+/, '');
                    
                    Logger.debug(`Processing Shopify variant - Raw SKU: ${rawSku}, Padded: ${paddedSku}, Numeric: ${numericSku}`);
                    
                    if (node.sku && /^\d{1,5}$/.test(numericSku)) {
                        // Store both padded and non-padded versions for matching
                        allVariants.set(paddedSku, node);
                        allVariants.set(numericSku, node);
                    }
                });

                hasNextPage = variants.pageInfo.hasNextPage;
                cursor = variants.pageInfo.endCursor;
                pageCount++;
                
                Logger.info(`Fetched page ${pageCount} of variants using GraphQL...`);
            } catch (error) {
                Logger.error('Error fetching variants with GraphQL:', error);
                throw error;
            }
        }
    }

    Logger.info(`Found ${allVariants.size} compatible variants in Shopify`);
    return allVariants;
}

// Add a helper function to format price changes
function formatPriceChange(oldPrice, newPrice) {
    const formatPrice = (price) => `$${parseFloat(price).toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
    return `${formatPrice(oldPrice)} → ${formatPrice(newPrice)}`;
}

// Add a helper function to format inventory changes
function formatInventoryChange(oldQty, newQty) {
    return `${oldQty} → ${newQty} units`;
}

// Add this before the SKU matching loop
const normalizeSkuForMatching = (sku) => {
    // Remove leading zeros
    const numericSku = sku.replace(/^0+/, '');
    // Pad to 5 digits for matching
    const paddedSku = numericSku.padStart(5, '0');
    return [numericSku, paddedSku];
};

// Add this to the price update logic
const validatePriceChange = (currentPrice, newPrice, threshold = 0.5) => {
    // Check if price change is more than 50%
    const changeRatio = Math.abs(newPrice - currentPrice) / currentPrice;
    if (changeRatio > threshold) {
        Logger.warn(`Large price change detected: ${formatPriceChange(currentPrice, newPrice)} (${(changeRatio * 100).toFixed(1)}% change)`);
        return false;
    }
    return true;
};

async function main() {
    try {
        Timer.startTimer();
        Logger.section('Initialization');
        Logger.info('Starting Shopify Price Updater');
        Logger.info(`API Mode: ${USE_REST_API === 'true' ? 'REST' : 'GraphQL'}`);
        Logger.info(`Update Mode: ${UPDATE_MODE}`);
        Logger.info(`Rate Limit: ${RATE_LIMIT} requests per second`);
        
        // First, fetch all compatible Shopify variants
        Logger.section('Fetching Shopify Variants');
        const shopifyVariants = await getAllShopifyVariants();
        
        // Load original data from API
        Logger.section('Loading Original Data');
        const originalData = await getOriginalData();
        
        // Modify the matching logic
        Logger.section('SKU Matching');
        const filteredData = new Map();
        let matchedSkus = new Set();
        let skippedSkus = new Set();
        let matchedVariants = new Map(); // Store matched variants for later use

        for (const [sku, data] of originalData) {
            const [numericSku, paddedSku] = normalizeSkuForMatching(sku);
            // Try all possible SKU formats
            const possibleSkus = new Set([sku, numericSku, paddedSku]);
            
            let matched = false;
            let matchedVariant = null;
            
            for (const possibleSku of possibleSkus) {
                if (shopifyVariants.has(possibleSku)) {
                    const variant = shopifyVariants.get(possibleSku);
                    if (variant && variant.product) {  // Ensure we have both variant and product data
                        matchedVariant = variant;
                        filteredData.set(sku, data);
                        matchedSkus.add(sku);
                        matchedVariants.set(sku, variant); // Store the matched variant
                        matched = true;
                        Logger.debug(`Matched SKU ${sku} to Shopify variant "${variant.product.title}" (${variant.displayName || possibleSku})`);
                        break;
                    } else {
                        Logger.warn(`Found variant for SKU ${sku} but missing product data`);
                    }
                }
            }
            
            if (!matched) {
                skippedSkus.add(sku);
                Logger.debug(`No match found for SKU ${sku}`);
            }
        }

        Logger.section('Matching Results');
        Logger.info(`Total Shopify variants: ${shopifyVariants.size}`);
        Logger.info(`Total local products: ${originalData.size}`);
        Logger.info(`Successfully matched: ${matchedSkus.size}`);
        Logger.info(`Unmatched SKUs: ${skippedSkus.size}`);
        Logger.info(`Match rate: ${(matchedSkus.size / originalData.size * 100).toFixed(2)}%`);

        // Sample of unmatched SKUs for debugging
        const sampleUnmatched = Array.from(skippedSkus).slice(0, 5);
        if (sampleUnmatched.length > 0) {
            Logger.debug(`Sample of unmatched SKUs: ${sampleUnmatched.join(', ')}`);
        }
        
        // Load and filter discount prices
        Logger.section('Loading Discount Prices');
        let discountPrices = new Map();
        try {
            const allDiscounts = await loadDiscountPrices();
            // Only keep discounts for SKUs that exist in Shopify
            for (const [sku, price] of allDiscounts) {
                if (shopifyVariants.has(sku)) {
                    discountPrices.set(sku, price);
                    Logger.debug(`Valid discount price for SKU ${sku}: ${price}`);
                } else {
                    Logger.debug(`Skipping discount price for non-existent SKU ${sku}`);
                }
            }
            Logger.info(`Found ${discountPrices.size} valid discount prices for existing Shopify variants`);
        } catch (error) {
            Logger.error('Failed to load discount prices, continuing without discounts:', error);
        }

        // Separate products into discount and regular
        Logger.section('Processing Products');
        const discountProducts = new Map();
        const regularProducts = new Map();

        for (const [sku, data] of filteredData) {
            if (discountPrices.has(sku)) {
                discountProducts.set(sku, {
                    ...data,
                    discountPrice: discountPrices.get(sku)
                });
            } else {
                regularProducts.set(sku, data);
            }
        }

        const stats = {
            total: filteredData.size,
            updated: 0,
            discountUpdated: 0,
            regularUpdated: 0,
            priceUpdates: 0,
            inventoryUpdates: 0,
            skippedPriceUpdates: 0,
            skippedInventoryUpdates: 0,
            failed: 0,
            matchRate: 0
        };

        // Process discount products
        Logger.section('Processing Discount Products');
        Logger.info(`Found ${discountProducts.size} products with discounts`);
        for (const [sku, data] of discountProducts) {
            try {
                const variant = matchedVariants.get(sku); // Use stored variant instead of looking up again
                if (!variant || !variant.product) {
                    Logger.warn(`Skipping discount SKU ${sku}: Missing variant or product data`);
                    stats.skippedInventoryUpdates++;
                    continue;
                }

                const productName = variant.product.title || 'Unknown Product';
                const variantName = variant.displayName || sku;
                const updates = {};
                let needsUpdate = false;
                let changes = [];

                if (UPDATE_MODE === 'price' || UPDATE_MODE === 'both') {
                    const currentPrice = parseFloat(variant.price);
                    const currentCompareAtPrice = parseFloat(variant.compareAtPrice || variant.compare_at_price) || null;
                    const newPrice = parseFloat(data.discountPrice);
                    const newCompareAtPrice = parseFloat(data.price);

                    // Check if either price or compare-at-price needs updating
                    if (currentPrice !== newPrice || currentCompareAtPrice !== newCompareAtPrice) {
                        if (validatePriceChange(currentPrice, newPrice)) {
                            updates.price = newPrice.toString();
                            updates[USE_REST_API === 'true' ? 'compare_at_price' : 'compareAtPrice'] = newCompareAtPrice.toString();
                            needsUpdate = true;
                            
                            // Log both price changes
                            const priceChangeMsg = Logger.logPriceChange(sku, productName, currentPrice, newPrice);
                            const compareChangeMsg = Logger.logPriceChange(
                                sku, 
                                productName,
                                currentCompareAtPrice || 'None',
                                newCompareAtPrice,
                                'compare'
                            );
                            
                            changes.push(priceChangeMsg);
                            changes.push(compareChangeMsg);
                            stats.priceUpdates++;
                            
                            Logger.debug(`Updating discount product ${sku}:
                                Current Price: ${currentPrice} -> New Price: ${newPrice}
                                Current Compare At: ${currentCompareAtPrice} -> New Compare At: ${newCompareAtPrice}`);
                        } else {
                            Logger.warn(`Skipping suspicious price update for SKU ${sku} (${productName})`);
                            stats.skippedPriceUpdates++;
                        }
                    } else {
                        Logger.debug(`No price update needed for SKU ${sku} (current: ${currentPrice}, discount: ${newPrice})`);
                    }
                }

                if (UPDATE_MODE === 'inventory' || UPDATE_MODE === 'both') {
                    const currentInventory = variant.inventoryQuantity || 0;
                    if (currentInventory !== data.inventory) {
                        if (USE_REST_API === 'true') {
                            updates.inventory_quantity = data.inventory;
                        } else if (variant.inventoryItem?.id && LOCATION_ID) {
                            await updateInventoryLevel(
                                variant.inventoryItem.id,
                                LOCATION_ID,
                                data.inventory - currentInventory
                            );
                        }
                        needsUpdate = true;
                        const inventoryChangeMsg = Logger.logInventoryChange(sku, productName, currentInventory, data.inventory);
                        changes.push(inventoryChangeMsg);
                        stats.inventoryUpdates++;
                    }
                }

                if (needsUpdate && Object.keys(updates).length > 0) {
                    await updateVariant(variant.id, updates);
                    Logger.success(`Updated discount product "${productName}" (${variantName}):\n   ${changes.join('\n   ')}`);
                    stats.updated++;
                    stats.discountUpdated++;
                }
            } catch (error) {
                Logger.error(`Failed to process discount SKU ${sku}:`, error);
                stats.failed++;
            }
        }

        // Process regular products
        Logger.section('Processing Regular Products');
        Logger.info(`Processing ${regularProducts.size} regular products`);
        for (const [sku, data] of regularProducts) {
            try {
                const variant = matchedVariants.get(sku); // Use stored variant instead of looking up again
                if (!variant || !variant.product) {
                    Logger.warn(`Skipping regular SKU ${sku}: Missing variant or product data`);
                    stats.skippedInventoryUpdates++;
                    continue;
                }

                const productName = variant.product.title || 'Unknown Product';
                const variantName = variant.displayName || sku;
                const updates = {};
                let needsUpdate = false;
                let changes = [];

                if (UPDATE_MODE === 'price' || UPDATE_MODE === 'both') {
                    const currentPrice = parseFloat(variant.price);
                    if (currentPrice !== data.price) {
                        if (validatePriceChange(currentPrice, data.price)) {
                            updates.price = data.price.toString();
                            updates[USE_REST_API === 'true' ? 'compare_at_price' : 'compareAtPrice'] = null;
                            needsUpdate = true;
                            
                            // Log price changes with more detail
                            const priceChangeMsg = Logger.logPriceChange(sku, productName, currentPrice, data.price);
                            changes.push(priceChangeMsg);
                            
                            if (variant.compareAtPrice || variant.compare_at_price) {
                                const compareChangeMsg = Logger.logPriceChange(
                                    sku, 
                                    productName,
                                    variant.compareAtPrice || variant.compare_at_price,
                                    null,
                                    'compare'
                                );
                                changes.push(compareChangeMsg);
                            }
                            stats.priceUpdates++;
                        } else {
                            Logger.warn(`Skipping suspicious price update for SKU ${sku} (${productName})`);
                            stats.skippedPriceUpdates++;
                        }
                    }
                }

                if (UPDATE_MODE === 'inventory' || UPDATE_MODE === 'both') {
                    const currentInventory = variant.inventoryQuantity || 0;
                    if (currentInventory !== data.inventory) {
                        if (USE_REST_API === 'true') {
                            updates.inventory_quantity = data.inventory;
                        } else if (variant.inventoryItem?.id && LOCATION_ID) {
                            await updateInventoryLevel(
                                variant.inventoryItem.id,
                                LOCATION_ID,
                                data.inventory - currentInventory
                            );
                        }
                        needsUpdate = true;
                        const inventoryChangeMsg = Logger.logInventoryChange(sku, productName, currentInventory, data.inventory);
                        changes.push(inventoryChangeMsg);
                        stats.inventoryUpdates++;
                    }
                }

                if (needsUpdate && Object.keys(updates).length > 0) {
                    await updateVariant(variant.id, updates);
                    Logger.success(`Updated regular product "${productName}" (${variantName}):\n   ${changes.join('\n   ')}`);
                    stats.updated++;
                    stats.regularUpdated++;
                }
            } catch (error) {
                Logger.error(`Failed to process regular SKU ${sku}:`, error);
                stats.failed++;
            }
        }

        // Final statistics
        Logger.section('Final Statistics');
        stats.matchRate = (matchedSkus.size / originalData.size * 100).toFixed(2);
        Logger.info(`Match Rate: ${stats.matchRate}%`);
        Logger.info(`Price Updates: ${stats.priceUpdates}`);
        Logger.info(`Inventory Updates: ${stats.inventoryUpdates}`);
        Logger.info(`Skipped Price Updates: ${stats.skippedPriceUpdates}`);
        Logger.info(`Skipped Inventory Updates: ${stats.skippedInventoryUpdates}`);

        Logger.section('Summary');
        const duration = Timer.endTimer();
        Logger.info(`Total execution time: ${duration}`);
        Logger.info(`Total products matched with Shopify: ${stats.total}`);
        Logger.info(`Discount products updated: ${stats.discountUpdated}`);
        Logger.info(`Regular products updated: ${stats.regularUpdated}`);
        Logger.info(`Total updated: ${stats.updated}`);
        Logger.info(`Failed: ${stats.failed}`);

    } catch (error) {
        const duration = Timer.endTimer();
        Logger.error(`Fatal error after ${duration}:`, error);
        process.exit(1);
    }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    const duration = Timer.endTimer();
    Logger.info(`\nReceived SIGINT after ${duration}. Graceful shutdown initiated.`);
    process.exit(0);
});

process.on('SIGTERM', () => {
    const duration = Timer.endTimer();
    Logger.info(`\nReceived SIGTERM after ${duration}. Graceful shutdown initiated.`);
    process.exit(0);
});

// Run the updater
main().catch(error => {
    const duration = Timer.endTimer();
    Logger.error(`Fatal error after ${duration}:`, error);
    process.exit(1);
}); 