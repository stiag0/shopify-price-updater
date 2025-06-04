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
    LOG_FILE_PATH = path.join('logs', `shopify-sync_${new Date().toISOString().replace(/T/, '_').replace(/:/g, '-').split('.')[0]}.log`)
} = process.env;

const SHOPIFY_API_VERSION = '2024-01';
const SHOPIFY_GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const SHOPIFY_REST_BASE_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}`;
const MAX_RETRIES = 3;
const RATE_LIMIT = USE_REST_API === 'true' ? 1 : 2; // More conservative rate limit for REST API

// --- Validation ---
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !DATA_API_URL || !INVENTORY_API_URL) {
    console.error("Error: Missing required environment variables");
    process.exit(1);
}

// --- Setup ---
const shopifyLimiter = new RateLimiter({ tokensPerInterval: RATE_LIMIT, interval: 'second' });

// Axios instances for different endpoints
const axiosShopify = axios.create({
    baseURL: USE_REST_API === 'true' ? SHOPIFY_REST_BASE_URL : SHOPIFY_GRAPHQL_URL,
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
        fs.writeFileSync(this.logPath, `[${new Date().toISOString()}] [INFO] Logger initialized\n`);
    },

    log(message, level = 'INFO') {
        const logEntry = this.formatMessage(message, level) + '\n';
        
        // Console output with emoji indicators
        const consoleMessage = level === 'ERROR' ? `❌ ${message}` :
                             level === 'WARN' ? `⚠️ ${message}` :
                             level === 'SUCCESS' ? `✅ ${message}` : message;

        if (level === 'ERROR') {
            console.error(consoleMessage);
        } else {
            console.log(consoleMessage);
        }

        this.logQueue.push(logEntry);
        this.processQueue();
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
    warn(message) { this.log(message, 'WARN'); },
    debug(message) { this.log(message, 'DEBUG'); },

    formatMessage(message, level = 'INFO') {
        let formattedMessage = message;
        
        // Handle objects and arrays
        if (typeof message === 'object' && message !== null) {
            try {
                formattedMessage = JSON.stringify(message, (key, value) => {
                    if (typeof value === 'string') {
                        // Truncate long strings and clean special characters
                        return value.length > 500 ? value.substring(0, 500) + '...' : value;
                    }
                    return value;
                }, 2);
            } catch (e) {
                formattedMessage = '[Unserializable Object]';
            }
        }

        // Clean and format SKU references
        if (typeof formattedMessage === 'string') {
            // Replace problematic characters in SKUs while maintaining readability
            formattedMessage = formattedMessage.replace(/SKU\s+([^:,\s]+)/g, 'SKU "$1"');
            // Ensure proper spacing after colons
            formattedMessage = formattedMessage.replace(/:\s*([^\s])/g, ': $1');
        }

        const timestamp = new Date().toISOString();
        return `[${timestamp}] [${level}] ${formattedMessage}`;
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
        const response = await axiosShopify.put(`/variants/${variantId}.json`, {
            variant: {
                id: variantId,
                ...updates
            }
        });
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
            responseType: 'stream'
        });

        return new Promise((resolve, reject) => {
            response.data
                .pipe(csv())
                .on('data', (row) => {
                    const sku = row.sku?.toString().trim();
                    const price = parseFloat(row.discount);
                    
                    if (sku && !isNaN(price) && price > 0) {
                        discountPrices.set(sku, price);
                        Logger.info(`Found discount for SKU ${sku}: ${price}`);
                    }
                })
                .on('end', () => {
                    Logger.info(`Loaded ${discountPrices.size} discount prices from CSV`);
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
        // REST API Implementation
        let hasMorePages = true;
        let page = 1;
        const LIMIT = 250;  // Maximum allowed by Shopify REST API

        while (hasMorePages && pageCount < MAX_PAGES) {
            try {
                await shopifyLimiter.removeTokens(1);
                const response = await axiosShopify.get(`/variants.json?limit=${LIMIT}&page=${page}`);
                const variants = response.data.variants;

                if (!variants || variants.length === 0) {
                    hasMorePages = false;
                    continue;
                }

                // Store only variants with numeric SKUs (1-5 digits)
                variants.forEach(variant => {
                    if (variant.sku && /^\d{1,5}$/.test(variant.sku)) {
                        allVariants.set(variant.sku, {
                            ...variant,
                            compareAtPrice: variant.compare_at_price, // Normalize field name
                            inventoryQuantity: variant.inventory_quantity,
                            inventoryItem: {
                                id: variant.inventory_item_id,
                                tracked: true // REST API doesn't provide this info directly
                            }
                        });
                    }
                });

                page++;
                pageCount++;
                Logger.info(`Fetched page ${pageCount} of variants using REST API...`);

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

async function main() {
    try {
        Timer.startTimer();
        Logger.info('Using ' + (USE_REST_API === 'true' ? 'REST API' : 'GraphQL API') + ' for Shopify operations');
        Logger.info(`Update mode: ${UPDATE_MODE}`);
        
        // First, fetch all compatible Shopify variants
        const shopifyVariants = await getAllShopifyVariants();
        
        // Load original data from API
        const originalData = await getOriginalData();
        
        // Modify the matching logic
        const filteredData = new Map();
        let matchedSkus = new Set();
        let skippedSkus = new Set();

        Logger.info('Matching products with Shopify variants...');

        for (const [sku, data] of originalData) {
            const paddedSku = sku.padStart(5, '0');
            const numericSku = sku.replace(/^0+/, '');
            
            if (shopifyVariants.has(paddedSku) || shopifyVariants.has(numericSku)) {
                const variant = shopifyVariants.get(paddedSku) || shopifyVariants.get(numericSku);
                const productName = variant.product?.title || 'Unknown Product';
                Logger.info(`Matched SKU ${sku} to Shopify product "${productName}"`);
                filteredData.set(sku, data);
                matchedSkus.add(sku);
            } else {
                skippedSkus.add(sku);
            }
        }

        Logger.info(`Matching results:`);
        Logger.info(`- Total Shopify variants: ${shopifyVariants.size}`);
        Logger.info(`- Total local products: ${originalData.size}`);
        Logger.info(`- Successfully matched: ${matchedSkus.size}`);
        Logger.info(`- Unmatched SKUs: ${skippedSkus.size}`);

        // Sample of unmatched SKUs for debugging
        const sampleUnmatched = Array.from(skippedSkus).slice(0, 5);
        if (sampleUnmatched.length > 0) {
            Logger.info(`Sample of unmatched SKUs: ${sampleUnmatched.join(', ')}`);
        }
        
        // Load and filter discount prices
        let discountPrices = new Map();
        try {
            const allDiscounts = await loadDiscountPrices();
            // Only keep discounts for SKUs that exist in Shopify
            for (const [sku, price] of allDiscounts) {
                if (shopifyVariants.has(sku)) {
                    discountPrices.set(sku, price);
                }
            }
            Logger.info(`Found ${discountPrices.size} valid discount prices for existing Shopify variants`);
        } catch (error) {
            Logger.error('Failed to load discount prices, continuing without discounts:', error);
        }

        // Separate products into discount and regular (only for existing Shopify variants)
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
            failed: 0
        };

        // Process discount products
        Logger.info(`Processing ${discountProducts.size} products with discounts...`);
        for (const [sku, data] of discountProducts) {
            try {
                const variant = shopifyVariants.get(sku);
                const productName = variant.product?.title || 'Unknown Product';
                const variantName = variant.displayName || sku;
                const updates = {};
                let needsUpdate = false;
                let changes = [];

                if (UPDATE_MODE === 'price' || UPDATE_MODE === 'both') {
                    const currentPrice = parseFloat(variant.price);
                    if (currentPrice !== data.discountPrice) {
                        updates.price = data.discountPrice.toString();
                        updates[USE_REST_API === 'true' ? 'compare_at_price' : 'compareAtPrice'] = data.price.toString();
                        needsUpdate = true;
                        changes.push(`Price: ${formatPriceChange(currentPrice, data.discountPrice)}`);
                        changes.push(`Compare at Price: ${formatPriceChange(variant.compareAtPrice || variant.compare_at_price || 'None', data.price)}`);
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
                        changes.push(`Inventory: ${formatInventoryChange(currentInventory, data.inventory)}`);
                    }
                }

                if (needsUpdate && Object.keys(updates).length > 0) {
                    await updateVariant(variant.id, updates);
                    Logger.info(`✅ Updated discount product "${productName}" (${variantName}):\n   ${changes.join('\n   ')}`);
                    stats.updated++;
                    stats.discountUpdated++;
                }
            } catch (error) {
                Logger.error(`Failed to process discount SKU ${sku}:`, error);
                stats.failed++;
            }
        }

        // Process regular products
        Logger.info(`Processing ${regularProducts.size} regular products...`);
        for (const [sku, data] of regularProducts) {
            try {
                const variant = shopifyVariants.get(sku);
                const productName = variant.product?.title || 'Unknown Product';
                const variantName = variant.displayName || sku;
                const updates = {};
                let needsUpdate = false;
                let changes = [];

                if (UPDATE_MODE === 'price' || UPDATE_MODE === 'both') {
                    const currentPrice = parseFloat(variant.price);
                    if (currentPrice !== data.price) {
                        updates.price = data.price.toString();
                        updates[USE_REST_API === 'true' ? 'compare_at_price' : 'compareAtPrice'] = null;
                        needsUpdate = true;
                        changes.push(`Price: ${formatPriceChange(currentPrice, data.price)}`);
                        if (variant.compareAtPrice || variant.compare_at_price) {
                            changes.push(`Compare at Price: ${formatPriceChange(variant.compareAtPrice || variant.compare_at_price, 'None')}`);
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
                        changes.push(`Inventory: ${formatInventoryChange(currentInventory, data.inventory)}`);
                    }
                }

                if (needsUpdate && Object.keys(updates).length > 0) {
                    await updateVariant(variant.id, updates);
                    Logger.info(`✅ Updated regular product "${productName}" (${variantName}):\n   ${changes.join('\n   ')}`);
                    stats.updated++;
                    stats.regularUpdated++;
                }
            } catch (error) {
                Logger.error(`Failed to process regular SKU ${sku}:`, error);
                stats.failed++;
            }
        }

        Logger.info('\nUpdate completed:');
        const duration = Timer.endTimer();
        Logger.info(`Total execution time: ${duration}`);
        Logger.info(`Total products matched with Shopify: ${stats.total}`);
        Logger.info(`Discount products updated: ${stats.discountUpdated}`);
        Logger.info(`Regular products updated: ${stats.regularUpdated}`);
        Logger.info(`Total updated: ${stats.updated}`);
        Logger.info(`Failed: ${stats.failed}`);

    } catch (error) {
        const duration = Timer.endTimer();
        Logger.error(`Fatal error after ${duration}: ${error.message}`);
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
    Logger.error(`Fatal error after ${duration}: ${error.message}`);
    process.exit(1);
}); 