/**
 * Shopify Price Updater - Direct Price Version
 * Uses the same robust functionality as shopify-updater.js but takes prices directly from CSV
 */

require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { RateLimiter } = require('limiter');

// --- Environment Variables ---
const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    DATA_API_URL,  // Keep this for compare-at prices
    DISCOUNT_CSV_PATH,
    INVENTORY_API_URL,  // Add this
    SHOPIFY_API_VERSION = '2024-01',
    USE_REST_API = 'false',
    MAX_RETRIES = '3',
    SHOPIFY_RATE_LIMIT = '2',
    SAFETY_STOCK = '5'  // Units to reserve for physical store
} = process.env;

// --- Validation ---
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !DATA_API_URL || !DISCOUNT_CSV_PATH || !INVENTORY_API_URL) {
    console.error(`
Error: Missing required environment variables!
Required variables:
- SHOPIFY_SHOP_NAME
- SHOPIFY_ACCESS_TOKEN
- DATA_API_URL (for compare-at prices)
- DISCOUNT_CSV_PATH (Google Sheets URL)
- INVENTORY_API_URL (for inventory data)
`);
    process.exit(1);
}

// --- Constants ---
const GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const MAX_PAGES = 20; // Maximum number of pages to fetch
const RETRY_DELAY = 1000; // 1 second
const SAFETY_STOCK_UNITS = parseInt(SAFETY_STOCK, 10); // Convert to integer

// --- Utilities ---
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

// --- Enhanced Logger with file logging and graceful shutdown ---
const Logger = {
    logFile: null,
    logPath: path.join(__dirname, 'logs', `shopify-sync-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.log`),
    
    init() {
        // Create logs directory if it doesn't exist
        const logDir = path.dirname(this.logPath);
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir, { recursive: true });
        }
        
        // Create log file
        this.logFile = fs.createWriteStream(this.logPath, { flags: 'a' });
        this.info(`Log file created: ${this.logPath}`);
    },
    
    log(message, level = 'INFO') {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] [${level}] ${message}`;
        
        // Log to console
        console.log(logMessage);
        
        // Log to file if available
        if (this.logFile) {
            this.logFile.write(logMessage + '\n');
        }
    },
    
    info(message) { this.log(message, 'INFO'); },
    warn(message) { this.log(message, 'WARN'); },
    error(message) { this.log(message, 'ERROR'); },
    success(message) { this.log(message, 'SUCCESS'); },
    section(title) {
        const sectionLine = '\n' + '='.repeat(20) + ' ' + title + ' ' + '='.repeat(20);
        console.log(sectionLine);
        if (this.logFile) {
            this.logFile.write(sectionLine + '\n');
        }
    },
    
    async close() {
        return new Promise((resolve) => {
            if (this.logFile) {
                this.logFile.end(() => {
                    console.log(`Log file saved: ${this.logPath}`);
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }
};

// Initialize logger
Logger.init();

// --- Graceful Shutdown Handler ---
let isShuttingDown = false;
let currentOperation = null;

async function gracefulShutdown(signal) {
    if (isShuttingDown) {
        console.log('\nForce shutdown initiated...');
        process.exit(1);
    }
    
    isShuttingDown = true;
    console.log(`\n\nReceived ${signal}. Initiating graceful shutdown...`);
    Logger.warn(`Shutdown signal received: ${signal}`);
    
    try {
        // If we're in the middle of an operation, log it
        if (currentOperation) {
            Logger.warn(`Interrupting current operation: ${currentOperation}`);
        }
        
        // Log shutdown summary
        const endTime = Timer.endTimer();
        if (endTime) {
            Logger.info(`Script execution time before shutdown: ${endTime}`);
        }
        
        Logger.info('Performing cleanup operations...');
        
        // Give any pending operations a moment to complete
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        Logger.info('Graceful shutdown completed');
        
        // Close log file
        await Logger.close();
        
        console.log('Cleanup completed. Exiting...');
        process.exit(0);
        
    } catch (error) {
        console.error('Error during graceful shutdown:', error.message);
        await Logger.close();
        process.exit(1);
    }
}

// Register signal handlers
process.on('SIGINT', () => gracefulShutdown('SIGINT'));   // Ctrl+C
process.on('SIGTERM', () => gracefulShutdown('SIGTERM')); // Termination signal
process.on('SIGQUIT', () => gracefulShutdown('SIGQUIT')); // Quit signal

// Handle uncaught exceptions
process.on('uncaughtException', async (error) => {
    console.error('Uncaught Exception:', error);
    Logger.error(`Uncaught Exception: ${error.message}`);
    Logger.error(`Stack: ${error.stack}`);
    await Logger.close();
    process.exit(1);
});

// Handle unhandled promise rejections
process.on('unhandledRejection', async (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    Logger.error(`Unhandled Rejection: ${reason}`);
    await Logger.close();
    process.exit(1);
});

// --- API Setup ---
const shopifyLimiter = new RateLimiter({
    tokensPerInterval: parseInt(SHOPIFY_RATE_LIMIT, 10),
    interval: 'second'
});

// Create a separate axios instance for local API with timeout
const localApiClient = axios.create({
    timeout: 5000, // 5 seconds timeout
    headers: {
        'Accept': 'application/json'
    }
});

const shopifyClient = axios.create({
    baseURL: `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}`,
    headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    }
});

// --- Helper Functions ---
function cleanSku(sku) {
    if (!sku) return null;
    const cleaned = String(sku).trim();
    const normalized = cleaned.replace(/[^a-zA-Z0-9-_]/g, '');
    return normalized || null;
}

function normalizeSkuForMatching(sku) {
    const cleaned = cleanSku(sku);
    if (!cleaned) {
        return { isValid: false, cleaned: null, padded: null };
    }

    if (/^\d+$/.test(cleaned)) {
        const unpadded = cleaned.replace(/^0+/, '');
        const padded = unpadded.padStart(5, '0');
        return { 
            isValid: true, 
            cleaned: unpadded,
            padded,
            original: cleaned
        };
    }

    return { 
        isValid: true, 
        cleaned: cleaned, 
        padded: cleaned,
        original: cleaned 
    };
}

async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithRetry(operation, retries = parseInt(MAX_RETRIES, 10)) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            if (attempt === retries) throw error;
            Logger.warn(`Attempt ${attempt} failed, retrying in ${RETRY_DELAY}ms...`);
            await delay(RETRY_DELAY);
        }
    }
}

// --- Shopify API Functions ---
async function getAllShopifyVariants() {
    Logger.info("Fetching all product variants from Shopify...");
    
    const query = `
        query GetVariants($limit: Int!, $cursor: String) {
            productVariants(first: $limit, after: $cursor) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        id
                        sku
                        price
                        compareAtPrice
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
                        product {
                            id
                            title
                        }
                    }
                }
            }
            locations(first: 1) {
                edges {
                    node {
                        id
                        name
                    }
                }
            }
        }
    `;

    try {
        // Extract location ID first
        await shopifyLimiter.removeTokens(1);
        const locationResponse = await fetchWithRetry(() => 
            shopifyClient.post('/graphql.json', { 
                query: `query GetLocations { locations(first: 1) { edges { node { id name } } } }`
            })
        );

        const locations = locationResponse.data.data.locations?.edges || [];
        if (locations.length === 0) {
            throw new Error('No locations found in Shopify');
        }
        const locationId = locations[0].node.id;
        Logger.info(`Found Shopify location: ${locations[0].node.name} (${locationId})`);

        // Fetch all variants with pagination
        const variants = new Map();
        let hasNextPage = true;
        let cursor = null;
        let totalFetched = 0;

        while (hasNextPage) {
            const variables = { limit: 250 };
            if (cursor) {
                variables.cursor = cursor;
            }

            await shopifyLimiter.removeTokens(1);
            const response = await fetchWithRetry(() => 
                shopifyClient.post('/graphql.json', { 
                    query, 
                    variables 
                })
            );

            // Error handling
            if (!response.data) {
                throw new Error(`No response data received. Full response: ${JSON.stringify(response)}`);
            }

            if (response.data.errors) {
                throw new Error(`GraphQL Errors: ${JSON.stringify(response.data.errors)}`);
            }

            if (!response.data.data) {
                throw new Error(`No data field in response. Response structure: ${JSON.stringify(Object.keys(response.data))}`);
            }

            // Process this batch of variants
            const productVariants = response.data.data.productVariants?.edges || [];
            
            productVariants.forEach(edge => {
                const node = edge.node;
                if (node.sku) {
                    const normalized = normalizeSkuForMatching(node.sku);
                    if (normalized.isValid) {
                        // Extract inventory using the new quantities structure
                        let currentInventory = 0;
                        const inventoryLevelEdge = node.inventoryItem?.inventoryLevels?.edges?.[0]?.node;
                        if (inventoryLevelEdge?.quantities?.length > 0) {
                            const availableObj = inventoryLevelEdge.quantities.find(q => q.name === 'available');
                            currentInventory = availableObj?.quantity || 0;
                        }

                        const variantData = {
                            id: node.id,
                            sku: node.sku,
                            price: node.price,
                            compareAtPrice: node.compareAtPrice,
                            inventoryItem: node.inventoryItem,
                            currentInventory: currentInventory,
                            product: {
                                title: node.product.title
                            }
                        };
                        variants.set(normalized.cleaned, variantData);
                        if (normalized.padded !== normalized.cleaned) {
                            variants.set(normalized.padded, variantData);
                        }
                    }
                }
            });

            totalFetched += productVariants.length;
            Logger.info(`Fetched batch: ${productVariants.length} variants (Total: ${totalFetched})`);

            // Check if there are more pages
            const pageInfo = response.data.data.productVariants?.pageInfo;
            hasNextPage = pageInfo?.hasNextPage || false;
            cursor = pageInfo?.endCursor || null;
        }

        Logger.info(`Successfully fetched ${totalFetched} total variants from Shopify`);
        return { variants, locationId };

    } catch (error) {
        // Enhanced error logging
        Logger.error('Error fetching variants from Shopify:');
        Logger.error(`Error message: ${error.message}`);
        
        if (error.response) {
            Logger.error(`HTTP Status: ${error.response.status}`);
            Logger.error(`Response headers: ${JSON.stringify(error.response.headers)}`);
            Logger.error(`Response data: ${JSON.stringify(error.response.data)}`);
        }
        
        throw error;
    }
}

async function updateVariantPrice(variant, newPrice, compareAtPrice, newInventory, locationId) {
    const mutation = `
        mutation variantUpdate($input: ProductVariantInput!) {
            productVariantUpdate(input: $input) {
                productVariant {
                    id
                    price
                    compareAtPrice
                }
                userErrors {
                    field
                    message
                }
            }
        }
    `;

    const variables = {
        input: {
            id: variant.id,
            price: newPrice.toString(),
            compareAtPrice: compareAtPrice ? compareAtPrice.toString() : null
        }
    };

    try {
        // Update price
        await shopifyLimiter.removeTokens(1);
        const response = await fetchWithRetry(() =>
            shopifyClient.post('/graphql.json', {
                query: mutation,
                variables
            })
        );

        const result = response.data.data.productVariantUpdate;
        if (result.userErrors && result.userErrors.length > 0) {
            throw new Error(JSON.stringify(result.userErrors));
        }

        // Update inventory if needed and tracked
        if (newInventory !== null && 
            newInventory !== undefined &&
            !isNaN(newInventory) &&
            newInventory !== variant.currentInventory && 
            variant.inventoryItem && 
            variant.inventoryItem.tracked) {
            
            const inventoryMutation = `
                mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
                    inventorySetOnHandQuantities(input: $input) {
                        inventoryAdjustmentGroup {
                            id
                        }
                        userErrors {
                            field
                            code
                            message
                        }
                    }
                }
            `;

            const inventoryVariables = {
                input: {
                    reason: "correction",
                    setQuantities: [{
                        inventoryItemId: variant.inventoryItem.id,
                        locationId: locationId,
                        quantity: newInventory
                    }]
                }
            };

            await shopifyLimiter.removeTokens(1);
            const inventoryResponse = await shopifyClient.post('/graphql.json', {
                query: inventoryMutation,
                variables: inventoryVariables
            });

            if (inventoryResponse.data.errors) {
                throw new Error(JSON.stringify(inventoryResponse.data.errors));
            }

            Logger.info(`Updated inventory for SKU ${variant.sku}: ${variant.currentInventory} -> ${newInventory}`);
        }

        return result.productVariant;
    } catch (error) {
        Logger.error('Error updating variant:', error.response?.data || error.message);
        throw error;
    }
}

// --- Data Loading Functions ---
async function getOriginalPrices() {
    try {
        Logger.info(`Fetching original prices from ${DATA_API_URL}`);
        const response = await fetchWithRetry(async () => {
            try {
                const result = await localApiClient.get(DATA_API_URL);
                
                // Debug the actual response structure
                Logger.info('API Response received. Analyzing structure...');
                Logger.info(`Response status: ${result.status}`);
                Logger.info(`Content-Type: ${result.headers['content-type']}`);
                
                // Check if we got any data
                if (!result.data) {
                    throw new Error('Empty response from API');
                }

                // Log the structure of the response
                Logger.info('Response data structure: ' + JSON.stringify({
                    keys: Object.keys(result.data),
                    hasD: 'd' in result.data,
                    dataType: typeof result.data,
                    isArray: Array.isArray(result.data),
                    preview: JSON.stringify(result.data).substring(0, 200) + '...'
                }));

                // Handle different response formats
                let products;
                if (result.data.d) {
                    // OData format
                    products = result.data.d;
                } else if (Array.isArray(result.data)) {
                    // Direct array format
                    products = result.data;
                } else if (typeof result.data === 'object' && Object.keys(result.data).length > 0) {
                    // Try to find an array in the response
                    const possibleArrays = Object.values(result.data).filter(val => Array.isArray(val));
                    if (possibleArrays.length === 1) {
                        products = possibleArrays[0];
                    } else {
                        throw new Error(`Unexpected API response structure. Available keys: ${Object.keys(result.data).join(', ')}`);
                    }
                } else {
                    throw new Error('Could not find product data in API response');
                }

                // Validate that we have an array of products
                if (!Array.isArray(products)) {
                    throw new Error(`Expected array of products but got ${typeof products}`);
                }

                // Validate the first product has the required fields
                if (products.length > 0) {
                    const firstProduct = products[0];
                    Logger.info('First product structure: ' + JSON.stringify(firstProduct));
                    
                    if (!firstProduct.CodigoProducto) {
                        // Check if we need to map different field names
                        const possibleSkuFields = ['sku', 'codigo', 'code', 'id'];
                        const foundSkuField = Object.keys(firstProduct).find(key => 
                            possibleSkuFields.includes(key.toLowerCase())
                        );
                        
                        if (foundSkuField) {
                            Logger.info(`Found alternative SKU field: ${foundSkuField}`);
                            // Remap the data structure
                            products = products.map(p => ({
                                CodigoProducto: p[foundSkuField],
                                Venta1: p.price || p.venta || p.precio || p.value || p.Venta1
                            }));
                        } else {
                            throw new Error(`Product structure missing CodigoProducto. Available fields: ${Object.keys(firstProduct).join(', ')}`);
                        }
                    }
                }

                result.data = { d: products }; // Normalize to expected format
                return result;
            } catch (err) {
                if (err.code === 'ECONNREFUSED') {
                    throw new Error(`Connection refused to local API at ${DATA_API_URL}. Is the API server running?`);
                }
                if (err.code === 'ETIMEDOUT') {
                    throw new Error(`Connection timed out while trying to reach ${DATA_API_URL}`);
                }
                if (err.response) {
                    // The request was made and the server responded with a status code
                    // that falls out of the range of 2xx
                    throw new Error(`API Error: ${err.response.status} - ${err.response.statusText}\nData: ${JSON.stringify(err.response.data)}`);
                } else if (err.request) {
                    // The request was made but no response was received
                    throw new Error(`No response received from ${DATA_API_URL}. Please check if the API is accessible.`);
                }
                throw err; // Re-throw other errors
            }
        });

        const products = response.data.d;
        if (!Array.isArray(products)) {
            throw new Error(`Expected array of products but got ${typeof products}: ${JSON.stringify(products).substring(0, 100)}...`);
        }

        const priceMap = new Map();
        let processedCount = 0;
        let invalidCount = 0;
        let skuFormats = new Set();

        products.forEach(product => {
            if (!product.CodigoProducto) {
                invalidCount++;
                return;
            }

            const normalized = normalizeSkuForMatching(product.CodigoProducto);
            if (normalized.isValid) {
                const price = parseFloat(product.Venta1);
                if (isNaN(price)) {
                    Logger.warn(`Invalid price for SKU ${product.CodigoProducto}: ${product.Venta1}`);
                    invalidCount++;
                    return;
                }

                const productData = {
                    originalPrice: price,
                    rawSku: product.CodigoProducto
                };

                priceMap.set(normalized.cleaned, productData);
                if (normalized.padded !== normalized.cleaned) {
                    priceMap.set(normalized.padded, productData);
                }
                
                skuFormats.add(`${product.CodigoProducto} -> ${normalized.cleaned} (padded: ${normalized.padded})`);
                processedCount++;
            } else {
                invalidCount++;
            }
        });

        // Log SKU format examples
        Logger.info('SKU format examples (first 5):');
        [...skuFormats].slice(0, 5).forEach(format => {
            Logger.info(`  ${format}`);
        });

        Logger.info(`Successfully processed ${processedCount} products (${invalidCount} invalid entries skipped)`);
        return priceMap;

    } catch (error) {
        Logger.error('Error fetching original prices:');
        Logger.error(`URL: ${DATA_API_URL}`);
        Logger.error(`Error: ${error.message}`);
        if (error.stack) {
            Logger.error(`Stack: ${error.stack}`);
        }
        throw error;
    }
}

async function getDiscountPrices() {
    try {
        const response = await axios.get(DISCOUNT_CSV_PATH);
        const lines = response.data.split('\n');
        const priceMap = new Map();
        let skuFormats = new Set();
        let uniqueSkuCount = 0;  // Track actual number of SKUs

        // Skip header row
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i].trim();
            if (!line) continue;

            const [sku, price] = line.split(',');
            if (sku && price) {
                const normalized = normalizeSkuForMatching(sku);
                if (normalized.isValid) {
                    const newPrice = parseFloat(price);
                    if (!isNaN(newPrice)) {
                        const priceData = {
                            newPrice: newPrice,
                            rawSku: sku
                        };
                        
                        // Store both padded and unpadded versions for numeric SKUs
                        priceMap.set(normalized.cleaned, priceData);
                        if (normalized.padded !== normalized.cleaned) {
                            priceMap.set(normalized.padded, priceData);
                        }
                        
                        skuFormats.add(`${sku} -> ${normalized.cleaned} (padded: ${normalized.padded})`);
                        uniqueSkuCount++; // Increment for each unique SKU
                    }
                }
            }
        }

        // Log SKU format examples
        Logger.info('Discount SKU format examples (first 5):');
        [...skuFormats].slice(0, 5).forEach(format => {
            Logger.info(`  ${format}`);
        });

        return {
            priceMap,
            uniqueCount: uniqueSkuCount  // Return both the map and the unique count
        };
    } catch (error) {
        Logger.error('Error fetching discount prices:', error.message);
        throw error;
    }
}

// Add getLocalInventory function
async function getLocalInventory() {
    try {
        Logger.info(`Fetching inventory from ${INVENTORY_API_URL}`);
        const response = await fetchWithRetry(async () => {
            try {
                const result = await localApiClient.get(INVENTORY_API_URL);
                return result;
            } catch (err) {
                if (err.code === 'ECONNREFUSED') {
                    throw new Error(`Connection refused to local API at ${INVENTORY_API_URL}. Is the API server running?`);
                }
                if (err.code === 'ETIMEDOUT') {
                    throw new Error(`Connection timed out while trying to reach ${INVENTORY_API_URL}`);
                }
                throw err;
            }
        });

        const inventoryData = response.data.value;
        const inventoryMap = new Map();
        let processedCount = 0;
        let invalidCount = 0;

        for (const item of inventoryData) {
            if (!item.CodigoProducto) {
                invalidCount++;
                continue;
            }

            const normalized = normalizeSkuForMatching(item.CodigoProducto);
            if (!normalized.isValid) {
                invalidCount++;
                continue;
            }

            const initial = parseFloat(item.CantidadInicial || 0);
            const received = parseFloat(item.CantidadEntradas || 0);
            const shipped = parseFloat(item.CantidadSalidas || 0);

            if (isNaN(initial) || isNaN(received) || isNaN(shipped)) {
                Logger.warn(`Invalid inventory values for SKU ${item.CodigoProducto}`);
                invalidCount++;
                continue;
            }

            const calculatedQuantity = Math.max(0, initial + received - shipped);
            
            // Safety stock logic: If inventory is 5 or less, don't sell online (keep all for store)
            let shopifyQuantity;
            if (calculatedQuantity <= SAFETY_STOCK_UNITS) {
                shopifyQuantity = 0; // Don't sell online, keep all units for physical store
            } else {
                shopifyQuantity = Math.floor(calculatedQuantity); // Sell full amount online (enough for store)
            }
            
            const inventoryData = {
                quantity: shopifyQuantity,
                actualQuantity: Math.floor(calculatedQuantity), // Keep track of actual inventory
                rawSku: item.CodigoProducto
            };

            inventoryMap.set(normalized.cleaned, inventoryData);
            if (normalized.padded !== normalized.cleaned) {
                inventoryMap.set(normalized.padded, inventoryData);
            }
            processedCount++;
        }

        Logger.info(`Successfully processed ${processedCount} inventory items (${invalidCount} invalid entries skipped)`);
        return inventoryMap;
    } catch (error) {
        Logger.error('Error fetching inventory:', error.message);
        throw error;
    }
}

// --- Main Function ---
async function updatePrices() {
    Timer.startTimer();
    Logger.section('Initialization');
    Logger.info('Starting Shopify Price Updater (Direct Price Version)');
    Logger.info(`API Mode: ${USE_REST_API === 'true' ? 'REST' : 'GraphQL'}`);
    Logger.info(`Safety Stock: ${SAFETY_STOCK_UNITS} units reserved for physical store`);

    try {
        // Fetch all data in parallel
        currentOperation = 'Data Fetching';
        Logger.section('Data Fetching');
        const [shopifyData, originalPrices, discountPricesResult, inventoryData] = await Promise.all([
            getAllShopifyVariants(),
            getOriginalPrices(),
            getDiscountPrices(),
            getLocalInventory()
        ]);

        const { variants: shopifyVariants, locationId } = shopifyData;
        const discountPrices = discountPricesResult.priceMap;

        Logger.info(`Found ${shopifyVariants.size} variants in Shopify`);
        Logger.info(`Loaded ${originalPrices.size} original prices`);
        Logger.info(`Loaded ${discountPricesResult.uniqueCount} discount prices`);
        Logger.info(`Loaded ${inventoryData.size} inventory records`);

        // Add this debug section after loading all data
        Logger.section('DEBUG INFO');

        // Show first 10 Shopify SKUs with product names
        Logger.info('First 10 Shopify SKUs found:');
        let count = 0;
        for (const [sku, variant] of shopifyVariants) {
            if (count < 10) {
                Logger.info(`  ${sku} (${variant.product.title}) -> Price: ${variant.price}, Compare-at: ${variant.compareAtPrice || 'null'}`);
                count++;
            }
        }

        // Show all discount SKUs and whether they exist
        Logger.info('Discount SKUs check:');
        for (const [sku, discountData] of discountPrices) {
            const exists = shopifyVariants.has(sku);
            const originalData = originalPrices.get(sku);
            
                            Logger.info(`  ${sku} -> Exists: ${exists}, Discount Price: ${discountData.newPrice}`);
            
            if (exists) {
                const variant = shopifyVariants.get(sku);
                const inventoryInfo = inventoryData.get(sku);
                
                Logger.info(`    Product: "${variant.product.title}"`);
                Logger.info(`    Current Shopify Price: ${variant.price}`);
                Logger.info(`    Current Shopify Compare-at: ${variant.compareAtPrice || 'null'}`);
                Logger.info(`    Current Shopify Inventory: ${variant.currentInventory}`);
                
                if (inventoryInfo) {
                    const reserveLogic = inventoryInfo.actualQuantity <= SAFETY_STOCK_UNITS ? 
                        `all ${inventoryInfo.actualQuantity} units reserved for store` : 
                        `${inventoryInfo.actualQuantity} units available, selling all online`;
                    Logger.info(`    Actual Inventory: ${inventoryInfo.actualQuantity}, Shopify Inventory: ${inventoryInfo.quantity} (${reserveLogic})`);
                }
                
                Logger.info(`    NEW Discount Price (from CSV): ${discountData.newPrice}`);
                Logger.info(`    NEW Compare-at Price (from Local API): ${originalData?.originalPrice || 'N/A'}`);
                
                // Show if update is needed
                const currentPrice = parseFloat(variant.price);
                const newPrice = discountData.newPrice;
                const compareAtPrice = originalData?.originalPrice;
                const currentCompareAt = parseFloat(variant.compareAtPrice || 0);
                
                const priceNeedsUpdate = currentPrice !== newPrice || currentCompareAt !== compareAtPrice;
                const inventoryNeedsUpdate = inventoryInfo && inventoryInfo.quantity !== variant.currentInventory;
                
                Logger.info(`    Update needed: ${priceNeedsUpdate || inventoryNeedsUpdate ? 'YES' : 'NO'}`);
                
                if (priceNeedsUpdate) {
                    Logger.info(`    Price Changes: ${currentPrice} -> ${newPrice}, Compare-at ${currentCompareAt} -> ${compareAtPrice}`);
                }
                if (inventoryNeedsUpdate) {
                    const changeLogic = inventoryInfo.actualQuantity <= SAFETY_STOCK_UNITS ? 
                        `reserving all ${inventoryInfo.actualQuantity} units for store` : 
                        `selling all ${inventoryInfo.actualQuantity} units online`;
                    Logger.info(`    Inventory Changes: ${variant.currentInventory} -> ${inventoryInfo.quantity} (${changeLogic})`);
                }
            } else {
                Logger.info(`    Product: NOT FOUND IN SHOPIFY`);
            }
            Logger.info(''); // Empty line for readability
        }

        // Add duplicate tracking before processing
        Logger.section('Processing Updates');
        const stats = {
            total: 0,
            updated: 0,
            skipped: 0,
            errors: 0,
            priceUpdates: 0,
            inventoryUpdates: 0,
            discountProducts: 0,
            regularProducts: 0
        };

        // Track processed variant IDs to avoid duplicates
        const processedVariants = new Set();

        // First, process discount products from CSV
        Logger.section('Processing Discount Products');
        for (const [sku, discountData] of discountPrices) {
            try {
                stats.total++;
                const variant = shopifyVariants.get(sku);
                const originalData = originalPrices.get(sku);
                const inventoryItem = inventoryData.get(sku);

                if (!variant) {
                    Logger.warn(`SKU ${sku} not found in Shopify`);
                    stats.skipped++;
                    continue;
                }

                // Check if we've already processed this variant
                if (processedVariants.has(variant.id)) {
                    Logger.info(`SKU ${sku}: Skipping duplicate (already processed as different SKU format)`);
                    stats.skipped++;
                    continue;
                }

                if (!originalData) {
                    Logger.warn(`SKU ${sku} not found in original prices`);
                    stats.skipped++;
                    continue;
                }

                const currentPrice = parseFloat(variant.price);
                const newPrice = discountData.newPrice;
                const compareAtPrice = originalData.originalPrice;
                const newInventory = inventoryItem?.quantity;

                // Skip if no changes needed
                const priceNeedsUpdate = currentPrice !== newPrice || parseFloat(variant.compareAtPrice || 0) !== compareAtPrice;
                const inventoryNeedsUpdate = newInventory !== null && newInventory !== variant.currentInventory;

                if (!priceNeedsUpdate && !inventoryNeedsUpdate) {
                    Logger.info(`SKU ${sku} (${variant.product.title}): No updates needed (Discount Product)`);
                    stats.skipped++;
                    processedVariants.add(variant.id); // Mark as processed
                    continue;
                }

                // Update variant
                await updateVariantPrice(variant, newPrice, compareAtPrice, newInventory, locationId);
                
                Logger.info(`SKU ${sku} (${variant.product.title}): Updated successfully`);
                
                if (priceNeedsUpdate) {
                    stats.priceUpdates++;
                    stats.discountProducts++;
                }
                if (inventoryNeedsUpdate) stats.inventoryUpdates++;
                stats.updated++;
                
                // Mark this variant as processed
                processedVariants.add(variant.id);

            } catch (error) {
                Logger.error(`Error processing discount SKU ${sku}: ${error.message}`);
                stats.errors++;
            }
        }

        // Then, process regular products (not in CSV)
        Logger.section('Processing Regular Products');
        for (const [sku, variant] of shopifyVariants) {
            try {
                // Skip if this is a discount product (already processed)
                if (discountPrices.has(sku)) {
                    continue;
                }

                // Check if we've already processed this variant
                if (processedVariants.has(variant.id)) {
                    continue; // Skip silently for regular products
                }

                stats.total++;
                const originalData = originalPrices.get(sku);
                const inventoryItem = inventoryData.get(sku);

                if (!originalData) {
                    Logger.warn(`SKU ${sku} not found in original prices`);
                    stats.skipped++;
                    continue;
                }

                const currentPrice = parseFloat(variant.price);
                const newPrice = originalData.originalPrice; // Use Venta1 as the selling price
                const compareAtPrice = null; // No compare-at price for regular products
                const newInventory = inventoryItem?.quantity;

                // Skip if no changes needed
                const priceNeedsUpdate = currentPrice !== newPrice || variant.compareAtPrice !== null;
                const inventoryNeedsUpdate = newInventory !== null && newInventory !== variant.currentInventory;

                if (!priceNeedsUpdate && !inventoryNeedsUpdate) {
                    Logger.info(`SKU ${sku} (${variant.product.title}): No updates needed (Regular Product)`);
                    stats.skipped++;
                    processedVariants.add(variant.id); // Mark as processed
                    continue;
                }

                // Update variant
                await updateVariantPrice(variant, newPrice, compareAtPrice, newInventory, locationId);
                
                Logger.info(`SKU ${sku} (${variant.product.title}): Updated successfully`);
                
                if (priceNeedsUpdate) {
                    stats.priceUpdates++;
                    stats.regularProducts++;
                }
                if (inventoryNeedsUpdate) stats.inventoryUpdates++;
                stats.updated++;
                
                // Mark this variant as processed
                processedVariants.add(variant.id);

            } catch (error) {
                Logger.error(`Error processing regular SKU ${sku}: ${error.message}`);
                stats.errors++;
            }
        }

        currentOperation = null; // Clear current operation when done
        
        // Calculate reserved products statistics
        let reservedProducts = 0;
        let onlineProducts = 0;
        let totalReservedUnits = 0;
        
        for (const [sku, inventoryInfo] of inventoryData) {
            if (inventoryInfo.actualQuantity <= SAFETY_STOCK_UNITS) {
                reservedProducts++;
                totalReservedUnits += inventoryInfo.actualQuantity;
            } else {
                onlineProducts++;
            }
        }

        // Final statistics
        Logger.section('Summary');
        const duration = Timer.endTimer();
        Logger.info(`Execution time: ${duration}`);
        Logger.info(`Total processed: ${stats.total}`);
        Logger.info(`Successfully updated: ${stats.updated}`);
        Logger.info(`- Price updates: ${stats.priceUpdates}`);
        Logger.info(`  • Discount products: ${stats.discountProducts}`);
        Logger.info(`  • Regular products: ${stats.regularProducts}`);
        Logger.info(`- Inventory updates: ${stats.inventoryUpdates}`);
        Logger.info(`Skipped: ${stats.skipped}`);
        Logger.info(`Errors: ${stats.errors}`);
        
        Logger.section('Inventory Management');
        Logger.info(`Safety stock threshold: ${SAFETY_STOCK_UNITS} units`);
        Logger.info(`Products reserved for store (≤${SAFETY_STOCK_UNITS} units): ${reservedProducts}`);
        Logger.info(`Products available online (>${SAFETY_STOCK_UNITS} units): ${onlineProducts}`);
        Logger.info(`Total units reserved for physical store: ${totalReservedUnits}`);

        // Add this new debug section after the existing DEBUG INFO
        Logger.section('MISSING SKU ANALYSIS');

        // Collect all missing SKUs from discount CSV
        const missingSkus = [];
        for (const [sku, discountData] of discountPrices) {
            if (!shopifyVariants.has(sku)) {
                missingSkus.push({
                    sku,
                    discountPrice: discountData.newPrice,
                    rawSku: discountData.rawSku
                });
            }
        }

        Logger.info(`Found ${missingSkus.length} missing SKUs in discount CSV:`);
        missingSkus.forEach(item => {
            Logger.info(`  SKU: ${item.sku} | Discount Price: $${item.discountPrice}`);
        });

        // Group existing products by similar names to find variants
        Logger.info('\nExisting products grouped by brand/type (first 15 groups):');
        const productGroups = new Map();
        let groupCount = 0;

        for (const [sku, variant] of shopifyVariants) {
            const name = variant.product.title.toLowerCase();
            // Extract brand/product type (first 3-4 words)
            const nameKey = name.split(' ').slice(0, 4).join(' ');
            
            if (!productGroups.has(nameKey)) {
                productGroups.set(nameKey, []);
            }
            productGroups.get(nameKey).push({
                sku,
                name: variant.product.title,
                price: parseFloat(variant.price),
                compareAt: variant.compareAtPrice
            });
        }

        // Sort groups by number of variants (most variants first)
        const sortedGroups = Array.from(productGroups.entries())
            .sort((a, b) => b[1].length - a[1].length);

        for (const [groupName, products] of sortedGroups) {
            if (groupCount < 15 && products.length > 1) {
                Logger.info(`\nGroup: "${groupName}..." (${products.length} variants)`);
                products.forEach(p => {
                    Logger.info(`  SKU: ${p.sku} | "${p.name}" | $${p.price}`);
                });
                groupCount++;
            }
        }

        // Suggest possible matches for missing SKUs
        Logger.info('\nMISSING SKU MATCH SUGGESTIONS:');
        for (const missingItem of missingSkus) {
            Logger.info(`\nMissing SKU: ${missingItem.sku} (Target price: $${missingItem.discountPrice})`);
            
            // Find products with similar prices (within 30% range)
            const priceMatches = [];
            for (const [sku, variant] of shopifyVariants) {
                const currentPrice = parseFloat(variant.price);
                const priceDiff = Math.abs(currentPrice - missingItem.discountPrice);
                const pricePercent = (priceDiff / missingItem.discountPrice) * 100;
                
                if (pricePercent <= 30) { // Within 30% of target price
                    priceMatches.push({
                        sku,
                        name: variant.product.title,
                        price: currentPrice,
                        priceDiff,
                        pricePercent: pricePercent.toFixed(1)
                    });
                }
            }
            
            // Sort by price difference (closest matches first)
            priceMatches.sort((a, b) => a.priceDiff - b.priceDiff);
            
            if (priceMatches.length > 0) {
                Logger.info(`  Found ${priceMatches.length} possible matches by price:`);
                priceMatches.slice(0, 5).forEach(match => {
                    Logger.info(`    SKU ${match.sku}: "${match.name.substring(0, 50)}..." ($${match.price}, ${match.pricePercent}% diff)`);
                });
            } else {
                Logger.info(`  No products found with similar price (±30%)`);
            }
            
            // Also look for SKU pattern matches (numeric similarity)
            if (/^\d+$/.test(missingItem.sku)) {
                const numericSku = parseInt(missingItem.sku);
                const nearbySkus = [];
                
                for (const [sku, variant] of shopifyVariants) {
                    if (/^\d+$/.test(sku)) {
                        const existingSku = parseInt(sku);
                        const skuDiff = Math.abs(existingSku - numericSku);
                        if (skuDiff <= 10 && skuDiff > 0) { // Within 10 numbers
                            nearbySkus.push({
                                sku,
                                name: variant.product.title,
                                price: parseFloat(variant.price),
                                skuDiff
                            });
                        }
                    }
                }
                
                if (nearbySkus.length > 0) {
                    nearbySkus.sort((a, b) => a.skuDiff - b.skuDiff);
                    Logger.info(`  Nearby SKUs (numeric pattern):`);
                    nearbySkus.slice(0, 3).forEach(nearby => {
                        Logger.info(`    SKU ${nearby.sku}: "${nearby.name.substring(0, 50)}..." ($${nearby.price})`);
                    });
                }
            }
        }

        // Summary of actionable recommendations
        Logger.info('\nRECOMMENDATIONS:');
        Logger.info(`1. Review ${missingSkus.length} missing SKUs above for potential matches`);
        Logger.info(`2. Check if missing SKUs are different product variants (sizes, quantities, etc.)`);
        Logger.info(`3. Update discount CSV with correct SKUs for existing products`);
        Logger.info(`4. Remove truly non-existent products from discount CSV`);

    } catch (error) {
        Logger.error('Fatal error:', error);
        throw error;
    }
}

// --- Script Execution ---
updatePrices().catch(error => {
    Logger.error('Script failed:', error);
    process.exit(1);
}); 