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
    LOG_FILE_PATH = path.join('logs', `shopify-sync_${new Date().toISOString().replace(/:/g, '-')}.log`)
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
        const timestamp = new Date().toISOString();
        const logEntry = `[${timestamp}] [${level}] ${message}\n`;
        
        // Always show in console
        if (level === 'ERROR') {
            console.error(message);
        } else {
            console.log(message);
        }

        // Queue for file writing
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
    error(message) { this.log(message, 'ERROR'); },
    warn(message) { this.log(message, 'WARN'); }
};

// Initialize logger
Logger.init();

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
    return USE_REST_API === 'true' ? 
        updateVariantRest(variantId, updates) : 
        updateVariantGraphQL(variantId, updates);
}

// --- Main Functions ---
async function getOriginalData() {
    try {
        Logger.info('Fetching data from API...');
        const response = await fetchWithRetry({ url: DATA_API_URL });
        
        // Handle OData response structure
        const products = response.data.value || [];
        
        if (!Array.isArray(products)) {
            throw new Error(`Invalid API response structure. Expected array in value property, got ${typeof products}. Response: ${JSON.stringify(response.data)}`);
        }

        const dataMap = new Map();
        for (const product of products) {
            // Use Referencia as SKU, fallback to CodigoProducto
            const sku = (product.Referencia || product.CodigoProducto || '').toString().trim();
            const price = parseFloat(product.Venta1 || 0);
            
            if (sku && !isNaN(price)) {
                dataMap.set(sku, {
                    price,
                    inventory: 0  // Will be updated when we fetch inventory data
                });
            }
        }
        
        // Now fetch inventory data
        Logger.info('Fetching inventory data...');
        const invResponse = await fetchWithRetry({ url: INVENTORY_API_URL });
        const inventory = invResponse.data.value || [];

        // Update inventory quantities
        for (const item of inventory) {
            const sku = (item.Referencia || item.CodigoProducto || '').toString().trim();
            if (dataMap.has(sku)) {
                const existingData = dataMap.get(sku);
                // Calculate real inventory: Initial + Entries - Exits
                const realInventory = parseFloat(item.CantidadInicial || 0) + 
                                    parseFloat(item.CantidadEntradas || 0) - 
                                    parseFloat(item.CantidadSalidas || 0);
                
                existingData.inventory = Math.max(0, Math.round(realInventory));  // Ensure non-negative
                dataMap.set(sku, existingData);
            }
        }
        
        Logger.info(`Loaded ${dataMap.size} products from API`);
        return dataMap;
    } catch (error) {
        Logger.error('Error fetching data:');
        Logger.error(error.message);
        if (error.response?.data) {
            Logger.error('API Response:');
            Logger.error(JSON.stringify(error.response.data, null, 2));
        }
        throw error;
    }
}

async function loadDiscountPrices() {
    return new Promise((resolve, reject) => {
        const discountPrices = new Map();
        
        fs.createReadStream(DISCOUNT_CSV_PATH)
            .pipe(csv())
            .on('data', (row) => {
                const sku = row.sku?.toString().trim();
                const price = parseFloat(row.discount_price);
                if (sku && !isNaN(price)) {
                    discountPrices.set(sku, price);
                }
            })
            .on('end', () => {
                Logger.info(`Loaded ${discountPrices.size} discount prices from CSV`);
                resolve(discountPrices);
            })
            .on('error', reject);
    });
}

async function main() {
    try {
        Logger.info('Using ' + (USE_REST_API === 'true' ? 'REST API' : 'GraphQL API') + ' for Shopify operations');
        Logger.info(`Update mode: ${UPDATE_MODE}`);
        
        // Load original data from API
        const originalData = await getOriginalData();
        
        // Load discount prices from CSV if they exist
        let discountPrices = new Map();
        if (fs.existsSync(DISCOUNT_CSV_PATH)) {
            discountPrices = await loadDiscountPrices();
        }

        Logger.info('Starting updates...');
        const stats = {
            total: originalData.size,
            updated: 0,
            failed: 0,
            skipped: 0
        };

        // Process each SKU
        for (const [sku, data] of originalData) {
            try {
                // Get the variant from Shopify
                const variant = await getVariantBySku(sku);
                if (!variant) {
                    Logger.warn(`No variant found in Shopify for SKU ${sku}, skipping...`);
                    stats.skipped++;
                    continue;
                }

                const updates = {};
                let needsUpdate = false;

                // Handle price updates if enabled
                if (UPDATE_MODE === 'price' || UPDATE_MODE === 'both') {
                    const targetPrice = discountPrices.has(sku) ? discountPrices.get(sku) : data.price;
                    const currentPrice = parseFloat(variant.price);
                    
                    if (currentPrice !== targetPrice) {
                        updates.price = targetPrice.toString();
                        if (discountPrices.has(sku)) {
                            // Use the correct field name based on API type
                            if (USE_REST_API === 'true') {
                                updates.compare_at_price = data.price.toString();
                            } else {
                                updates.compareAtPrice = data.price.toString();
                            }
                        } else {
                            // Clear compare at price when not using discount
                            if (USE_REST_API === 'true') {
                                updates.compare_at_price = null;
                            } else {
                                updates.compareAtPrice = null;
                            }
                        }
                        needsUpdate = true;
                    }
                }

                // Handle inventory updates if enabled
                if (UPDATE_MODE === 'inventory' || UPDATE_MODE === 'both') {
                    const currentInventory = variant.inventoryQuantity || 0;
                    if (currentInventory !== data.inventory) {
                        if (USE_REST_API === 'true') {
                            updates.inventory_quantity = data.inventory;
                        } else if (variant.inventoryItem?.id && LOCATION_ID) {
                            // For GraphQL, we'll update inventory separately
                            await updateInventoryLevelGraphQL(
                                variant.inventoryItem.id,
                                LOCATION_ID,
                                data.inventory - currentInventory
                            );
                        }
                        needsUpdate = true;
                    }
                }

                if (needsUpdate) {
                    if (Object.keys(updates).length > 0) {
                        await updateVariant(variant.id, updates);
                    }
                    Logger.info(`Updated ${sku}: ${JSON.stringify(updates)}`);
                    stats.updated++;
                } else {
                    Logger.info(`No updates needed for ${sku}`);
                    stats.skipped++;
                }
            } catch (error) {
                Logger.error(`Failed to process SKU ${sku}:`);
                Logger.error(error.message);
                stats.failed++;
            }
        }

        Logger.info('\nUpdate completed:');
        Logger.info(`Total products: ${stats.total}`);
        Logger.info(`Updated: ${stats.updated}`);
        Logger.info(`Skipped: ${stats.skipped}`);
        Logger.info(`Failed: ${stats.failed}`);

    } catch (error) {
        Logger.error(`Fatal error: ${error.message}`);
        process.exit(1);
    }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    Logger.info('\nReceived SIGINT. Graceful shutdown initiated.');
    process.exit(0);
});

process.on('SIGTERM', () => {
    Logger.info('\nReceived SIGTERM. Graceful shutdown initiated.');
    process.exit(0);
});

// Run the updater
main().catch(error => {
    Logger.error(`Fatal error: ${error.message}`);
    process.exit(1);
}); 