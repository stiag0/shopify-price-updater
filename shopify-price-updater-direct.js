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
    SHOPIFY_API_VERSION = '2024-01',
    USE_REST_API = 'false',
    MAX_RETRIES = '3',
    SHOPIFY_RATE_LIMIT = '2'
} = process.env;

// --- Validation ---
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !DATA_API_URL || !DISCOUNT_CSV_PATH) {
    console.error(`
Error: Missing required environment variables!
Required variables:
- SHOPIFY_SHOP_NAME
- SHOPIFY_ACCESS_TOKEN
- DATA_API_URL (for compare-at prices)
- DISCOUNT_CSV_PATH (Google Sheets URL)
`);
    process.exit(1);
}

// --- Constants ---
const GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const MAX_PAGES = 20; // Maximum number of pages to fetch
const RETRY_DELAY = 1000; // 1 second

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

const Logger = {
    log(message, level = 'INFO') {
        const timestamp = new Date().toISOString();
        console.log(`[${timestamp}] [${level}] ${message}`);
    },
    info(message) { this.log(message, 'INFO'); },
    warn(message) { this.log(message, 'WARN'); },
    error(message) { this.log(message, 'ERROR'); },
    success(message) { this.log(message, 'SUCCESS'); },
    section(title) {
        console.log('\n' + '='.repeat(20) + ' ' + title + ' ' + '='.repeat(20));
    }
};

// --- API Setup ---
const shopifyLimiter = new RateLimiter({
    tokensPerInterval: parseInt(SHOPIFY_RATE_LIMIT, 10),
    interval: 'second'
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
        const padded = cleaned.padStart(5, '0');
        return { isValid: true, cleaned, padded };
    }

    return { isValid: true, cleaned, padded: cleaned };
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
    const query = `
        query {
            products(first: 250) {
                edges {
                    node {
                        title
                        variants(first: 250) {
                            edges {
                                node {
                                    id
                                    sku
                                    price
                                    compareAtPrice
                                    inventoryItem {
                                        id
                                        tracked
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    `;

    try {
        await shopifyLimiter.removeTokens(1);
        const response = await fetchWithRetry(() => 
            shopifyClient.post('/graphql.json', { query })
        );

        const variants = new Map();
        response.data.data.products.edges.forEach(product => {
            product.node.variants.edges.forEach(variant => {
                const { node } = variant;
                if (node.sku) {
                    const normalized = normalizeSkuForMatching(node.sku);
                    if (normalized.isValid) {
                        const variantData = {
                            id: node.id,
                            sku: node.sku,
                            price: node.price,
                            compareAtPrice: node.compareAtPrice,
                            inventoryItem: node.inventoryItem,
                            product: {
                                title: product.node.title
                            }
                        };
                        variants.set(normalized.cleaned, variantData);
                        if (normalized.padded !== normalized.cleaned) {
                            variants.set(normalized.padded, variantData);
                        }
                    }
                }
            });
        });

        return variants;
    } catch (error) {
        Logger.error('Error fetching variants:', error.response?.data || error.message);
        throw error;
    }
}

async function updateVariantPrice(variant, newPrice, compareAtPrice) {
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

        return result.productVariant;
    } catch (error) {
        Logger.error('Error updating variant:', error.response?.data || error.message);
        throw error;
    }
}

// --- Data Loading Functions ---
async function getOriginalPrices() {
    try {
        const response = await axios.get(DATA_API_URL);
        const products = response.data.d;
        const priceMap = new Map();

        products.forEach(product => {
            if (product.CodigoProducto) {
                const normalized = normalizeSkuForMatching(product.CodigoProducto);
                if (normalized.isValid) {
                    priceMap.set(normalized.cleaned, {
                        originalPrice: parseFloat(product.Venta1) || 0
                    });
                }
            }
        });

        return priceMap;
    } catch (error) {
        Logger.error('Error fetching original prices:', error.message);
        throw error;
    }
}

async function getDiscountPrices() {
    try {
        const response = await axios.get(DISCOUNT_CSV_PATH);
        const lines = response.data.split('\n');
        const priceMap = new Map();

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
                        priceMap.set(normalized.cleaned, {
                            newPrice: newPrice
                        });
                    }
                }
            }
        }

        return priceMap;
    } catch (error) {
        Logger.error('Error fetching discount prices:', error.message);
        throw error;
    }
}

// --- Main Function ---
async function updatePrices() {
    Timer.startTimer();
    Logger.section('Initialization');
    Logger.info('Starting Shopify Price Updater (Direct Price Version)');
    Logger.info(`API Mode: ${USE_REST_API === 'true' ? 'REST' : 'GraphQL'}`);

    try {
        // Fetch all data
        Logger.section('Data Fetching');
        const [shopifyVariants, originalPrices, discountPrices] = await Promise.all([
            getAllShopifyVariants(),
            getOriginalPrices(),
            getDiscountPrices()
        ]);

        Logger.info(`Found ${shopifyVariants.size} variants in Shopify`);
        Logger.info(`Loaded ${originalPrices.size} original prices`);
        Logger.info(`Loaded ${discountPrices.size} discount prices`);

        // Process updates
        Logger.section('Processing Updates');
        const stats = {
            total: 0,
            updated: 0,
            skipped: 0,
            errors: 0
        };

        for (const [sku, discountData] of discountPrices) {
            try {
                stats.total++;
                const variant = shopifyVariants.get(sku);
                const originalData = originalPrices.get(sku);

                if (!variant) {
                    Logger.warn(`SKU ${sku} not found in Shopify`);
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

                // Skip if no change needed
                if (currentPrice === newPrice && parseFloat(variant.compareAtPrice || 0) === compareAtPrice) {
                    Logger.info(`SKU ${sku}: No price change needed`);
                    stats.skipped++;
                    continue;
                }

                // Update price
                Logger.info(`Updating SKU ${sku} (${variant.product.title})`);
                Logger.info(`- Price: ${currentPrice} -> ${newPrice}`);
                Logger.info(`- Compare at: ${variant.compareAtPrice || 'None'} -> ${compareAtPrice}`);

                await updateVariantPrice(variant, newPrice, compareAtPrice);
                stats.updated++;

            } catch (error) {
                Logger.error(`Error processing SKU ${sku}: ${error.message}`);
                stats.errors++;
            }
        }

        // Final statistics
        Logger.section('Summary');
        const duration = Timer.endTimer();
        Logger.info(`Execution time: ${duration}`);
        Logger.info(`Total processed: ${stats.total}`);
        Logger.info(`Successfully updated: ${stats.updated}`);
        Logger.info(`Skipped: ${stats.skipped}`);
        Logger.info(`Errors: ${stats.errors}`);

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