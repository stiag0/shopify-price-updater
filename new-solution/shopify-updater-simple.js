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
    DISCOUNT_CSV_PATH = 'discounts.csv',
    USE_REST_API = 'false' // Default to GraphQL if not specified
} = process.env;

const SHOPIFY_API_VERSION = '2024-01';
const SHOPIFY_GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const SHOPIFY_REST_BASE_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}`;
const MAX_RETRIES = 3;
const RATE_LIMIT = USE_REST_API === 'true' ? 1 : 2; // More conservative rate limit for REST API

// --- Validation ---
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !INVENTORY_API_URL) {
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

async function updateVariantRest(variantId, newPrice, compareAtPrice) {
    try {
        await shopifyLimiter.removeTokens(1);
        const response = await axiosShopify.put(`/variants/${variantId}.json`, {
            variant: {
                id: variantId,
                price: newPrice.toString(),
                compare_at_price: compareAtPrice ? compareAtPrice.toString() : null
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

async function updateVariantGraphQL(variantId, newPrice, compareAtPrice) {
    const mutation = `
        mutation variantUpdate($input: ProductVariantInput!) {
            variantUpdate(input: $input) {
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

    try {
        await shopifyLimiter.removeTokens(1);
        const response = await axiosShopify.post('', {
            query: mutation,
            variables: {
                input: {
                    id: variantId,
                    price: newPrice.toString(),
                    compareAtPrice: compareAtPrice ? compareAtPrice.toString() : null
                }
            }
        });

        if (response.data.errors) {
            throw new Error(JSON.stringify(response.data.errors));
        }

        const result = response.data.data.variantUpdate;
        if (result.userErrors.length > 0) {
            throw new Error(JSON.stringify(result.userErrors));
        }

        return result.productVariant;
    } catch (error) {
        console.error('GraphQL Error updating variant:', error.message);
        throw error;
    }
}

// --- API Wrapper Functions ---
async function getVariantBySku(sku) {
    return USE_REST_API === 'true' ? 
        getVariantBySkuRest(sku) : 
        getVariantBySkuGraphQL(sku);
}

async function updateVariant(variantId, newPrice, compareAtPrice) {
    return USE_REST_API === 'true' ? 
        updateVariantRest(variantId, newPrice, compareAtPrice) : 
        updateVariantGraphQL(variantId, newPrice, compareAtPrice);
}

// --- Main Functions ---
async function getOriginalPrices() {
    try {
        console.log('Fetching original prices from API...');
        const response = await fetchWithRetry({ url: INVENTORY_API_URL });
        const products = response.data;
        
        const priceMap = new Map();
        for (const product of products) {
            const sku = product.sku || product.SKU;
            const price = parseFloat(product.price || product.PRICE);
            if (sku && !isNaN(price)) {
                priceMap.set(sku.toString().trim(), price);
            }
        }
        
        console.log(`Loaded ${priceMap.size} original prices`);
        return priceMap;
    } catch (error) {
        console.error('Error fetching original prices:', error.message);
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
                console.log(`Loaded ${discountPrices.size} discount prices from CSV`);
                resolve(discountPrices);
            })
            .on('error', reject);
    });
}

async function main() {
    try {
        console.log(`Using ${USE_REST_API === 'true' ? 'REST API' : 'GraphQL API'} for Shopify operations`);
        
        // Load original prices from API
        const originalPrices = await getOriginalPrices();
        
        // Load discount prices from CSV
        const discountPrices = await loadDiscountPrices();

        console.log('Starting price updates...');
        const stats = {
            total: discountPrices.size,
            updated: 0,
            failed: 0,
            skipped: 0
        };

        // Process each SKU in the discount CSV
        for (const [sku, discountPrice] of discountPrices) {
            try {
                const originalPrice = originalPrices.get(sku);
                if (!originalPrice) {
                    console.warn(`No original price found for SKU ${sku}, skipping...`);
                    stats.skipped++;
                    continue;
                }

                // Get the variant from Shopify
                const variant = await getVariantBySku(sku);
                if (!variant) {
                    console.warn(`No variant found in Shopify for SKU ${sku}, skipping...`);
                    stats.skipped++;
                    continue;
                }

                // Update prices if they're different
                const currentPrice = parseFloat(variant.price);
                const currentCompareAtPrice = variant.compareAtPrice ? 
                    parseFloat(variant.compareAtPrice) : 
                    (USE_REST_API === 'true' ? parseFloat(variant.compare_at_price) : null);

                if (currentPrice !== discountPrice || currentCompareAtPrice !== originalPrice) {
                    await updateVariant(variant.id, discountPrice, originalPrice);
                    console.log(`Updated ${sku}: Price ${currentPrice} → ${discountPrice}, Compare At ${currentCompareAtPrice} → ${originalPrice}`);
                    stats.updated++;
                } else {
                    console.log(`Prices already correct for ${sku}, skipping...`);
                    stats.skipped++;
                }
            } catch (error) {
                console.error(`Failed to process SKU ${sku}:`, error.message);
                stats.failed++;
            }
        }

        console.log('\nUpdate completed:');
        console.log(`Total products: ${stats.total}`);
        console.log(`Updated: ${stats.updated}`);
        console.log(`Skipped: ${stats.skipped}`);
        console.log(`Failed: ${stats.failed}`);

    } catch (error) {
        console.error('Fatal error:', error.message);
        process.exit(1);
    }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\nReceived SIGINT. Graceful shutdown initiated.');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\nReceived SIGTERM. Graceful shutdown initiated.');
    process.exit(0);
});

// Run the updater
main().catch(error => {
    console.error('Fatal error:', error.message);
    process.exit(1);
}); 