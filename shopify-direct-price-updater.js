// shopify-direct-price-updater.js
require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const csv = require('csv-parser');
const { RateLimiter } = require('limiter');
const logger = require('./common/logger');

// Environment variables
const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    CSV_FILE_PATH,
    SHOPIFY_RATE_LIMIT = '2'
} = process.env;

// Constants
const SHOPIFY_API_VERSION = '2024-10';
const SHOPIFY_GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const MAX_RETRIES = 3;

// Validate required environment variables
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN || !CSV_FILE_PATH) {
    logger.error("Error: Missing required environment variables (SHOPIFY_SHOP_NAME, SHOPIFY_ACCESS_TOKEN, CSV_FILE_PATH)");
    process.exit(1);
}

// Initialize rate limiter
const shopifyLimiter = new RateLimiter({
    tokensPerInterval: parseInt(SHOPIFY_RATE_LIMIT, 10),
    interval: 'second'
});

// Axios instance for Shopify
const shopifyClient = axios.create({
    baseURL: `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}`,
    headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    }
});

// Helper function to fetch all variants
async function fetchAllVariants() {
    const query = `
        query {
            products(first: 250) {
                edges {
                    node {
                        variants(first: 250) {
                            edges {
                                node {
                                    id
                                    sku
                                    price
                                    compareAtPrice
                                    product {
                                        title
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
        const response = await shopifyClient.post('/graphql.json', { query });
        
        const variants = new Map();
        response.data.data.products.edges.forEach(product => {
            product.node.variants.edges.forEach(variant => {
                const { node } = variant;
                if (node.sku) {
                    variants.set(node.sku, {
                        id: node.id,
                        price: node.price,
                        compareAtPrice: node.compareAtPrice,
                        productTitle: node.product.title
                    });
                }
            });
        });
        
        return variants;
    } catch (error) {
        logger.error('Error fetching variants:', error);
        throw error;
    }
}

// Helper function to update variant price
async function updateVariantPrice(variantId, newPrice, compareAtPrice = null) {
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
            id: variantId,
            price: newPrice.toString(),
            compareAtPrice: compareAtPrice ? compareAtPrice.toString() : null
        }
    };

    try {
        await shopifyLimiter.removeTokens(1);
        const response = await shopifyClient.post('/graphql.json', {
            query: mutation,
            variables
        });

        const result = response.data.data.productVariantUpdate;
        if (result.userErrors && result.userErrors.length > 0) {
            throw new Error(JSON.stringify(result.userErrors));
        }

        return result.productVariant;
    } catch (error) {
        logger.error('Error updating variant:', error);
        throw error;
    }
}

// Main function to process CSV and update prices
async function updatePricesFromCSV() {
    logger.log('Starting price update process...');
    
    // Fetch all variants from Shopify
    logger.log('Fetching variants from Shopify...');
    const shopifyVariants = await fetchAllVariants();
    logger.log(`Found ${shopifyVariants.size} variants in Shopify`);

    // Stats for tracking updates
    const stats = {
        total: 0,
        updated: 0,
        skipped: 0,
        errors: 0
    };

    // Process CSV file
    console.log('Processing CSV file...');
    const processCSV = new Promise((resolve, reject) => {
        fs.createReadStream(CSV_FILE_PATH)
            .pipe(csv())
            .on('data', async (row) => {
                stats.total++;
                
                try {
                    // Skip if no SKU or price
                    if (!row.sku || !row.price) {
                        logger.warn(`Skipping row: Missing SKU or price`);
                        stats.skipped++;
                        return;
                    }

                    const variant = shopifyVariants.get(row.sku);
                    if (!variant) {
                        logger.warn(`SKU ${row.sku} not found in Shopify`);
                        stats.skipped++;
                        return;
                    }

                    const newPrice = parseFloat(row.price);
                    const currentPrice = parseFloat(variant.price);

                    // Skip if prices are the same
                    if (newPrice === currentPrice) {
                        logger.log(`SKU ${row.sku}: Price unchanged (${currentPrice})`);
                        stats.skipped++;
                        return;
                    }

                    // Update price
                    logger.log(`Updating SKU ${row.sku} price: ${currentPrice} -> ${newPrice}`);
                    await updateVariantPrice(variant.id, newPrice);
                    stats.updated++;
                    
                } catch (error) {
                    logger.error(`Error processing SKU ${row.sku}:`, error);
                    stats.errors++;
                }
            })
            .on('end', () => {
                logger.log('\nUpdate process completed!');
                logger.log('Summary:');
                logger.log(`- Total processed: ${stats.total}`);
                logger.log(`- Successfully updated: ${stats.updated}`);
                logger.log(`- Skipped: ${stats.skipped}`);
                logger.log(`- Errors: ${stats.errors}`);
                resolve();
            })
            .on('error', (error) => {
                logger.error('Error reading CSV:', error);
                reject(error);
            });
    });

    await processCSV;
}

// Execute the script
(async () => {
    try {
        logger.init(new Date());
        logger.log('Script started at ' + new Date().toLocaleString());
        await updatePricesFromCSV();
    } catch (error) {
        logger.error('Script failed:', error);
        await logger.flush();
        process.exit(1);
    } finally {
        await logger.flush();
    }
})(); 