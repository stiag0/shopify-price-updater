/**
 * Shopify Price Updater - Single File Solution
 * 
 * SETUP INSTRUCTIONS:
 * 1. Install required packages:
 *    npm install axios csv-parser dotenv limiter
 * 
 * 2. Use existing .env file with:
 *    SHOPIFY_SHOP_NAME=your-store-name
 *    SHOPIFY_ACCESS_TOKEN=your-access-token
 * 
 * 3. CSV file format (prices.csv):
 *    sku,discount
 *    ABC123,19.99
 *    XYZ789,29.99
 * 
 * 4. Run the script:
 *    node shopify-price-updater-single.js
 */

require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const csv = require('csv-parser');
const { RateLimiter } = require('limiter');
const path = require('path');

// Environment variables with defaults
const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    SHOPIFY_RATE_LIMIT = '2'
} = process.env;

// Fixed file paths to match original setup
const CSV_FILE_PATH = path.join(process.cwd(), 'prices.csv');

// Validate required environment variables
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN) {
    console.error(`
Error: Missing required environment variables!

Required .env file contents:
SHOPIFY_SHOP_NAME=your-store-name
SHOPIFY_ACCESS_TOKEN=your-access-token
SHOPIFY_RATE_LIMIT=2 (optional)

Please check your .env file and try again.
`);
    process.exit(1);
}

// Validate CSV file exists
if (!fs.existsSync(CSV_FILE_PATH)) {
    console.error(`
Error: CSV file not found!

Please ensure 'prices.csv' exists in the current directory with format:
sku,discount
ABC123,19.99
XYZ789,29.99
`);
    process.exit(1);
}

// Constants
const SHOPIFY_API_VERSION = '2024-10';
const GRAPHQL_URL = `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

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

// Helper function for delayed retry
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Helper function to retry failed requests
async function withRetry(operation, retries = MAX_RETRIES) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            if (attempt === retries) throw error;
            console.warn(`Attempt ${attempt} failed, retrying in ${RETRY_DELAY}ms...`);
            await delay(RETRY_DELAY);
        }
    }
}

// Helper function to clean and normalize SKUs
function cleanSku(sku) {
    if (!sku) return null;
    
    // Convert to string and trim
    const cleaned = String(sku).trim();
    
    // Remove any non-alphanumeric characters except hyphen and underscore
    const normalized = cleaned.replace(/[^a-zA-Z0-9-_]/g, '');
    
    return normalized || null;
}

// Helper function to normalize SKU for matching
function normalizeSkuForMatching(sku) {
    const cleaned = cleanSku(sku);
    if (!cleaned) {
        return { isValid: false, cleaned: null, padded: null };
    }

    // For numeric SKUs, handle padding
    if (/^\d+$/.test(cleaned)) {
        const padded = cleaned.padStart(5, '0');
        return { isValid: true, cleaned, padded };
    }

    return { isValid: true, cleaned, padded: cleaned };
}

// Helper function to fetch all variants with improved SKU matching
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
        const response = await withRetry(() => 
            shopifyClient.post('/graphql.json', { query })
        );
        
        const variants = new Map();
        response.data.data.products.edges.forEach(product => {
            product.node.variants.edges.forEach(variant => {
                const { node } = variant;
                if (node.sku) {
                    const normalized = normalizeSkuForMatching(node.sku);
                    if (normalized.isValid) {
                        // Store both cleaned and padded versions for better matching
                        const variantData = {
                            id: node.id,
                            price: node.price,
                            compareAtPrice: node.compareAtPrice,
                            productTitle: node.product.title,
                            originalSku: node.sku
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
        console.error('Error fetching variants:', error.response?.data || error.message);
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
        const response = await withRetry(() =>
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
        console.error('Error updating variant:', error.response?.data || error.message);
        throw error;
    }
}

// Main function to process CSV and update prices
async function updatePricesFromCSV() {
    console.log('Starting price update process...');
    
    // Fetch all variants from Shopify
    console.log('Fetching variants from Shopify...');
    const shopifyVariants = await fetchAllVariants();
    console.log(`Found ${shopifyVariants.size} variants in Shopify`);

    // Stats for tracking updates
    const stats = {
        total: 0,
        updated: 0,
        skipped: 0,
        errors: 0,
        notFound: 0
    };

    // Process CSV file
    console.log('Processing prices.csv file...');
    const processCSV = new Promise((resolve, reject) => {
        fs.createReadStream(CSV_FILE_PATH)
            .pipe(csv())
            .on('data', async (row) => {
                stats.total++;
                
                try {
                    // Skip if no SKU or discount price
                    if (!row.sku || !row.discount) {
                        console.warn(`Skipping row: Missing SKU or discount price`);
                        stats.skipped++;
                        return;
                    }

                    // Normalize the SKU from CSV
                    const normalizedCsvSku = normalizeSkuForMatching(row.sku);
                    if (!normalizedCsvSku.isValid) {
                        console.warn(`Skipping invalid SKU format: ${row.sku}`);
                        stats.skipped++;
                        return;
                    }

                    // Try to find matching variant
                    let variant = null;
                    const possibleSkus = [normalizedCsvSku.cleaned];
                    if (normalizedCsvSku.padded !== normalizedCsvSku.cleaned) {
                        possibleSkus.push(normalizedCsvSku.padded);
                    }

                    for (const possibleSku of possibleSkus) {
                        if (shopifyVariants.has(possibleSku)) {
                            variant = shopifyVariants.get(possibleSku);
                            break;
                        }
                    }

                    if (!variant) {
                        console.warn(`SKU ${row.sku} not found in Shopify (tried: ${possibleSkus.join(', ')})`);
                        stats.notFound++;
                        return;
                    }

                    const newPrice = parseFloat(row.discount);
                    const currentPrice = parseFloat(variant.price);

                    // Skip if prices are the same
                    if (newPrice === currentPrice) {
                        console.log(`SKU ${row.sku} (${variant.originalSku}): Price unchanged (${currentPrice})`);
                        stats.skipped++;
                        return;
                    }

                    // Update price
                    console.log(`Updating SKU ${row.sku} (${variant.originalSku}) price: ${currentPrice} -> ${newPrice}`);
                    await updateVariantPrice(variant.id, newPrice);
                    stats.updated++;
                    
                } catch (error) {
                    console.error(`Error processing SKU ${row.sku}:`, error.message);
                    stats.errors++;
                }
            })
            .on('end', () => {
                console.log('\nUpdate process completed!');
                console.log('Summary:');
                console.log(`- Total processed: ${stats.total}`);
                console.log(`- Successfully updated: ${stats.updated}`);
                console.log(`- Skipped (unchanged): ${stats.skipped}`);
                console.log(`- Not found in Shopify: ${stats.notFound}`);
                console.log(`- Errors: ${stats.errors}`);
                resolve();
            })
            .on('error', (error) => {
                console.error('Error reading CSV:', error);
                reject(error);
            });
    });

    await processCSV;
}

// Execute the script
(async () => {
    try {
        console.log('Script started at', new Date().toLocaleString());
        await updatePricesFromCSV();
    } catch (error) {
        console.error('Script failed:', error);
        process.exit(1);
    }
})(); 