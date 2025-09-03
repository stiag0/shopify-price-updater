/**
 * Diagnostic Script: SKU Checker
 * This script will help identify which SKUs exist in Shopify but aren't being processed
 * by the main sync script.
 */

require('dotenv').config();
const axios = require('axios');

// Environment variables
const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    SHOPIFY_API_VERSION = '2024-01'
} = process.env;

// Validation
if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN) {
    console.error('Error: Missing SHOPIFY_SHOP_NAME or SHOPIFY_ACCESS_TOKEN in .env file');
    process.exit(1);
}

// Client's problematic SKUs
const CLIENT_SKUS = ['9644', '9649', '9654', '9659', '9645', '9650', '9655'];

// Shopify client
const shopifyClient = axios.create({
    baseURL: `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}`,
    headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    }
});

// SKU normalization functions (copied from main script)
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

async function getAllShopifyVariants() {
    console.log("🔍 Fetching ALL product variants from Shopify...");
    
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
                        product {
                            id
                            title
                            handle
                        }
                    }
                }
            }
        }
    `;

    const allVariants = [];
    let hasNextPage = true;
    let cursor = null;
    let totalFetched = 0;

    while (hasNextPage) {
        const variables = { limit: 250 };
        if (cursor) {
            variables.cursor = cursor;
        }

        try {
            const response = await shopifyClient.post('/graphql.json', { 
                query, 
                variables 
            });

            if (response.data.errors) {
                throw new Error(`GraphQL Errors: ${JSON.stringify(response.data.errors)}`);
            }

            const productVariants = response.data.data.productVariants?.edges || [];
            
            productVariants.forEach(edge => {
                const node = edge.node;
                if (node.sku) {
                    allVariants.push({
                        id: node.id,
                        sku: node.sku,
                        price: node.price,
                        compareAtPrice: node.compareAtPrice,
                        productTitle: node.product.title,
                        productHandle: node.product.handle
                    });
                }
            });

            totalFetched += productVariants.length;
            console.log(`📦 Fetched batch: ${productVariants.length} variants (Total: ${totalFetched})`);

            const pageInfo = response.data.data.productVariants?.pageInfo;
            hasNextPage = pageInfo?.hasNextPage || false;
            cursor = pageInfo?.endCursor || null;

            // Add a small delay to be respectful to Shopify's API
            await new Promise(resolve => setTimeout(resolve, 100));

        } catch (error) {
            console.error('❌ Error fetching variants:', error.message);
            throw error;
        }
    }

    console.log(`✅ Successfully fetched ${totalFetched} total variants from Shopify\n`);
    return allVariants;
}

async function analyzeSkus() {
    try {
        console.log('🚀 Starting SKU Analysis...\n');
        
        // Get all variants
        const allVariants = await getAllShopifyVariants();
        
        // Create maps for different SKU formats
        const skuMap = new Map(); // original SKU -> variant
        const normalizedMap = new Map(); // normalized SKU -> variant
        
        console.log('🔄 Processing and normalizing SKUs...');
        
        allVariants.forEach(variant => {
            // Store by original SKU
            skuMap.set(variant.sku, variant);
            
            // Store by normalized SKU
            const normalized = normalizeSkuForMatching(variant.sku);
            if (normalized.isValid) {
                normalizedMap.set(normalized.cleaned, variant);
                if (normalized.padded !== normalized.cleaned) {
                    normalizedMap.set(normalized.padded, variant);
                }
            }
        });
        
        console.log(`📊 Found ${skuMap.size} unique SKUs in Shopify\n`);
        
        // Check client's problematic SKUs
        console.log('🎯 CHECKING CLIENT\'S PROBLEMATIC SKUs:');
        console.log('=' .repeat(60));
        
        CLIENT_SKUS.forEach(clientSku => {
            console.log(`\n🔍 Checking SKU: ${clientSku}`);
            
            // Check original format
            const originalMatch = skuMap.get(clientSku);
            if (originalMatch) {
                console.log(`  ✅ Found by original SKU: "${clientSku}"`);
                console.log(`     Product: "${originalMatch.productTitle}"`);
                console.log(`     Price: $${originalMatch.price}`);
                console.log(`     Compare-at: ${originalMatch.compareAtPrice || 'null'}`);
                return;
            }
            
            // Check normalized format
            const normalizedMatch = normalizedMap.get(clientSku);
            if (normalizedMatch) {
                console.log(`  ✅ Found by normalized SKU: "${clientSku}" (original: "${normalizedMatch.sku}")`);
                console.log(`     Product: "${normalizedMatch.productTitle}"`);
                console.log(`     Price: $${normalizedMatch.price}`);
                console.log(`     Compare-at: ${normalizedMatch.compareAtPrice || 'null'}`);
                return;
            }
            
            // Not found - show similar SKUs
            console.log(`  ❌ NOT FOUND: "${clientSku}"`);
            
            // Find similar SKUs (same prefix)
            const prefix = clientSku.substring(0, 3); // First 3 characters
            const similarSkus = [];
            
            for (const [sku, variant] of skuMap) {
                if (sku.startsWith(prefix) && sku !== clientSku) {
                    similarSkus.push({ sku, title: variant.productTitle });
                }
            }
            
            if (similarSkus.length > 0) {
                console.log(`     Similar SKUs found (prefix "${prefix}"):`);
                similarSkus.slice(0, 5).forEach(similar => {
                    console.log(`       • ${similar.sku} - "${similar.title}"`);
                });
                if (similarSkus.length > 5) {
                    console.log(`       ... and ${similarSkus.length - 5} more`);
                }
            } else {
                console.log(`     No similar SKUs found with prefix "${prefix}"`);
            }
        });
        
        // Show some statistics
        console.log('\n📈 STATISTICS:');
        console.log('=' .repeat(40));
        console.log(`Total variants in Shopify: ${allVariants.length}`);
        console.log(`Unique SKUs: ${skuMap.size}`);
        console.log(`Client SKUs found: ${CLIENT_SKUS.filter(sku => skuMap.has(sku) || normalizedMap.has(sku)).length}/${CLIENT_SKUS.length}`);
        
        // Show sample of actual SKUs that start with '96'
        console.log('\n🔢 ACTUAL SKUs STARTING WITH "96":');
        console.log('=' .repeat(40));
        const skus96 = [...skuMap.keys()].filter(sku => sku.startsWith('96')).sort();
        if (skus96.length > 0) {
            console.log(`Found ${skus96.length} SKUs starting with "96":`);
            skus96.slice(0, 20).forEach(sku => {
                const variant = skuMap.get(sku);
                console.log(`  • ${sku} - "${variant.productTitle}"`);
            });
            if (skus96.length > 20) {
                console.log(`  ... and ${skus96.length - 20} more`);
            }
        } else {
            console.log('No SKUs found starting with "96"');
        }
        
    } catch (error) {
        console.error('❌ Analysis failed:', error.message);
        process.exit(1);
    }
}

// Run the analysis
analyzeSkus().then(() => {
    console.log('\n✅ Analysis completed!');
}).catch(error => {
    console.error('❌ Script failed:', error.message);
    process.exit(1);
});
