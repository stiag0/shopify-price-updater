/**
 * Test Script: productVariantsBulkUpdate Mutation
 * This script will test the new bulk update mutation to ensure it works
 */

require('dotenv').config();
const axios = require('axios');

const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    SHOPIFY_API_VERSION = '2024-01'
} = process.env;

if (!SHOPIFY_SHOP_NAME || !SHOPIFY_ACCESS_TOKEN) {
    console.error('Error: Missing SHOPIFY_SHOP_NAME or SHOPIFY_ACCESS_TOKEN in .env file');
    process.exit(1);
}

const shopifyClient = axios.create({
    baseURL: `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}`,
    headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    }
});

async function testBulkVariantUpdate() {
    console.log('🧪 Testing productVariantsBulkUpdate mutation...');
    console.log('=' .repeat(50));
    
    try {
        // First, get a test variant
        const variantsQuery = `
            query getVariants($first: Int!) {
                productVariants(first: $first) {
                    edges {
                        node {
                            id
                            sku
                            price
                            compareAtPrice
                            product {
                                id
                                title
                            }
                        }
                    }
                }
            }
        `;

        const variantsResponse = await shopifyClient.post('/graphql.json', {
            query: variantsQuery,
            variables: { first: 1 }
        });

        if (variantsResponse.data.errors) {
            throw new Error(`GraphQL Errors: ${JSON.stringify(variantsResponse.data.errors)}`);
        }

        const variants = variantsResponse.data.data.productVariants.edges;
        if (variants.length === 0) {
            throw new Error('No variants found for testing');
        }

        const testVariant = variants[0].node;
        console.log(`📦 Found test variant: ${testVariant.sku} - ${testVariant.product.title}`);
        console.log(`   Current price: $${testVariant.price}`);
        console.log(`   Current compare-at: ${testVariant.compareAtPrice || 'null'}`);
        console.log('');

        // Test the bulk update mutation
        const updateMutation = `
            mutation testBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
                productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                    productVariants {
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

        const currentPrice = parseFloat(testVariant.price);
        const testPrice = currentPrice; // Keep the same price for testing
        
        const updateVariables = {
            productId: testVariant.product.id,
            variants: [{
                id: testVariant.id,
                price: testPrice.toString(),
                compareAtPrice: testVariant.compareAtPrice
            }]
        };

        console.log('🔄 Testing bulk update mutation...');
        const updateResponse = await shopifyClient.post('/graphql.json', {
            query: updateMutation,
            variables: updateVariables
        });

        if (updateResponse.data.errors) {
            console.log('❌ Mutation errors:', JSON.stringify(updateResponse.data.errors, null, 2));
            return;
        }

        const result = updateResponse.data.data.productVariantsBulkUpdate;
        
        if (result.userErrors && result.userErrors.length > 0) {
            console.log('❌ User errors:', JSON.stringify(result.userErrors, null, 2));
            return;
        }

        console.log('✅ Bulk update mutation successful!');
        console.log(`   Updated variants: ${result.productVariants.length}`);
        
        if (result.productVariants.length > 0) {
            const updatedVariant = result.productVariants[0];
            console.log(`   New price: $${updatedVariant.price}`);
            console.log(`   New compare-at: ${updatedVariant.compareAtPrice || 'null'}`);
        }

        console.log('');
        console.log('🎉 Test completed successfully!');
        console.log('   The productVariantsBulkUpdate mutation is working correctly.');
        
    } catch (error) {
        console.error('❌ Test failed:', error.message);
        if (error.response) {
            console.error('Response:', JSON.stringify(error.response.data, null, 2));
        }
    }
}

// Run the test
testBulkVariantUpdate().catch(console.error);
