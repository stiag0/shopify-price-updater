/**
 * Test Shopify GraphQL Mutations
 * This script will test what mutations are available in the current API version
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

async function testIntrospection() {
    console.log('🔍 Testing Shopify GraphQL API...');
    console.log(`API Version: ${SHOPIFY_API_VERSION}`);
    console.log('=' .repeat(50));
    
    // Test introspection query to see available mutations
    const introspectionQuery = `
        query IntrospectionQuery {
            __schema {
                mutationType {
                    name
                    fields {
                        name
                        description
                        args {
                            name
                            type {
                                name
                            }
                        }
                    }
                }
            }
        }
    `;
    
    try {
        const response = await shopifyClient.post('/graphql.json', {
            query: introspectionQuery
        });
        
        if (response.data.errors) {
            console.log('❌ GraphQL Errors:', JSON.stringify(response.data.errors, null, 2));
            return;
        }
        
        const mutations = response.data.data.__schema.mutationType.fields;
        
        // Look for variant-related mutations
        console.log('🔍 Looking for variant-related mutations...');
        const variantMutations = mutations.filter(m => 
            m.name.toLowerCase().includes('variant') || 
            m.name.toLowerCase().includes('product')
        );
        
        console.log(`\nFound ${variantMutations.length} variant/product mutations:`);
        variantMutations.forEach(mutation => {
            console.log(`  • ${mutation.name}`);
            if (mutation.description) {
                console.log(`    ${mutation.description}`);
            }
        });
        
        // Check if productVariantUpdate exists
        const productVariantUpdate = mutations.find(m => m.name === 'productVariantUpdate');
        if (productVariantUpdate) {
            console.log('\n✅ productVariantUpdate mutation exists!');
            console.log('Arguments:');
            productVariantUpdate.args.forEach(arg => {
                console.log(`  • ${arg.name}: ${arg.type.name || 'Complex Type'}`);
            });
        } else {
            console.log('\n❌ productVariantUpdate mutation NOT found!');
            console.log('\nAlternative mutations that might work:');
            const alternatives = mutations.filter(m => 
                m.name.includes('variant') || 
                m.name.includes('Variant')
            );
            alternatives.forEach(alt => {
                console.log(`  • ${alt.name}`);
            });
        }
        
    } catch (error) {
        console.error('❌ Error testing mutations:', error.message);
        if (error.response?.data) {
            console.error('Response data:', JSON.stringify(error.response.data, null, 2));
        }
    }
}

async function testSimpleVariantUpdate() {
    console.log('\n🧪 Testing simple variant update mutation...');
    
    // First, get a variant to test with
    const getVariantQuery = `
        query getFirstVariant {
            productVariants(first: 1) {
                edges {
                    node {
                        id
                        price
                        sku
                        product {
                            title
                        }
                    }
                }
            }
        }
    `;
    
    try {
        const variantResponse = await shopifyClient.post('/graphql.json', {
            query: getVariantQuery
        });
        
        if (variantResponse.data.errors) {
            console.log('❌ Error getting variant:', JSON.stringify(variantResponse.data.errors, null, 2));
            return;
        }
        
        const variant = variantResponse.data.data.productVariants.edges[0]?.node;
        if (!variant) {
            console.log('❌ No variants found');
            return;
        }
        
        console.log(`Found test variant: ${variant.sku} - ${variant.product.title}`);
        console.log(`Current price: $${variant.price}`);
        
        // Test the mutation (without actually changing anything)
        const testMutation = `
            mutation testVariantUpdate($input: ProductVariantInput!) {
                productVariantUpdate(input: $input) {
                    productVariant {
                        id
                        price
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
                price: variant.price // Same price, no actual change
            }
        };
        
        const updateResponse = await shopifyClient.post('/graphql.json', {
            query: testMutation,
            variables
        });
        
        if (updateResponse.data.errors) {
            console.log('❌ Mutation errors:', JSON.stringify(updateResponse.data.errors, null, 2));
        } else if (updateResponse.data.data.productVariantUpdate.userErrors.length > 0) {
            console.log('❌ User errors:', JSON.stringify(updateResponse.data.data.productVariantUpdate.userErrors, null, 2));
        } else {
            console.log('✅ Mutation test successful!');
        }
        
    } catch (error) {
        console.error('❌ Error testing variant update:', error.message);
        if (error.response?.data) {
            console.error('Response data:', JSON.stringify(error.response.data, null, 2));
        }
    }
}

// Run tests
testIntrospection().then(() => {
    return testSimpleVariantUpdate();
}).then(() => {
    console.log('\n✅ Testing completed!');
}).catch(error => {
    console.error('❌ Test failed:', error.message);
});
