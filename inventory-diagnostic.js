/**
 * Inventory Diagnostic Script
 * This script will help debug inventory calculation issues
 */

require('dotenv').config();
const axios = require('axios');

// Environment variables
const {
    INVENTORY_API_URL,
    SAFETY_STOCK = '3'
} = process.env;

const SAFETY_STOCK_UNITS = parseInt(SAFETY_STOCK, 10);

if (!INVENTORY_API_URL) {
    console.error('Error: Missing INVENTORY_API_URL in .env file');
    process.exit(1);
}

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

async function checkInventoryForSku(targetSku) {
    try {
        console.log(`🔍 Checking inventory calculation for SKU: ${targetSku}`);
        console.log(`Safety Stock Units: ${SAFETY_STOCK_UNITS}`);
        console.log('=' .repeat(60));
        
        // Fetch inventory data
        const response = await axios.get(INVENTORY_API_URL);
        const inventoryData = response.data.value;
        
        // Find all records for the target SKU
        const skuRecords = [];
        
        for (const item of inventoryData) {
            if (!item.CodigoProducto) continue;
            
            const normalized = normalizeSkuForMatching(item.CodigoProducto);
            if (!normalized.isValid) continue;
            
            if (normalized.cleaned === targetSku || normalized.padded === targetSku || item.CodigoProducto === targetSku) {
                skuRecords.push(item);
            }
        }
        
        if (skuRecords.length === 0) {
            console.log(`❌ No inventory records found for SKU: ${targetSku}`);
            return;
        }
        
        console.log(`📦 Found ${skuRecords.length} inventory records for SKU ${targetSku}:`);
        
        // Find the latest record (same logic as main script)
        let latestRecord = null;
        let latestTimestamp = null;
        
        for (const item of skuRecords) {
            let timestamp = null;
            
            if (item.Fecha) {
                timestamp = new Date(item.Fecha);
                if (isNaN(timestamp.getTime())) {
                    console.log(`⚠️ Invalid date format: ${item.Fecha}`);
                    timestamp = new Date(); // Fallback
                }
            } else {
                timestamp = new Date(); // Fallback
            }
            
            if (!latestRecord || timestamp > latestTimestamp) {
                latestRecord = item;
                latestTimestamp = timestamp;
            }
        }
        
        console.log(`\n📅 Latest record (${latestTimestamp.toISOString()}):`);
        console.log(`   Raw SKU: ${latestRecord.CodigoProducto}`);
        console.log(`   Initial: ${latestRecord.CantidadInicial || 0}`);
        console.log(`   Received: ${latestRecord.CantidadEntradas || 0}`);
        console.log(`   Shipped: ${latestRecord.CantidadSalidas || 0}`);
        
        // Calculate inventory (same logic as main script)
        const initial = parseFloat(latestRecord.CantidadInicial || 0);
        const received = parseFloat(latestRecord.CantidadEntradas || 0);
        const shipped = parseFloat(latestRecord.CantidadSalidas || 0);
        
        if (isNaN(initial) || isNaN(received) || isNaN(shipped)) {
            console.log(`❌ Invalid inventory values`);
            return;
        }
        
        const calculatedQuantity = Math.max(0, initial + received - shipped);
        
        // Safety stock logic (same as main script)
        let shopifyQuantity;
        if (calculatedQuantity <= SAFETY_STOCK_UNITS) {
            shopifyQuantity = 0; // Don't sell online, keep all units for physical store
        } else {
            shopifyQuantity = Math.floor(calculatedQuantity); // Sell full amount online (enough for store)
        }
        
        console.log(`\n🧮 CALCULATION RESULTS:`);
        console.log(`   Formula: max(0, ${initial} + ${received} - ${shipped})`);
        console.log(`   Calculated Quantity: ${calculatedQuantity}`);
        console.log(`   Safety Stock Threshold: ${SAFETY_STOCK_UNITS}`);
        console.log(`   Shopify Quantity: ${shopifyQuantity}`);
        
        if (calculatedQuantity <= SAFETY_STOCK_UNITS) {
            console.log(`   📍 LOGIC: Quantity (${calculatedQuantity}) ≤ Safety Stock (${SAFETY_STOCK_UNITS}) → Set Shopify to 0`);
        } else {
            console.log(`   📍 LOGIC: Quantity (${calculatedQuantity}) > Safety Stock (${SAFETY_STOCK_UNITS}) → Set Shopify to ${shopifyQuantity}`);
        }
        
        // Show all records for debugging
        console.log(`\n📋 ALL RECORDS FOR SKU ${targetSku}:`);
        skuRecords.forEach((record, index) => {
            const recordTimestamp = record.Fecha ? new Date(record.Fecha) : new Date();
            const isLatest = record === latestRecord;
            console.log(`   ${index + 1}. ${isLatest ? '👑 [LATEST]' : '  '} Date: ${recordTimestamp.toISOString()}`);
            console.log(`      Initial: ${record.CantidadInicial || 0}, Received: ${record.CantidadEntradas || 0}, Shipped: ${record.CantidadSalidas || 0}`);
            const calc = Math.max(0, parseFloat(record.CantidadInicial || 0) + parseFloat(record.CantidadEntradas || 0) - parseFloat(record.CantidadSalidas || 0));
            console.log(`      Calculated: ${calc}`);
        });
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }
}

// Check specific SKU
const targetSku = process.argv[2] || '8033';
checkInventoryForSku(targetSku).then(() => {
    console.log('\n✅ Diagnostic completed!');
}).catch(error => {
    console.error('❌ Diagnostic failed:', error.message);
});
