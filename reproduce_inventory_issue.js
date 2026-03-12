
require('dotenv').config();
const axios = require('axios');
const { RateLimiter } = require('limiter');

// --- Environment Variables ---
const {
    INVENTORY_API_URL,
    SAFETY_STOCK = '5'
} = process.env;

if (!INVENTORY_API_URL) {
    console.error('Error: INVENTORY_API_URL is missing in .env');
    process.exit(1);
}

const SAFETY_STOCK_UNITS = parseInt(SAFETY_STOCK, 10);

// --- Helper Functions ---
function cleanSku(sku) {
    if (!sku) return null;
    const cleaned = String(sku).trim();
    const normalized = cleaned.replace(/[^a-zA-Z0-9-_]/g, '');
    return normalized || null;
}

function normalizeSkuForMatching(sku) {
    const cleaned = cleanSku(sku);
    if (!cleaned) return { isValid: false };

    if (/^\d+$/.test(cleaned)) {
        const unpadded = cleaned.replace(/^0+/, '');
        const padded = unpadded.padStart(5, '0');
        return { isValid: true, cleaned: unpadded, padded };
    }
    return { isValid: true, cleaned: cleaned, padded: cleaned };
}

async function getLocalInventory() {
    console.log(`Fetching inventory from ${INVENTORY_API_URL}...`);
    try {
        const response = await axios.get(INVENTORY_API_URL, { timeout: 60000 });
        const inventoryData = response.data?.value || [];

        console.log(`Received ${inventoryData.length} records.`);
        if (inventoryData.length > 0) {
            console.log('DEBUG: First record keys:', Object.keys(inventoryData[0]));
            console.log('DEBUG: First record sample:', JSON.stringify(inventoryData[0]));
        }

        // Target SKUs to debug
        const targetSkus = ['1154', '001154', '9864', '009864', '9865', '9863'];

        // Store all records for target SKUs
        const debugRecords = [];

        // 1. Analyze Raw Data for Targets
        inventoryData.forEach(item => {
            if (!item.CodigoProducto) return;
            const normalized = normalizeSkuForMatching(item.CodigoProducto);
            if (!normalized.isValid) return;

            if (targetSkus.includes(normalized.cleaned) || item.CodigoProducto.includes('1154')) {
                debugRecords.push({
                    rawSku: item.CodigoProducto,
                    normalized: normalized.cleaned,
                    timestamp: item.Fecha || item.FechaCreacion || 'No Date',
                    initial: parseFloat(item.CantidadInicial || 0),
                    in: parseFloat(item.CantidadEntradas || 0),
                    out: parseFloat(item.CantidadSalidas || 0),
                    calculated: parseFloat(item.CantidadInicial || 0) + parseFloat(item.CantidadEntradas || 0) - parseFloat(item.CantidadSalidas || 0)
                });
            }
        });

        console.log('\n--- RAW RECORDS FOR TARGET SKUS ---');
        debugRecords.forEach(r => {
            console.log(`SKU: ${r.rawSku} | Date: ${r.timestamp} | Init: ${r.initial} + In: ${r.in} - Out: ${r.out} = ${r.calculated}`);
        });

        // 2. Simulate OLD Logic (Latest Record Only)
        console.log('\n--- OLD LOGIC (Latest Record Only) ---');
        const latestRecords = new Map();
        inventoryData.forEach(item => {
            if (!item.CodigoProducto) return;
            const normalized = normalizeSkuForMatching(item.CodigoProducto);
            if (!normalized.isValid) return;

            let timestamp = new Date(item.Fecha || new Date());
            const skuKey = normalized.cleaned;

            if (!latestRecords.has(skuKey) || timestamp > latestRecords.get(skuKey).timestamp) {
                latestRecords.set(skuKey, { item, timestamp });
            }
        });

        targetSkus.forEach(sku => {
            if (latestRecords.has(sku)) {
                const record = latestRecords.get(sku);
                const item = record.item;
                const calc = parseFloat(item.CantidadInicial || 0) + parseFloat(item.CantidadEntradas || 0) - parseFloat(item.CantidadSalidas || 0);
                const final = Math.max(0, calc);
                console.log(`SKU ${sku}: Calculated ${final} (from single record date ${record.timestamp.toISOString()})`);
            } else {
                console.log(`SKU ${sku}: Not found in OLD logic`);
            }
        });

        // 3. Simulate NEW Logic (Sum of Latest Timestamp)
        console.log('\n--- NEW LOGIC (Sum of Latest Timestamp) ---');

        // First pass: find max timestamp per SKU
        const maxTimestamps = new Map();
        inventoryData.forEach(item => {
            if (!item.CodigoProducto) return;
            const normalized = normalizeSkuForMatching(item.CodigoProducto);
            if (!normalized.isValid) return;

            let timestamp = new Date(item.Fecha || new Date());
            const skuKey = normalized.cleaned;

            if (!maxTimestamps.has(skuKey) || timestamp > maxTimestamps.get(skuKey)) {
                maxTimestamps.set(skuKey, timestamp);
            }
        });

        // Second pass: sum records matching max timestamp
        const finalInventory = new Map();
        inventoryData.forEach(item => {
            if (!item.CodigoProducto) return;
            const normalized = normalizeSkuForMatching(item.CodigoProducto);
            if (!normalized.isValid) return;

            const skuKey = normalized.cleaned;
            const maxTs = maxTimestamps.get(skuKey);
            const currentTs = new Date(item.Fecha || new Date());

            // Check if timestamps match (using getTime to compare values)
            if (maxTs && currentTs.getTime() === maxTs.getTime()) {
                const currentVal = finalInventory.get(skuKey) || 0;
                const calc = parseFloat(item.CantidadInicial || 0) + parseFloat(item.CantidadEntradas || 0) - parseFloat(item.CantidadSalidas || 0);
                finalInventory.set(skuKey, currentVal + calc);
            }
        });

        targetSkus.forEach(sku => {
            if (finalInventory.has(sku)) {
                const rawTotal = finalInventory.get(sku);
                const final = Math.max(0, rawTotal);
                console.log(`SKU ${sku}: Calculated ${final} (Sum of records for latest date)`);
            } else {
                console.log(`SKU ${sku}: Not found in NEW logic`);
            }
        });

    } catch (error) {
        console.error('Error:', error.message);
    }
}

getLocalInventory();
