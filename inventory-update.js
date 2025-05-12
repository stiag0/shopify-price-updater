const axios = require('axios');
const { RateLimiter } = require('limiter');

const {
  SHOPIFY_SHOP_NAME,
  SHOPIFY_ACCESS_TOKEN,
  INVENTORY_API_URL,
  API_TIMEOUT,
  SHOPIFY_RATE_LIMIT,
  SHOPIFY_API_VERSION,
} = require('./config');                                                                  // :contentReference[oaicite:2]{index=2}:contentReference[oaicite:3]{index=3}

const Logger = require('./logger');
const { cleanSku } = require('./utils');

const SHOPIFY_GRAPHQL_URL = 
  `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

const shopifyLimiter = new RateLimiter({ tokensPerInterval: SHOPIFY_RATE_LIMIT, interval: 'second' });

async function fetchWithRetry(config, useLimiter = false, retries = 3) {
  if (useLimiter) await shopifyLimiter.removeTokens(1);
  try {
    return await axios({ timeout: API_TIMEOUT, ...config });
  } catch (err) {
    if (retries > 0) return fetchWithRetry(config, useLimiter, retries - 1);
    throw err;
  }
}

/** 1) Trae inventario local */
async function getLocalInventory() {
  Logger.log('🔎 Fetching local inventory...');
  const resp = await fetchWithRetry({
    method: 'GET',
    url: INVENTORY_API_URL,
    headers: { Accept: 'application/json' }
  });
  const data = resp.data?.value || resp.data || [];
  const map = {};
  for (const item of data) {
    const sku = cleanSku(item.CodigoProducto);
    if (!sku) continue;
    const initial = +item.CantidadInicial || 0;
    const inRec   = +item.CantidadEntradas || 0;
    const out     = +item.CantidadSalidas || 0;
    map[sku] = Math.max(0, initial + inRec - out);
  }
  Logger.log(`✅ Processed ${Object.keys(map).length} SKUs from local inventory`);
  return map;
}

/** 2) Obtiene Location ID activo */
async function getActiveLocationId() {
  Logger.log('🔎 Fetching active location ID...');
  const query = `
    query { locations(first:1, query:"status:active") {
      edges { node { id } }
    }}`;
  const resp = await fetchWithRetry({
    method: 'POST',
    url: SHOPIFY_GRAPHQL_URL,
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    },
    data: JSON.stringify({ query })
  }, true);
  return resp.data.data.locations.edges[0]?.node?.id || null;
}

/** 3) Obtiene variantes Shopify (solo inventoryItem.id) */
async function getAllShopifyVariants() {
  Logger.log('🔎 Fetching Shopify variants for inventory...');
  let all = [], hasNext = true, cursor = null;
  while (hasNext) {
    const q = `
      query ($cursor:String) {
        productVariants(first:100, after:$cursor) {
          pageInfo { hasNextPage endCursor }
          edges { node { sku inventoryItem { id } } }
        }
      }`;
    const vars = { cursor };
    const resp = await fetchWithRetry({
      method: 'POST',
      url: SHOPIFY_GRAPHQL_URL,
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
      },
      data: JSON.stringify({ query: q, variables: vars })
    }, true);

    const data = resp.data.data.productVariants;
    data.edges.forEach(e => all.push(e.node));
    hasNext = data.pageInfo.hasNextPage;
    cursor  = data.pageInfo.endCursor;
  }
  Logger.log(`✅ Fetched ${all.length} Shopify variants`);
  return all;
}

/** 4) Ejecuta el update */
async function runInventoryUpdate() {
  const localInv    = await getLocalInventory();
  const locationId  = await getActiveLocationId();
  if (!locationId) throw new Error('No active Location ID found');

  const shopifyVars = await getAllShopifyVariants();
  const shopMap = new Map(shopifyVars.map(v => [cleanSku(v.sku), v.inventoryItem.id]));

  for (const [sku, qty] of Object.entries(localInv)) {
    if (!shopMap.has(sku)) {
      Logger.warn(`⚠️ SKU ${sku} not in Shopify, skipping`);
      continue;
    }
    const inventoryItemId = shopMap.get(sku);
    Logger.log(`🔢 Updating SKU ${sku} → ${qty}`);
    const mutation = `
      mutation ($input:InventorySetOnHandQuantitiesInput!) {
        inventorySetOnHandQuantities(input:$input) {
          inventoryAdjustmentGroup { id }
          userErrors { field message }
        }
      }`;
    const variables = {
      input: {
        reason: 'correction',
        setQuantities: [{
          inventoryItemId,
          locationId,
          quantity: qty,
        }]
      }
    };
    await fetchWithRetry({
      method: 'POST',
      url: SHOPIFY_GRAPHQL_URL,
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
      },
      data: JSON.stringify({ query: mutation, variables })
    }, true);

    Logger.log(`✅ Inventory updated for SKU ${sku}`);
  }

  Logger.log('🏁 Inventory update complete');
}

runInventoryUpdate().catch(err => {
  Logger.error('Fatal error in inventory-update.js', err);
  process.exit(1);
});
