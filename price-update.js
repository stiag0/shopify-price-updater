const axios = require('axios');
const csv   = require('csv-parser');
const fs    = require('fs');
const path  = require('path');

const {
  SHOPIFY_SHOP_NAME,
  SHOPIFY_ACCESS_TOKEN,
  DATA_API_URL,
  DISCOUNT_CSV_PATH,
  API_TIMEOUT,
  SHOPIFY_RATE_LIMIT,
  SHOPIFY_API_VERSION,
} = require('./config');                                                                  // :contentReference[oaicite:0]{index=0}:contentReference[oaicite:1]{index=1}

const Logger    = require('./logger');
const { cleanSku } = require('./utils');

const SHOPIFY_GRAPHQL_URL = 
  `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

/** Helper: Axios con reintentos simple */
async function fetchWithRetry(config, retries = 3) {
  try {
    return await axios({ timeout: API_TIMEOUT, ...config });
  } catch (err) {
    if (retries > 0) {
      Logger.warn(`Fetch failed (${retries} left), retrying: ${config.url}`);
      return fetchWithRetry(config, retries - 1);
    }
    throw err;
  }
}

/** 1) Carga descuentos desde CSV o URL */
async function loadDiscounts(csvPath) {
    let inputStream;
    if (/^https?:\/\//i.test(csvPath)) {
      const resp = await axios.get(csvPath, { responseType: 'stream' });
      inputStream = resp.data;
    } else {
      inputStream = fs.createReadStream(csvPath);
    }
    return new Promise((resolve, reject) => {
      const discounts = new Map();
      inputStream
        .pipe(csv({ headers: ['sku','discount'] }))
        .on('data', row => { /* … */ })
        .on('end', () => resolve(discounts))
        .on('error', reject);
    });
  }
  

/** 2) Obtiene productos locales de tu API */
async function getLocalProducts() {
  Logger.log('🔎 Fetching local products...');
  const resp = await fetchWithRetry({
    method: 'GET',
    url: DATA_API_URL,
    headers: { Accept: 'application/json' }
  });
  const products = resp.data?.value || resp.data || [];
  if (!Array.isArray(products)) {
    throw new Error(`Invalid response structure from DATA_API_URL: ${JSON.stringify(resp.data)}`);
  }
  Logger.log(`✅ Fetched ${products.length} local products`);
  return products;
}

/** 3) Obtiene variantes de Shopify via GraphQL */
async function getAllShopifyVariants() {
  Logger.log('🔎 Fetching Shopify variants...');
  let variants = [];
  let hasNext = true, cursor = null;

  while (hasNext) {
    const query = `
      query ($limit: Int!, $cursor: String) {
        productVariants(first: $limit, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          edges {
            node { id sku price }
          }
        }
      }`;
    const variables = { limit: 100, cursor };
    const resp = await fetchWithRetry({
      method: 'POST',
      url: SHOPIFY_GRAPHQL_URL,
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
      },
      data: JSON.stringify({ query, variables }),
    });

    const data = resp.data?.data?.productVariants;
    if (!data) break;
    data.edges.forEach(e => variants.push(e.node));
    hasNext = data.pageInfo.hasNextPage;
    cursor  = data.pageInfo.endCursor;
  }

  Logger.log(`✅ Fetched ${variants.length} Shopify variants`);
  return variants;
}

/** 4) Update de precios */
async function runPriceUpdate() {
  const discounts = await loadDiscounts(DISCOUNT_CSV_PATH);
  const localProds = await getLocalProducts();
  const shopifyVars = await getAllShopifyVariants();

  const localMap = new Map(
    localProds.map(p => [cleanSku(p.CodigoProducto), parseFloat(p.Venta1)])
  );

  for (const v of shopifyVars) {
    const sku = cleanSku(v.sku);
    if (!sku || !localMap.has(sku)) continue;

    const base = localMap.get(sku);
    let finalPrice = base;
    if (discounts.has(sku)) {
      const pct = discounts.get(sku);
      finalPrice = +(base * (1 - pct / 100)).toFixed(2);
      Logger.log(`💸 SKU ${sku}: ${base} → ${finalPrice} (-${pct}%)`);
    }

    if (String(v.price) !== String(finalPrice)) {
      const mutation = `
        mutation ($input: ProductVariantInput!) {
          productVariantUpdate(input: $input) {
            productVariant { id price }
            userErrors { field message }
          }
        }`;
      const variables = { input: { id: v.id, price: String(finalPrice) } };
      await fetchWithRetry({
        method: 'POST',
        url: SHOPIFY_GRAPHQL_URL,
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        },
        data: JSON.stringify({ query: mutation, variables }),
      });
      Logger.log(`✅ Price updated for SKU ${sku}`);
    } else {
      Logger.log(`ℹ️ SKU ${sku} price already up-to-date`);
    }
  }

  Logger.log('🏁 Price update complete');
}

runPriceUpdate().catch(err => {
  Logger.error('Fatal error in price-update.js', err);
  process.exit(1);
});
