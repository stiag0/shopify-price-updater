const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    INVENTORY_API_URL,
    SHOPIFY_API_VERSION,
} = require('./common/config');                                                    // :contentReference[oaicite:0]{index=0}

const Logger = require('./common/logger');
const { cleanSku, fetchWithRetry } = require('./common/utils'); // Importar de utils

const SHOPIFY_GRAPHQL_URL =
    `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;


async function getLocalInventory() {
    // (Lógica sin cambios, pero usa fetchWithRetry de utils)
    Logger.log('🔎 Obteniendo inventario local...');
    const responseData = await fetchWithRetry({ // Usa el fetchWithRetry de utils
        method: 'GET',
        url: INVENTORY_API_URL,
        headers: { Accept: 'application/json' }
    });
    const data = responseData?.value || responseData || [];
    const map = {};
    for (const item of data) {
        const sku = cleanSku(item.CodigoProducto);
        if (!sku) continue;
        const initial = +item.CantidadInicial || 0;
        const inRec = +item.CantidadEntradas || 0;
        const out = +item.CantidadSalidas || 0;
        map[sku] = Math.max(0, initial + inRec - out);
    }
    Logger.log(`✅ Procesados ${Object.keys(map).length} SKUs del inventario local`);
    return map;
}

async function getActiveLocationId() {
    // (Lógica sin cambios, pero usa fetchWithRetry de utils)
    Logger.log('🔎 Obteniendo Location ID activo...');
    const query = `query { locations(first:1, query:"status:active") { edges { node { id name } } }}`; // Incluir nombre para logs
    const responseData = await fetchWithRetry({ // Usa el fetchWithRetry de utils
        method: 'POST',
        url: SHOPIFY_GRAPHQL_URL,
        headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
        },
        data: JSON.stringify({ query })
    }, true); // Indicar que use el rate limiter de Shopify

    if (responseData.errors) { /* ... manejo de error ... */ throw new Error(`Error GraphQL obteniendo Location ID: ${JSON.stringify(responseData.errors)}`); }
    const locationNode = responseData?.data?.locations?.edges?.[0]?.node;
    if (locationNode?.id) {
        Logger.log(`📍 Location ID activo encontrado: ${locationNode.name} (${locationNode.id})`);
        return locationNode.id;
    }
    Logger.error('❌ No se encontró Location ID activo.');
    return null;
}

async function getAllShopifyVariantsForInventory() {
    // (Lógica sin cambios, pero usa fetchWithRetry de utils y pide 'quantities')
    Logger.log('🔎 Obteniendo variantes de Shopify (para inventario)...');
    let variants = [], hasNext = true, cursor = null;
    const MAX_PAGES = 500;
    let pageCount = 0;

    while (hasNext && pageCount < MAX_PAGES) {
        pageCount++;
        const query = `
          query GetVariantsForInventory($limit: Int!, $cursor: String) {
            productVariants(first: $limit, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              edges {
                node {
                  sku
                  displayName # Para logs
                  inventoryItem {
                    id
                    tracked
                    inventoryLevels(first: 1) { # Siempre pedir, incluso para una sola ubicación
                        edges { node { location {id} quantities(names: "available") { name quantity } } }
                    }
                  }
                }
              }
            }
          }`;
        const variables = { limit: 100, cursor };
        const responseData = await fetchWithRetry({ // Usa el fetchWithRetry de utils
            method: 'POST',
            url: SHOPIFY_GRAPHQL_URL,
            headers: {
                'Content-Type': 'application/json',
                'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
            },
            data: JSON.stringify({ query, variables })
        }, true); // Indicar que use el rate limiter de Shopify

        if (responseData.errors) { /* ... manejo de error ... */ throw new Error(`Error GraphQL obteniendo variantes de inventario: ${JSON.stringify(responseData.errors)}`); }
        const data = responseData?.data?.productVariants;
        if (!data) { Logger.warn(`No se encontró productVariants en la respuesta de la página ${pageCount}`); break; }

        data.edges.forEach(e => variants.push(e.node));
        hasNext = data.pageInfo.hasNextPage;
        cursor = data.pageInfo.endCursor;
        Logger.debug(`Obtenidas ${data.edges.length} variantes de inventario en página ${pageCount}. Total: ${variants.length}. Siguiente: ${hasNext}`);
        if (hasNext) await new Promise(resolve => setTimeout(resolve, 250));
    }
    if (pageCount >= MAX_PAGES) Logger.warn(`Se alcanzó el límite máximo de páginas (${MAX_PAGES}) para variantes de inventario.`);
    Logger.log(`✅ Obtenidas ${variants.length} variantes de Shopify (para inventario)`);
    return variants;
}

async function updateShopifyInventory(inventoryItemId, locationId, quantity) {
    Logger.log(`🔢 Actualizando inventario para Item ${inventoryItemId} en Location ${locationId} → ${quantity}`);
    const mutation = `
      mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
        inventorySetOnHandQuantities(input: $input) {
          inventoryAdjustmentGroup { id }
          userErrors { field code message }
        }
      }`;
    const variables = {
        input: {
            reason: 'correction',
            setQuantities: [{ inventoryItemId, locationId, quantity }]
        }
    };
    const responseData = await fetchWithRetry({ // Usa el fetchWithRetry de utils
        method: 'POST',
        url: SHOPIFY_GRAPHQL_URL,
        headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
        },
        data: JSON.stringify({ query: mutation, variables })
    }, true); // Indicar que use el rate limiter de Shopify

    const userErrors = responseData?.data?.inventorySetOnHandQuantities?.userErrors;
    if (userErrors && userErrors.length > 0) {
        Logger.error(`❌ Error al actualizar inventario para Item ${inventoryItemId}: ${JSON.stringify(userErrors)}`);
        return { success: false, errors: userErrors };
    }
    if (responseData?.data?.inventorySetOnHandQuantities?.inventoryAdjustmentGroup) {
        Logger.log(`✅ Inventario actualizado para Item ${inventoryItemId}`);
        return { success: true };
    }
    Logger.warn(`Respuesta desconocida al actualizar inventario para Item ${inventoryItemId}: ${JSON.stringify(responseData)}`);
    return { success: false, errors: [{ message: "Respuesta desconocida de Shopify" }] };
}

async function runInventoryUpdate(sharedShopifyVariants = null, locationId = null) {
    Logger.log('🚀 Iniciando actualización de inventario...');
    let stats = { updated: 0, noChange: 0, notFoundInShopify: 0, notTracked:0, errors: 0, totalLocal: 0 };

    try {
        const localInventoryMap = await getLocalInventory();
        stats.totalLocal = Object.keys(localInventoryMap).length;

        if (!locationId) { // Obtener solo si no se pasó como argumento
            locationId = await getActiveLocationId();
        }
        if (!locationId) {
            throw new Error('No se pudo obtener un Location ID activo para la actualización de inventario.');
        }

        const shopifyVariants = sharedShopifyVariants || await getAllShopifyVariantsForInventory();
        const shopifyVariantMap = new Map();
        shopifyVariants.forEach(v => {
            const sku = cleanSku(v.sku);
            if (sku && v.inventoryItem) { // Asegurar que inventoryItem existe
                shopifyVariantMap.set(sku, v);
            }
        });

        for (const [sku, localQuantity] of Object.entries(localInventoryMap)) {
            const shopifyVariant = shopifyVariantMap.get(sku);
            if (!shopifyVariant) {
                Logger.warn(`️SKU ${sku} (local) no encontrado en Shopify para inventario.`);
                stats.notFoundInShopify++;
                continue;
            }
            if (!shopifyVariant.inventoryItem.tracked) {
                Logger.warn(`️SKU ${sku} (${shopifyVariant.displayName}) no tiene seguimiento de inventario en Shopify. Saltando.`);
                stats.notTracked++;
                continue;
            }

            // Obtener inventario actual de Shopify
            let currentShopifyQuantity = null;
            const invLevelNode = shopifyVariant.inventoryItem.inventoryLevels?.edges?.[0]?.node;
            if (invLevelNode?.quantities?.length > 0) {
                 const availableObj = invLevelNode.quantities.find(q => q.name === "available");
                 if (availableObj?.quantity !== undefined) {
                    currentShopifyQuantity = Number(availableObj.quantity);
                 }
            }
            
            if (currentShopifyQuantity === null) {
                Logger.warn(`No se pudo determinar la cantidad actual para SKU ${sku} (${shopifyVariant.displayName}). Saltando comparación.`);
                // Podrías decidir actualizar de todas formas o manejarlo como error.
                // Por ahora, actualizaremos si localQuantity es un número.
            }

            const newQuantity = Math.floor(localQuantity); // Asegurar que es entero

            if (currentShopifyQuantity === null || newQuantity !== currentShopifyQuantity) {
                const updateResult = await updateShopifyInventory(shopifyVariant.inventoryItem.id, locationId, newQuantity);
                if (updateResult.success) {
                    stats.updated++;
                } else {
                    stats.errors++;
                }
            } else {
                Logger.log(`ℹ️ SKU ${sku} (${shopifyVariant.displayName}) - Inventario ya correcto (${newQuantity}).`);
                stats.noChange++;
            }
        }
    } catch (error) {
        Logger.error('Error general en la actualización de inventario', error);
        stats.errors++;
    }

    Logger.log("\n📦 ===== RESUMEN ACTUALIZACIÓN DE INVENTARIO =====");
    Logger.log(`SKUs en inventario local: ${stats.totalLocal}`);
    Logger.log(`Inventarios actualizados en Shopify: ${stats.updated}`);
    Logger.log(`Sin cambios necesarios: ${stats.noChange}`);
    Logger.log(`SKUs locales no encontrados en Shopify: ${stats.notFoundInShopify}`);
    Logger.log(`SKUs no rastreados en Shopify: ${stats.notTracked}`);
    Logger.log(`Errores durante el proceso: ${stats.errors}`);
    Logger.log("================================================\n");
}

// Si este script se ejecuta directamente:
if (require.main === module) {
    Logger.init(); // Asegurar que el logger se inicialice
    runInventoryUpdate().catch(err => {
        Logger.error('Error fatal ejecutando inventory-update.js', err);
        process.exit(1);
    });
}

module.exports = { runInventoryUpdate, getLocalInventory, getActiveLocationId, getAllShopifyVariantsForInventory };