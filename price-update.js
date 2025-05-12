const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    DATA_API_URL,
    DISCOUNT_CSV_PATH,
    SHOPIFY_API_VERSION,
} = require('./common/config');                                                // :contentReference[oaicite:3]{index=1}

const Logger = require('./common/logger');
const { cleanSku, fetchWithRetry, loadDiscounts } = require('./common/utils'); // Importar de utils

const SHOPIFY_GRAPHQL_URL =
    `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;


async function getLocalProducts() {
    Logger.log('🔎 Obteniendo productos locales (para precios)...');
    const responseData = await fetchWithRetry({ // Usa el fetchWithRetry de utils
        method: 'GET',
        url: DATA_API_URL,
        headers: { Accept: 'application/json' }
    });
    const products = responseData?.value || responseData || [];
    if (!Array.isArray(products)) {
        throw new Error(`Estructura de respuesta inválida desde DATA_API_URL: ${JSON.stringify(responseData)}`);
    }
    Logger.log(`✅ Obtenidos ${products.length} productos locales`);
    return products;
}

async function getAllShopifyVariantsForPricing() {
    Logger.log('🔎 Obteniendo variantes de Shopify (para precios)...');
    let variants = [];
    let hasNext = true, cursor = null;
    const MAX_PAGES = 500;
    let pageCount = 0;

    while (hasNext && pageCount < MAX_PAGES) {
        pageCount++;
        const query = `
          query GetVariantsForPricing($limit: Int!, $cursor: String) {
            productVariants(first: $limit, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              edges {
                node {
                  id
                  sku
                  price
                  compareAtPrice # Necesario para comparar
                  displayName # Para logs
                  product { title }
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
                'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
            },
            data: JSON.stringify({ query, variables }),
        }, true); // Indicar que use el rate limiter de Shopify

        if (responseData.errors) { /* ... manejo de error ... */ throw new Error(`Error GraphQL obteniendo variantes de precio: ${JSON.stringify(responseData.errors)}`); }
        const data = responseData?.data?.productVariants;
        if (!data) { Logger.warn(`No se encontró productVariants en la respuesta de la página ${pageCount}`); break; }

        data.edges.forEach(e => variants.push(e.node));
        hasNext = data.pageInfo.hasNextPage;
        cursor = data.pageInfo.endCursor;
        Logger.debug(`Obtenidas ${data.edges.length} variantes de precio en página ${pageCount}. Total: ${variants.length}. Siguiente: ${hasNext}`);
        if (hasNext) await new Promise(resolve => setTimeout(resolve, 250));
    }
    if (pageCount >= MAX_PAGES) Logger.warn(`Se alcanzó el límite máximo de páginas (${MAX_PAGES}) para variantes de precio.`);
    Logger.log(`✅ Obtenidas ${variants.length} variantes de Shopify (para precios)`);
    return variants;
}


async function updateShopifyPrice(variantId, newPrice, newCompareAtPrice) {
    Logger.log(`✨ Actualizando precio para variante ${variantId}: Precio=${newPrice}, CompararEn=${newCompareAtPrice}`);
    const mutation = `
        mutation ProductVariantUpdate($input: ProductVariantInput!) {
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
        }`;
    const variables = {
        input: {
            id: variantId,
            price: String(newPrice), // Asegurar que el precio es string
            compareAtPrice: newCompareAtPrice ? String(newCompareAtPrice) : null // Enviar null si no hay precio de comparación
        }
    };
    const responseData = await fetchWithRetry({ // Usa el fetchWithRetry de utils
        method: 'POST',
        url: SHOPIFY_GRAPHQL_URL,
        headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        },
        data: JSON.stringify({ query: mutation, variables }),
    }, true); // Indicar que use el rate limiter de Shopify

    const userErrors = responseData?.data?.productVariantUpdate?.userErrors;
    if (userErrors && userErrors.length > 0) {
        Logger.error(`❌ Error al actualizar precio para variante ${variantId}: ${JSON.stringify(userErrors)}`);
        return { success: false, errors: userErrors };
    }
    if (responseData?.data?.productVariantUpdate?.productVariant) {
        Logger.log(`✅ Precio actualizado para variante ${variantId}`);
        return { success: true };
    }
    Logger.warn(`Respuesta desconocida al actualizar precio para variante ${variantId}: ${JSON.stringify(responseData)}`);
    return { success: false, errors: [{ message: "Respuesta desconocida de Shopify" }] };
}


async function runPriceUpdate(sharedShopifyVariants = null, discountMap = null) {
    Logger.log('🚀 Iniciando actualización de precios...');
    let stats = { updated: 0, noChange: 0, notFound: 0, errors: 0, totalLocal: 0 };

    try {
        if (!discountMap) { // Cargar descuentos solo si no se pasaron
            discountMap = await loadDiscounts(DISCOUNT_CSV_PATH);
        }
        const localProducts = await getLocalProducts();
        stats.totalLocal = localProducts.length;

        const shopifyVariants = sharedShopifyVariants || await getAllShopifyVariantsForPricing();
        const shopifyVariantMap = new Map(shopifyVariants.map(v => [cleanSku(v.sku), v]));

        for (const localProd of localProducts) {
            const sku = cleanSku(localProd.CodigoProducto);
            if (!sku) {
                Logger.warn(`SKU local inválido o vacío para producto: ${JSON.stringify(localProd)}`);
                continue;
            }

            const shopifyVariant = shopifyVariantMap.get(sku);
            if (!shopifyVariant) {
                Logger.warn(`️SKU ${sku} (local) no encontrado en Shopify.`);
                stats.notFound++;
                continue;
            }

            const basePrice = parseFloat(localProd.Venta1);
            if (isNaN(basePrice)) {
                Logger.warn(`Precio base inválido para SKU ${sku} (local): '${localProd.Venta1}'. Saltando.`);
                stats.errors++;
                continue;
            }

            let finalPrice = basePrice;
            let compareAtPrice = null; // Por defecto, no hay precio de comparación

            if (discountMap.has(sku)) {
                const pct = discountMap.get(sku);
                if (pct > 0 && pct <= 100) { // Aplicar solo si el descuento es válido
                    compareAtPrice = basePrice.toFixed(2); // El precio original es el de comparación
                    finalPrice = parseFloat((basePrice * (1 - pct / 100)).toFixed(2));
                    Logger.log(`💸 SKU ${sku}: Descuento ${pct}% aplicado. Original: ${compareAtPrice}, Final: ${finalPrice.toFixed(2)}`);
                } else {
                    Logger.warn(`Descuento inválido para SKU ${sku}: ${pct}%. Usando precio base.`);
                    finalPrice = basePrice.toFixed(2); // Asegurar formato
                }
            } else {
                 finalPrice = basePrice.toFixed(2); // Asegurar formato
            }

            // Comparar con precios actuales de Shopify
            const currentShopifyPrice = shopifyVariant.price ? parseFloat(shopifyVariant.price).toFixed(2) : null;
            const currentShopifyCompareAtPrice = shopifyVariant.compareAtPrice ? parseFloat(shopifyVariant.compareAtPrice).toFixed(2) : null;

            if (finalPrice !== currentShopifyPrice || String(compareAtPrice) !== String(currentShopifyCompareAtPrice)) { // String(null) === "null"
                const updateResult = await updateShopifyPrice(shopifyVariant.id, finalPrice, compareAtPrice);
                if (updateResult.success) {
                    stats.updated++;
                } else {
                    stats.errors++;
                }
            } else {
                Logger.log(`ℹ️ SKU ${sku} (${shopifyVariant.displayName}) - Precio y Precio de Comparación ya correctos.`);
                stats.noChange++;
            }
        }
    } catch (error) {
        Logger.error('Error general en la actualización de precios', error);
        stats.errors++; // Contar como error general
    }

    Logger.log("\n📊 ===== RESUMEN ACTUALIZACIÓN DE PRECIOS =====");
    Logger.log(`Productos locales considerados: ${stats.totalLocal}`);
    Logger.log(`Precios/Comparación actualizados en Shopify: ${stats.updated}`);
    Logger.log(`Sin cambios necesarios: ${stats.noChange}`);
    Logger.log(`SKUs locales no encontrados en Shopify: ${stats.notFound}`);
    Logger.log(`Errores durante el proceso: ${stats.errors}`);
    Logger.log("=============================================\n");
}

// Si este script se ejecuta directamente:
if (require.main === module) {
    Logger.init(); // Asegurar que el logger se inicialice
    runPriceUpdate().catch(err => {
        Logger.error('Error fatal ejecutando price-update.js', err);
        process.exit(1);
    });
}

module.exports = { runPriceUpdate, getLocalProducts, getAllShopifyVariantsForPricing };