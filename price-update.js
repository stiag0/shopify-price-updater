const {
    SHOPIFY_SHOP_NAME,
    SHOPIFY_ACCESS_TOKEN,
    DATA_API_URL,
    DISCOUNT_CSV_PATH, // Aunque no se usa directamente en este archivo si se pasa discountMap
    SHOPIFY_API_VERSION,
} = require('./common/config');

const Logger = require('./common/logger');
const { cleanSku, fetchWithRetry, loadDiscounts } = require('./common/utils');

const SHOPIFY_GRAPHQL_URL =
    `https://${SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

/**
 * Obtiene los productos desde la API local.
 * @returns {Promise<Array<Object>>} Una promesa que resuelve a un array de productos locales.
 */
async function getLocalProducts() {
    Logger.log('🔎 Obteniendo productos locales (para precios)...');
    const responseData = await fetchWithRetry({
        method: 'GET',
        url: DATA_API_URL,
        headers: { Accept: 'application/json' }
    });
    // Asegurar que la respuesta es un array, incluso si está anidada en 'value'
    const products = responseData?.value || responseData || [];
    if (!Array.isArray(products)) {
        Logger.error(`Estructura de respuesta inválida desde DATA_API_URL: ${JSON.stringify(responseData)}`);
        throw new Error(`Estructura de respuesta inválida desde DATA_API_URL.`);
    }
    Logger.log(`✅ Obtenidos ${products.length} productos locales`);
    return products;
}

/**
 * Obtiene todas las variantes de producto de Shopify con campos relevantes para precios.
 * @returns {Promise<Array<Object>>} Una promesa que resuelve a un array de variantes de Shopify.
 */
async function getAllShopifyVariantsForPricing() {
    Logger.log('🔎 Obteniendo variantes de Shopify (para precios)...');
    let variants = [];
    let hasNext = true, cursor = null;
    const MAX_PAGES = 500; // Límite para evitar bucles infinitos
    let pageCount = 0;

    while (hasNext && pageCount < MAX_PAGES) {
        pageCount++;
        const query = `
          query GetVariantsForPricing($limit: Int!, $cursor: String) {
            productVariants(first: $limit, after: $cursor, query:"status:active") { # Agregado query de estado activo
              pageInfo { hasNextPage endCursor }
              edges {
                node {
                  id
                  sku
                  price
                  compareAtPrice
                  displayName # Para logs
                  product { title } # Para logs
                }
              }
            }
          }`;
        const variables = { limit: 100, cursor }; // Límite razonable por página
        const responseData = await fetchWithRetry({
            method: 'POST',
            url: SHOPIFY_GRAPHQL_URL,
            headers: {
                'Content-Type': 'application/json',
                'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
            },
            data: JSON.stringify({ query, variables }),
        }, true); // Usar el limitador de tasa de Shopify

        if (responseData.errors) {
            Logger.error(`Error GraphQL obteniendo variantes de precio (página ${pageCount}): ${JSON.stringify(responseData.errors)}`);
            // Decidir si continuar o lanzar error. Por ahora, lanzamos para detener el proceso.
            throw new Error(`Error GraphQL obteniendo variantes de precio: ${JSON.stringify(responseData.errors)}`);
        }
        const data = responseData?.data?.productVariants;
        if (!data) {
            Logger.warn(`No se encontró productVariants en la respuesta de la página ${pageCount}. Respuesta: ${JSON.stringify(responseData)}`);
            break; // Salir del bucle si no hay datos
        }

        data.edges.forEach(e => variants.push(e.node));
        hasNext = data.pageInfo.hasNextPage;
        cursor = data.pageInfo.endCursor;
        Logger.debug(`Obtenidas ${data.edges.length} variantes de precio en página ${pageCount}. Total: ${variants.length}. Siguiente: ${hasNext}`);
        if (hasNext) await new Promise(resolve => setTimeout(resolve, 250)); // Pequeña pausa entre páginas
    }
    if (pageCount >= MAX_PAGES) Logger.warn(`Se alcanzó el límite máximo de páginas (${MAX_PAGES}) para variantes de precio.`);
    Logger.log(`✅ Obtenidas ${variants.length} variantes de Shopify (para precios)`);
    return variants;
}

/**
 * Actualiza el precio y el precio de comparación de una variante de producto en Shopify.
 * @param {string} variantId El GID de la variante de producto.
 * @param {string|number} newPrice El nuevo precio.
 * @param {string|number|null} newCompareAtPrice El nuevo precio de comparación (o null para quitarlo).
 * @returns {Promise<Object>} Un objeto indicando éxito o fracaso.
 */
async function updateShopifyPrice(variantId, newPrice, newCompareAtPrice) {
    Logger.log(`✨ Actualizando precio para variante ${variantId}: Precio=${newPrice}, CompararEn=${newCompareAtPrice}`);

    // Mutación GraphQL simplificada (anónima)
    const mutation = `
        mutation ($input: ProductVariantInput!) {
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
            price: String(newPrice), // Shopify espera precios como strings (Money)
            compareAtPrice: newCompareAtPrice ? String(newCompareAtPrice) : null
        }
    };

    const responseData = await fetchWithRetry({
        method: 'POST',
        url: SHOPIFY_GRAPHQL_URL,
        headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        },
        data: JSON.stringify({ query: mutation, variables }),
    }, true); // Usar el limitador de tasa de Shopify

    const userErrors = responseData?.data?.productVariantUpdate?.userErrors;
    if (userErrors && userErrors.length > 0) {
        Logger.error(`❌ Error al actualizar precio para variante ${variantId}: ${JSON.stringify(userErrors)}`);
        return { success: false, errors: userErrors };
    }
    // Verificar también errores a nivel raíz de la respuesta GraphQL
    if (responseData.errors) {
        Logger.error(`❌ Error GraphQL raíz al actualizar precio para variante ${variantId}: ${JSON.stringify(responseData.errors)}`);
        return { success: false, errors: responseData.errors };
    }

    if (responseData?.data?.productVariantUpdate?.productVariant) {
        Logger.log(`✅ Precio actualizado para variante ${variantId}`);
        return { success: true };
    }

    Logger.warn(`Respuesta desconocida o sin datos de éxito al actualizar precio para variante ${variantId}: ${JSON.stringify(responseData)}`);
    return { success: false, errors: [{ message: "Respuesta desconocida o sin datos de éxito de Shopify" }] };
}

/**
 * Ejecuta el proceso completo de actualización de precios.
 * @param {Array<Object>|null} sharedShopifyVariants Variantes de Shopify ya cargadas (opcional).
 * @param {Map<string, number>|null} discountMap Mapa de descuentos ya cargado (opcional).
 */
async function runPriceUpdate(sharedShopifyVariants = null, discountMap = null) {
    Logger.log('🚀 Iniciando actualización de precios...');
    let stats = { updated: 0, noChange: 0, notFound: 0, errors: 0, totalLocal: 0, localInvalidPrice: 0 };

    try {
        // Cargar descuentos solo si no se pasaron como argumento
        if (!discountMap) {
            Logger.debug("El mapa de descuentos no fue proporcionado, cargando desde CSV...");
            // Asegúrate que DISCOUNT_CSV_PATH esté disponible en config si se llama así.
            // La función loadDiscounts ya maneja si la ruta es una URL o local.
            discountMap = await loadDiscounts(DISCOUNT_CSV_PATH);
        }

        const localProducts = await getLocalProducts();
        stats.totalLocal = localProducts.length;

        // Cargar variantes de Shopify solo si no se pasaron como argumento
        const shopifyVariants = sharedShopifyVariants || await getAllShopifyVariantsForPricing();
        // Crear un mapa de SKU -> Variante para búsqueda rápida
        const shopifyVariantMap = new Map();
        shopifyVariants.forEach(v => {
            const sku = cleanSku(v.sku);
            if (sku) { // Solo añadir si el SKU es válido
                if (shopifyVariantMap.has(sku)) {
                    Logger.warn(`SKU duplicado en Shopify: ${sku}. Se usará la primera variante encontrada con este SKU.`);
                } else {
                    shopifyVariantMap.set(sku, v);
                }
            }
        });

        for (const localProd of localProducts) {
            const sku = cleanSku(localProd.CodigoProducto); // Asume que el campo SKU local es 'CodigoProducto'
            if (!sku) {
                Logger.warn(`SKU local inválido o vacío para producto: ${JSON.stringify(localProd)}. Saltando.`);
                continue;
            }

            const shopifyVariant = shopifyVariantMap.get(sku);
            if (!shopifyVariant) {
                // Logger.warn(`️SKU ${sku} (local) no encontrado en Shopify. Producto local: ${localProd.NombreProducto || JSON.stringify(localProd)}`); // Log más detallado
                stats.notFound++;
                continue;
            }

            // Asume que el campo de precio local es 'Venta1'
            const basePrice = parseFloat(localProd.Venta1);
            if (isNaN(basePrice) || basePrice < 0) { // Validar que el precio sea un número positivo
                Logger.warn(`Precio base inválido para SKU ${sku} (local): '${localProd.Venta1}'. Saltando.`);
                stats.localInvalidPrice++;
                continue;
            }

            let finalPrice = basePrice;
            let compareAtPrice = null; // Por defecto, no hay precio de comparación

            if (discountMap.has(sku)) {
                const pct = discountMap.get(sku);
                if (pct > 0 && pct <= 100) { // Aplicar solo si el descuento es válido y positivo
                    compareAtPrice = parseFloat(basePrice.toFixed(2)); // El precio original es el de comparación
                    finalPrice = parseFloat((basePrice * (1 - pct / 100)).toFixed(2));
                    Logger.log(`💸 SKU ${sku}: Descuento ${pct}% aplicado. Original: ${compareAtPrice}, Final: ${finalPrice}`);
                } else if (pct === 0) { // Si el descuento es 0%, no hay precio de comparación.
                     finalPrice = parseFloat(basePrice.toFixed(2));
                     compareAtPrice = null; // Asegurar que no haya compareAtPrice
                     Logger.log(`ℹ️ SKU ${sku}: Descuento 0% encontrado. Precio final: ${finalPrice}, sin precio de comparación.`);
                }
                else {
                    Logger.warn(`Descuento inválido (fuera de rango 0-100) para SKU ${sku}: ${pct}%. Usando precio base ${basePrice.toFixed(2)} sin precio de comparación.`);
                    finalPrice = parseFloat(basePrice.toFixed(2));
                }
            } else {
                 finalPrice = parseFloat(basePrice.toFixed(2)); // Asegurar formato y tipo
            }


            // Comparar con precios actuales de Shopify, manejando posibles nulls
            const currentShopifyPrice = shopifyVariant.price !== null && shopifyVariant.price !== undefined ? parseFloat(shopifyVariant.price).toFixed(2) : null;
            const currentShopifyCompareAtPrice = shopifyVariant.compareAtPrice !== null && shopifyVariant.compareAtPrice !== undefined ? parseFloat(shopifyVariant.compareAtPrice).toFixed(2) : null;
            
            const finalPriceStr = finalPrice.toFixed(2);
            const compareAtPriceStr = compareAtPrice !== null ? parseFloat(compareAtPrice).toFixed(2) : null;


            if (finalPriceStr !== currentShopifyPrice || compareAtPriceStr !== currentShopifyCompareAtPrice) {
                const updateResult = await updateShopifyPrice(shopifyVariant.id, finalPriceStr, compareAtPriceStr);
                if (updateResult.success) {
                    stats.updated++;
                } else {
                    stats.errors++;
                }
            } else {
                Logger.log(`ℹ️ SKU ${sku} (${shopifyVariant.displayName || shopifyVariant.product?.title}) - Precio (${finalPriceStr}) y Precio de Comparación (${compareAtPriceStr || 'ninguno'}) ya correctos.`);
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
    Logger.log(`Productos locales con precio base inválido: ${stats.localInvalidPrice}`);
    Logger.log(`Errores durante el proceso de actualización de precios: ${stats.errors}`);
    Logger.log("=============================================\n");
}

// Si este script se ejecuta directamente (para pruebas, por ejemplo):
if (require.main === module) {
    (async () => {
        try {
            Logger.init(); // Asegurar que el logger se inicialice
            // Para probar, podrías cargar descuentos y luego ejecutar:
            // const discounts = await loadDiscounts(DISCOUNT_CSV_PATH);
            // await runPriceUpdate(null, discounts);
            await runPriceUpdate();
        } catch (err) {
            Logger.error('Error fatal ejecutando price-update.js directamente', err);
            process.exit(1);
        }
    })();
}

module.exports = { runPriceUpdate, getLocalProducts, getAllShopifyVariantsForPricing };
