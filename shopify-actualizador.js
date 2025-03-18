require('dotenv').config();
const axios = require('axios');
const fetch = require('node-fetch');

// Variables de entorno en tu archivo .env
// SHOPIFY_SHOP_NAME=tu-tienda
// SHOPIFY_ACCESS_TOKEN=tu_token_de_acceso
// DATA_API_URL=http://localhost/DelfinApi/v1/OrganizacionOData.svc/Producto/?$format=json
// MAX_RETRIES=3 (opcional, valor predeterminado: 3)

const SHOPIFY_API_URL = `https://${process.env.SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/2023-10/graphql.json`;
const MAX_RETRIES = process.env.MAX_RETRIES || 3;

/**
 * Función para limpiar el SKU eliminando caracteres no numéricos y ceros a la izquierda.
 * @param {String} sku - El SKU original.
 * @returns {String} - El SKU limpio.
 */
function cleanSku(sku) {
  if (!sku) return '0';
  const cleaned = sku.toString().trim().replace(/[^0-9]/g, '').replace(/^0+/, '');
  return cleaned || '0';
}

/**
 * Función para realizar solicitudes a la API con reintentos
 * @param {String} url - URL de la solicitud
 * @param {Object} options - Opciones de fetch
 * @param {Number} retries - Número de reintentos restantes
 * @returns {Promise<Object>} - Respuesta de la API
 */
async function fetchWithRetry(url, options, retries = MAX_RETRIES) {
  try {
    const response = await fetch(url, options);
    
    if (!response.ok && retries > 0) {
      console.log(`Intento fallido, reintentando... (${retries} intentos restantes)`);
      await new Promise(resolve => setTimeout(resolve, 1000)); // Esperar 1 segundo antes de reintentar
      return fetchWithRetry(url, options, retries - 1);
    }
    
    return await response.json();
  } catch (error) {
    if (retries > 0) {
      console.log(`Error de red, reintentando... (${retries} intentos restantes): ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 1000));
      return fetchWithRetry(url, options, retries - 1);
    }
    throw new Error(`Error después de ${MAX_RETRIES} intentos: ${error.message}`);
  }
}

/**
 * Función que obtiene los datos locales y actualiza el precio en Shopify según el SKU.
 */
async function updatePricesFromLocalAPI() {
  try {
    console.log("Iniciando actualización de precios...");
    
    // Obtenemos los datos locales desde la API
    const response = await axios.get(process.env.DATA_API_URL, { timeout: 1000000 });
    const localProducts = response.data.value || response.data;
    
    console.log(`Se encontraron ${localProducts.length} productos para procesar.`);

    // Variables para estadísticas
    let updatesMade = 0;
    let productsNotFound = 0;
    let errorsCount = 0;
    let alreadyUpdated = 0;

    // Recorremos cada producto local
    for (const localProd of localProducts) {
      try {
        const rawSku = localProd["CodigoProducto"];
        const cleanedSku = cleanSku(rawSku);
        const newPrice = parseFloat(localProd["Venta1"]).toFixed(2);

        // Consulta GraphQL para obtener variantes por SKU
        const query = `
          {
            productVariants(first: 1, query: "sku:${cleanedSku}") {
              edges {
                node {
                  id
                  price
                  product {
                    title
                  }
                }
              }
            }
          }
        `;

        const graphqlOptions = {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
          },
          body: JSON.stringify({ query }),
        };

        const { data, errors } = await fetchWithRetry(SHOPIFY_API_URL, graphqlOptions);

        if (errors) {
          console.error(`Errores en la consulta GraphQL para SKU ${cleanedSku}:`, errors);
          errorsCount++;
          continue;
        }

        const variant = data.productVariants.edges[0]?.node;

        if (!variant) {
          console.log(`No se encontró la variante con SKU ${cleanedSku}`);
          productsNotFound++;
          continue;
        }

        const productName = variant.product?.title || 'Nombre no disponible';

        // Verificar si el precio actual es diferente al nuevo precio
        if (parseFloat(variant.price) !== parseFloat(newPrice)) {
          // Mutación GraphQL para actualizar el precio de la variante
          const mutation = `
            mutation {
              productVariantUpdate(input: {
                id: "${variant.id}",
                price: "${newPrice}"
              }) {
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

          const mutationOptions = {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
            },
            body: JSON.stringify({ query: mutation }),
          };

          const mutationResult = await fetchWithRetry(SHOPIFY_API_URL, mutationOptions);

          if (mutationResult.errors) {
            console.error(`Errores en la mutación GraphQL para SKU ${cleanedSku}:`, mutationResult.errors);
            errorsCount++;
            continue;
          }

          const userErrors = mutationResult.data.productVariantUpdate.userErrors;

          if (userErrors.length > 0) {
            console.error(`Errores al actualizar la variante ${cleanedSku}:`, userErrors);
            errorsCount++;
          } else {
            console.log(`✅ SKU ${cleanedSku} (${productName}) actualizado: ${variant.price} → ${newPrice}`);
            updatesMade++;
          }
        } else {
          console.log(`ℹ️ SKU ${cleanedSku} (${productName}) ya tiene el precio correcto: ${newPrice}`);
          alreadyUpdated++;
        }
      } catch (productError) {
        console.error(`Error al procesar producto:`, productError.message);
        errorsCount++;
      }
    }

    // Resumen de la actualización
    console.log("\n===== RESUMEN DE LA ACTUALIZACIÓN =====");
    console.log(`Total de productos procesados: ${localProducts.length}`);
    console.log(`Precios actualizados: ${updatesMade}`);
    console.log(`Precios ya correctos: ${alreadyUpdated}`);
    console.log(`Productos no encontrados: ${productsNotFound}`);
    console.log(`Errores: ${errorsCount}`);
    console.log("======================================\n");

    if (updatesMade === 0 && alreadyUpdated > 0) {
      console.log("Todos los precios ya estaban actualizados. No se realizaron cambios.");
    }
  } catch (error) {
    console.error("Error al obtener datos de la API local:", error.message);
    if (error.response) {
      console.error("Detalles de la respuesta:", {
        status: error.response.status,
        headers: error.response.headers,
        data: error.response.data
      });
    }
  }
}

// Ejecutar la función principal
console.log(`Iniciando sincronización con Shopify (${new Date().toLocaleString()})`);
updatePricesFromLocalAPI()
  .then(() => console.log(`Sincronización finalizada (${new Date().toLocaleString()})`))
  .catch(err => console.error("Error fatal:", err.message));
