// Primero, instala las dependencias:
// npm install axios dotenv node-fetch

require('dotenv').config();
const axios = require('axios');
const fetch = require('node-fetch');

// Variables de entorno en tu archivo .env
// SHOPIFY_SHOP_NAME=tu-tienda
// SHOPIFY_ACCESS_TOKEN=tu_token_de_acceso
// DATA_API_URL=http://localhost/DelfinApi/v1/OrganizacionOData.svc/Producto/?$format=json

const SHOPIFY_API_URL = `https://${process.env.SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/2023-10/graphql.json`;

/**
 * Función para limpiar el SKU eliminando caracteres no numéricos y ceros a la izquierda.
 * @param {String} sku - El SKU original.
 * @returns {String} - El SKU limpio.
 */
function cleanSku(sku) {
  const cleaned = sku.trim().replace(/[^0-9]/g, '').replace(/^0+/, '');
  return cleaned || '0';
}

/**
 * Función que obtiene los datos locales y actualiza el precio en Shopify según el SKU.
 */
async function updatePricesFromLocalAPI() {
  try {
    // Obtenemos los datos locales desde la API
    const response = await axios.get(process.env.DATA_API_URL);
    const localProducts = response.data.value || response.data;

    // Variable para rastrear si se realizaron actualizaciones
    let updatesMade = false;

    // Recorremos cada producto local
    for (const localProd of localProducts) {
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
              }
            }
          }
        }
      `;

      const graphqlResponse = await fetch(SHOPIFY_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
        },
        body: JSON.stringify({ query }),
      });

      const { data, errors } = await graphqlResponse.json();

      if (errors) {
        console.error(`Errores en la consulta GraphQL:`, errors);
        continue;
      }

      const variant = data.productVariants.edges[0]?.node;

      if (!variant) {
        console.log(`No se encontró la variante con SKU ${cleanedSku}`);
        continue;
      }

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

      const mutationResponse = await fetch(SHOPIFY_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
        },
        body: JSON.stringify({ query: mutation }),
      });

      const mutationResult = await mutationResponse.json();

      if (mutationResult.errors) {
        console.error(`Errores en la mutación GraphQL:`, mutationResult.errors);
        continue;
      }

      const userErrors = mutationResult.data.productVariantUpdate.userErrors;

      if (userErrors.length > 0) {
        console.error(`Errores al actualizar la variante:`, userErrors);
      } else {
        console.log(`SKU ${cleanedSku} actualizado a precio ${newPrice}`);
        updatesMade = true;
      }
    }
  }
  // Mensaje final si no se realizaron actualizaciones
  if (!updatesMade) {
    console.log("Todos los precios ya estaban actualizados. No se realizaron cambios.");
  }
  } catch (error) {
    console.error("Error al obtener datos de la API local:", error.message);
  }
}

updatePricesFromLocalAPI();
