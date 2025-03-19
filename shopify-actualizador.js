require('dotenv').config();
const axios = require('axios');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// Variables de entorno en tu archivo .env
// SHOPIFY_SHOP_NAME=tu-tienda
// SHOPIFY_ACCESS_TOKEN=tu_token_de_acceso
// DATA_API_URL=http://localhost/DelfinApi/v1/OrganizacionOData.svc/Producto/?$format=json
// MAX_RETRIES=3 (opcional, valor predeterminado: 3)
// LOG_FILE_PATH=logs/shopify-sync.log (opcional, ruta del archivo de log)
// LOG_MAX_SIZE=100 (opcional, tamaño máximo en MB)

const SHOPIFY_API_URL = `https://${process.env.SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/2023-10/graphql.json`;
const MAX_RETRIES = process.env.MAX_RETRIES || 3;
const LOG_FILE_PATH = process.env.LOG_FILE_PATH || 'logs/shopify-sync.log';
const LOG_MAX_SIZE = (process.env.LOG_MAX_SIZE || 100) * 1024 * 1024; // Convertir MB a bytes

/**
 * Configuración del sistema de logs
 */
const Logger = {
  logDir: path.dirname(LOG_FILE_PATH),
  logPath: LOG_FILE_PATH,
  
  /**
   * Inicializa el sistema de logs creando el directorio si no existe
   */
  init() {
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
    if (!fs.existsSync(this.logPath)) {
      fs.writeFileSync(this.logPath, '');
    }
  },
  
  /**
   * Escribe un mensaje en el archivo de log y en la consola
   * @param {String} message - Mensaje a registrar
   * @param {String} level - Nivel del log (INFO, ERROR, etc.)
   */
  log(message, level = 'INFO') {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] [${level}] ${message}\n`;
    
    // Mostrar en consola
    console.log(message);
    
    // Verificar el tamaño del archivo antes de escribir
    this.checkLogSize();
    
    // Escribir en el archivo
    fs.appendFileSync(this.logPath, logEntry);
  },
  
  /**
   * Verifica el tamaño del archivo y lo rota si es necesario
   */
  checkLogSize() {
    try {
      const stats = fs.statSync(this.logPath);
      
      if (stats.size >= LOG_MAX_SIZE) {
        // Crear nombre de archivo de respaldo con timestamp
        const timestamp = new Date().toISOString().replace(/:/g, '-');
        const backupPath = `${this.logDir}/shopify-sync.${timestamp}.log`;
        
        // Mover el archivo actual a respaldo
        fs.renameSync(this.logPath, backupPath);
        
        // Crear un nuevo archivo vacío
        fs.writeFileSync(this.logPath, `[${new Date().toISOString()}] [INFO] Archivo de log rotado. Archivo anterior: ${backupPath}\n`);
        
        // Eliminar logs antiguos (mantener solo los más recientes)
        this.cleanOldLogs();
      }
    } catch (error) {
      console.error(`Error al verificar tamaño del log: ${error.message}`);
    }
  },
  
  /**
   * Elimina los archivos de log más antiguos, manteniendo solo los más recientes
   */
  cleanOldLogs() {
    try {
      const logFiles = fs.readdirSync(this.logDir)
        .filter(file => file.startsWith('shopify-sync.') && file.endsWith('.log'))
        .map(file => ({ 
          name: file, 
          path: path.join(this.logDir, file),
          time: fs.statSync(path.join(this.logDir, file)).mtime.getTime() 
        }))
        .sort((a, b) => b.time - a.time); // Ordenar de más reciente a más antiguo
      
      // Mantener solo los 5 más recientes
      if (logFiles.length > 5) {
        logFiles.slice(5).forEach(file => {
          fs.unlinkSync(file.path);
          console.log(`Archivo de log antiguo eliminado: ${file.name}`);
        });
      }
    } catch (error) {
      console.error(`Error al limpiar logs antiguos: ${error.message}`);
    }
  },
  
  /**
   * Registra un error
   * @param {String} message - Mensaje de error
   * @param {Error} error - Objeto de error (opcional)
   */
  error(message, error = null) {
    let logMessage = message;
    if (error) {
      logMessage += `: ${error.message}`;
      if (error.response) {
        logMessage += ` (Status: ${error.response.status})`;
      }
    }
    this.log(logMessage, 'ERROR');
  }
};

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
      Logger.log(`Intento fallido, reintentando... (${retries} intentos restantes)`, 'WARN');
      await new Promise(resolve => setTimeout(resolve, 1000)); // Esperar 1 segundo antes de reintentar
      return fetchWithRetry(url, options, retries - 1);
    }
    
    return await response.json();
  } catch (error) {
    if (retries > 0) {
      Logger.error(`Error de red, reintentando... (${retries} intentos restantes)`, error);
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
    Logger.log("Iniciando actualización de precios...");
    
    // Obtenemos los datos locales desde la API
    const response = await axios.get(process.env.DATA_API_URL, { timeout: 1000000 });
    const localProducts = response.data.value || response.data;
    
    Logger.log(`Se encontraron ${localProducts.length} productos para procesar.`);

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
          Logger.error(`Errores en la consulta GraphQL para SKU ${cleanedSku}:`, { message: JSON.stringify(errors) });
          errorsCount++;
          continue;
        }

        const variant = data.productVariants.edges[0]?.node;

        if (!variant) {
          Logger.log(`No se encontró la variante con SKU ${cleanedSku}`, 'WARN');
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
            Logger.error(`Errores en la mutación GraphQL para SKU ${cleanedSku}:`, { message: JSON.stringify(mutationResult.errors) });
            errorsCount++;
            continue;
          }

          const userErrors = mutationResult.data.productVariantUpdate.userErrors;

          if (userErrors.length > 0) {
            Logger.error(`Errores al actualizar la variante ${cleanedSku}:`, { message: JSON.stringify(userErrors) });
            errorsCount++;
          } else {
            Logger.log(`✅ SKU ${cleanedSku} (${productName}) actualizado: ${variant.price} → ${newPrice}`, 'SUCCESS');
            updatesMade++;
          }
        } else {
          Logger.log(`ℹ️ SKU ${cleanedSku} (${productName}) ya tiene el precio correcto: ${newPrice}`);
          alreadyUpdated++;
        }
      } catch (productError) {
        Logger.error(`Error al procesar producto:`, productError);
        errorsCount++;
      }
    }

    // Resumen de la actualización
    Logger.log("\n===== RESUMEN DE LA ACTUALIZACIÓN =====");
    Logger.log(`Total de productos procesados: ${localProducts.length}`);
    Logger.log(`Precios actualizados: ${updatesMade}`);
    Logger.log(`Precios ya correctos: ${alreadyUpdated}`);
    Logger.log(`Productos no encontrados: ${productsNotFound}`);
    Logger.log(`Errores: ${errorsCount}`);
    Logger.log("======================================\n");

    if (updatesMade === 0 && alreadyUpdated > 0) {
      Logger.log("Todos los precios ya estaban actualizados. No se realizaron cambios.");
    }
  } catch (error) {
    Logger.error("Error al obtener datos de la API local", error);
    if (error.response) {
      Logger.error("Detalles de la respuesta:", {
        message: JSON.stringify({
          status: error.response.status,
          headers: error.response.headers,
          data: error.response.data
        })
      });
    }
  }
}

// Inicializar el sistema de logs
Logger.init();

// Ejecutar la función principal
Logger.log(`Iniciando sincronización con Shopify (${new Date().toLocaleString()})`);
updatePricesFromLocalAPI()
  .then(() => Logger.log(`Sincronización finalizada (${new Date().toLocaleString()})`))
  .catch(err => Logger.error("Error fatal:", err));
