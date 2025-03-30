require('dotenv').config();
const axios = require('axios');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// Variables de entorno en tu archivo .env
// SHOPIFY_SHOP_NAME=tu-tienda
// SHOPIFY_ACCESS_TOKEN=tu_token_de_acceso
// DATA_API_URL=http://localhost/DelfinApi/v1/OrganizacionOData.svc/Producto/?$format=json
// INVENTORY_API_URL=http://localhost/DelfinApi/v1/OrganizacionOData.svc/InventarioMensual/?$format=json
// MAX_RETRIES=3 (opcional, valor predeterminado: 3)
// LOG_FILE_PATH=shopify-sync.log (opcional, predeterminado directo en el directorio del script)
// LOG_MAX_SIZE=5 (opcional, tamaño máximo en MB)
// SYNC_MODE=shopify_first (opcional, 'local_first' o 'shopify_first', predeterminado: 'local_first')

const SHOPIFY_API_URL = `https://${process.env.SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/2023-10/graphql.json`;
const MAX_RETRIES = process.env.MAX_RETRIES || 3;
const LOG_FILE_PATH = process.env.LOG_FILE_PATH || 'shopify-sync.log';
const LOG_MAX_SIZE = (process.env.LOG_MAX_SIZE || 5) * 1024 * 1024; // Convertir MB a bytes
const SYNC_MODE = process.env.SYNC_MODE || 'local_first'; // 'local_first' o 'shopify_first'

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
        
        // Eliminar logs antiguos (mantener solo los 5 más recientes)
        this.cleanOldLogs();
      }
    } catch (error) {
      console.error(`Error al verificar tamaño del log: ${error.message}`);
    }
  },
  
  /**
   * Elimina los archivos de log más antiguos, manteniendo solo los 5 más recientes
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
 * Obtiene todos los productos locales con precios
 * @returns {Promise<Array>} - Array de productos locales
 */
async function getLocalProducts() {
  try {
    Logger.log("Obteniendo datos de productos locales...");
    const response = await axios.get(process.env.DATA_API_URL, { timeout: 1000000 });
    const localProducts = response.data.value || response.data;
    Logger.log(`Se encontraron ${localProducts.length} productos locales para procesar.`);
    return localProducts;
  } catch (error) {
    Logger.error("Error al obtener datos de la API local de productos", error);
    throw error;
  }
}

/**
 * Obtiene todos los datos de inventario locales
 * @returns {Promise<Object>} - Objeto con el inventario indexado por código de producto
 */
async function getLocalInventory() {
  try {
    Logger.log("Obteniendo datos de inventario local...");
    const response = await axios.get(process.env.INVENTORY_API_URL, { timeout: 1000000 });
    const inventoryData = response.data.value || response.data;
    Logger.log(`Se encontraron ${inventoryData.length} registros de inventario local.`);
    
    // Crear un mapa de inventario indexado por código de producto
    const inventoryMap = {};
    for (const item of inventoryData) {
      const sku = cleanSku(item.CodigoProducto);
      if (!inventoryMap[sku] || new Date(item.Fecha) > new Date(inventoryMap[sku].Fecha)) {
        inventoryMap[sku] = item;
      }
    }
    
    Logger.log(`Inventario procesado para ${Object.keys(inventoryMap).length} productos únicos.`);
    return inventoryMap;
  } catch (error) {
    Logger.error("Error al obtener datos de la API local de inventario", error);
    throw error;
  }
}

/**
 * Obtiene todas las variantes de Shopify con paginación
 * @returns {Promise<Array>} - Array de variantes de Shopify
 */
async function getAllShopifyVariants() {
  Logger.log("Obteniendo variantes de Shopify...");
  const allVariants = [];
  let hasNextPage = true;
  let cursor = null;
  
  while (hasNextPage) {
    const afterParam = cursor ? `, after: "${cursor}"` : '';
    const query = `
      {
        productVariants(first: 100${afterParam}) {
          edges {
            cursor
            node {
              id
              sku
              price
              inventoryQuantity
              inventoryItem {
                id
              }
              product {
                title
              }
            }
          }
          pageInfo {
            hasNextPage
          }
        }
      }
    `;
    
    const options = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
      },
      body: JSON.stringify({ query }),
    };
    
    const { data, errors } = await fetchWithRetry(SHOPIFY_API_URL, options);
    
    if (errors) {
      Logger.error("Error al obtener variantes de Shopify:", { message: JSON.stringify(errors) });
      throw new Error("Error en la consulta GraphQL de Shopify");
    }
    
    const variants = data.productVariants.edges;
    if (variants.length === 0) break;
    
    // Agregar las variantes al array
    for (const { node } of variants) {
      // Solo agregar variantes con SKU válido
      if (node.sku && node.sku.trim() !== '') {
        allVariants.push(node);
      }
    }
    
    // Verificar si hay más páginas
    hasNextPage = data.productVariants.pageInfo.hasNextPage;
    if (hasNextPage && variants.length > 0) {
      cursor = variants[variants.length - 1].cursor;
    }
    
    Logger.log(`Obtenidas ${allVariants.length} variantes de Shopify hasta ahora...`);
  }
  
  Logger.log(`Total de variantes de Shopify obtenidas: ${allVariants.length}`);
  return allVariants;
}

/**
 * Actualiza el precio y el inventario de una variante en Shopify
 * @param {Object} variant - Variante de Shopify
 * @param {String} newPrice - Nuevo precio
 * @param {Number} newInventory - Nuevo inventario (null si no se actualiza)
 * @returns {Promise<Object>} - Resultado de la actualización
 */
async function updateVariantInShopify(variant, newPrice, newInventory) {
  const productName = variant.product?.title || 'Nombre no disponible';
  const sku = variant.sku;
  let updateParts = [];
  let logMessage = '';
  
  // Construir los inputs para la mutación
  const inputs = {
    id: variant.id
  };
  
  // Si hay precio para actualizar
  if (newPrice !== null && parseFloat(variant.price) !== parseFloat(newPrice)) {
    inputs.price = newPrice;
    updateParts.push(`precio: ${variant.price} → ${newPrice}`);
  }
  
  // Si hay nada que actualizar, retornar sin hacer la mutación
  if (Object.keys(inputs).length <= 1 && newInventory === null) {
    return { success: true, updated: false, message: "No hay cambios para aplicar" };
  }
  
  // Crear la mutación para actualizar la variante
  const mutation = `
    mutation {
      productVariantUpdate(input: ${JSON.stringify(inputs).replace(/"([^"]+)":/g, '$1:')}) {
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
  
  const options = {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
    },
    body: JSON.stringify({ query: mutation }),
  };
  
  // Realizar la mutación para actualizar el precio
  const mutationResult = await fetchWithRetry(SHOPIFY_API_URL, options);
  
  if (mutationResult.errors) {
    Logger.error(`Errores en la mutación GraphQL para SKU ${sku}:`, { message: JSON.stringify(mutationResult.errors) });
    return { success: false, error: mutationResult.errors };
  }
  
  const userErrors = mutationResult.data.productVariantUpdate.userErrors;
  if (userErrors.length > 0) {
    Logger.error(`Errores al actualizar la variante ${sku}:`, { message: JSON.stringify(userErrors) });
    return { success: false, error: userErrors };
  }
  
  // Si hay inventario para actualizar
  if (newInventory !== null && newInventory !== variant.inventoryQuantity) {
    updateParts.push(`inventario: ${variant.inventoryQuantity} → ${newInventory}`);
    
    // Crear la mutación para ajustar el inventario
    const inventoryMutation = `
      mutation {
        inventoryAdjustQuantity(input: {
          inventoryLevelId: "${variant.inventoryItem.id}",
          availableDelta: ${newInventory - variant.inventoryQuantity}
        }) {
          inventoryLevel {
            available
          }
          userErrors {
            field
            message
          }
        }
      }
    `;
    
    const inventoryOptions = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
      },
      body: JSON.stringify({ query: inventoryMutation }),
    };
    
    // Realizar la mutación para actualizar el inventario
    const inventoryResult = await fetchWithRetry(SHOPIFY_API_URL, inventoryOptions);
    
    if (inventoryResult.errors) {
      Logger.error(`Errores en la mutación de inventario para SKU ${sku}:`, { message: JSON.stringify(inventoryResult.errors) });
      return { success: false, error: inventoryResult.errors };
    }
    
    const invUserErrors = inventoryResult.data.inventoryAdjustQuantity.userErrors;
    if (invUserErrors.length > 0) {
      Logger.error(`Errores al actualizar el inventario ${sku}:`, { message: JSON.stringify(invUserErrors) });
      return { success: false, error: invUserErrors };
    }
  }
  
  if (updateParts.length > 0) {
    logMessage = `✅ SKU ${sku} (${productName}) actualizado: ${updateParts.join(', ')}`;
    Logger.log(logMessage, 'SUCCESS');
    return { success: true, updated: true, message: logMessage };
  } else {
    logMessage = `ℹ️ SKU ${sku} (${productName}) ya tiene los valores correctos`;
    Logger.log(logMessage);
    return { success: true, updated: false, message: logMessage };
  }
}

/**
 * Función principal que sincroniza datos desde la API local a Shopify
 */
async function syncLocalDataToShopify() {
  try {
    Logger.log("Iniciando sincronización completa con Shopify...");
    
    // Obtener datos locales y de Shopify
    const localProducts = await getLocalProducts();
    const localInventory = await getLocalInventory();
    const shopifyVariants = await getAllShopifyVariants();
    
    // Crear un mapa de variantes de Shopify por SKU para acceso rápido
    const shopifyVariantMap = {};
    for (const variant of shopifyVariants) {
      const cleanedSku = cleanSku(variant.sku);
      if (cleanedSku) {
        shopifyVariantMap[cleanedSku] = variant;
      }
    }
    
    // Variables para estadísticas
    let priceUpdates = 0;
    let inventoryUpdates = 0;
    let bothUpdates = 0;
    let productsNotFound = 0;
    let errorsCount = 0;
    let alreadyUpdated = 0;
    
    if (SYNC_MODE === 'shopify_first') {
      // Modo optimizado: iterar sobre productos de Shopify
      Logger.log("Usando modo de sincronización: SHOPIFY_FIRST (procesando productos en línea)");
      
      // Crear un mapa de productos locales para buscar rápidamente
      const localProductMap = {};
      for (const product of localProducts) {
        const sku = cleanSku(product.CodigoProducto);
        if (sku) {
          localProductMap[sku] = product;
        }
      }
      
      // Procesar cada variante de Shopify
      for (const [sku, variant] of Object.entries(shopifyVariantMap)) {
        try {
          const localProduct = localProductMap[sku];
          const inventoryData = localInventory[sku];
          
          // Si no existe el producto local, continuar al siguiente
          if (!localProduct) {
            Logger.log(`⚠️ Producto con SKU ${sku} existe en Shopify pero no en sistema local`, 'WARN');
            productsNotFound++;
            continue;
          }
          
          // Obtener el nuevo precio y cantidad de inventario
          const newPrice = parseFloat(localProduct.Venta1).toFixed(2);
          const inventoryQty = inventoryData ? Math.max(0, parseFloat(inventoryData.CantidadActual)) : null;
          
          // Actualizar la variante en Shopify
          const result = await updateVariantInShopify(
            variant, 
            newPrice, 
            inventoryQty !== null ? Math.floor(inventoryQty) : null
          );
          
          if (result.success) {
            if (result.updated) {
              if (result.message.includes('precio') && result.message.includes('inventario')) {
                bothUpdates++;
              } else if (result.message.includes('precio')) {
                priceUpdates++;
              } else if (result.message.includes('inventario')) {
                inventoryUpdates++;
              }
            } else {
              alreadyUpdated++;
            }
          } else {
            errorsCount++;
          }
        } catch (variantError) {
          Logger.error(`Error al procesar variante con SKU ${sku}:`, variantError);
          errorsCount++;
        }
      }
    } else {
      // Modo estándar: iterar sobre productos locales
      Logger.log("Usando modo de sincronización: LOCAL_FIRST (procesando productos locales)");
      
      // Recorrer cada producto local
      for (const localProd of localProducts) {
        try {
          const rawSku = localProd.CodigoProducto;
          const cleanedSku = cleanSku(rawSku);
          
          // Buscar la variante en Shopify
          const variant = shopifyVariantMap[cleanedSku];
          
          if (!variant) {
            Logger.log(`No se encontró la variante con SKU ${cleanedSku} en Shopify`, 'WARN');
            productsNotFound++;
            continue;
          }
          
          // Obtener el nuevo precio y la cantidad de inventario
          const newPrice = parseFloat(localProd.Venta1).toFixed(2);
          const inventoryData = localInventory[cleanedSku];
          const inventoryQty = inventoryData ? Math.max(0, parseFloat(inventoryData.CantidadActual)) : null;
          
          // Actualizar la variante en Shopify
          const result = await updateVariantInShopify(
            variant, 
            newPrice, 
            inventoryQty !== null ? Math.floor(inventoryQty) : null
          );
          
          if (result.success) {
            if (result.updated) {
              if (result.message.includes('precio') && result.message.includes('inventario')) {
                bothUpdates++;
              } else if (result.message.includes('precio')) {
                priceUpdates++;
              } else if (result.message.includes('inventario')) {
                inventoryUpdates++;
              }
            } else {
              alreadyUpdated++;
            }
          } else {
            errorsCount++;
          }
        } catch (productError) {
          const sku = cleanSku(localProd.CodigoProducto);
          Logger.error(`Error al procesar producto con SKU ${sku}:`, productError);
          errorsCount++;
        }
      }
    }
    
    // Resumen de la actualización
    Logger.log("\n===== RESUMEN DE LA SINCRONIZACIÓN =====");
    Logger.log(`Modo de sincronización: ${SYNC_MODE === 'shopify_first' ? 'SHOPIFY_FIRST' : 'LOCAL_FIRST'}`);
    Logger.log(`Total de productos locales: ${localProducts.length}`);
    Logger.log(`Total de productos en Shopify: ${shopifyVariants.length}`);
    Logger.log(`Actualizaciones de precio: ${priceUpdates}`);
    Logger.log(`Actualizaciones de inventario: ${inventoryUpdates}`);
    Logger.log(`Actualizaciones de precio e inventario: ${bothUpdates}`);
    Logger.log(`Total de productos actualizados: ${priceUpdates + inventoryUpdates + bothUpdates}`);
    Logger.log(`Productos sin cambios necesarios: ${alreadyUpdated}`);
    Logger.log(`Productos no encontrados: ${productsNotFound}`);
    Logger.log(`Errores: ${errorsCount}`);
    Logger.log("==========================================\n");
    
    if (priceUpdates === 0 && inventoryUpdates === 0 && bothUpdates === 0 && alreadyUpdated > 0) {
      Logger.log("Todos los productos ya estaban actualizados. No se realizaron cambios.");
    }
  } catch (error) {
    Logger.error("Error general en la sincronización", error);
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
Logger.log(`Iniciando sincronización completa con Shopify (${new Date().toLocaleString()})`);
syncLocalDataToShopify()
  .then(() => Logger.log(`Sincronización finalizada (${new Date().toLocaleString()})`))
  .catch(err => Logger.error("Error fatal:", err));