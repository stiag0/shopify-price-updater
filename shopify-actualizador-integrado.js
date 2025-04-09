require('dotenv').config();
const axios = require('axios');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

// Variables de entorno en tu archivo .env
// SHOPIFY_SHOP_NAME=tu-tienda
// SHOPIFY_ACCESS_TOKEN=tu_token_de_acceso
// DATA_API_URL=http://localhost/DelfinApi/v1/OrganizacionOData.svc/Producto/?$format=json
// INVENTORY_API_URL=http://localhost/DelfinApi/v1/OrganizacionOData.svc/InventarioMensual/?$format=json
// MAX_RETRIES=3 (opcional, valor predeterminado: 3)
// LOG_FILE_PATH=logs/shopify-sync.log (opcional, ruta del archivo de log)
// LOG_MAX_SIZE=100 (opcional, tamaño máximo en MB)
// SYNC_MODE=local_first (opcional, 'local_first' o 'shopify_first', predeterminado: 'local_first')
// SYNC_TYPE=both (opcional, 'price', 'inventory', 'both', predeterminado: 'both')

const SHOPIFY_API_URL = `https://${process.env.SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/2023-10/graphql.json`;
const MAX_RETRIES = process.env.MAX_RETRIES || 3;
const LOG_FILE_PATH = process.env.LOG_FILE_PATH || 'logs/shopify-sync.log';
const LOG_MAX_SIZE = (process.env.LOG_MAX_SIZE || 100) * 1024 * 1024; // Convertir MB a bytes
const SYNC_MODE = process.env.SYNC_MODE || 'local_first'; // 'local_first' o 'shopify_first'
const SYNC_TYPE = process.env.SYNC_TYPE || 'both'; // 'price', 'inventory', 'both'

/**
 * Configuración del sistema de logs optimizado
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
    
    // Escribir en el archivo usando streams para evitar problemas de memoria
    const stream = fs.createWriteStream(this.logPath, { flags: 'a' });
    stream.write(logEntry);
    stream.end();
  },
  
  /**
   * Verifica el tamaño del archivo y lo rota si es necesario
   */
  checkLogSize() {
    try {
      if (!fs.existsSync(this.logPath)) {
        return;
      }
      
      const stats = fs.statSync(this.logPath);
      
      if (stats.size >= LOG_MAX_SIZE) {
        // Crear nombre de archivo de respaldo con timestamp
        const timestamp = new Date().toISOString().replace(/:/g, '-');
        const backupPath = `${this.logDir}/shopify-sync.${timestamp}.log`;
        
        // Usar streams para mover el archivo y evitar cargar todo en memoria
        this.moveFileUsingStreams(this.logPath, backupPath, () => {
          // Crear un nuevo archivo vacío
          fs.writeFileSync(this.logPath, `[${new Date().toISOString()}] [INFO] Archivo de log rotado. Archivo anterior: ${backupPath}\n`);
          
          // Eliminar logs antiguos (mantener solo el cuarto más reciente)
          this.cleanOldLogs();
        });
      }
    } catch (error) {
      console.error(`Error al verificar tamaño del log: ${error.message}`);
    }
  },
  
  /**
   * Mueve un archivo usando streams para evitar problemas de memoria
   * @param {String} source - Ruta del archivo origen
   * @param {String} destination - Ruta del archivo destino
   * @param {Function} callback - Función a ejecutar al terminar
   */
  moveFileUsingStreams(source, destination, callback) {
    const readStream = fs.createReadStream(source);
    const writeStream = fs.createWriteStream(destination);
    
    readStream.on('error', err => console.error(`Error al leer archivo: ${err.message}`));
    writeStream.on('error', err => console.error(`Error al escribir archivo: ${err.message}`));
    
    writeStream.on('finish', () => {
      fs.unlink(source, err => {
        if (err) console.error(`Error al eliminar archivo original: ${err.message}`);
        if (callback) callback();
      });
    });
    
    readStream.pipe(writeStream);
  },
  
  /**
   * Elimina los archivos de log más antiguos, manteniendo solo el cuarto más reciente
   */
  cleanOldLogs() {
    try {
      const logFiles = fs.readdirSync(this.logDir)
        .filter(file => file.startsWith('shopify-sync.') && file.endsWith('.log') && file !== path.basename(this.logPath))
        .map(file => ({ 
          name: file, 
          path: path.join(this.logDir, file),
          time: fs.statSync(path.join(this.logDir, file)).mtime.getTime() 
        }))
        .sort((a, b) => b.time - a.time); // Ordenar de más reciente a más antiguo
      
      // Calcular cuántos archivos mantener (un cuarto del total)
      const keepCount = Math.max(Math.ceil(logFiles.length / 4), 5);
      
      // Eliminar los archivos más antiguos
      if (logFiles.length > keepCount) {
        logFiles.slice(keepCount).forEach(file => {
          fs.unlinkSync(file.path);
          console.log(`Archivo de log antiguo eliminado: ${file.name}`);
        });
        
        const keptCount = Math.min(keepCount, logFiles.length);
        console.log(`Se mantuvieron los ${keptCount} archivos de log más recientes (${Math.round(keptCount/logFiles.length*100)}% del total)`);
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
  },
  
  /**
   * Lee el contenido de un archivo de log de forma paginada
   * @param {String} filePath - Ruta del archivo a leer
   * @param {Number} page - Número de página (empezando desde 1)
   * @param {Number} pageSize - Tamaño de la página en líneas
   * @returns {Promise<Object>} - Objeto con el contenido de la página y metadatos
   */
  async readLogPaginated(filePath = null, page = 1, pageSize = 1000) {
    const targetPath = filePath || this.logPath;
    
    return new Promise((resolve, reject) => {
      try {
        if (!fs.existsSync(targetPath)) {
          return resolve({
            content: [],
            page: 1,
            totalPages: 0,
            totalLines: 0,
            hasMore: false
          });
        }
        
        // Validar página y tamaño
        page = Math.max(1, page);
        pageSize = Math.max(10, Math.min(10000, pageSize));
        
        // Usar un contador de líneas para determinar el total
        let totalLines = 0;
        const lineCounter = fs.createReadStream(targetPath)
          .on('data', buffer => {
            let idx = -1;
            totalLines--; // Compensar por si no hay salto de línea al final
            do {
              idx = buffer.indexOf(10, idx + 1);
              totalLines++;
            } while (idx !== -1);
          })
          .on('end', () => {
            // Calcular total de páginas
            const totalPages = Math.ceil(totalLines / pageSize);
            
            // Validar que la página solicitada existe
            if (page > totalPages && totalPages > 0) {
              page = totalPages;
            }
            
            // Calcular líneas a saltar
            const linesToSkip = (page - 1) * pageSize;
            
            // Definir variables para la lectura
            let currentLine = 0;
            let content = [];
            let linesRead = 0;
            
            // Leer el archivo usando streams
            const lineReader = readline.createInterface({
              input: fs.createReadStream(targetPath),
              crlfDelay: Infinity
            });
            
            lineReader.on('line', (line) => {
              currentLine++;
              
              // Si estamos en el rango de la página actual
              if (currentLine > linesToSkip && linesRead < pageSize) {
                content.push(line);
                linesRead++;
              }
              
              // Si ya tenemos suficientes líneas, cerramos el stream
              if (linesRead >= pageSize) {
                lineReader.close();
              }
            });
            
            lineReader.on('close', () => {
              resolve({
                content,
                page,
                totalPages,
                totalLines,
                hasMore: page < totalPages
              });
            });
          })
          .on('error', err => {
            reject(new Error(`Error al leer archivo de log: ${err.message}`));
          });
      } catch (error) {
        reject(new Error(`Error al paginar log: ${error.message}`));
      }
    });
  },
  
  /**
   * Obtiene la lista de archivos de log disponibles
   * @returns {Array} - Array de objetos con información de los archivos
   */
  getLogFiles() {
    try {
      // Obtener todos los archivos de log
      const logFiles = fs.readdirSync(this.logDir)
        .filter(file => file.startsWith('shopify-sync.') && file.endsWith('.log'))
        .map(file => {
          const filePath = path.join(this.logDir, file);
          const stats = fs.statSync(filePath);
          return {
            name: file,
            path: filePath,
            size: stats.size,
            sizeFormatted: this.formatFileSize(stats.size),
            createdAt: stats.birthtime,
            modifiedAt: stats.mtime,
            isCurrentLog: filePath === this.logPath
          };
        })
        .sort((a, b) => b.modifiedAt.getTime() - a.modifiedAt.getTime());
      
      return logFiles;
    } catch (error) {
      console.error(`Error al obtener lista de logs: ${error.message}`);
      return [];
    }
  },
  
  /**
   * Formatea un tamaño de archivo en bytes a una representación legible
   * @param {Number} bytes - Tamaño en bytes
   * @returns {String} - Tamaño formateado
   */
  formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(2) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(2) + ' MB';
    return (bytes / 1073741824).toFixed(2) + ' GB';
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
  if (SYNC_TYPE === 'price') {
    Logger.log("Omitiendo carga de inventario porque el modo de sincronización es solo precios");
    return {};
  }
  
  try {
    Logger.log("Obteniendo datos de inventario local...");
    const response = await axios.get(process.env.INVENTORY_API_URL, { timeout: 1000000 });
    const inventoryData = response.data.value || response.data;
    Logger.log(`Se encontraron ${inventoryData.length} registros de inventario local.`);
    
    // Agrupar los registros de inventario por SKU
    const inventoryBySku = {};
    for (const item of inventoryData) {
      const sku = cleanSku(item.CodigoProducto);
      if (!inventoryBySku[sku]) {
        inventoryBySku[sku] = [];
      }
      inventoryBySku[sku].push(item);
    }
    
    // Crear un mapa de inventario indexado por código de producto
    // calculando correctamente el inventario actual
    const inventoryMap = {};
    for (const [sku, items] of Object.entries(inventoryBySku)) {
      // Ordenar los registros por fecha, del más reciente al más antiguo
      const sortedItems = items.sort((a, b) => new Date(b.Fecha) - new Date(a.Fecha));
      
      // Tomar el registro más reciente
      const mostRecentItem = sortedItems[0];
      
      // Calcular la cantidad real utilizando CantidadInicial + CantidadEntradas - CantidadSalidas
      const cantidadInicial = parseFloat(mostRecentItem.CantidadInicial || 0);
      const cantidadEntradas = parseFloat(mostRecentItem.CantidadEntradas || 0);
      const cantidadSalidas = parseFloat(mostRecentItem.CantidadSalidas || 0);
      
      // Calcular la cantidad actual
      const cantidadCalculada = Math.max(0, cantidadInicial + cantidadEntradas - cantidadSalidas);
      
      // Crear un objeto que incluya el ítem original pero con la cantidad calculada
      const inventoryItem = {
        ...mostRecentItem,
        CantidadActualCalculada: cantidadCalculada
      };
      
      // Log detallado para depuración (solo para algunos productos al azar)
      if (Math.random() < 0.05) { // Log ~5% de los productos para no saturar el log
        Logger.log(`Inventario para SKU ${sku}: Inicial=${cantidadInicial}, Entradas=${cantidadEntradas}, Salidas=${cantidadSalidas}, Calculado=${cantidadCalculada}, Original=${mostRecentItem.CantidadActual || 0}`);
      }
      
      inventoryMap[sku] = inventoryItem;
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
 * Busca una variante de producto por SKU en Shopify
 * @param {String} sku - SKU limpio a buscar
 * @returns {Promise<Object>} - Variante encontrada o null
 */
async function findVariantBySku(sku) {
  try {
    // Consulta GraphQL para obtener variantes por SKU
    const query = `
      {
        productVariants(first: 1, query: "sku:${sku}") {
          edges {
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
      Logger.error(`Errores en la consulta GraphQL para SKU ${sku}:`, { message: JSON.stringify(errors) });
      return null;
    }

    return data.productVariants.edges[0]?.node || null;
  } catch (error) {
    Logger.error(`Error al buscar variante con SKU ${sku}:`, error);
    return null;
  }
}

/**
 * Actualiza el precio y/o el inventario de una variante en Shopify
 * @param {Object} variant - Variante de Shopify
 * @param {String} newPrice - Nuevo precio (null si no se actualiza)
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
  
  // Si hay precio para actualizar y está habilitada la actualización de precios
  if (newPrice !== null && (SYNC_TYPE === 'price' || SYNC_TYPE === 'both')) {
    if (parseFloat(variant.price) !== parseFloat(newPrice)) {
      inputs.price = newPrice;
      updateParts.push(`precio: ${variant.price} → ${newPrice}`);
    }
  }
  
  // Si hay nada que actualizar en precio, verificamos si hay que actualizar inventario
  if (Object.keys(inputs).length <= 1 && 
      (newInventory === null || SYNC_TYPE === 'price' || newInventory === variant.inventoryQuantity)) {
    return { success: true, updated: false, message: "No hay cambios para aplicar" };
  }
  
  // Primero actualizamos el precio si es necesario
  if (Object.keys(inputs).length > 1) {
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
  }
  
  // Luego actualizamos el inventario si es necesario
  if (newInventory !== null && (SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') && 
      newInventory !== variant.inventoryQuantity) {
    updateParts.push(`inventario: ${variant.inventoryQuantity} → ${newInventory}`);
    
    // Crear la mutación para ajustar el inventario
    const inventoryMutation = `
      mutation {
        inventoryBulkAdjust(
          inventoryItemAdjustments: [
            {
              inventoryItemId: "${variant.inventoryItem.id}",
              availableDelta: ${newInventory - variant.inventoryQuantity}
            }
          ]
        ) {
          inventoryLevels {
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
    
    const invUserErrors = inventoryResult.data.inventoryBulkAdjust.userErrors;
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
 * Actualiza un producto individual por SKU
 * @param {String} sku - SKU del producto a actualizar
 * @param {Object} localProduct - Datos del producto local
 * @param {Object} inventoryData - Datos del inventario (opcional)
 * @returns {Promise<Object>} - Resultado de la operación
 */
async function updateProductBySku(sku, localProduct, inventoryData = null) {
  try {
    // Buscar la variante en Shopify
    const variant = await findVariantBySku(sku);
    
    if (!variant) {
      return { 
        success: false, 
        error: `No se encontró la variante con SKU ${sku} en Shopify`,
        notFound: true
      };
    }
    
    // Obtener el nuevo precio y cantidad de inventario
    const newPrice = localProduct ? parseFloat(localProduct.Venta1).toFixed(2) : null;
    const inventoryQty = inventoryData ? 
      Math.max(0, Math.floor(parseFloat(inventoryData.CantidadActualCalculada || 0))) : 
      null;
    
    // Actualizar la variante en Shopify
    return await updateVariantInShopify(variant, newPrice, inventoryQty);
  } catch (error) {
    Logger.error(`Error al actualizar producto con SKU ${sku}:`, error);
    return { success: false, error: error.message };
  }
}

/**
 * Función principal que sincroniza datos desde la API local a Shopify
 */
async function syncLocalDataToShopify() {
  try {
    Logger.log(`Iniciando sincronización con Shopify (Tipo: ${SYNC_TYPE.toUpperCase()}, Modo: ${SYNC_MODE.toUpperCase()})`);
    
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
          const inventoryQty = inventoryData ? Math.max(0, parseFloat(inventoryData.CantidadActualCalculada)) : null;
          
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
          const inventoryQty = inventoryData ? Math.max(0, parseFloat(inventoryData.CantidadActualCalculada)) : null;
          
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
    Logger.log(`Modo de sincronización: ${SYNC_MODE.toUpperCase()}`);
    Logger.log(`Tipo de sincronización: ${SYNC_TYPE.toUpperCase()}`);
    Logger.log(`Total de productos locales: ${localProducts.length}`);
    Logger.log(`Total de productos en Shopify: ${shopifyVariants.length}`);
    
    if (SYNC_TYPE === 'price' || SYNC_TYPE === 'both') {
      Logger.log(`Actualizaciones de precio: ${priceUpdates + bothUpdates}`);
    }
    
    if (SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') {
      Logger.log(`Actualizaciones de inventario: ${inventoryUpdates + bothUpdates}`);
    }
    
    if (SYNC_TYPE === 'both') {
      Logger.log(`Actualizaciones de precio e inventario simultáneas: ${bothUpdates}`);
    }
    
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
Logger.log(`Iniciando sincronización con Shopify (${new Date().toLocaleString()})`);
syncLocalDataToShopify()
  .then(() => Logger.log(`Sincronización finalizada (${new Date().toLocaleString()})`))
  .catch(err => Logger.error("Error fatal:", err));