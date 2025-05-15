const path = require('path');
const Logger = require('./common/logger'); // Asegúrate que la ruta a logger.js sea correcta
const { 
    DISCOUNT_CSV_PATH, 
    SYNC_TYPE = 'both', 
    LOG_DIR, // Opcional, si quieres configurar el directorio de logs desde config.js
    LOG_MAX_SIZE_MB // Opcional, para el tamaño máximo del log
} = require('./common/config');
const { loadDiscounts } = require('./common/utils');

const { runPriceUpdate, getAllShopifyVariantsForPricing } = require('./price-update');
const { runInventoryUpdate, getActiveLocationId, getAllShopifyVariantsForInventory } = require('./inventory-update');

async function main() {
    const executionStartTime = new Date();
    // Inicializar el logger con la hora de inicio de esta ejecución.
    // Puedes pasar LOG_DIR y LOG_MAX_SIZE_MB desde tu config si están definidos allí.
    Logger.init(executionStartTime, LOG_DIR, LOG_MAX_SIZE_MB); 

    Logger.log(`🚀 INICIANDO SINCRONIZACIÓN GENERAL (Tipo: ${SYNC_TYPE.toUpperCase()}) - ${executionStartTime.toLocaleString()}`);
    
    let discountMap = new Map();
    let activeLocationId = null;
    let allShopifyVariants = null;

    try {
        Logger.log("--- Paso 1: Cargando Descuentos ---");
        try {
            discountMap = await loadDiscounts(DISCOUNT_CSV_PATH);
        } catch (e) {
            Logger.warn(`No se pudieron cargar los descuentos desde ${DISCOUNT_CSV_PATH}. Continuando sin ellos. Error: ${e.message}`);
        }

        if (SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') {
            Logger.log("--- Paso 2: Obteniendo Location ID de Shopify ---");
            activeLocationId = await getActiveLocationId();
            if (!activeLocationId) {
                Logger.error("No se pudo obtener un Location ID activo. La actualización de inventario no procederá.");
                if (SYNC_TYPE === 'inventory') {
                    throw new Error("Location ID es requerido para la sincronización de inventario y no se pudo obtener.");
                }
            }
        }

        Logger.log("--- Paso 3: Obteniendo Variantes de Shopify ---");
        if (SYNC_TYPE === 'both') {
            Logger.log("(Usando consulta de variantes para precios y potencialmente inventario)");
            // Idealmente, getAllShopifyVariantsForPricing pediría todos los campos necesarios para ambos.
            // Si getAllShopifyVariantsForInventory pide campos muy diferentes, considera una función combinada
            // o llamar a ambas si es necesario y fusionar resultados (complejo).
            allShopifyVariants = await getAllShopifyVariantsForPricing(); // Asume que esta es suficiente
        } else if (SYNC_TYPE === 'price') {
            allShopifyVariants = await getAllShopifyVariantsForPricing();
        } else if (SYNC_TYPE === 'inventory' && activeLocationId) {
            allShopifyVariants = await getAllShopifyVariantsForInventory();
        } else {
            Logger.log("No se obtendrán variantes de Shopify (no se actualizará precio/inventario, o falta Location ID).");
            allShopifyVariants = [];
        }
        Logger.log(`Total de variantes de Shopify obtenidas para procesar: ${allShopifyVariants.length}`);


        if (SYNC_TYPE === 'price' || SYNC_TYPE === 'both') {
            if (allShopifyVariants.length > 0 || discountMap.size > 0) { // Solo ejecutar si hay algo que procesar
                Logger.log("\n--- Iniciando Sub-proceso: Actualización de Precios ---");
                await runPriceUpdate(allShopifyVariants, discountMap);
            } else {
                Logger.log("\n--- Sub-proceso: Actualización de Precios OMITIDO (no hay variantes de Shopify o descuentos cargados) ---");
            }
        }

        if ((SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') && activeLocationId) {
            if (allShopifyVariants.length > 0) { // Solo ejecutar si hay variantes
                Logger.log("\n--- Iniciando Sub-proceso: Actualización de Inventario ---");
                await runInventoryUpdate(allShopifyVariants, activeLocationId);
            } else {
                 Logger.log("\n--- Sub-proceso: Actualización de Inventario OMITIDO (no hay variantes de Shopify para procesar) ---");
            }
        } else if ((SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') && !activeLocationId) {
            Logger.warn("Se omitió la actualización de inventario porque no se pudo obtener un Location ID activo.");
        }

    } catch (error) {
        Logger.error('💥 ERROR FATAL en el proceso de sincronización principal', error);
        process.exitCode = 1; // Indicar error al sistema operativo
    } finally {
        const executionEndTime = Date.now();
        const duration = ((executionEndTime - executionStartTime.getTime()) / 1000).toFixed(2);
        Logger.log(`🏁 SINCRONIZACIÓN GENERAL FINALIZADA. Duración total: ${duration} segundos - ${new Date(executionEndTime).toLocaleString()}`);
        
        // Asegurar que todos los logs en cola se escriban antes de salir.
        await Logger.flush(); 
        // Pequeña pausa para asegurar que el flush termine, especialmente si hay escrituras de último momento.
        // No siempre es necesario si Logger.flush() es robusto.
        // await new Promise(resolve => setTimeout(resolve, 500)); 
    }
}

main();

// Manejadores de cierre para asegurar el flush de logs
async function gracefulShutdown(signal) {
    Logger.warn(`Recibido ${signal}. Finalizando ordenadamente...`);
    if (Logger) { // Asegurarse que Logger esté disponible
        await Logger.flush();
    }
    process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT')); // Ctrl+C
process.on('SIGTERM', () => gracefulShutdown('SIGTERM')); // kill
