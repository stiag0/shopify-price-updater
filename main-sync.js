const path = require('path'); // Necesario si config.js usa __dirname y es llamado desde aquí
const Logger = require('./common/logger');
const { DISCOUNT_CSV_PATH, SYNC_TYPE = 'both' } = require('./common/config'); // Leer SYNC_TYPE desde config
const { loadDiscounts, cleanSku } = require('./common/utils'); // Asumiendo que cleanSku también está en utils

// Importar las funciones principales de los módulos de actualización
const { runPriceUpdate, getAllShopifyVariantsForPricing } = require('./price-update');
const { runInventoryUpdate, getActiveLocationId, getAllShopifyVariantsForInventory } = require('./inventory-update');

async function main() {
    Logger.init(); // Inicializar el logger una vez
    Logger.log(`🚀 INICIANDO SINCRONIZACIÓN GENERAL (Tipo: ${SYNC_TYPE.toUpperCase()}) - ${new Date().toLocaleString()}`);
    const startTime = Date.now();

    let discountMap = new Map();
    let activeLocationId = null;
    let allShopifyVariants = null; // Para guardar las variantes y no pedirlas dos veces

    try {
        // 1. Cargar descuentos (siempre, price-update lo usará)
        Logger.log("--- Paso 1: Cargando Descuentos ---");
        try {
            discountMap = await loadDiscounts(DISCOUNT_CSV_PATH);
        } catch (e) {
            Logger.warn(`No se pudieron cargar los descuentos desde ${DISCOUNT_CSV_PATH}. Continuando sin ellos. Error: ${e.message}`);
        }

        // 2. Obtener Location ID si se actualizará inventario
        if (SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') {
            Logger.log("--- Paso 2: Obteniendo Location ID de Shopify ---");
            activeLocationId = await getActiveLocationId();
            if (!activeLocationId) {
                Logger.error("No se pudo obtener un Location ID activo. La actualización de inventario no procederá.");
                // Decidir si continuar solo con precios o detenerse
                if (SYNC_TYPE === 'inventory') { // Si solo era inventario, no hay nada más que hacer
                    throw new Error("Location ID es requerido para la sincronización de inventario y no se pudo obtener.");
                }
            }
        }

        // 3. Obtener todas las variantes de Shopify UNA SOLA VEZ
        // Escoger la consulta más completa si se actualizan ambos, o la específica si solo uno
        Logger.log("--- Paso 3: Obteniendo Variantes de Shopify ---");
        if (SYNC_TYPE === 'both') {
            // Aquí podríamos necesitar una función getAllShopifyVariants que pida TODOS los campos necesarios
            // Por simplicidad, usaremos la de precios que pide compareAtPrice.
            // Si inventario necesita más campos específicos, se podría crear una consulta combinada.
            Logger.log("(Usando consulta de variantes para precios, que incluye 'compareAtPrice')");
            allShopifyVariants = await getAllShopifyVariantsForPricing();
        } else if (SYNC_TYPE === 'price') {
            allShopifyVariants = await getAllShopifyVariantsForPricing();
        } else if (SYNC_TYPE === 'inventory' && activeLocationId) { // Solo si hay locationId para inventario
            allShopifyVariants = await getAllShopifyVariantsForInventory();
        } else {
            Logger.log("No se obtendrán variantes de Shopify ya que no se actualizará ni precio ni inventario, o falta Location ID para inventario.");
            allShopifyVariants = [];
        }


        // 4. Ejecutar actualización de precios
        if (SYNC_TYPE === 'price' || SYNC_TYPE === 'both') {
            Logger.log("\n--- Iniciando Sub-proceso: Actualización de Precios ---");
            await runPriceUpdate(allShopifyVariants, discountMap); // Pasar variantes y descuentos
        }

        // 5. Ejecutar actualización de inventario
        if ((SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') && activeLocationId) { // Solo si se necesita y tenemos locationId
            Logger.log("\n--- Iniciando Sub-proceso: Actualización de Inventario ---");
            await runInventoryUpdate(allShopifyVariants, activeLocationId); // Pasar variantes y locationId
        } else if ((SYNC_TYPE === 'inventory' || SYNC_TYPE === 'both') && !activeLocationId) {
            Logger.warn("Se omitió la actualización de inventario porque no se pudo obtener un Location ID activo.");
        }

    } catch (error) {
        Logger.error('💥 ERROR FATAL en el proceso de sincronización principal', error);
        process.exitCode = 1;
    } finally {
        const endTime = Date.now();
        const duration = ((endTime - startTime) / 1000).toFixed(2);
        Logger.log(`🏁 SINCRONIZACIÓN GENERAL FINALIZADA. Duración total: ${duration} segundos - ${new Date().toLocaleString()}`);
        await Logger.processQueue(); // Asegurar que todos los logs se escriban
        await new Promise(resolve => setTimeout(resolve, 300)); // Pequeña pausa para flush de logs
    }
}

// Ejecutar el orquestador principal
main();

// Manejadores de cierre (igual que antes)
process.on('SIGINT', async () => { Logger.log('Received SIGINT. Shutting down gracefully...'); await Logger.processQueue(); process.exit(0); });
process.on('SIGTERM', async () => { Logger.log('Received SIGTERM. Shutting down gracefully...'); await Logger.processQueue(); process.exit(0); });