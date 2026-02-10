const shopifyService = require('./services/shopifyService');
const dataService = require('./services/dataService');
const logger = require('./utils/logger');
const config = require('./config/config');

class ShopifyUpdater {
  constructor() {
    this.locationId = null;
    this.discounts = new Map();
    this.stats = {
      priceUpdates: { success: 0, failed: 0 },
      inventoryUpdates: { success: 0, failed: 0 },
      errors: [],
    };
  }

  async initialize() {
    try {
      logger.info('Initializing Shopify updater');
      this.locationId = await shopifyService.getDefaultLocationId();
      this.discounts = await dataService.loadDiscounts();
      logger.info('Initialization complete', {
        locationId: this.locationId,
        discountsLoaded: this.discounts.size,
      });
    } catch (error) {
      logger.error('Failed to initialize', { error: error.message, stack: error.stack });
      throw error;
    }
  }

  async processProduct(localProduct) {
    try {
      const variant = await shopifyService.getVariantsBySku(localProduct.sku);
      
      if (!variant) {
        logger.warn('Product not found in Shopify', { sku: localProduct.sku });
        return;
      }

      // Apply discount if available
      const discount = this.discounts.get(localProduct.sku);
      const finalPrice = discount ? 
        dataService.applyDiscount(localProduct.price, discount) : 
        localProduct.price;

      // Only update if prices are different
      if (parseFloat(variant.price) !== finalPrice) {
        await shopifyService.updateVariantPrice(variant.id, finalPrice);
        this.stats.priceUpdates.success++;
        logger.logProductUpdate(localProduct.sku, variant.price, finalPrice, true);
      }
    } catch (error) {
      this.stats.priceUpdates.failed++;
      this.stats.errors.push({ sku: localProduct.sku, error: error.message });
      logger.logProductUpdate(localProduct.sku, null, null, false, error);
    }
  }

  async processInventory(localInventory) {
    try {
      const variant = await shopifyService.getVariantsBySku(localInventory.sku);
      
      if (!variant) {
        logger.warn('Product not found in Shopify', { sku: localInventory.sku });
        return;
      }

      const inventoryLevel = variant.inventoryItem.inventoryLevels.edges[0]?.node;
      if (!inventoryLevel) {
        logger.warn('No inventory level found', { sku: localInventory.sku });
        return;
      }

      const currentQuantity = inventoryLevel.available;
      if (currentQuantity !== localInventory.quantity) {
        const delta = localInventory.quantity - currentQuantity;
        await shopifyService.updateInventoryLevel(
          variant.inventoryItem.id,
          this.locationId,
          delta
        );
        this.stats.inventoryUpdates.success++;
        logger.logInventoryUpdate(localInventory.sku, currentQuantity, localInventory.quantity, true);
      }
    } catch (error) {
      this.stats.inventoryUpdates.failed++;
      this.stats.errors.push({ sku: localInventory.sku, error: error.message });
      logger.logInventoryUpdate(localInventory.sku, null, null, false, error);
    }
  }

  async syncProducts() {
    try {
      const products = await dataService.getLocalProducts();
      logger.info('Starting price sync', { totalProducts: products.length });

      for (const product of products) {
        await this.processProduct(product);
      }

      logger.logBatchOperation(
        'price_sync',
        products.length,
        this.stats.priceUpdates.success,
        this.stats.priceUpdates.failed,
        this.stats.errors
      );
    } catch (error) {
      logger.error('Failed to sync products', { error: error.message, stack: error.stack });
      throw error;
    }
  }

  async syncInventory() {
    try {
      const inventory = await dataService.getLocalInventory();
      logger.info('Starting inventory sync', { totalItems: inventory.length });

      for (const item of inventory) {
        await this.processInventory(item);
      }

      logger.logBatchOperation(
        'inventory_sync',
        inventory.length,
        this.stats.inventoryUpdates.success,
        this.stats.inventoryUpdates.failed,
        this.stats.errors
      );
    } catch (error) {
      logger.error('Failed to sync inventory', { error: error.message, stack: error.stack });
      throw error;
    }
  }

  async run() {
    try {
      await this.initialize();

      if (config.sync.type === 'price' || config.sync.type === 'both') {
        await this.syncProducts();
      }

      if (config.sync.type === 'inventory' || config.sync.type === 'both') {
        await this.syncInventory();
      }

      logger.info('Sync completed', {
        priceUpdates: this.stats.priceUpdates,
        inventoryUpdates: this.stats.inventoryUpdates,
        totalErrors: this.stats.errors.length,
      });
    } catch (error) {
      logger.error('Sync failed', { error: error.message, stack: error.stack });
      process.exit(1);
    }
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  logger.info('Received SIGINT. Graceful shutdown initiated.');
  process.exit(0);
});

process.on('SIGTERM', () => {
  logger.info('Received SIGTERM. Graceful shutdown initiated.');
  process.exit(0);
});

// Run the updater
const updater = new ShopifyUpdater();
updater.run().catch(error => {
  logger.error('Fatal error', { error: error.message, stack: error.stack });
  process.exit(1);
}); 