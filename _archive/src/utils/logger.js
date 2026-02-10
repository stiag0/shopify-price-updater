const winston = require('winston');
const path = require('path');
const fs = require('fs');
const config = require('../config/config');

// Ensure logs directory exists
if (!fs.existsSync(config.logging.directory)) {
  fs.mkdirSync(config.logging.directory, { recursive: true });
}

const logFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

const logger = winston.createLogger({
  level: config.logging.level,
  format: logFormat,
  defaultMeta: { service: 'shopify-updater' },
  transports: [
    new winston.transports.File({
      filename: path.join(config.logging.directory, 'error.log'),
      level: 'error',
      maxsize: config.logging.maxSize,
      maxFiles: config.logging.maxFiles,
    }),
    new winston.transports.File({
      filename: path.join(config.logging.directory, `update-${new Date().toISOString().split('T')[0]}.log`),
      maxsize: config.logging.maxSize,
      maxFiles: config.logging.maxFiles,
    }),
  ],
});

// Add console output if not in production
if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.combine(
      winston.format.colorize(),
      winston.format.simple()
    ),
  }));
}

// Add methods for structured logging
logger.logProductUpdate = (sku, oldPrice, newPrice, success, error = null) => {
  const logData = {
    event: 'product_update',
    sku,
    oldPrice,
    newPrice,
    success,
    ...(error && { error: error.message, stack: error.stack }),
  };
  
  if (success) {
    logger.info('Product price update', logData);
  } else {
    logger.error('Product price update failed', logData);
  }
};

logger.logInventoryUpdate = (sku, oldQuantity, newQuantity, success, error = null) => {
  const logData = {
    event: 'inventory_update',
    sku,
    oldQuantity,
    newQuantity,
    success,
    ...(error && { error: error.message, stack: error.stack }),
  };
  
  if (success) {
    logger.info('Inventory update', logData);
  } else {
    logger.error('Inventory update failed', logData);
  }
};

logger.logBatchOperation = (operation, totalItems, successCount, failureCount, errors = []) => {
  logger.info('Batch operation completed', {
    event: 'batch_operation',
    operation,
    totalItems,
    successCount,
    failureCount,
    errors: errors.map(e => ({
      message: e.message,
      item: e.item,
      stack: e.stack,
    })),
  });
};

module.exports = logger; 