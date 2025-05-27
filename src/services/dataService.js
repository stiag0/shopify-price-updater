const axios = require('axios');
const { parse } = require('csv-parse');
const fs = require('fs').promises;
const config = require('../config/config');
const logger = require('../utils/logger');

class DataService {
  constructor() {
    this.axiosInstance = axios.create({
      timeout: config.shopify.timeout,
    });
  }

  async getLocalProducts() {
    try {
      const response = await this.axiosInstance.get(config.apis.dataUrl);
      return this.normalizeProducts(response.data);
    } catch (error) {
      logger.error('Failed to fetch local products', { error: error.message, stack: error.stack });
      throw new Error(`Failed to fetch local products: ${error.message}`);
    }
  }

  async getLocalInventory() {
    try {
      const response = await this.axiosInstance.get(config.apis.inventoryUrl);
      return this.normalizeInventory(response.data);
    } catch (error) {
      logger.error('Failed to fetch local inventory', { error: error.message, stack: error.stack });
      throw new Error(`Failed to fetch local inventory: ${error.message}`);
    }
  }

  async loadDiscounts() {
    try {
      const discounts = new Map();
      
      if (!config.discounts.csvPath) {
        return discounts;
      }

      const fileContent = await fs.readFile(config.discounts.csvPath, 'utf-8');
      
      return new Promise((resolve, reject) => {
        parse(fileContent, {
          columns: true,
          skip_empty_lines: true,
          trim: true,
        })
        .on('data', (row) => {
          if (row.sku && row.discount) {
            const discount = parseFloat(row.discount);
            if (!isNaN(discount)) {
              discounts.set(this.cleanSku(row.sku), discount);
            }
          }
        })
        .on('error', (error) => {
          logger.error('Error parsing discounts CSV', { error: error.message });
          reject(error);
        })
        .on('end', () => {
          resolve(discounts);
        });
      });
    } catch (error) {
      if (error.code === 'ENOENT') {
        logger.warn('Discounts file not found, proceeding without discounts', {
          path: config.discounts.csvPath,
        });
        return new Map();
      }
      throw error;
    }
  }

  normalizeProducts(data) {
    if (!Array.isArray(data)) {
      throw new Error('Product data must be an array');
    }

    return data.map(product => ({
      sku: this.cleanSku(product.sku || product.SKU),
      price: this.normalizePrice(product.price || product.PRICE),
      name: product.name || product.NAME,
      originalData: product,
    })).filter(product => product.sku && !isNaN(product.price));
  }

  normalizeInventory(data) {
    if (!Array.isArray(data)) {
      throw new Error('Inventory data must be an array');
    }

    return data.map(item => ({
      sku: this.cleanSku(item.sku || item.SKU),
      quantity: this.normalizeQuantity(item.quantity || item.QUANTITY),
      originalData: item,
    })).filter(item => item.sku && !isNaN(item.quantity));
  }

  cleanSku(sku) {
    if (!sku) return null;
    // Remove any non-alphanumeric characters except hyphen and underscore
    return sku.toString().trim().replace(/[^a-zA-Z0-9\-_]/g, '');
  }

  normalizePrice(price) {
    if (typeof price === 'string') {
      // Remove currency symbols and convert to number
      price = price.replace(/[^0-9.-]/g, '');
    }
    const normalizedPrice = parseFloat(price);
    return isNaN(normalizedPrice) ? null : Math.round(normalizedPrice * 100) / 100;
  }

  normalizeQuantity(quantity) {
    if (typeof quantity === 'string') {
      quantity = quantity.replace(/[^0-9.-]/g, '');
    }
    const normalizedQuantity = parseInt(quantity, 10);
    return isNaN(normalizedQuantity) ? 0 : normalizedQuantity;
  }

  applyDiscount(price, discount) {
    if (!discount || discount <= 0 || discount >= 100) return price;
    return Math.round((price * (100 - discount)) / 100 * 100) / 100;
  }
}

module.exports = new DataService(); 