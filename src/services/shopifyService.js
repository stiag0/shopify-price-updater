const axios = require('axios');
const pLimit = require('p-limit');
const NodeCache = require('node-cache');
const config = require('../config/config');
const logger = require('../utils/logger');

class ShopifyService {
  constructor() {
    this.baseUrl = `https://${config.shopify.shopName}.myshopify.com/admin/api/${config.shopify.apiVersion}`;
    this.axiosInstance = axios.create({
      baseURL: this.baseUrl,
      timeout: config.shopify.timeout,
      headers: {
        'X-Shopify-Access-Token': config.shopify.accessToken,
        'Content-Type': 'application/json',
      },
    });

    // Rate limiting setup
    this.limiter = pLimit(config.shopify.rateLimit);
    
    // Cache setup for variant data
    this.cache = new NodeCache({
      stdTTL: config.cache.ttl,
      checkperiod: config.cache.ttl * 0.2,
    });

    // Bind methods
    this.getVariantsBySku = this.getVariantsBySku.bind(this);
    this.updateVariantPrice = this.updateVariantPrice.bind(this);
    this.updateInventoryLevel = this.updateInventoryLevel.bind(this);
  }

  async makeRequest(config, retries = config.shopify.maxRetries) {
    return this.limiter(async () => {
      try {
        const response = await this.axiosInstance(config);
        return response.data;
      } catch (error) {
        if (retries > 0 && this.isRetryableError(error)) {
          const delay = Math.pow(2, config.shopify.maxRetries - retries) * 1000;
          await new Promise(resolve => setTimeout(resolve, delay));
          return this.makeRequest(config, retries - 1);
        }
        throw this.enhanceError(error);
      }
    });
  }

  isRetryableError(error) {
    if (!error.response) return true; // Network errors are retryable
    const status = error.response.status;
    return status === 429 || status === 503 || (status >= 500 && status < 600);
  }

  enhanceError(error) {
    if (error.response?.data) {
      error.message = `Shopify API Error: ${error.response.status} - ${JSON.stringify(error.response.data)}`;
    }
    return error;
  }

  async getVariantsBySku(sku) {
    const cacheKey = `variant_${sku}`;
    const cached = this.cache.get(cacheKey);
    if (cached) return cached;

    const query = `
      query getVariantsBySku($query: String!) {
        productVariants(first: 1, query: $query) {
          edges {
            node {
              id
              sku
              price
              inventoryItem {
                id
                inventoryLevels(first: 1) {
                  edges {
                    node {
                      id
                      available
                      locationId
                    }
                  }
                }
              }
            }
          }
        }
      }
    `;

    const variables = {
      query: `sku:${sku}`,
    };

    const response = await this.makeRequest({
      method: 'POST',
      url: '/graphql.json',
      data: { query, variables },
    });

    const variant = response.data.productVariants.edges[0]?.node;
    if (variant) {
      this.cache.set(cacheKey, variant);
    }
    return variant;
  }

  async updateVariantPrice(variantId, price) {
    const mutation = `
      mutation variantUpdate($input: ProductVariantInput!) {
        productVariantUpdate(input: $input) {
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

    const variables = {
      input: {
        id: variantId,
        price: price.toString(),
      },
    };

    const response = await this.makeRequest({
      method: 'POST',
      url: '/graphql.json',
      data: { query: mutation, variables },
    });

    if (response.data.productVariantUpdate.userErrors.length > 0) {
      throw new Error(JSON.stringify(response.data.productVariantUpdate.userErrors));
    }

    // Invalidate cache for this variant
    const sku = await this.getSkuFromVariantId(variantId);
    if (sku) {
      this.cache.del(`variant_${sku}`);
    }

    return response.data.productVariantUpdate.productVariant;
  }

  async updateInventoryLevel(inventoryItemId, locationId, available) {
    const mutation = `
      mutation inventoryAdjustQuantity($input: InventoryAdjustQuantityInput!) {
        inventoryAdjustQuantity(input: $input) {
          inventoryLevel {
            id
            available
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      input: {
        inventoryItemId,
        locationId,
        availableDelta: available,
      },
    };

    const response = await this.makeRequest({
      method: 'POST',
      url: '/graphql.json',
      data: { query: mutation, variables },
    });

    if (response.data.inventoryAdjustQuantity.userErrors.length > 0) {
      throw new Error(JSON.stringify(response.data.inventoryAdjustQuantity.userErrors));
    }

    return response.data.inventoryAdjustQuantity.inventoryLevel;
  }

  async getSkuFromVariantId(variantId) {
    const query = `
      query getVariant($id: ID!) {
        productVariant(id: $id) {
          sku
        }
      }
    `;

    const variables = {
      id: variantId,
    };

    const response = await this.makeRequest({
      method: 'POST',
      url: '/graphql.json',
      data: { query, variables },
    });

    return response.data.productVariant?.sku;
  }

  async getDefaultLocationId() {
    if (config.sync.locationId) {
      return config.sync.locationId;
    }

    const query = `
      query getLocations {
        locations(first: 1, sortKey: CREATED_AT) {
          edges {
            node {
              id
              isActive
            }
          }
        }
      }
    `;

    const response = await this.makeRequest({
      method: 'POST',
      url: '/graphql.json',
      data: { query },
    });

    const location = response.data.locations.edges[0]?.node;
    if (!location) {
      throw new Error('No active location found');
    }

    return location.id;
  }
}

module.exports = new ShopifyService(); 