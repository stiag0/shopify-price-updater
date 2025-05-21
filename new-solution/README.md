# Shopify Price and Inventory Updater

A tool to synchronize product prices and inventory levels between a local database and Shopify, with support for discounts.

## Key Features

- Compatible with Shopify Admin API 2025-04
- Syncs both prices and inventory quantities
- Supports applying discounts from CSV files
- Supports both local-first and Shopify-first sync modes
- Comprehensive error handling and logging system
- Creates a new timestamped log file for each run
- Rate limiting to prevent API throttling
- Graceful shutdown handling

## Requirements

- Node.js 14+
- npm modules: axios, limiter, csv-parser, dotenv

## Setup

1. Install dependencies:
   ```
   npm install axios limiter csv-parser dotenv
   ```

2. Create a `.env` file with the following variables:
   ```
   SHOPIFY_SHOP_NAME=your-shop-name
   SHOPIFY_ACCESS_TOKEN=your-access-token
   DATA_API_URL=your-product-data-api-url
   INVENTORY_API_URL=your-inventory-api-url
   LOCATION_ID=gid://shopify/Location/your-location-id (optional for single-location stores)
   DISCOUNT_CSV_PATH=path-to-discounts.csv (optional)
   ```

3. Optional: Create a `discounts.csv` file with SKUs and discount percentages:
   ```
   sku,discount
   12345,15
   67890,20
   ```

## Usage

```
node shopify-updater.js
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| SHOPIFY_SHOP_NAME | Your Shopify shop name (required) | |
| SHOPIFY_ACCESS_TOKEN | Your Shopify Admin API access token (required) | |
| DATA_API_URL | URL to fetch product data (required) | |
| INVENTORY_API_URL | URL to fetch inventory data (required) | |
| LOCATION_ID | Shopify location ID for inventory (optional for single-location) | First active location |
| DISCOUNT_CSV_PATH | Path to CSV file with SKU discounts | discounts.csv |
| SYNC_MODE | Sync mode: 'shopify_first' or 'local_first' | shopify_first |
| SYNC_TYPE | What to sync: 'price', 'inventory', or 'both' | both |
| MAX_RETRIES | Max API retry attempts | 3 |
| SHOPIFY_RATE_LIMIT | API rate limit (requests per second) | 2 |
| LOG_FILE_PATH | Path to log file | logs/shopify-sync.log |
| LOG_MAX_SIZE | Max log file size in MB | 100 |

## Fixes in This Version

This version addresses the GraphQL mutation name changes in Shopify API 2025-04, specifically:
- Changed `productVariantUpdate` to `variantUpdate` in the price update mutation
- Properly structured the response handling for the updated mutation
- Set the API version explicitly to 2025-04

## Notes

- This script is designed to work with Shopify API version 2025-04 or later
- The script cleans SKUs by keeping only numeric values and removing leading zeros
- Products are matched between Shopify and the local database using SKUs