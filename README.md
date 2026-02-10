# Shopify Direct Price Updater

A simple Node.js script to update Shopify product prices directly from a CSV file.

## Features

- Reads product prices from a CSV file
- Updates Shopify product prices using the Admin API
- Handles rate limiting for Shopify API calls
- Provides detailed logging and statistics
- Simple and straightforward price updates without complex calculations

## Prerequisites

- Node.js 14.x or higher
- A Shopify store with Admin API access
- A CSV file with SKUs and prices

## Installation

1. Clone this repository
2. Install dependencies:
```bash
npm install
```

## Configuration

Create a `.env` file in the root directory with the following variables:

```env
SHOPIFY_SHOP_NAME=your-store-name
SHOPIFY_ACCESS_TOKEN=your-access-token
CSV_FILE_PATH=path/to/your/prices.csv
SHOPIFY_RATE_LIMIT=2
```

### Environment Variables

- `SHOPIFY_SHOP_NAME`: Your Shopify store name (without .myshopify.com)
- `SHOPIFY_ACCESS_TOKEN`: Your Shopify Admin API access token
- `CSV_FILE_PATH`: Path to your CSV file containing SKUs and prices
- `SHOPIFY_RATE_LIMIT`: (Optional) API rate limit per second (default: 2)

## CSV File Format

Your CSV file should have the following columns:
- `sku`: The product SKU
- `price`: The new price for the product

Example:
```csv
sku,price
ABC123,19.99
XYZ789,29.99
```

## Usage

Run the script:
```bash
node shopify-direct-price-updater.js
```

The script will:
1. Load all variants from your Shopify store
2. Read the CSV file
3. Update prices for matching SKUs
4. Display a summary of the updates

## Error Handling

The script includes error handling for:
- Missing environment variables
- Invalid CSV format
- Network errors
- API errors
- Rate limiting

## Logging

The script provides detailed logging of:
- Start and completion of the process
- Each price update
- Skipped items (with reasons)
- Errors
- Final statistics

## Notes

- The script uses Shopify's GraphQL Admin API
- Rate limiting is implemented to avoid API throttling
- Only prices are updated; other variant properties remain unchanged
- SKUs must match exactly for updates to occur 