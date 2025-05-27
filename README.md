# Shopify Price and Inventory Updater

A robust Node.js application for synchronizing product prices and inventory levels between a local database and Shopify, with support for discounts and comprehensive error handling.

## Features

- **Robust Error Handling**: Comprehensive error catching and logging for all operations
- **Rate Limiting**: Built-in rate limiting to prevent API throttling
- **Caching**: Efficient caching of Shopify variant data to reduce API calls
- **Batch Processing**: Optimized batch processing of updates
- **Discount Support**: CSV-based discount application system
- **Detailed Logging**: Structured logging with Winston for better debugging
- **Data Validation**: Thorough validation of all data before processing
- **Graceful Shutdown**: Proper handling of shutdown signals
- **Flexible Configuration**: Environment-based configuration for all settings

## Prerequisites

- Node.js 14 or higher
- A Shopify store with Admin API access
- Access to product and inventory data APIs

## Installation

1. Clone the repository:
   ```bash
   git clone [repository-url]
   cd shopify-price-updater
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Create and configure your environment file:
   ```bash
   cp .env.example .env
   ```

4. Edit the `.env` file with your configuration:
   ```env
   SHOPIFY_SHOP_NAME=your-shop-name
   SHOPIFY_ACCESS_TOKEN=your-access-token
   DATA_API_URL=http://your-api/products
   INVENTORY_API_URL=http://your-api/inventory
   ```

## Configuration

### Required Environment Variables

- `SHOPIFY_SHOP_NAME`: Your Shopify shop name
- `SHOPIFY_ACCESS_TOKEN`: Your Shopify Admin API access token
- `DATA_API_URL`: URL to fetch product data
- `INVENTORY_API_URL`: URL to fetch inventory data

### Optional Environment Variables

- `SHOPIFY_RATE_LIMIT`: API rate limit (requests per second, default: 2)
- `SHOPIFY_BATCH_SIZE`: Number of items to process in each batch (default: 250)
- `MAX_RETRIES`: Maximum number of retry attempts for failed requests (default: 3)
- `API_TIMEOUT`: API request timeout in milliseconds (default: 30000)
- `LOCATION_ID`: Shopify location ID for inventory updates
- `SYNC_MODE`: Sync mode ('shopify_first' or 'local_first', default: 'shopify_first')
- `SYNC_TYPE`: What to sync ('price', 'inventory', or 'both', default: 'both')
- `DISCOUNT_CSV_PATH`: Path to the discounts CSV file
- `LOG_LEVEL`: Logging level (default: 'info')
- `LOG_DIR`: Directory for log files (default: 'logs')
- `LOG_MAX_SIZE`: Maximum log file size in MB (default: 100)
- `LOG_MAX_FILES`: Maximum number of log files to keep (default: 5)
- `CACHE_TTL`: Cache time-to-live in seconds (default: 3600)

## Usage

1. Start the application:
   ```bash
   npm start
   ```

2. Monitor the logs in the `logs` directory for progress and any issues.

### Discount CSV Format

Create a CSV file with the following format to apply discounts:

```csv
sku,discount
ABC123,15
XYZ789,20
```

- `sku`: Product SKU
- `discount`: Discount percentage (0-100)

## Logging

Logs are written to:
- `logs/update-YYYY-MM-DD.log`: Daily operation logs
- `logs/error.log`: Error-specific logs

## Error Handling

The application handles various error scenarios:
- Network connectivity issues
- API rate limiting
- Invalid data formats
- Missing products
- Authentication failures

Each error is logged with:
- Timestamp
- Error type
- Affected SKU
- Stack trace (when available)
- Relevant data context

## Troubleshooting

1. Check the logs in the `logs` directory for detailed error information
2. Verify your API endpoints are accessible
3. Confirm your Shopify API credentials are correct
4. Ensure your data format matches the expected schema

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License. 