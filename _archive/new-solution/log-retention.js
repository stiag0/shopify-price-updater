require('dotenv').config();
const fs = require('fs').promises;
const path = require('path');

// Get configuration from environment variables
const {
    LOG_DIR = path.join(process.cwd(), 'logs'),
    LOG_MAX_SIZE_MB = '100',
    LOG_DRY_RUN = 'false',
    LOG_VERBOSE = 'true'
} = process.env;

class LogRetentionManager {
    constructor(options = {}) {
        this.logDir = options.logDir || LOG_DIR;
        this.maxSizeMB = options.maxSizeMB || parseInt(LOG_MAX_SIZE_MB);
        this.dryRun = options.dryRun || LOG_DRY_RUN.toLowerCase() === 'true';
        this.verbose = options.verbose || LOG_VERBOSE.toLowerCase() === 'true';
    }

    async getFileStats(filePath) {
        try {
            const stats = await fs.stat(filePath);
            return {
                path: filePath,
                size: stats.size,
                created: stats.birthtime,
                modified: stats.mtime
            };
        } catch (error) {
            console.error(`Error getting stats for ${filePath}:`, error.message);
            return null;
        }
    }

    formatSize(bytes) {
        const units = ['B', 'KB', 'MB', 'GB'];
        let size = bytes;
        let unitIndex = 0;
        
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        
        return `${size.toFixed(2)} ${units[unitIndex]}`;
    }

    async cleanupLogs() {
        try {
            // Ensure log directory exists
            await fs.mkdir(this.logDir, { recursive: true });

            // Read all files in the log directory
            const files = await fs.readdir(this.logDir);
            
            if (files.length === 0) {
                this.log('No log files found.');
                return;
            }

            // Get stats for all files
            const fileStats = await Promise.all(
                files
                    .filter(file => file.endsWith('.log')) // Only process .log files
                    .map(file => this.getFileStats(path.join(this.logDir, file)))
            );

            // Filter out any null results and sort by creation date (oldest first)
            const validStats = fileStats
                .filter(stat => stat !== null)
                .sort((a, b) => a.created - b.created);

            // Calculate total size
            const totalSize = validStats.reduce((sum, stat) => sum + stat.size, 0);
            const maxSizeBytes = this.maxSizeMB * 1024 * 1024;

            this.log(`Current log directory size: ${this.formatSize(totalSize)}`);
            this.log(`Maximum allowed size: ${this.formatSize(maxSizeBytes)}`);

            if (totalSize <= maxSizeBytes) {
                this.log('Log directory is within size limits. No cleanup needed.');
                return;
            }

            let sizeToFree = totalSize - maxSizeBytes;
            let filesDeleted = 0;
            let sizeFreed = 0;

            this.log(`Need to free up: ${this.formatSize(sizeToFree)}`);

            // Delete oldest files until we're under the limit
            for (const stat of validStats) {
                if (sizeFreed >= sizeToFree) break;

                if (this.dryRun) {
                    this.log(`[DRY RUN] Would delete: ${path.basename(stat.path)} (${this.formatSize(stat.size)})`);
                } else {
                    try {
                        await fs.unlink(stat.path);
                        this.log(`Deleted: ${path.basename(stat.path)} (${this.formatSize(stat.size)})`);
                    } catch (error) {
                        console.error(`Error deleting ${stat.path}:`, error.message);
                        continue;
                    }
                }

                sizeFreed += stat.size;
                filesDeleted++;
            }

            this.log(`\nCleanup Summary:`);
            this.log(`Files processed: ${validStats.length}`);
            this.log(`Files ${this.dryRun ? 'to be deleted' : 'deleted'}: ${filesDeleted}`);
            this.log(`Space ${this.dryRun ? 'to be freed' : 'freed'}: ${this.formatSize(sizeFreed)}`);
            this.log(`New total size: ${this.formatSize(totalSize - sizeFreed)}`);

        } catch (error) {
            console.error('Error during log cleanup:', error.message);
            throw error;
        }
    }

    log(message) {
        if (this.verbose) {
            console.log(message);
        }
    }
}

// Example usage
async function main() {
    const manager = new LogRetentionManager({
        logDir: path.join(__dirname, 'logs'),
        maxSizeMB: 100,
        dryRun: false,
        verbose: true
    });

    try {
        await manager.cleanupLogs();
    } catch (error) {
        console.error('Failed to run log retention:', error);
        process.exit(1);
    }
}

// Run if called directly
if (require.main === module) {
    main();
}

module.exports = LogRetentionManager; 