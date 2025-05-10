const fs = require('fs');
const path = require('path');
const { LOG_FILE_PATH, LOG_MAX_SIZE_BYTES } = require('./common/config');

class Logger {
    constructor() {
        this.logDir = path.dirname(LOG_FILE_PATH);
        this.logPath = LOG_FILE_PATH;
        this.logQueue = [];
        this.isWriting = false;
        this.currentLogStartTime = null;
    }
    
    formatDateForFilename(date) {
        if (!date || !(date instanceof Date) || isNaN(date)) {
          console.error("Invalid date passed to formatDateForFilename");
          return new Date().toISOString().replace(/:/g, '-').replace(/\..+/, '');
        }
        
        const year = date.getFullYear();
        const mm   = String(date.getMonth() + 1).padStart(2,'0');
        const dd   = String(date.getDate()).padStart(2,'0');
        const hh   = String(date.getHours()).padStart(2,'0');
        const min  = String(date.getMinutes()).padStart(2,'0');
        const ss   = String(date.getSeconds()).padStart(2,'0');
        return `${year}${mm}${dd}-${hh}${min}${ss}`;
      }

    init() {
        try {
            if (!fs.existsSync(this.logDir)) {
                fs.mkdirSync(this.logDir, { recursive: true });
            }
            if (!fs.existsSync(this.logPath)) {
                // File doesn't exist, create it and set start time
                this.currentLogStartTime = new Date();
                fs.writeFileSync(this.logPath, `[${this.currentLogStartTime.toISOString()}] [INFO] Logger initialized. New log file created.\n`);
            }else {
                // File exists, estimate start time from file stats (birthtime or mtime)
                try {
                    const stats = fs.statSync(this.logPath);
                    // Prefer birthtime, fallback to mtime
                    this.currentLogStartTime = stats.birthtimeMs ? new Date(stats.birthtimeMs) : new Date(stats.mtimeMs);
                    // Log initialization without using Logger.log to avoid queue issues during init
                    console.log(`[${new Date().toISOString()}] [INFO] Logger initialized. Existing log file found. Estimated start time: ${this.currentLogStartTime.toISOString()}`);
                    // Optionally write directly to file if needed, but console log is safer during init
                    // fs.appendFileSync(this.logPath, `[${new Date().toISOString()}] [INFO] Logger continuing in existing file. Estimated start time: ${this.currentLogStartTime.toISOString()}\n`);

                } catch (statError) {
                    // If stats fail, default to now
                    console.error(`[${new Date().toISOString()}] [ERROR] Could not get stats for existing log file. Setting start time to now. ${statError.message}`);
                    this.currentLogStartTime = new Date();
                     // Attempt to write error to log file if possible
                     try {
                         fs.appendFileSync(this.logPath, `[${new Date().toISOString()}] [ERROR] Could not get stats for existing log file. Setting start time to now. ${statError.message}\n`);
                     } catch (appendErr) { /* Ignore if append fails */ }
                }
            }
        } catch (error) {
            console.error(`Fatal Error: Could not initialize logger at ${this.logPath}. ${error.message}`);
            process.exit(1);
        }
    }

    async processQueue() {
        if (this.isWriting || this.logQueue.length === 0) {
            return;
        }
        this.isWriting = true;

        await this.checkLogSize();

        const messagesToWrite = this.logQueue.splice(0, this.logQueue.length);
        const logContent = messagesToWrite.join('');

        try {
            // Check if the log file still exists (it might have been rotated)
            if (!fs.existsSync(this.logPath)) {
                console.warn(`[${new Date().toISOString()}] [WARN] Log file ${this.logPath} disappeared before writing. Re-initializing.`);
                // Re-initialize to create the file and reset start time
                this.init(); // This will create the file and set currentLogStartTime
            }
            await fs.promises.appendFile(this.logPath, logContent, 'utf8');
        } catch (error) {
            console.error(`Error writing to log file ${this.logPath}: ${error.message}`);
        } finally {
            this.isWriting = false;
            if (this.logQueue.length > 0) {
                setImmediate(() => this.processQueue());
            }
        }
    }

    log(message, level = 'INFO') {
        const timestamp = new Date().toISOString();
        let formattedMessage = message;
        if (typeof message === 'object' && message !== null) {
            try {
                formattedMessage = JSON.stringify(message, (key, value) =>
                    typeof value === 'string' && value.length > 500 ? value.substring(0, 500) + '...' : value,
                2);
            } catch (e) {
                formattedMessage = '[Unserializable Object]';
            }
        }

        const logEntry = `[${timestamp}] [${level}] ${formattedMessage}\n`;

        if (level === 'ERROR') {
            console.error(formattedMessage);
        } else if (level !== 'DEBUG') {
            console.log(formattedMessage);
        }

        this.logQueue.push(logEntry);
        setTimeout(() => this.processQueue(), 50);
    }

    debug(message) {
        this.log(message, 'DEBUG');
    }

    warn(message) {
        this.log(message, 'WARN');
    }

    error(message, error = null) {
        let logMessage = message;
        if (error) {
            logMessage += `: ${error.message || JSON.stringify(error)}`;
            if (error.stack) {
                logMessage += `\nStack: ${error.stack}`;
            }
            if (error.response) {
                logMessage += `\nResponse: Status=${error.response.status}, Data=${JSON.stringify(error.response.data)}`;
            }
        }
        this.log(logMessage, 'ERROR');
    }

    async checkLogSize() {
        try {
            if (!fs.existsSync(this.logPath)) {
                return;
            }
            const stats = await fs.promises.stat(this.logPath);
            if (stats.size >= LOG_MAX_SIZE_BYTES) {
                await this.rotateLog();
            }
        } catch (error) {
            console.error(`Error checking log size for ${this.logPath}: ${error.message}`);
        }
    }

    async rotateLog() {
        const endTime = new Date(); // Rotation happens now
        // Ensure we have a valid start time, default to a short interval before end time if missing
        const startTime = this.currentLogStartTime instanceof Date && !isNaN(this.currentLogStartTime)
                          ? this.currentLogStartTime
                          : new Date(endTime.getTime() - 60000); // Fallback to 1 min before end

        const startTimeFormatted = this.formatDateForFilename(startTime);
        const endTimeFormatted = this.formatDateForFilename(endTime);

        // Construct the new filename with date range
        const backupFilename = `shopify-sync_${startTimeFormatted}_${endTimeFormatted}.log`;
        const backupPath = path.join(this.logDir, backupFilename);
        const rotateMessage = `Rotating log file. Previous log covers range starting ~${startTime.toISOString()}. Archived as: ${backupFilename}`;

        try {
            // Check if the source file exists before renaming
            if (!fs.existsSync(this.logPath)) {
                console.warn(`[${new Date().toISOString()}] [WARN] Attempted to rotate log, but current file ${this.logPath} does not exist. Creating new log.`);
                this.currentLogStartTime = new Date(); // Reset start time
                await fs.promises.writeFile(this.logPath, `[${this.currentLogStartTime.toISOString()}] [INFO] New log file started (previous missing during rotation).\n`, 'utf8');
                return;
            }

            console.log(`[${endTime.toISOString()}] [INFO] ${rotateMessage}`); // Log rotation info to console

            // Rename current log file
            await fs.promises.rename(this.logPath, backupPath);

            // Create a new empty log file and record the new start time
            this.currentLogStartTime = new Date(); // Reset start time for the new file
            await fs.promises.writeFile(this.logPath, `[${this.currentLogStartTime.toISOString()}] [INFO] New log file started after rotation.\nArchived previous log to: ${backupFilename}\n`, 'utf8');

            console.log("Log rotation complete. New log file started.");

        } catch (error) {
            console.error(`Error rotating log file ${this.logPath} to ${backupPath}: ${error.message}`);
            // Attempt to continue logging to the original file if rename failed, but reset start time aggressively
            this.currentLogStartTime = new Date(); // Reset start time even on failure
            try {
                // Try appending error to the *original* path, it might still exist or get recreated
                await fs.promises.appendFile(this.logPath, `[${new Date().toISOString()}] [ERROR] Log rotation failed: ${error.message}\n`, 'utf8');
            } catch (appendError) {
                console.error(`CRITICAL: Failed to write rotation error to log file: ${appendError.message}`);
            }
        }
    }

}
const logger = new Logger();
logger.init();
module.exports = logger;