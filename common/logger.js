const fs = require('fs');
const path = require('path');
// Necesitamos importar LOG_FILE_PATH y LOG_MAX_SIZE_BYTES desde config aquí
// Pero para evitar dependencia circular, es mejor que config.js no dependa de logger.js
// Asumiremos que config es cargado antes y estas constantes se pasan o se leen de process.env

const LOG_FILE_PATH_FROM_ENV = process.env.LOG_FILE_PATH || path.join(__dirname, '..', 'logs', 'shopify-sync.log');
const LOG_MAX_SIZE_MB_FROM_ENV = parseInt(process.env.LOG_MAX_SIZE_MB || '50', 10);
const LOG_MAX_SIZE_BYTES_CALCULATED = LOG_MAX_SIZE_MB_FROM_ENV * 1024 * 1024;


class Logger {
    constructor() {
        this.logDir = path.dirname(LOG_FILE_PATH_FROM_ENV);
        this.logPath = LOG_FILE_PATH_FROM_ENV;
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
                this.currentLogStartTime = new Date();
                fs.writeFileSync(this.logPath, `[${this.currentLogStartTime.toISOString()}] [INFO] Logger initialized. New log file created.\n`);
            } else {
                try {
                    const stats = fs.statSync(this.logPath);
                    this.currentLogStartTime = stats.birthtimeMs ? new Date(stats.birthtimeMs) : new Date(stats.mtimeMs);
                    console.log(`[${new Date().toISOString()}] [INFO] Logger initialized. Existing log file found. Estimated start time: ${this.currentLogStartTime.toISOString()}`);
                } catch (statError) {
                    console.error(`[${new Date().toISOString()}] [ERROR] Could not get stats for existing log file. Setting start time to now. ${statError.message}`);
                    this.currentLogStartTime = new Date();
                     try {
                         fs.appendFileSync(this.logPath, `[${new Date().toISOString()}] [ERROR] Could not get stats for existing log file. Setting start time to now. ${statError.message}\n`);
                     } catch (appendErr) { /* Ignore */ }
                }
            }
        } catch (error) {
            console.error(`Fatal Error: Could not initialize logger at ${this.logPath}. ${error.message}`);
            process.exit(1);
        }
    }

    async processQueue() {
        if (this.isWriting || this.logQueue.length === 0) return;
        this.isWriting = true;
        await this.checkLogSize();
        const messagesToWrite = this.logQueue.splice(0, this.logQueue.length);
        const logContent = messagesToWrite.join('');
        try {
            if (!fs.existsSync(this.logPath)) {
                console.warn(`[${new Date().toISOString()}] [WARN] Log file ${this.logPath} disappeared before writing. Re-initializing.`);
                this.init();
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
        if (level === 'ERROR') { console.error(formattedMessage); }
        else if (level !== 'DEBUG' || process.env.LOG_LEVEL === 'DEBUG') { console.log(formattedMessage); }
        this.logQueue.push(logEntry);
        setTimeout(() => this.processQueue(), 100);
    }

    debug(message) { if (process.env.LOG_LEVEL === 'DEBUG') { this.log(message, 'DEBUG'); } }
    warn(message) { this.log(message, 'WARN'); }
    error(message, error = null) { let logMessage = message; if (error) { logMessage += `: ${error.message || JSON.stringify(error)}`; if (error.stack) { logMessage += `\nStack: ${error.stack}`; } if (error.response) { logMessage += `\nResponse: Status=${error.response.status}, Data=${JSON.stringify(error.response.data)}`; } } this.log(logMessage, 'ERROR'); }

    async checkLogSize() {
        try {
            if (!fs.existsSync(this.logPath)) {
                 console.warn(`[${new Date().toISOString()}] [WARN] Log file ${this.logPath} not found during size check.`);
                 this.currentLogStartTime = null;
                 return;
            }
            const stats = await fs.promises.stat(this.logPath);
            if (stats.size >= LOG_MAX_SIZE_BYTES_CALCULATED) { // Usa la constante calculada
                await this.rotateLog();
            }
        } catch (error) {
            console.error(`Error checking log size for ${this.logPath}: ${error.message}`);
        }
    }

    async rotateLog() {
        const endTime = new Date();
        const startTime = this.currentLogStartTime instanceof Date && !isNaN(this.currentLogStartTime)
                          ? this.currentLogStartTime
                          : new Date(endTime.getTime() - 60000); 
        const startTimeFormatted = this.formatDateForFilename(startTime);
        const endTimeFormatted = this.formatDateForFilename(endTime);
        const backupFilename = `shopify-sync_${startTimeFormatted}_${endTimeFormatted}.log`;
        const backupPath = path.join(this.logDir, backupFilename);
        const rotateMessage = `Rotating log file. Previous log covers range starting ~${startTime.toISOString()}. Archived as: ${backupFilename}`;
        try {
            if (!fs.existsSync(this.logPath)) {
                console.warn(`[${new Date().toISOString()}] [WARN] Attempted to rotate log, but current file ${this.logPath} does not exist. Creating new log.`);
                this.currentLogStartTime = new Date();
                await fs.promises.writeFile(this.logPath, `[${this.currentLogStartTime.toISOString()}] [INFO] New log file started (previous missing during rotation).\n`, 'utf8');
                return;
            }
            console.log(`[${endTime.toISOString()}] [INFO] ${rotateMessage}`);
            await fs.promises.rename(this.logPath, backupPath);
            this.currentLogStartTime = new Date();
            await fs.promises.writeFile(this.logPath, `[${this.currentLogStartTime.toISOString()}] [INFO] New log file started after rotation.\nArchived previous log to: ${backupFilename}\n`, 'utf8');
            console.log("Log rotation complete. New log file started.");
        } catch (error) {
            console.error(`Error rotating log file ${this.logPath} to ${backupPath}: ${error.message}`);
            this.currentLogStartTime = new Date();
            try {
                await fs.promises.appendFile(this.logPath, `[${new Date().toISOString()}] [ERROR] Log rotation failed: ${error.message}\n`, 'utf8');
            } catch (appendError) {
                console.error(`CRITICAL: Failed to write rotation error to log file: ${appendError.message}`);
            }
        }
    }
}
const loggerInstance = new Logger(); // Exporta una instancia
// loggerInstance.init(); // Init se llamará explícitamente en el script principal o al importar
module.exports = loggerInstance;