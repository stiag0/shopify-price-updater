const fs = require('fs');
const path = require('path');

// Estas constantes se leerán de process.env o se usarán los valores por defecto de config.js
// Es importante que config.js se cargue antes de que se inicialice el logger si se depende de él
// para LOG_FILE_PATH_FROM_ENV o LOG_MAX_SIZE_MB_FROM_ENV.
// Por ahora, el logger es independiente y usa sus propios defaults si no se pasan en init.

class Logger {
    constructor() {
        this.logDir = null;
        this.logPath = null;
        this.logQueue = [];
        this.isWriting = false;
        this.currentLogFileStartTime = null; // Hora de inicio del archivo de log actual
        this.logMaxSizeBytes = 50 * 1024 * 1024; // Default 50MB, se puede sobreescribir en init
        this.logBaseDirFromEnv = process.env.LOG_DIR || path.join(__dirname, '..', 'logs'); // Directorio base para logs
    }

    /**
     * Formatea un objeto Date a una cadena YYYYMMDD-HHMMSS.
     * @param {Date} date - El objeto Date a formatear.
     * @returns {string} La fecha formateada.
     */
    formatDateForFilename(date) {
        if (!date || !(date instanceof Date) || isNaN(date)) {
            // Fallback a la fecha actual si la entrada es inválida
            console.error("[Logger] Invalid date passed to formatDateForFilename. Using current date.");
            date = new Date();
        }
        const YYYY = date.getFullYear();
        const MM = String(date.getMonth() + 1).padStart(2, '0');
        const DD = String(date.getDate()).padStart(2, '0');
        const hh = String(date.getHours()).padStart(2, '0');
        const mm = String(date.getMinutes()).padStart(2, '0');
        const ss = String(date.getSeconds()).padStart(2, '0');
        return `${YYYY}${MM}${DD}-${hh}${mm}${ss}`;
    }

    /**
     * Inicializa el logger con una hora de inicio específica para el archivo de log.
     * @param {Date} executionStartTime - La hora de inicio de la ejecución actual del script.
     * @param {string} [baseLogDir] - Directorio base para los logs (opcional, toma de LOG_DIR o default).
     * @param {number} [maxSizeMB] - Tamaño máximo del log en MB (opcional).
     */
    init(executionStartTime, baseLogDir, maxSizeMB) {
        this.currentLogFileStartTime = executionStartTime || new Date();
        this.logDir = path.resolve(baseLogDir || this.logBaseDirFromEnv); // Usar path.resolve para rutas absolutas
        
        if (maxSizeMB && !isNaN(parseInt(maxSizeMB))) {
            this.logMaxSizeBytes = parseInt(maxSizeMB) * 1024 * 1024;
        }

        const formattedStartTime = this.formatDateForFilename(this.currentLogFileStartTime);
        const logFilename = `sync_${formattedStartTime}.log`;
        this.logPath = path.join(this.logDir, logFilename);

        try {
            if (!fs.existsSync(this.logDir)) {
                fs.mkdirSync(this.logDir, { recursive: true });
                console.log(`[Logger] Created log directory: ${this.logDir}`);
            }
            // Crear/Abrir el nuevo archivo de log para esta ejecución
            fs.writeFileSync(this.logPath, `[${new Date().toISOString()}] [INFO] Logger initialized. Log file: ${logFilename}\nExecution Start Time: ${this.currentLogFileStartTime.toISOString()}\n`);
            console.log(`[Logger] Logging to: ${this.logPath}`);
        } catch (error) {
            console.error(`[Logger] FATAL ERROR: Could not initialize logger at ${this.logPath}. ${error.message}`);
            // Si el logger falla al inicializar, las operaciones de log posteriores podrían fallar
            // o los logs de consola podrían ser la única salida.
            // Considerar process.exit(1) si el logging es crítico.
        }
    }

    async processQueue() {
        if (this.isWriting || this.logQueue.length === 0 || !this.logPath) return;

        this.isWriting = true;
        // La rotación de logs es menos crítica si cada ejecución tiene su propio archivo,
        // pero se mantiene por si un solo script corre por mucho tiempo y genera un log muy grande.
        await this.checkLogSize(); 
        
        const messagesToWrite = this.logQueue.splice(0, this.logQueue.length); // Tomar todos los mensajes pendientes
        const logContent = messagesToWrite.join('');

        try {
            if (!fs.existsSync(this.logPath)) {
                // Esto podría ocurrir si el archivo fue borrado manualmente durante la ejecución.
                console.warn(`[${new Date().toISOString()}] [WARN] Log file ${this.logPath} disappeared. Attempting to re-create.`);
                // Re-inicializar con la hora de inicio original para mantener consistencia en el nombre si es posible,
                // o simplemente recrear el archivo.
                fs.writeFileSync(this.logPath, `[${new Date().toISOString()}] [WARN] Log file re-created after disappearing.\nOriginal execution Start Time: ${this.currentLogFileStartTime.toISOString()}\n`);
            }
            await fs.promises.appendFile(this.logPath, logContent, 'utf8');
        } catch (error) {
            console.error(`[Logger] Error writing to log file ${this.logPath}: ${error.message}`);
        } finally {
            this.isWriting = false;
            if (this.logQueue.length > 0) {
                setImmediate(() => this.processQueue()); // Procesar el resto de la cola inmediatamente
            }
        }
    }

    log(message, level = 'INFO') {
        if (!this.logPath) { // Si el logger no se inicializó correctamente
            console.log(`[${level}] ${message}`);
            return;
        }
        const timestamp = new Date().toISOString();
        let formattedMessage = message;

        // Formatear objetos para el log, truncando strings largos
        if (typeof message === 'object' && message !== null) {
            try {
                formattedMessage = JSON.stringify(message, (key, value) =>
                    typeof value === 'string' && value.length > 1000 ? value.substring(0, 1000) + '... (truncated)' : value,
                2); // indentación de 2 espacios
            } catch (e) {
                formattedMessage = '[Unserializable Object]';
            }
        }

        const logEntry = `[${timestamp}] [${level}] ${formattedMessage}\n`;

        // Salida a consola también
        if (level === 'ERROR') { console.error(logEntry.trim()); }
        else if (level === 'WARN') { console.warn(logEntry.trim()); }
        else if (level !== 'DEBUG' || process.env.LOG_LEVEL === 'DEBUG') { console.log(logEntry.trim()); }
        
        this.logQueue.push(logEntry);
        // Disparar el procesamiento de la cola de forma asíncrona pero pronto
        if (!this.isWriting) {
             setTimeout(() => this.processQueue(), 50); // Pequeño delay para agrupar logs rápidos
        }
    }

    debug(message) { if (process.env.LOG_LEVEL === 'DEBUG') { this.log(message, 'DEBUG'); } }
    warn(message) { this.log(message, 'WARN'); }
    error(message, error = null) {
        let logMessage = message;
        if (error) {
            logMessage += `: ${error.message || JSON.stringify(error)}`;
            if (error.stack) { logMessage += `\nStack: ${error.stack}`; }
            if (error.response && error.response.data) {
                logMessage += `\nResponse Data: ${JSON.stringify(error.response.data)}`;
            } else if (error.response && error.response.status) {
                 logMessage += `\nResponse Status: ${error.response.status}`;
            }
        }
        this.log(logMessage, 'ERROR');
    }

    /**
     * Verifica el tamaño del log actual y lo rota si excede el máximo.
     * La rotación en este esquema de un log por ejecución es más para prevenir
     * archivos gigantes si una sola ejecución es extremadamente larga.
     */
    async checkLogSize() {
        if (!this.logPath) return;
        try {
            if (!fs.existsSync(this.logPath)) {
                 console.warn(`[${new Date().toISOString()}] [WARN] Log file ${this.logPath} not found during size check.`);
                 return;
            }
            const stats = await fs.promises.stat(this.logPath);
            if (stats.size >= this.logMaxSizeBytes) {
                await this.rotateLog();
            }
        } catch (error) {
            // Usar console.error aquí porque el logger podría estar en medio de una escritura
            console.error(`[Logger] Error checking log size for ${this.logPath}: ${error.message}`);
        }
    }

    /**
     * Rota el archivo de log actual.
     * En el nuevo esquema, esto archivaría el log actual de la ejecución si se vuelve demasiado grande.
     */
    async rotateLog() {
        if (!this.logPath || !this.currentLogFileStartTime) return;

        const logFileBaseName = path.basename(this.logPath, '.log'); // ej. sync_20250515-103000
        // Añadir un timestamp de rotación para hacerlo único
        const rotationTimestamp = this.formatDateForFilename(new Date());
        const backupFilename = `${logFileBaseName}_rotated_${rotationTimestamp}.log`;
        const backupPath = path.join(this.logDir, backupFilename);
        
        const rotateMessage = `Rotating large log file. Current log for execution started at ${this.currentLogFileStartTime.toISOString()}. Archived as: ${backupFilename}`;
        this.log(rotateMessage, 'INFO'); // Registrar el intento de rotación

        try {
            if (!fs.existsSync(this.logPath)) {
                const warnMsg = `Attempted to rotate log, but current file ${this.logPath} does not exist.`;
                console.warn(`[Logger] ${warnMsg}`);
                this.log(warnMsg, 'WARN');
                // No se puede rotar si no existe, simplemente continuar con el mismo nombre de archivo.
                // Se creará uno nuevo en la próxima escritura si es necesario.
                return;
            }
            
            await fs.promises.rename(this.logPath, backupPath);
            
            // Crear un nuevo archivo de log para la misma ejecución, indicando que es una continuación.
            const continuationMessage = `[${new Date().toISOString()}] [INFO] New log segment started after rotation for execution started at ${this.currentLogFileStartTime.toISOString()}.\nPrevious segment archived to: ${backupFilename}\n`;
            await fs.promises.writeFile(this.logPath, continuationMessage, 'utf8');
            this.log(`Log rotation complete. Continuing log in ${this.logPath}`, 'INFO');

        } catch (error) {
            const errorMsg = `Error rotating log file ${this.logPath} to ${backupPath}: ${error.message}`;
            console.error(`[Logger] ${errorMsg}`);
            this.log(errorMsg, 'ERROR');
            // Intentar seguir logueando en el archivo original si la rotación falla.
            try {
                await fs.promises.appendFile(this.logPath, `[${new Date().toISOString()}] [ERROR] Log rotation failed: ${error.message}\n`, 'utf8');
            } catch (appendError) {
                console.error(`[Logger] CRITICAL: Failed to write rotation error to log file: ${appendError.message}`);
            }
        }
    }

    /**
     * Asegura que todos los logs en cola se escriban antes de salir.
     * Debe llamarse antes de que el proceso termine.
     */
    async flush() {
        if (!this.logPath && this.logQueue.length === 0) return; // Nada que hacer si no hay path y la cola está vacía
        
        const maxWaitTime = 5000; // Máximo 5 segundos de espera para el flush
        const startTime = Date.now();

        // Procesar la cola una última vez
        if (this.logQueue.length > 0) {
            await this.processQueue();
        }

        // Esperar a que la escritura actual (si la hay) termine
        while (this.isWriting && (Date.now() - startTime) < maxWaitTime) {
            await new Promise(resolve => setTimeout(resolve, 50));
        }

        if (this.isWriting) {
            console.warn("[Logger] Timeout waiting for log writing to finish during flush. Some logs might be lost.");
        }
        console.log("[Logger] Log queue flushed.");
    }
}

const loggerInstance = new Logger();
module.exports = loggerInstance;
