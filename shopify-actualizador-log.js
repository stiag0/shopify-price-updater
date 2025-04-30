/**
 * Sistema de logs para la aplicación de sincronización con Shopify
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');

// Configuración
const LOG_FILE_PATH = process.env.LOG_FILE_PATH || path.join(__dirname, '../logs/shopify-sync.log');
const LOG_MAX_SIZE = parseInt(process.env.LOG_MAX_SIZE || '10485760', 10); // 10MB por defecto

const Logger = {
  logDir: path.dirname(LOG_FILE_PATH),
  logPath: LOG_FILE_PATH,
  
  /**
   * Inicializa el sistema de logs creando el directorio si no existe
   */
  init() {
    try {
      if (!fs.existsSync(this.logDir)) {
        fs.mkdirSync(this.logDir, { recursive: true });
      }
      if (!fs.existsSync(this.logPath)) {
        fs.writeFileSync(this.logPath, '');
      }
      return true;
    } catch (error) {
      console.error(`Error al inicializar sistema de logs: ${error.message}`);
      return false;
    }
  },
  
  /**
   * Escribe un mensaje en el archivo de log y en la consola
   * @param {String} message - Mensaje a registrar
   * @param {String} level - Nivel del log (INFO, ERROR, etc.)
   * @returns {Promise<boolean>} - Promesa que resuelve true si se escribió correctamente
   */
  log(message, level = 'INFO') {
    return new Promise((resolve) => {
      const timestamp = new Date().toISOString();
      const logEntry = `[${timestamp}] [${level}] ${message}\n`;
      
      // Mostrar en consola
      console.log(`[${level}] ${message}`);
      
      // Verificar el tamaño del archivo antes de escribir
      this.checkLogSize()
        .then(() => {
          // Escribir en el archivo usando streams para evitar problemas de memoria
          const stream = fs.createWriteStream(this.logPath, { flags: 'a' });
          
          stream.on('error', (err) => {
            console.error(`Error al escribir en el log: ${err.message}`);
            resolve(false);
          });
          
          stream.on('finish', () => {
            resolve(true);
          });
          
          stream.write(logEntry);
          stream.end();
        })
        .catch(err => {
          console.error(`Error al rotar logs: ${err.message}`);
          resolve(false);
        });
    });
  },
  
  /**
   * Verifica el tamaño del archivo y lo rota si es necesario
   * @returns {Promise<void>} - Promesa que resuelve cuando termina el proceso
   */
  checkLogSize() {
    return new Promise((resolve, reject) => {
      try {
        if (!fs.existsSync(this.logPath)) {
          return resolve();
        }
        
        const stats = fs.statSync(this.logPath);
        
        if (stats.size >= LOG_MAX_SIZE) {
          // Crear nombre de archivo de respaldo con timestamp
          const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-');
          const backupPath = `${this.logDir}/shopify-sync.${timestamp}.log`;
          
          // Usar streams para mover el archivo y evitar cargar todo en memoria
          this.moveFileUsingStreams(this.logPath, backupPath)
            .then(() => {
              // Crear un nuevo archivo vacío
              fs.writeFileSync(this.logPath, `[${new Date().toISOString()}] [INFO] Archivo de log rotado. Archivo anterior: ${backupPath}\n`);
              
              // Eliminar logs antiguos (mantener solo el cuarto más reciente)
              return this.cleanOldLogs();
            })
            .then(resolve)
            .catch(reject);
        } else {
          resolve();
        }
      } catch (error) {
        reject(new Error(`Error al verificar tamaño del log: ${error.message}`));
      }
    });
  },
  
  /**
   * Mueve un archivo usando streams para evitar problemas de memoria
   * @param {String} source - Ruta del archivo origen
   * @param {String} destination - Ruta del archivo destino
   * @returns {Promise<void>} - Promesa que resuelve cuando termina la operación
   */
  moveFileUsingStreams(source, destination) {
    return new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(source);
      const writeStream = fs.createWriteStream(destination);
      
      readStream.on('error', err => {
        reject(new Error(`Error al leer archivo: ${err.message}`));
      });
      
      writeStream.on('error', err => {
        reject(new Error(`Error al escribir archivo: ${err.message}`));
      });
      
      writeStream.on('finish', () => {
        fs.unlink(source, err => {
          if (err) {
            reject(new Error(`Error al eliminar archivo original: ${err.message}`));
          } else {
            resolve();
          }
        });
      });
      
      readStream.pipe(writeStream);
    });
  },
  
  /**
   * Elimina los archivos de log más antiguos, manteniendo solo el cuarto más reciente
   * @returns {Promise<void>} - Promesa que resuelve cuando termina la operación
   */
  cleanOldLogs() {
    return new Promise((resolve, reject) => {
      try {
        const logFiles = fs.readdirSync(this.logDir)
          .filter(file => file.startsWith('shopify-sync.') && file.endsWith('.log') && file !== path.basename(this.logPath))
          .map(file => ({ 
            name: file, 
            path: path.join(this.logDir, file),
            time: fs.statSync(path.join(this.logDir, file)).mtime.getTime() 
          }))
          .sort((a, b) => b.time - a.time); // Ordenar de más reciente a más antiguo
        
        if (logFiles.length === 0) {
          return resolve();
        }
        
        // Calcular cuántos archivos mantener (un cuarto del total)
        const keepCount = Math.max(Math.ceil(logFiles.length / 4), 1);
        
        // Eliminar los archivos más antiguos
        if (logFiles.length > keepCount) {
          const deletePromises = logFiles.slice(keepCount).map(file => {
            return new Promise((resolveDelete) => {
              fs.unlink(file.path, (err) => {
                if (err) {
                  console.error(`Error al eliminar archivo ${file.name}: ${err.message}`);
                } else {
                  console.log(`Archivo de log antiguo eliminado: ${file.name}`);
                }
                resolveDelete();
              });
            });
          });
          
          Promise.all(deletePromises).then(() => {
            const keptCount = Math.min(keepCount, logFiles.length);
            console.log(`Se mantuvieron los ${keptCount} archivos de log más recientes (${Math.round(keptCount/logFiles.length*100)}% del total)`);
            resolve();
          });
        } else {
          resolve();
        }
      } catch (error) {
        reject(new Error(`Error al limpiar logs antiguos: ${error.message}`));
      }
    });
  },
  
  /**
   * Registra un error
   * @param {String} message - Mensaje de error
   * @param {Error} error - Objeto de error (opcional)
   * @returns {Promise<boolean>} - Promesa que resuelve true si se escribió correctamente
   */
  error(message, error = null) {
    let logMessage = message;
    if (error) {
      logMessage += `: ${error.message}`;
      if (error.stack) {
        logMessage += `\nStack: ${error.stack}`;
      }
      if (error.response) {
        logMessage += ` (Status: ${error.response.status})`;
        if (error.response.data) {
          try {
            logMessage += `\nResponse data: ${JSON.stringify(error.response.data)}`;
          } catch (e) {
            logMessage += `\nResponse data: [Unable to stringify]`;
          }
        }
      }
    }
    return this.log(logMessage, 'ERROR');
  },
  
  /**
   * Registra un mensaje informativo
   * @param {String} message - Mensaje a registrar
   * @returns {Promise<boolean>} - Promesa que resuelve true si se escribió correctamente
   */
  info(message) {
    return this.log(message, 'INFO');
  },
  
  /**
   * Registra un mensaje de advertencia
   * @param {String} message - Mensaje a registrar
   * @returns {Promise<boolean>} - Promesa que resuelve true si se escribió correctamente
   */
  warn(message) {
    return this.log(message, 'WARN');
  },
  
  /**
   * Registra un mensaje de depuración
   * @param {String} message - Mensaje a registrar
   * @returns {Promise<boolean>} - Promesa que resuelve true si se escribió correctamente
   */
  debug(message) {
    // Solo registrar si estamos en modo debug
    if (process.env.DEBUG === 'true') {
      return this.log(message, 'DEBUG');
    }
    return Promise.resolve(true);
  },
  
  /**
   * Lee el contenido de un archivo de log de forma paginada
   * @param {String} filePath - Ruta del archivo a leer
   * @param {Number} page - Número de página (empezando desde 1)
   * @param {Number} pageSize - Tamaño de la página en líneas
   * @returns {Promise<Object>} - Objeto con el contenido de la página y metadatos
   */
  async readLogPaginated(filePath = null, page = 1, pageSize = 1000) {
    const targetPath = filePath || this.logPath;
    
    return new Promise((resolve, reject) => {
      try {
        if (!fs.existsSync(targetPath)) {
          return resolve({
            content: [],
            page: 1,
            totalPages: 0,
            totalLines: 0,
            hasMore: false
          });
        }
        
        // Validar página y tamaño
        page = Math.max(1, parseInt(page, 10) || 1);
        pageSize = Math.max(10, Math.min(10000, parseInt(pageSize, 10) || 1000));
        
        // Usar un contador de líneas para determinar el total
        let totalLines = 0;
        const lineCounter = fs.createReadStream(targetPath)
          .on('data', buffer => {
            let idx = -1;
            totalLines--; // Compensar por si no hay salto de línea al final
            do {
              idx = buffer.indexOf(10, idx + 1);
              totalLines++;
            } while (idx !== -1);
          })
          .on('end', () => {
            // Calcular total de páginas
            const totalPages = Math.ceil(totalLines / pageSize);
            
            // Validar que la página solicitada existe
            const validPage = Math.min(Math.max(1, page), Math.max(1, totalPages));
            
            // Calcular líneas a saltar
            const linesToSkip = (validPage - 1) * pageSize;
            
            // Definir variables para la lectura
            let currentLine = 0;
            let content = [];
            let linesRead = 0;
            
            // Leer el archivo usando streams
            const lineReader = readline.createInterface({
              input: fs.createReadStream(targetPath),
              crlfDelay: Infinity
            });
            
            lineReader.on('line', (line) => {
              currentLine++;
              
              // Si estamos en el rango de la página actual
              if (currentLine > linesToSkip && linesRead < pageSize) {
                content.push(line);
                linesRead++;
              }
              
              // Si ya tenemos suficientes líneas, cerramos el stream
              if (linesRead >= pageSize) {
                lineReader.close();
              }
            });
            
            lineReader.on('close', () => {
              resolve({
                content,
                page: validPage,
                totalPages,
                totalLines,
                hasMore: validPage < totalPages
              });
            });
          })
          .on('error', err => {
            reject(new Error(`Error al leer archivo de log: ${err.message}`));
          });
      } catch (error) {
        reject(new Error(`Error al paginar log: ${error.message}`));
      }
    });
  },
  
  /**
   * Obtiene la lista de archivos de log disponibles
   * @returns {Array} - Array de objetos con información de los archivos
   */
  getLogFiles() {
    try {
      if (!fs.existsSync(this.logDir)) {
        return [];
      }
      
      // Obtener todos los archivos de log
      const logFiles = fs.readdirSync(this.logDir)
        .filter(file => file.startsWith('shopify-sync.') && file.endsWith('.log'))
        .map(file => {
          const filePath = path.join(this.logDir, file);
          const stats = fs.statSync(filePath);
          return {
            name: file,
            path: filePath,
            size: stats.size,
            sizeFormatted: this.formatFileSize(stats.size),
            createdAt: stats.birthtime,
            modifiedAt: stats.mtime,
            isCurrentLog: filePath === this.logPath
          };
        })
        .sort((a, b) => b.modifiedAt.getTime() - a.modifiedAt.getTime());
      
      return logFiles;
    } catch (error) {
      console.error(`Error al obtener lista de logs: ${error.message}`);
      return [];
    }
  },
  
  /**
   * Formatea un tamaño de archivo en bytes a una representación legible
   * @param {Number} bytes - Tamaño en bytes
   * @returns {String} - Tamaño formateado
   */
  formatFileSize(bytes) {
    if (bytes === undefined || bytes === null) return '0 B';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(2) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(2) + ' MB';
    return (bytes / 1073741824).toFixed(2) + ' GB';
  }
};

module.exports = Logger;