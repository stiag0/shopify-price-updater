const Logger = require('./common/logger');
console.log('Type of Logger:', typeof Logger);
console.log('Has flush?', typeof Logger.flush === 'function');
console.log('Keys:', Object.keys(Logger));
console.log('Proto Keys:', Object.getOwnPropertyNames(Object.getPrototypeOf(Logger)));
