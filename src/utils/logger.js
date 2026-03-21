/**
 * Simple structured logger for campaign scheduler
 */

function formatLog(level, message, data = {}) {
    const timestamp = new Date().toISOString();
    const dataStr = Object.keys(data).length > 0 ? ` ${JSON.stringify(data)}` : '';
    return `[${timestamp}] [${level}] ${message}${dataStr}`;
}

module.exports = {
    log: (message, data) => console.log(formatLog('LOG', message, data)),
    info: (message, data) => console.log(formatLog('INFO', message, data)),
    warn: (message, data) => console.warn(formatLog('WARN', message, data)),
    error: (message, data) => console.error(formatLog('ERROR', message, data)),
    debug: (message, data) => {
        if (process.env.DEBUG || process.env.NODE_ENV === 'development') {
            console.log(formatLog('DEBUG', message, data));
        }
    }
};
