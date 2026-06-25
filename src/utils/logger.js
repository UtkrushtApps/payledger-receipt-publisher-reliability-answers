const pino = require('pino');

const base = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level(label) {
      return { level: label };
    },
  },
});

function createLogger(component) {
  return base.child({ component });
}

module.exports = { createLogger, base };
