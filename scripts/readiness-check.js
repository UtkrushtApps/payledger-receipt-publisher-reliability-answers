const config = require('../src/config');

function assert(cond, message) {
  if (!cond) {
    console.error('READINESS FAILED:', message);
    process.exit(1);
  }
}

try {
  require('../src/messaging/connection');
  require('../src/messaging/topology');
  require('../src/messaging/producer');
  require('../src/messaging/consumer');
  require('../src/routes');
  require('../src/services/workflowService');
  require('../src/utils/logger');

  assert(typeof config.amqpUrl === 'string' && config.amqpUrl.length > 0, 'amqpUrl must be configured');
  assert(typeof config.billingExchange === 'string', 'billingExchange must be configured');
  assert(typeof config.receiptQueue === 'string', 'receiptQueue must be configured');
  assert(typeof config.unroutedExchange === 'string', 'unroutedExchange must be configured');
  assert(typeof config.deadLetterExchange === 'string', 'deadLetterExchange must be configured');
  assert(typeof config.retryExchange === 'string', 'retryExchange must be configured');
  assert(Number.isInteger(config.maxAttempts) && config.maxAttempts > 0, 'maxAttempts must be configured');

  console.log('READINESS OK: starter modules load and config is present.');
  process.exit(0);
} catch (err) {
  console.error('READINESS FAILED:', err.message);
  process.exit(1);
}
