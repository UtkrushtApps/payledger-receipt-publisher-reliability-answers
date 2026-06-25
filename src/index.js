const { createLogger } = require('./utils/logger');
const config = require('./config');
const { startApi } = require('./routes');
const { startWorker } = require('./messaging/consumer');

const log = createLogger('bootstrap');

async function main() {
  const role = process.argv[2] || process.env.SERVICE_ROLE || 'api';

  if (role === 'api') {
    log.info({ role, port: config.port }, 'starting api service');
    await startApi();
  } else if (role === 'worker') {
    log.info({ role, port: config.workerPort }, 'starting worker service');
    await startWorker();
  } else {
    log.error({ role }, 'unknown service role');
    process.exit(1);
  }
}

main().catch((err) => {
  log.error({ err: err.message, stack: err.stack }, 'fatal startup error');
  process.exit(1);
});
