const express = require('express');
const config = require('./config');
const { initProducer, publishPaymentSettled } = require('./messaging/producer');
const { createLogger } = require('./utils/logger');

const log = createLogger('api');

function requestCorrelationId(req, paymentId) {
  return (
    req.get('x-correlation-id') ||
    req.get('x-request-id') ||
    req.get('payledger-correlation-id') ||
    `webhook:${paymentId}:${Date.now()}`
  );
}

function buildApp() {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req, res) => res.json({ status: 'ok', role: 'api' }));

  app.post('/api/payments/webhook', async (req, res) => {
    const { paymentId, customerId, amountCents, currency, settledAt } = req.body || {};

    if (!paymentId || !customerId) {
      return res.status(400).json({ error: 'paymentId and customerId are required' });
    }

    const correlationId = requestCorrelationId(req, paymentId);

    try {
      const result = await publishPaymentSettled({
        paymentId,
        customerId,
        amountCents,
        currency,
        settledAt,
        correlationId,
      });

      log.info(
        { paymentId, customerId, correlationId, messageId: result.messageId },
        'payment webhook accepted after confirmed receipt publish'
      );

      return res.status(200).json({ status: 'accepted' });
    } catch (err) {
      log.error(
        { err: err.message, paymentId, customerId, correlationId },
        'failed to durably publish receipt job; returning retryable failure'
      );

      return res.status(503).json({
        error: 'receipt_job_publish_failed',
        retryable: true,
      });
    }
  });

  return app;
}

async function startApi() {
  await initProducer();
  const app = buildApp();
  app.listen(config.port, () => {
    log.info({ port: config.port }, 'api listening');
  });
}

module.exports = { buildApp, startApi };
