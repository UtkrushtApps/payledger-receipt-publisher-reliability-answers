const config = require('../config');
const {
  connectConfirm,
  getConfirmChannel,
  invalidateConfirmChannel,
} = require('./connection');
const { declareTopology } = require('./topology');
const { createLogger } = require('../utils/logger');

const log = createLogger('producer');

let initPromise = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function ensureProducerChannel() {
  const existing = getConfirmChannel();
  if (existing) {
    return existing;
  }

  if (!initPromise) {
    initPromise = (async () => {
      const channel = await connectConfirm();
      await declareTopology(channel);

      if (!channel.__payledgerReturnLoggerAttached) {
        channel.on('return', (msg) => {
          log.error(
            {
              exchange: msg.fields.exchange,
              routingKey: msg.fields.routingKey,
              replyCode: msg.fields.replyCode,
              replyText: msg.fields.replyText,
              correlationId: msg.properties.correlationId,
              messageId: msg.properties.messageId,
            },
            'publisher received returned unroutable message'
          );
        });
        channel.__payledgerReturnLoggerAttached = true;
      }

      return channel;
    })().finally(() => {
      initPromise = null;
    });
  }

  return initPromise;
}

async function initProducer() {
  return ensureProducerChannel();
}

function buildReceiptJob(payment) {
  return {
    paymentId: payment.paymentId,
    customerId: payment.customerId,
    amountCents: payment.amountCents,
    currency: payment.currency,
    settledAt: payment.settledAt,
    jobType: 'receipt-email',
    enqueuedAt: new Date().toISOString(),
    correlationId: payment.correlationId,
  };
}

function publishWithConfirm(channel, exchange, routingKey, payload, options, correlation) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let returned = null;

    const cleanup = () => {
      clearTimeout(timer);
      channel.off('return', onReturn);
    };

    const finish = (err) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      if (err) {
        reject(err);
      } else if (returned) {
        const unroutable = new Error(`message was returned unroutable: ${returned.replyText}`);
        unroutable.unroutable = true;
        unroutable.returnInfo = returned;
        reject(unroutable);
      } else {
        resolve();
      }
    };

    const onReturn = (msg) => {
      const sameCorrelation = msg.properties.correlationId === options.correlationId;
      const sameMessage = msg.properties.messageId === options.messageId;
      if (sameCorrelation || sameMessage) {
        returned = {
          exchange: msg.fields.exchange,
          routingKey: msg.fields.routingKey,
          replyCode: msg.fields.replyCode,
          replyText: msg.fields.replyText,
        };
      }
    };

    const timer = setTimeout(() => {
      finish(new Error('timed out waiting for rabbitmq publisher confirm'));
    }, config.publishConfirmTimeoutMs);

    channel.on('return', onReturn);

    try {
      channel.publish(exchange, routingKey, payload, options, (err) => {
        finish(err || null);
      });
    } catch (err) {
      finish(err);
    }
  }).catch((err) => {
    log.error({ err: err.message, ...correlation }, 'publisher confirm failed');
    throw err;
  });
}

async function publishPaymentSettled(payment) {
  const correlationId = payment.correlationId || payment.paymentId;
  const messageId = `receipt:${payment.paymentId}`;
  const job = buildReceiptJob({ ...payment, correlationId });
  const payload = Buffer.from(JSON.stringify(job));

  const options = {
    mandatory: true,
    persistent: true,
    contentType: 'application/json',
    messageId,
    correlationId,
    timestamp: Math.floor(Date.now() / 1000),
    headers: {
      jobType: 'receipt-email',
      paymentId: payment.paymentId,
      idempotencyKey: payment.paymentId,
      'x-attempt': 1,
      'x-retry-count': 0,
    },
  };

  const correlation = {
    paymentId: payment.paymentId,
    customerId: payment.customerId,
    correlationId,
    messageId,
    exchange: config.billingExchange,
    routingKey: config.receiptRoutingKey,
  };

  for (let attempt = 1; attempt <= 2; attempt += 1) {
    try {
      const channel = await ensureProducerChannel();
      await publishWithConfirm(
        channel,
        config.billingExchange,
        config.receiptRoutingKey,
        payload,
        options,
        correlation
      );

      log.info({ ...correlation, publishAttempt: attempt }, 'published confirmed receipt job');
      return { published: true, correlationId, messageId };
    } catch (err) {
      log.warn(
        { ...correlation, err: err.message, publishAttempt: attempt },
        'receipt publish attempt failed'
      );
      await invalidateConfirmChannel();

      if (attempt >= 2) {
        throw err;
      }

      await sleep(config.reconnectDelayMs);
    }
  }

  throw new Error('unreachable publish failure');
}

module.exports = { initProducer, publishPaymentSettled };
