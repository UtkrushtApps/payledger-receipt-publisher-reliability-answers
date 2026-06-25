const express = require('express');
const config = require('../config');
const { connectConfirm, invalidateConfirmChannel } = require('./connection');
const { declareTopology } = require('./topology');
const { sendReceiptEmailOnce } = require('../services/workflowService');
const { createLogger } = require('../utils/logger');

const log = createLogger('consumer');

let currentChannel = null;
let consumerStatus = 'starting';
let reconnectTimer = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getHeader(headers, name, fallback) {
  if (!headers || headers[name] === undefined || headers[name] === null) {
    return fallback;
  }
  return headers[name];
}

function getAttempt(msg) {
  const attempt = Number(getHeader(msg.properties.headers, 'x-attempt', 1));
  return Number.isFinite(attempt) && attempt > 0 ? attempt : 1;
}

function parseMessage(msg) {
  try {
    return JSON.parse(msg.content.toString('utf8'));
  } catch (err) {
    err.permanent = true;
    throw err;
  }
}

function confirmPublish(channel, exchange, routingKey, payload, options) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error('timed out waiting for retry publish confirm'));
    }, config.publishConfirmTimeoutMs);

    try {
      channel.publish(exchange, routingKey, payload, options, (err) => {
        clearTimeout(timer);
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    } catch (err) {
      clearTimeout(timer);
      reject(err);
    }
  });
}

async function publishRetry(channel, msg, job, nextAttempt) {
  const headers = {
    ...(msg.properties.headers || {}),
    'x-attempt': nextAttempt,
    'x-retry-count': nextAttempt - 1,
    originalQueue: config.receiptQueue,
    lastFailureAt: new Date().toISOString(),
  };

  const payload = Buffer.from(JSON.stringify(job));
  const messageId = msg.properties.messageId || `receipt:${job.paymentId}`;
  const correlationId = msg.properties.correlationId || job.correlationId || job.paymentId;

  await confirmPublish(channel, config.retryExchange, config.retryRoutingKey, payload, {
    persistent: true,
    mandatory: true,
    contentType: 'application/json',
    messageId,
    correlationId,
    timestamp: Math.floor(Date.now() / 1000),
    headers,
  });
}

async function handleMessage(channel, msg) {
  if (!msg) {
    return;
  }

  const correlationId = msg.properties.correlationId;
  const messageId = msg.properties.messageId;
  const attempt = getAttempt(msg);
  let job;

  try {
    job = parseMessage(msg);

    if (!job.paymentId || !job.customerId) {
      const err = new Error('receipt job missing paymentId or customerId');
      err.permanent = true;
      throw err;
    }

    log.info(
      {
        paymentId: job.paymentId,
        customerId: job.customerId,
        correlationId,
        messageId,
        attempt,
      },
      'received receipt job'
    );

    const result = await sendReceiptEmailOnce(job, job.paymentId);
    channel.ack(msg);

    log.info(
      {
        paymentId: job.paymentId,
        correlationId,
        messageId,
        attempt,
        deduplicated: result.deduplicated,
      },
      'receipt job acknowledged'
    );
  } catch (err) {
    const paymentId = job && job.paymentId;
    const logContext = { paymentId, correlationId, messageId, attempt, err: err.message };

    if (err.permanent) {
      log.error(logContext, 'permanently invalid receipt job; dead-lettering');
      channel.nack(msg, false, false);
      return;
    }

    if (attempt < config.maxAttempts) {
      const nextAttempt = attempt + 1;
      try {
        await publishRetry(channel, msg, job || parseMessage(msg), nextAttempt);
        channel.ack(msg);
        log.warn({ ...logContext, nextAttempt }, 'receipt job scheduled for bounded retry');
      } catch (publishErr) {
        log.error(
          { ...logContext, retryPublishErr: publishErr.message },
          'failed to publish retry job; requeueing original message'
        );
        channel.nack(msg, false, true);
      }
      return;
    }

    log.error(logContext, 'receipt job exhausted retries; dead-lettering');
    channel.nack(msg, false, false);
  }
}

async function startConsumingOnce() {
  const channel = await connectConfirm();
  currentChannel = channel;
  consumerStatus = 'declaring_topology';

  await declareTopology(channel);
  await channel.prefetch(config.prefetch);

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
      'worker publish returned unroutable message'
    );
  });

  channel.once('close', () => {
    if (currentChannel === channel) {
      currentChannel = null;
      consumerStatus = 'channel_closed';
      scheduleConsumerReconnect();
    }
  });

  await channel.consume(
    config.receiptQueue,
    (msg) => {
      handleMessage(channel, msg).catch((err) => {
        log.error({ err: err.message, stack: err.stack }, 'unhandled receipt consumer error');
        if (msg) {
          try {
            channel.nack(msg, false, true);
          } catch (nackErr) {
            log.error({ err: nackErr.message }, 'failed to nack after unhandled consumer error');
          }
        }
      });
    },
    { noAck: false }
  );

  consumerStatus = 'consuming';
  log.info({ queue: config.receiptQueue, prefetch: config.prefetch }, 'receipt worker consuming');
}

function scheduleConsumerReconnect() {
  if (reconnectTimer) {
    return;
  }

  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    try {
      consumerStatus = 'reconnecting';
      await invalidateConfirmChannel();
      await startConsumingOnce();
    } catch (err) {
      consumerStatus = 'reconnect_failed';
      log.error({ err: err.message }, 'worker amqp reconnect failed');
      scheduleConsumerReconnect();
    }
  }, config.reconnectDelayMs);
}

async function startConsumerLoop() {
  for (;;) {
    try {
      await startConsumingOnce();
      return;
    } catch (err) {
      consumerStatus = 'connect_failed';
      log.error({ err: err.message }, 'worker amqp startup failed; retrying');
      await sleep(config.reconnectDelayMs);
    }
  }
}

async function startWorker() {
  startConsumerLoop().catch((err) => {
    consumerStatus = 'fatal';
    log.error({ err: err.message, stack: err.stack }, 'fatal worker consumer loop error');
    scheduleConsumerReconnect();
  });

  const app = express();
  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', role: 'worker', consumerStatus });
  });
  app.listen(config.workerPort, () => {
    log.info({ port: config.workerPort }, 'worker health endpoint listening');
  });
}

module.exports = { startWorker, handleMessage };
