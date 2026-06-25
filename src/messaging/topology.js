const config = require('../config');
const { createLogger } = require('../utils/logger');

const log = createLogger('topology');

async function declareTopology(channel) {
  await channel.assertExchange(config.unroutedExchange, 'fanout', { durable: true });
  await channel.assertQueue(config.unroutedQueue, { durable: true });
  await channel.bindQueue(config.unroutedQueue, config.unroutedExchange, '');

  await channel.assertExchange(config.deadLetterExchange, 'direct', { durable: true });
  await channel.assertQueue(config.deadLetterQueue, { durable: true });
  await channel.bindQueue(
    config.deadLetterQueue,
    config.deadLetterExchange,
    config.deadLetterRoutingKey
  );

  await channel.assertExchange(config.retryExchange, 'direct', { durable: true });
  await channel.assertQueue(config.retryQueue, {
    durable: true,
    arguments: {
      'x-message-ttl': config.retryTtlMs,
      'x-dead-letter-exchange': config.billingExchange,
      'x-dead-letter-routing-key': config.receiptRoutingKey,
    },
  });
  await channel.bindQueue(config.retryQueue, config.retryExchange, config.retryRoutingKey);

  await channel.assertExchange(config.billingExchange, 'direct', {
    durable: true,
    alternateExchange: config.unroutedExchange,
  });

  await channel.assertQueue(config.receiptQueue, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': config.deadLetterExchange,
      'x-dead-letter-routing-key': config.deadLetterRoutingKey,
    },
  });
  await channel.bindQueue(config.receiptQueue, config.billingExchange, config.receiptRoutingKey);

  log.info(
    {
      exchange: config.billingExchange,
      queue: config.receiptQueue,
      unroutedExchange: config.unroutedExchange,
      unroutedQueue: config.unroutedQueue,
      retryExchange: config.retryExchange,
      retryQueue: config.retryQueue,
      deadLetterExchange: config.deadLetterExchange,
      deadLetterQueue: config.deadLetterQueue,
    },
    'declared durable rabbitmq topology'
  );
}

module.exports = { declareTopology };
