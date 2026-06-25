module.exports = {
  amqpUrl: process.env.AMQP_URL || 'amqp://task_user:task_password@localhost:5672/task_vhost',
  port: parseInt(process.env.PORT || '3000', 10),
  workerPort: parseInt(process.env.PORT || '3100', 10),

  billingExchange: 'billing.events',
  receiptQueue: 'receipt_email_q',
  receiptRoutingKey: 'payment.settled',

  unroutedExchange: 'billing.unrouted',
  unroutedQueue: 'billing_unrouted_q',

  retryExchange: 'receipt.retry',
  retryRoutingKey: 'receipt.retry',
  retryQueue: 'receipt_email_retry_q',

  deadLetterExchange: 'receipt.dead',
  deadLetterRoutingKey: 'receipt.dead',
  deadLetterQueue: 'receipt_email_dlq',

  retryTtlMs: parseInt(process.env.RECEIPT_RETRY_TTL_MS || '60000', 10),
  maxAttempts: parseInt(process.env.RECEIPT_MAX_ATTEMPTS || '3', 10),

  prefetch: parseInt(process.env.RECEIPT_PREFETCH || '10', 10),

  reconnectDelayMs: parseInt(process.env.AMQP_RECONNECT_DELAY_MS || '2000', 10),
  publishConfirmTimeoutMs: parseInt(process.env.PUBLISH_CONFIRM_TIMEOUT_MS || '5000', 10),
};
