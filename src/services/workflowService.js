const { createLogger } = require('../utils/logger');

const log = createLogger('workflow');

const sentReceipts = new Map();
const completedReceiptJobs = new Map();
const inFlightReceiptJobs = new Map();

let failNextCount = 0;

function failNext(times) {
  failNextCount = times;
}

async function sendReceiptEmail(job) {
  if (failNextCount > 0) {
    failNextCount -= 1;
    const err = new Error('SMTP 421 temporary failure');
    err.transient = true;
    throw err;
  }

  const record = {
    paymentId: job.paymentId,
    customerId: job.customerId,
    sentAt: new Date().toISOString(),
  };

  const existing = sentReceipts.get(job.paymentId) || 0;
  sentReceipts.set(job.paymentId, existing + 1);

  log.info({ paymentId: job.paymentId, deliveries: existing + 1 }, 'receipt email sent');
  return record;
}

async function sendReceiptEmailOnce(job, idempotencyKey = job.paymentId) {
  const key = String(idempotencyKey || job.paymentId);

  if (completedReceiptJobs.has(key)) {
    log.info({ paymentId: job.paymentId, idempotencyKey: key }, 'duplicate receipt job suppressed');
    return { record: completedReceiptJobs.get(key), deduplicated: true };
  }

  if (inFlightReceiptJobs.has(key)) {
    const record = await inFlightReceiptJobs.get(key);
    log.info({ paymentId: job.paymentId, idempotencyKey: key }, 'duplicate in-flight receipt job joined');
    return { record, deduplicated: true };
  }

  const promise = sendReceiptEmail(job)
    .then((record) => {
      completedReceiptJobs.set(key, record);
      return record;
    })
    .finally(() => {
      inFlightReceiptJobs.delete(key);
    });

  inFlightReceiptJobs.set(key, promise);
  const record = await promise;
  return { record, deduplicated: false };
}

function getDeliveryCount(paymentId) {
  return sentReceipts.get(paymentId) || 0;
}

module.exports = { sendReceiptEmail, sendReceiptEmailOnce, getDeliveryCount, failNext };
