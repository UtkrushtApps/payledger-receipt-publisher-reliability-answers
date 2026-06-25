const test = require('node:test');
const assert = require('node:assert');

const { buildApp } = require('../src/routes');
const workflow = require('../src/services/workflowService');

test('webhook rejects payloads missing required identifiers', async () => {
  const app = buildApp();
  const server = app.listen(0);
  const { port } = server.address();

  const res = await fetch(`http://127.0.0.1:${port}/api/payments/webhook`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ amountCents: 100 }),
  });

  assert.strictEqual(res.status, 400);
  server.close();
});

test('workflow tracks receipt delivery count per payment', async () => {
  const before = workflow.getDeliveryCount('contract_pay_1');
  await workflow.sendReceiptEmail({ paymentId: 'contract_pay_1', customerId: 'cus_1' });
  const after = workflow.getDeliveryCount('contract_pay_1');
  assert.strictEqual(after, before + 1);
});
