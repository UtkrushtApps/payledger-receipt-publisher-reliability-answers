# Solution Steps

1. Declare a durable RabbitMQ topology: the main direct billing exchange, durable receipt queue, alternate unrouted exchange/queue, retry exchange/queue with TTL, and a dead-letter exchange/queue for exhausted or malformed jobs.

2. Switch the API publisher from a regular AMQP channel to a confirm channel so every webhook waits for RabbitMQ publisher confirmation before returning HTTP 200.

3. Publish receipt jobs with persistent delivery, content metadata, messageId, correlationId, idempotency headers, and mandatory routing so unroutable returned messages can be detected instead of silently ignored.

4. Attach return/error/close handlers to AMQP resources and recreate confirm channels after broker or channel failures, retrying one fresh channel before failing the webhook.

5. Change the webhook error path to return a retryable non-2xx response, such as HTTP 503, when the receipt job cannot be durably accepted by RabbitMQ.

6. Run the worker with manual acknowledgements and configure prefetch to provide back-pressure for the downstream email workflow.

7. Process receipt jobs through an idempotent workflow wrapper keyed by paymentId so duplicate RabbitMQ deliveries do not send duplicate business emails.

8. On transient worker failures, confirm-publish a persistent retry message to the retry queue before acknowledging the original message; let the retry queue TTL dead-letter it back to the receipt queue.

9. After the configured maximum attempts, or for permanently invalid JSON/payloads, reject without requeue so RabbitMQ routes the message to the dead-letter queue.

10. Add correlation-rich structured logs throughout publishing, consuming, retrying, deduplication, acknowledgement, and dead-letter paths to make the webhook-to-receipt flow observable.

