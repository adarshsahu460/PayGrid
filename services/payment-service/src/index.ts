import express from 'express';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { createClient } from 'redis';
import dotenv from 'dotenv';
import { PaymentRequest, PaymentResponse, generateTransactionId } from '@paygrid/lib';
import promClient from 'prom-client';

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

// Database setup
const pool = new Pool({
  user: process.env.POSTGRES_USER || 'paygrid',
  password: process.env.POSTGRES_PASSWORD || 'paygrid',
  host: process.env.POSTGRES_HOST || 'postgres',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'paygrid'
});

// Redis setup
const redis = createClient({
  url: `redis://${process.env.REDIS_HOST || 'redis'}:${process.env.REDIS_PORT || '6379'}`
});

// Kafka setup
const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'payment-service-group' });

// Middleware
app.use(express.json());

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

// Query payment status
app.get('/payments/:transactionId/status', async (req, res) => {
  const { transactionId } = req.params;
  const result = await pool.query(
    'SELECT status, message FROM payment_transactions WHERE transaction_id = $1',
    [transactionId]
  );
  if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
  res.json(result.rows[0]);
});

// Payment initiation with idempotency and status tracking
app.post('/payments', async (req, res) => {
  try {
    const idempotencyKey = req.headers['idempotency-key'] || req.body.idempotencyKey;
    if (!idempotencyKey) return res.status(400).json({ error: 'Idempotency-Key is required' });

    // Check Redis for idempotency
    const cached = await redis.get(`idempotency:${idempotencyKey}`);
    if (cached) {
      return res.json(JSON.parse(cached));
    }

    const paymentRequest: PaymentRequest = req.body;
    const transactionId = generateTransactionId();

    // Insert initial payment record with PENDING status
    await pool.query(
      'INSERT INTO payment_transactions (transaction_id, status, message, created_at) VALUES ($1, $2, $3, NOW())',
      [transactionId, 'PENDING', 'Payment initiated']
    );

    // Store payment request in Redis for idempotency (optional, for quick lookup)
    await redis.set(`payment:${transactionId}`, JSON.stringify(paymentRequest), { EX: 3600 });

    // Send to Kafka for processing
    await producer.send({
      topic: 'payment-requests',
      messages: [
        {
          key: transactionId,
          value: JSON.stringify({
            ...paymentRequest,
            transactionId
          })
        }
      ]
    });

    const response = {
      transactionId,
      status: 'PROCESSING',
      message: 'Payment request received'
    };
    // Store idempotency response in Redis (24h expiry)
    await redis.set(`idempotency:${idempotencyKey}`, JSON.stringify(response), { EX: 86400 });

    res.json(response);
  } catch (error) {
    console.error('Error processing payment:', error);
    res.status(500).json({
      status: 'FAILED',
      message: 'Error processing payment request'
    });
  }
});

// Kafka message handling with dead-letter topic
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await redis.connect();

  await consumer.subscribe({ topic: 'payment-responses', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }: { message: any }) => {
      if (!message.value) return;
      try {
        const response: PaymentResponse = JSON.parse(message.value.toString());
        console.log('Received payment response:', response);

        // Update payment status in database
        await updatePaymentStatus(response.transactionId, response.status, response.message);
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        console.error('Error processing payment response, sending to dead-letter:', err);
        await producer.send({
          topic: 'payment-responses-dead-letter',
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    }
  });

  // Retry consumer for dead-letter topic
  const retryConsumer = kafka.consumer({ groupId: 'payment-service-retry-group' });
  await retryConsumer.connect();
  await retryConsumer.subscribe({ topic: 'payment-responses-dead-letter', fromBeginning: true });
  await retryConsumer.run({
    eachMessage: async ({ message }: { message: any }) => {
      if (!message.value) return;
      try {
        const response = JSON.parse(message.value.toString());
        console.log('[Payment Service] Retrying payment response from dead-letter:', response);
        // Re-process the payment response (re-publish to main topic)
        await producer.send({
          topic: 'payment-responses',
          messages: [{ value: message.value.toString() }],
        });
      } catch (err) {
        console.error('[Payment Service] Error retrying payment response from dead-letter:', err);
      }
    }
  });
}

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

// Add payment status history tracking
async function updatePaymentStatus(transactionId: string, newStatus: string, message: string, reason?: string) {
  // Get old status
  const result = await pool.query('SELECT status FROM payment_transactions WHERE transaction_id = $1', [transactionId]);
  const oldStatus = result.rows[0]?.status || null;
  await pool.query(
    'UPDATE payment_transactions SET status = $1, message = $2 WHERE transaction_id = $3',
    [newStatus, message, transactionId]
  );
  await pool.query(
    'INSERT INTO payment_status_history (transaction_id, old_status, new_status, reason) VALUES ($1, $2, $3, $4)',
    [transactionId, oldStatus, newStatus, reason || message]
  );
}

// Add refund endpoint with idempotency
app.post('/payments/:transactionId/refund', async (req, res) => {
  const { transactionId } = req.params;
  const idempotencyKey = req.headers['idempotency-key'] || req.body.idempotencyKey;
  if (!idempotencyKey) return res.status(400).json({ error: 'Idempotency-Key is required' });
  const cacheKey = `refund:${transactionId}:${idempotencyKey}`;
  const cached = await redis.get(cacheKey);
  if (cached) return res.json(JSON.parse(cached));
  // Check if payment is eligible for refund
  const result = await pool.query('SELECT status FROM payment_transactions WHERE transaction_id = $1', [transactionId]);
  if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
  const status = result.rows[0].status;
  if (status !== 'SETTLED') return res.status(400).json({ error: 'Only SETTLED payments can be refunded' });
  // Mark as REFUNDED
  await updatePaymentStatus(transactionId, 'REFUNDED', 'Payment refunded');
  const responseObj = { transactionId, status: 'REFUNDED', message: 'Payment refunded' };
  await redis.set(cacheKey, JSON.stringify(responseObj), { EX: 86400 });
  res.json(responseObj);
});

// Add endpoint to get payment status history
app.get('/payments/:transactionId/history', async (req, res) => {
  const { transactionId } = req.params;
  const result = await pool.query('SELECT old_status, new_status, changed_at, reason FROM payment_status_history WHERE transaction_id = $1 ORDER BY changed_at ASC', [transactionId]);
  res.json(result.rows);
});

// Kafka producer for refund events
const refundProducer = producer;

// Emit refund event
async function emitRefundEvent(transactionId: string, amount: number, reason: string) {
  const event = {
    transactionId,
    amount,
    reason,
    timestamp: new Date().toISOString(),
  };
  await refundProducer.send({
    topic: 'refund-requests',
    messages: [{ value: JSON.stringify(event) }],
  });
}

// Listen for RefundCompletedEvent
const refundConsumer = kafka.consumer({ groupId: 'payment-service-refund-group' });
async function startRefundConsumer() {
  await refundConsumer.connect();
  await refundConsumer.subscribe({ topic: 'refund-completed', fromBeginning: true });
  await refundConsumer.run({
    eachMessage: async ({ message }: { message: any }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      if (event.status === 'REFUNDED') {
        await updatePaymentStatus(event.transactionId, 'REFUNDED', 'Automatic refund completed', event.reason);
      } else if (event.status === 'REFUND_FAILED') {
        await updatePaymentStatus(event.transactionId, 'REFUND_FAILED', 'Automatic refund failed', event.reason);
      }
    }
  });
}
startRefundConsumer().catch(console.error);

// In compensation logic, trigger refund if payment was SETTLED or AUTHORIZED before FAILED
setInterval(async () => {
  const stuck = await pool.query("SELECT transaction_id, status FROM payment_transactions WHERE status = 'FAILED' AND created_at < NOW() - INTERVAL '1 minute'");
  for (const row of stuck.rows) {
    // Check history for SETTLED or AUTHORIZED before FAILED
    const hist = await pool.query("SELECT old_status FROM payment_status_history WHERE transaction_id = $1 AND new_status = 'FAILED' ORDER BY changed_at DESC LIMIT 1", [row.transaction_id]);
    const prev = hist.rows[0]?.old_status;
    if (prev === 'SETTLED' || prev === 'AUTHORIZED') {
      // Avoid duplicate refunds
      const refundHist = await pool.query("SELECT 1 FROM payment_status_history WHERE transaction_id = $1 AND new_status = 'REFUND_PENDING'", [row.transaction_id]);
      if (refundHist.rows.length === 0) {
        await updatePaymentStatus(row.transaction_id, 'REFUND_PENDING', 'Automatic refund initiated');
        // Assume amount is available in payment_transactions (add column if needed)
        const amtRes = await pool.query('SELECT amount FROM payment_transactions WHERE transaction_id = $1', [row.transaction_id]);
        const amount = amtRes.rows[0]?.amount || 0;
        await emitRefundEvent(row.transaction_id, amount, 'Automatic compensation for failed payment');
      }
    }
  }
}, 60000);

// Start the service
app.listen(port, async () => {
  console.log(`Payment Service listening on port ${port}`);
  await startKafka();
}); 