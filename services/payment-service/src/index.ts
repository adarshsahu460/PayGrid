import express from 'express';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { createClient } from 'redis';
import dotenv from 'dotenv';
import { z } from 'zod';
import opossum from 'opossum';
import logger from './logger';

const PaymentRequestSchema = z.object({
  requestId: z.string(),
  merchantId: z.string(),
  amount: z.number().positive(),
  currency: z.string().length(3),
  cardNumber: z.string().min(13).max(19),
  expiryMonth: z.number().min(1).max(12),
  expiryYear: z.number().min(new Date().getFullYear()),
  cvv: z.string().length(3),
  description: z.string().optional(),
  metadata: z.record(z.string()).optional(),
  transactionId: z.string().optional(),
  cardToken: z.string(),
  clientId: z.string(),
  userId: z.string(),
});
type PaymentRequest = z.infer<typeof PaymentRequestSchema>;

function generateTransactionId(): string {
  return `txn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

dotenv.config();
const app = express();
const port = process.env.PORT || 3001;

const pool = new Pool({
  user: process.env.POSTGRES_USER || 'paygrid',
  password: process.env.POSTGRES_PASSWORD || 'paygrid',
  host: process.env.POSTGRES_HOST || 'postgres',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'paygrid'
});

const redis = createClient({
  url: `redis://${process.env.REDIS_HOST || 'redis'}:${process.env.REDIS_PORT || '6379'}`
});

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'payment-service-group' });

app.use(express.json());
app.get('/health', (_, res) => {
  return res.json({ status: 'healthy' });
});

// Query payment status
app.get('/payments/:transactionId/status', async (req, res) => {
  const { transactionId } = req.params;
  const result = await pool.query(
    'SELECT status, message FROM payment_transactions WHERE transaction_id = $1',
    [transactionId]
  );
  if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
  return res.json(result.rows[0]);
});

// Payment initiation with idempotency and status tracking
app.post('/payments', async (req, res) => {
  try {
    const idempotencyKey = req.headers['idempotency-key'] || req.body.idempotencyKey;
    if (!idempotencyKey) return res.status(400).json({ error: 'Idempotency-Key is required' });

    const cached = await redis.get(`idempotency:${idempotencyKey}`);
    if (cached) {
      const cachedObj = JSON.parse(cached);
      if (cachedObj.state === 'PROCESSING') {
        return res.status(202).json({ status: 'PROCESSING', message: 'Request is still being processed' });
      } else if (cachedObj.state === 'COMPLETED') {
        return res.json(cachedObj.response);
      } else {
        return res.status(409).json({ error: 'Idempotency key in unknown state' });
      }
    }

    await redis.set(`idempotency:${idempotencyKey}`,
      JSON.stringify({ state: 'PROCESSING' }),
      { EX: 86400 });

    const paymentRequest: PaymentRequest = req.body;
    if (!paymentRequest.userId) {
      return res.status(400).json({ error: 'userId is required in the request body' });
    }
    // Ensure cardNumber and requestId are present for schema validation
    const transactionId = generateTransactionId();
    const outgoingPaymentRequest = {
      ...paymentRequest,
      transactionId,
      requestId: paymentRequest.requestId || `req_${transactionId}`,
      cardNumber: paymentRequest.cardNumber || '4111111111111111', // fallback test card
      userId: paymentRequest.userId
    };
    await pool.query(
      'INSERT INTO payment_transactions (transaction_id, user_id, status, message, created_at) VALUES ($1, $2, $3, $4, NOW())',
      [transactionId, paymentRequest.userId, 'PENDING', 'Payment initiated']
    );
    // Send to Kafka for processing
    const kafkaProducerBreaker = new opossum(
      async () =>
        producer.send({
          topic: 'payment-requests',
          messages: [
            {
              key: transactionId,
              value: JSON.stringify(outgoingPaymentRequest)
            }
          ]
        }),
      {
        timeout: 3000, // If the function fails to complete in 3 seconds, trigger a failure
        errorThresholdPercentage: 50, // When 50% of requests fail, open the circuit
        resetTimeout: 30000 // After 30 seconds, try again.
      }
    );
    await kafkaProducerBreaker.fire();

    const response = {
      transactionId,
      status: 'PROCESSING',
      message: 'Payment request received'
    };
    return res.status(202).json(response);
  } catch (error) {
    logger.error({ err: error }, 'Error processing payment:');
    return res.status(500).json({
      status: 'FAILED',
      message: 'Error processing payment request'
    });
  }
});

async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await redis.connect();

  await consumer.subscribe({ topic: 'issuing-bank-responses', fromBeginning: true });
  await consumer.subscribe({ topic: 'payment-settlement-updates', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, message }: { topic: string, message: any }) => {
      if (!message.value) return;
      try {
        const response: any = JSON.parse(message.value.toString());
        logger.info({ response, topic }, 'Received Kafka message:');
        if (topic === 'issuing-bank-responses') {
          if (!response.transactionId) {
            logger.error({ response }, 'Received issuing-bank-responses with missing transactionId, sending to dead-letter:');
            await producer.send({
              topic: 'issuing-bank-responses-dead-letter',
              messages: [{ value: message.value?.toString() || '' }]
            });
            return;
          }
          await updatePaymentStatus(response.transactionId, response.status, response.message);
        } else if (topic === 'payment-settlement-updates'){
          if (!response.transactionId) {
            logger.error({ response }, 'Received payment-settlement-updates with missing transactionId, sending to dead-letter:');
            await producer.send({
              topic: 'payment-settlement-updates-dead-letter',
              messages: [{ value: message.value?.toString() || '' }]
            });
            return;
          }
          if (response.status === 'SETTLED') {
            await updatePaymentStatus(response.transactionId, 'SETTLED', 'Transaction settled by ledger service');
          }
        }
      } catch (err) {
        logger.error({ err, topic }, 'Error processing Kafka response, sending to dead-letter:');
        await producer.send({
          topic: `${topic}-dead-letter`,
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    }
  });
}


async function updatePaymentStatus(transactionId: string, newStatus: string, message: string, reason?: string) {
  // Get previous status and user_id
  const txRes = await pool.query('SELECT status, user_id FROM payment_transactions WHERE transaction_id = $1', [transactionId]);
  const oldStatus = txRes.rows[0]?.status;
  const userId = txRes.rows[0]?.user_id;
  await pool.query(
    'UPDATE payment_transactions SET status = $1, message = $2 WHERE transaction_id = $3',
    [newStatus, message, transactionId]
  );
  // Insert into status history
  if (userId) {
    await pool.query(
      'INSERT INTO payment_status_history (transaction_id, user_id, old_status, new_status, changed_at, reason) VALUES ($1, $2, $3, $4, NOW(), $5)',
      [transactionId, userId, oldStatus, newStatus, reason || null]
    );
  }
  await producer.send({
    topic: 'payment-status-changes',
    messages: [{ value: JSON.stringify({ transactionId, status: newStatus }) }]
  });
}

app.get('/payments/:transactionId/history', async (req, res) => {
  const { transactionId } = req.params;
  const result = await pool.query('SELECT old_status, new_status, changed_at, reason FROM payment_status_history WHERE transaction_id = $1 ORDER BY changed_at ASC', [transactionId]);
  return res.json(result.rows);
});

// Kafka producer for refund events
const refundProducer = producer;
const refundConsumer = kafka.consumer({ groupId: 'payment-service-refund-group' });

// Emit refund event
async function emitRefundEvent(transactionId: string, amount: number, reason: string, userId?: string) {
  const event = {
    transactionId,
    userId,
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

setInterval(async () => {
  const stuck = await pool.query("SELECT transaction_id, status FROM payment_transactions WHERE status = 'FAILED' AND created_at < NOW() - INTERVAL '1 minute'");
  for (const row of stuck.rows) {
    const hist = await pool.query("SELECT old_status FROM payment_status_history WHERE transaction_id = $1 AND new_status = 'FAILED' ORDER BY changed_at DESC LIMIT 1", [row.transaction_id]);
    const prev = hist.rows[0]?.old_status;
    if (prev === 'SETTLED' || prev === 'AUTHORIZED') {  
      const refundHist = await pool.query("SELECT 1 FROM payment_status_history WHERE transaction_id = $1 AND new_status = 'REFUND_PENDING'", [row.transaction_id]);
      if (refundHist.rows.length === 0) {
        await updatePaymentStatus(row.transaction_id, 'REFUND_PENDING', 'Automatic refund initiated');
        const amtRes = await pool.query('SELECT amount FROM payment_transactions WHERE transaction_id = $1', [row.transaction_id]);
        const amount = amtRes.rows[0]?.amount || 0;
        // Fetch userId for the transaction
        const userRes = await pool.query('SELECT user_id FROM payment_transactions WHERE transaction_id = $1', [row.transaction_id]);
        const userId = userRes.rows[0]?.user_id;
        await emitRefundEvent(row.transaction_id, amount, 'Automatic compensation for failed payment', userId);
      }
    }
  }
}, 60000);


// --- Dead-letter consumers ---
async function startDeadLetterConsumers() {
  const deadLetterConsumer = kafka.consumer({ groupId: 'payment-service-dead-letter-group' });
  await deadLetterConsumer.connect();
  // List of known dead-letter topics
  const dlTopics = [
    'issuing-bank-responses-dead-letter',
    'payment-settlement-updates-dead-letter',
    // Add more if needed
  ];
  for (const topic of dlTopics) {
    await deadLetterConsumer.subscribe({ topic, fromBeginning: true });
  }
  await deadLetterConsumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      let parsed;
      try {
        parsed = JSON.parse(message.value.toString());
      } catch {
        logger.error({ topic, value: message.value.toString() }, '[DLQ] Could not parse message');
        return;
      }
      // Try to get timestamp from message, fallback to Kafka timestamp
      let msgTimestamp = parsed.timestamp ? new Date(parsed.timestamp) : (message.timestamp ? new Date(Number(message.timestamp)) : new Date());
      const now = new Date();
      if (now.getTime() - msgTimestamp.getTime() > 60 * 1000) {
        logger.warn({ topic, value: parsed }, '[DLQ] Dropping message older than 1 minute');
        // Publish to dlq-failures topic for payment-service to mark as FAILED
        let transactionId = parsed.transactionId || (parsed.payload && parsed.payload.transactionId);
        await producer.send({
          topic: 'dlq-failures',
          messages: [{
            value: JSON.stringify({
              transactionId,
              originalTopic: topic,
              droppedAt: now.toISOString(),
              reason: 'Dead-letter message dropped after 1 minute',
              payload: parsed
            })
          }]
        });
        return;
      }
      // Retry: publish to original topic
      let originalTopic = topic.replace('-dead-letter', '');
      logger.info({ topic, originalTopic, value: parsed }, '[DLQ] Retrying message');
      await producer.send({
        topic: originalTopic,
        messages: [{ value: message.value.toString() }]
      });
    }
  });
}

async function startDLQFailureConsumer() {
  const dlqFailureConsumer = kafka.consumer({ groupId: 'payment-service-dlq-failure-group' });
  await dlqFailureConsumer.connect();
  await dlqFailureConsumer.subscribe({ topic: 'dlq-failures', fromBeginning: true });
  await dlqFailureConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let parsed;
      try {
        parsed = JSON.parse(message.value.toString());
      } catch {
        logger.error({ value: message.value.toString() }, '[DLQ-FAILURE] Could not parse message');
        return;
      }
      const transactionId = parsed.transactionId;
      if (!transactionId) {
        logger.error({ parsed }, '[DLQ-FAILURE] No transactionId found in message');
        return;
      }
      // Update payment_transactions to FAILED
      await pool.query(
        'UPDATE payment_transactions SET status = $1, message = $2 WHERE transaction_id = $3',
        ['FAILED', `[DLQ-FAILURE] ${parsed.reason || 'Message dropped from DLQ'}`, transactionId]
      );
      logger.info({ transactionId }, '[DLQ-FAILURE] Marked payment as FAILED');
    }
  });
}

app.listen(port, async () => {
  logger.info(`Payment Service listening on port ${port}`);
  await startKafka();
  await startDeadLetterConsumers();
  await startDLQFailureConsumer();
});
