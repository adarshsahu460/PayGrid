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
  transactionId: z.string().optional()
});


// Type definitions
type BasePaymentRequest = z.infer<typeof PaymentRequestSchema>;

function generateTransactionId(): string {
  return `txn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}


// Extend PaymentRequest to include cardToken and clientId
interface PaymentRequest extends BasePaymentRequest {
  cardToken: string;
  clientId: string;
}

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
 
// Add webhook registration
const webhookMap: Map<string, string> = new Map();
app.post('/webhooks/register', (req, res) => {
  const { clientId, url } = req.body;
  if (!clientId || !url) return res.status(400).json({ error: 'clientId and url required' });
  webhookMap.set(clientId, url);
  return res.json({ status: 'registered' });
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
    console.log("transactionID is :", transactionId)
    // Send to Kafka for processing
    const kafkaProducerBreaker = new opossum(
      async () =>
        producer.send({
          topic: 'payment-requests',
          messages: [
            {
              key: transactionId,
              value: JSON.stringify({
                ...paymentRequest,
                transactionId,
                requestId: paymentRequest.requestId // Explicitly include requestId
              })
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
    // Store idempotency response in Redis (24h expiry)
    await redis.set(`idempotency:${idempotencyKey}`, JSON.stringify(response), { EX: 86400 });
    return res.json(response);
  } catch (error) {
    logger.error({ err: error }, 'Error processing payment:');
    return res.status(500).json({
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

  // Listen for final payment status from issuing bank
  await consumer.subscribe({ topic: 'issuing-bank-responses', fromBeginning: true });
  // NEW: Listen for settlement updates from Ledger Service
  await consumer.subscribe({ topic: 'payment-settlement-updates', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, message }: { topic: string, message: any }) => { // Add 'topic' to destructuring
      if (!message.value) return;
      try {
        const response: any = JSON.parse(message.value.toString()); // Use 'any' for now, or define a new schema
        logger.info({ response, topic }, 'Received Kafka message:');

        if (topic === 'issuing-bank-responses') {
          // Existing logic for issuing-bank-responses
          if (!response.transactionId) { // NEW: Check for missing transactionId
            logger.error({ response }, 'Received issuing-bank-responses with missing transactionId, sending to dead-letter:');
            await producer.send({
              topic: 'issuing-bank-responses-dead-letter',
              messages: [{ value: message.value?.toString() || '' }]
            });
            return; // Skip processing this message
          }
          await updatePaymentStatus(response.transactionId, response.status, response.message);
        } else if (topic === 'payment-settlement-updates') { // NEW BLOCK
          if (!response.transactionId) { // NEW: Check for missing transactionId
            logger.error({ response }, 'Received payment-settlement-updates with missing transactionId, sending to dead-letter:');
            await producer.send({
              topic: 'payment-settlement-updates-dead-letter',
              messages: [{ value: message.value?.toString() || '' }]
            });
            return; // Skip processing this message
          }
          if (response.status === 'SETTLED') {
            await updatePaymentStatus(response.transactionId, 'SETTLED', 'Transaction settled by ledger service');
          }
        }
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        logger.error({ err, topic }, 'Error processing Kafka response, sending to dead-letter:');
        await producer.send({
          topic: `${topic}-dead-letter`, // Use dynamic dead-letter topic
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    }
  });

  // The retry consumer is disabled to prevent infinite loops with malformed messages.
  // Dead-lettered messages should be handled with a more robust strategy, such as manual inspection or a delayed retry with an exponential backoff.
  //
  // // Retry consumer for dead-letter topic
  // const retryConsumer = kafka.consumer({ groupId: 'payment-service-retry-group' });
  // await retryConsumer.connect();
  // await retryConsumer.subscribe({ topic: 'issuing-bank-responses-dead-letter', fromBeginning: true });
  // await retryConsumer.run({
  //   eachMessage: async ({ message }: { message: any }) => {
  //     if (!message.value) return;
  //     try {
  //       const response = JSON.parse(message.value.toString());
  //       logger.info({ response }, '[Payment Service] Retrying payment response from dead-letter:');
  //       // Re-process the payment response (re-publish to main topic)
  //       await producer.send({
  //         topic: 'issuing-bank-responses',
  //         messages: [{ value: message.value.toString() }],
  //       });
  //     } catch (err) {
  //       logger.error({ err }, '[Payment Service] Error retrying payment response from dead-letter:');
  //     }
  //   }
  // });
}


// Add payment status history tracking
async function updatePaymentStatus(transactionId: string, newStatus: string, message: string, reason?: string) {
  // Get old status
  console.log('--- Inside updatePaymentStatus ---');
  console.log(`Received transactionId: ${transactionId} (Type: ${typeof transactionId})`);
  console.log('------------------------------------');
  await pool.query(
    'UPDATE payment_transactions SET status = $1, message = $2 WHERE transaction_id = $3',
    [newStatus, message, transactionId]
  );
  if(!transactionId){
    console.log("No transactionID");
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
    const prev = hist.rows[0]?.old_status;``
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
  logger.info(`Payment Service listening on port ${port}`);
  await startKafka();
});
