import express from 'express';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import dotenv from 'dotenv';
import { PaymentRequest, PaymentResponse } from '@paygrid/lib';
import promClient from 'prom-client';

dotenv.config();

const app = express();
const port = process.env.PORT || 3008;

// Redis setup for session storage
const redis = createClient({
  url: `redis://${process.env.REDIS_HOST || 'redis'}:${process.env.REDIS_PORT || '6379'}`
});

// Kafka setup
const kafka = new Kafka({
  clientId: '3ds-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: '3ds-service-group' });

// Middleware
app.use(express.json());

// Routes
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

// 3DS Authentication endpoint
app.post('/authenticate', async (req, res) => {
  try {
    const { transactionId, cardNumber } = req.body;

    // Simulate 3DS authentication
    const requires3DS = cardNumber.startsWith('4'); // Example: Visa cards require 3DS
    const authenticationUrl = requires3DS ? `/3ds-challenge/${transactionId}` : null;

    // Store authentication state in Redis
    await redis.set(`3ds:${transactionId}`, JSON.stringify({
      requires3DS,
      status: requires3DS ? 'PENDING' : 'COMPLETED',
      timestamp: new Date().toISOString()
    }), {
      EX: 3600 // 1 hour expiry
    });

    res.json({
      transactionId,
      requires3DS,
      authenticationUrl
    });
  } catch (error) {
    console.error('Error in 3DS authentication:', error);
    res.status(500).json({
      status: 'FAILED',
      message: 'Error processing 3DS authentication'
    });
  }
});

// 3DS Challenge completion endpoint
app.post('/challenge/:transactionId/complete', async (req, res) => {
  const idempotencyKey = req.headers['idempotency-key'] || req.body.idempotencyKey;
  if (!idempotencyKey) {
    return res.status(400).json({ error: 'Idempotency-Key is required' });
  }
  const cacheKey = `3ds-challenge-complete:${idempotencyKey}`;
  const cached = await redis.get(cacheKey);
  if (cached) {
    return res.json(JSON.parse(cached));
  }
  try {
    const { transactionId } = req.params;
    const { success } = req.body;
    // Update authentication state
    await redis.set(`3ds:${transactionId}`, JSON.stringify({
      requires3DS: true,
      status: success ? 'AUTHENTICATED' : 'FAILED',
      timestamp: new Date().toISOString()
    }), {
      EX: 3600
    });
    // Send response to payment service
    await producer.send({
      topic: 'payment-responses',
      messages: [
        {
          key: transactionId,
          value: JSON.stringify({
            transactionId,
            requestId: transactionId,
            status: success ? 'AUTHENTICATED' : 'DECLINED',
            message: success ? '3DS authentication successful' : '3DS authentication failed',
            timestamp: new Date().toISOString()
          })
        }
      ]
    });
    const response = {
      transactionId,
      status: success ? 'AUTHENTICATED' : 'FAILED'
    };
    await redis.set(cacheKey, JSON.stringify(response), { EX: 86400 });
    res.json(response);
  } catch (error) {
    console.error('Error completing 3DS challenge:', error);
    res.status(500).json({
      status: 'FAILED',
      message: 'Error completing 3DS challenge'
    });
  }
});

// Kafka message handling
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await redis.connect();
  
  await consumer.subscribe({ topic: 'payment-requests', fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let paymentRequest;
      try {
        paymentRequest = JSON.parse(message.value.toString());
        console.log('Received payment request for 3DS:', paymentRequest);
        // Check if 3DS is required
        const requires3DS = paymentRequest.cardNumber.startsWith('4');
        if (requires3DS) {
          // Store payment request in Redis
          await redis.set(`payment:${paymentRequest.requestId}`, JSON.stringify(paymentRequest), {
            EX: 3600
          });
        }
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        console.error('[3DS] Error processing payment, sending to dead-letter:', err);
        await producer.send({
          topic: 'payment-requests-dead-letter',
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    }
  });

  // Retry consumer for dead-letter topic
  const retryConsumer = kafka.consumer({ groupId: '3ds-service-retry-group' });
  await retryConsumer.connect();
  await retryConsumer.subscribe({ topic: 'payment-requests-dead-letter', fromBeginning: true });
  await retryConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const paymentRequest = JSON.parse(message.value.toString());
        console.log('[3DS] Retrying payment from dead-letter:', paymentRequest);
        // Re-process the payment (re-publish to main topic)
        await producer.send({
          topic: 'payment-requests',
          messages: [{ value: message.value.toString() }],
        });
      } catch (err) {
        console.error('[3DS] Error retrying payment from dead-letter:', err);
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

// Start the service
app.listen(port, async () => {
  console.log(`3DS Service listening on port ${port}`);
  await startKafka();
}); 