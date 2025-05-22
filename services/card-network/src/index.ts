import express from 'express';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import dotenv from 'dotenv';
import { PaymentRequest, PaymentResponse } from '@paygrid/lib';
import promClient from 'prom-client';

dotenv.config();

const app = express();
const port = process.env.PORT || 3003;

// PostgreSQL setup
const pool = new Pool({
  host: process.env.POSTGRES_HOST || 'postgres',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'paygrid',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres'
});

// Kafka setup
const kafka = new Kafka({
  clientId: 'card-network',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'card-network-group' });

// Middleware
app.use(express.json());

// Routes
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

// Kafka message handling
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  
  await consumer.subscribe({ topic: 'payment-requests', fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let paymentRequest;
      try {
        paymentRequest = JSON.parse(message.value.toString());
        console.log('Received payment request:', paymentRequest);
        // Simulate card network processing
        const isApproved = Math.random() > 0.1; // 90% approval rate
        const response: PaymentResponse = {
          requestId: paymentRequest.requestId,
          transactionId: paymentRequest.transactionId || paymentRequest.requestId,
          status: isApproved ? 'AUTHORIZED' : 'DECLINED',
          message: isApproved ? 'Transaction authorized by card network' : 'Transaction declined by card network',
          timestamp: new Date().toISOString()
        };
        // Send response to payment service
        await producer.send({
          topic: 'payment-responses',
          messages: [
            {
              key: paymentRequest.requestId,
              value: JSON.stringify(response)
            }
          ]
        });
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        console.error('[Card Network] Error processing payment, sending to dead-letter:', err);
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
  const retryConsumer = kafka.consumer({ groupId: 'card-network-retry-group' });
  await retryConsumer.connect();
  await retryConsumer.subscribe({ topic: 'payment-requests-dead-letter', fromBeginning: true });
  await retryConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const paymentRequest = JSON.parse(message.value.toString());
        console.log('[Card Network] Retrying payment from dead-letter:', paymentRequest);
        // Re-process the payment (re-publish to main topic)
        await producer.send({
          topic: 'payment-requests',
          messages: [{ value: message.value.toString() }],
        });
      } catch (err) {
        console.error('[Card Network] Error retrying payment from dead-letter:', err);
      }
    }
  });
}

// Start the service
app.listen(port, async () => {
  console.log(`Card Network Service listening on port ${port}`);
  await startKafka();
}); 