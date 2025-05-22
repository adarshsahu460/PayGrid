import express from 'express';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import dotenv from 'dotenv';
import { PaymentRequest, PaymentResponse } from '@paygrid/lib';

dotenv.config();

const app = express();
const port = process.env.PORT || 3004;

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
  clientId: 'issuing-bank',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'issuing-bank-group' });

// Middleware
app.use(express.json());

// Routes
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
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
      // Simulate issuing bank processing
      const isApproved = Math.random() > 0.2; // 80% approval rate
      const response: PaymentResponse = {
        requestId: paymentRequest.requestId,
          transactionId: paymentRequest.transactionId || paymentRequest.requestId,
          status: isApproved ? 'AUTHORIZED' : 'DECLINED',
          message: isApproved ? 'Transaction authorized by issuing bank' : 'Transaction declined by issuing bank',
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
        console.error('[Issuing Bank] Error processing payment, sending to dead-letter:', err);
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
  const retryConsumer = kafka.consumer({ groupId: 'issuing-bank-retry-group' });
  await retryConsumer.connect();
  await retryConsumer.subscribe({ topic: 'payment-requests-dead-letter', fromBeginning: true });
  await retryConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const paymentRequest = JSON.parse(message.value.toString());
        console.log('[Issuing Bank] Retrying payment from dead-letter:', paymentRequest);
        // Re-process the payment (re-publish to main topic)
        await producer.send({
          topic: 'payment-requests',
          messages: [{ value: message.value.toString() }],
        });
      } catch (err) {
        console.error('[Issuing Bank] Error retrying payment from dead-letter:', err);
      }
    }
  });
}

// Refund consumer
const refundConsumer = kafka.consumer({ groupId: 'issuing-bank-refund-group' });
const refundProducer = producer;
async function startRefundConsumer() {
  await refundConsumer.connect();
  await refundConsumer.subscribe({ topic: 'refund-requests', fromBeginning: true });
  await refundConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      // Idempotency: check if already refunded
      if (event && event.transactionId) {
        // Simulate refund processing
        const success = Math.random() > 0.05; // 95% refund success
        const status = success ? 'REFUNDED' : 'REFUND_FAILED';
        // Emit RefundCompletedEvent
        await refundProducer.send({
          topic: 'refund-completed',
          messages: [{ value: JSON.stringify({
            transactionId: event.transactionId,
            status,
            reason: success ? undefined : 'Refund failed at issuing bank',
            timestamp: new Date().toISOString(),
          }) }],
        });
      }
    }
  });
}
startRefundConsumer().catch(console.error);

// Start the service
app.listen(port, async () => {
  console.log(`Issuing Bank Service listening on port ${port}`);
  await startKafka();
}); 