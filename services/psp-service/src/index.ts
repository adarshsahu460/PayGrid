import express from 'express';
import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
import { buildISO8583Message } from '@paygrid/lib';
import axios from 'axios';
import promClient from 'prom-client';
dotenv.config();

const app = express();
app.use(express.json());

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();

app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'] });
const consumer = kafka.consumer({ groupId: 'psp-service-group' });
const producer = kafka.producer();

// Set up consumer for PSP responses from Card Network
const responseConsumer = kafka.consumer({ groupId: 'psp-service-resp-group' });

async function simulate3DS(payment: any) {
  try {
    const response = await axios.post(
      process.env.THREEDS_URL || 'http://3ds-service:3008/challenge',
      { paymentId: payment.paymentId, userId: payment.userId }
    );
    return response.data.success;
  } catch (err) {
    if (err instanceof Error) {
      console.error('[PSP] 3DS service error:', err.message);
    } else {
      console.error('[PSP] 3DS service error:', err);
    }
    return false;
  }
}

async function emitPaymentFailed(payment: any, reason: string) {
  const failedEvent = {
    paymentId: payment.paymentId,
    status: 'FAILED',
    reason,
    timestamp: new Date().toISOString(),
  };
  await producer.send({
    topic: 'payments',
    messages: [{ value: JSON.stringify(failedEvent) }],
  });
}

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'payments', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let payment;
      try {
        payment = JSON.parse(message.value.toString());
        console.log('[PSP] Received PaymentInitiated event:', payment);
        if (payment.method === 'card') {
          // Always require 3DS for card payments
          const passed3DS = await simulate3DS(payment);
          if (!passed3DS) {
            console.log('[PSP] 3DS challenge failed. Payment will be marked as failed.');
            await emitPaymentFailed(payment, '3DS authentication failed');
            return;
          }
        }
        // Format ISO-8583-like message
        const isoMsg = buildISO8583Message(payment);
        console.log('[PSP] Outgoing ISO-8583 message:', isoMsg);
        // Forward to Card Network service via Kafka
        await producer.send({
          topic: 'card-network',
          messages: [{ value: isoMsg }],
        });
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        console.error('[PSP] Error processing payment, sending to dead-letter:', err);
        await producer.send({
          topic: 'payments-dead-letter',
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    },
  });
}

async function startResponseConsumer() {
  await responseConsumer.connect();
  await responseConsumer.subscribe({ topic: 'psp-responses', fromBeginning: true });
  await responseConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const response = JSON.parse(message.value.toString());
      console.log('[PSP] Received response from Card Network:', response);
      // Forward to Payment Service
      try {
        await axios.post(
          process.env.PAYMENT_SERVICE_RESPONSE_URL || 'http://payment-service:3001/payments/response',
          response
        );
        console.log(`[PSP] Forwarded response for payment ${response.paymentId} to Payment Service`);
      } catch (err) {
        if (err instanceof Error) {
          console.error('[PSP] Error forwarding response to Payment Service:', err.message);
        } else {
          console.error('[PSP] Error forwarding response to Payment Service:', err);
        }
      }
    },
  });
}

// Retry consumer for dead-letter topic
async function startDeadLetterRetryConsumer() {
  const retryConsumer = kafka.consumer({ groupId: 'psp-service-retry-group' });
  await retryConsumer.connect();
  await retryConsumer.subscribe({ topic: 'payments-dead-letter', fromBeginning: true });
  await retryConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const payment = JSON.parse(message.value.toString());
        console.log('[PSP] Retrying payment from dead-letter:', payment);
        // Re-process the payment (same logic as above, or just re-publish to main topic)
        await producer.send({
          topic: 'payments',
          messages: [{ value: message.value.toString() }],
        });
      } catch (err) {
        console.error('[PSP] Error retrying payment from dead-letter:', err);
      }
    },
  });
}

startConsumer().catch(console.error);
startResponseConsumer().catch(console.error);
startDeadLetterRetryConsumer().catch(console.error);

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`PSP Service listening on port ${PORT}`);
}); 