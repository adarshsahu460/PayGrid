import express from 'express';
import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
import { buildISO8583Message } from '@paygrid/lib';
import axios from 'axios';
dotenv.config();

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok' });
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
    status: 'failed',
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
      const payment = JSON.parse(message.value.toString());
      console.log('[PSP] Received PaymentInitiated event:', payment);
      try {
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
        if (err instanceof Error) {
          console.error('[PSP] Error processing payment:', err.message);
          await emitPaymentFailed(payment, err.message);
        } else {
          console.error('[PSP] Error processing payment:', err);
          await emitPaymentFailed(payment, String(err));
        }
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

startConsumer().catch(console.error);
startResponseConsumer().catch(console.error);

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`PSP Service listening on port ${PORT}`);
}); 