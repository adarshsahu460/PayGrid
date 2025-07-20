import express from 'express';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import opossum from 'opossum';
import logger from './logger';

dotenv.config();

const app = express();
const port = process.env.PORT || 3005;


// Kafka setup
const kafka = new Kafka({
  clientId: 'acquiring-bank',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'acquiring-bank-group' });

// Middleware
app.use(express.json());

// Routes
app.get('/health', (_, res) => {
  res.json({ status: 'healthy' });
});

const processPayment = async (paymentRequest: any) => {
  const isApproved = Math.random() > 0.15; // 85% approval rate
  return {
    requestId: paymentRequest.requestId,
    transactionId: paymentRequest.transactionId || paymentRequest.requestId,
    status: isApproved ? 'SETTLED' : 'DECLINED',
    message: isApproved ? 'Transaction settled by acquiring bank' : 'Transaction declined by acquiring bank',
    timestamp: new Date().toISOString()
  };
};
const breaker = new opossum(processPayment, { timeout: 5000, errorThresholdPercentage: 50, resetTimeout: 30000 });

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
        await breaker.fire(paymentRequest);
        // Publish to next stage
        await producer.send({ 
          topic: 'acquiring-bank-responses',
          messages: [
            {
              key: paymentRequest.requestId,
              value: JSON.stringify({
                ...paymentRequest,
                status: 'PROCESSED_BY_ACQUIRING',
                message: 'Processed by acquiring bank',
                timestamp: new Date().toISOString()
              })
            }
          ]
        });
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        logger.error({ err }, '[Acquiring Bank] Error processing payment, sending to dead-letter:');
        await producer.send({
          topic: 'payment-requests-dead-letter',
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    }
  });
  const retryConsumer = kafka.consumer({ groupId: 'acquiring-bank-retry-group' });
  await retryConsumer.connect();
  await retryConsumer.subscribe({ topic: 'payment-requests-dead-letter', fromBeginning: true });
  await retryConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const paymentRequest = JSON.parse(message.value.toString());
        logger.info({ paymentRequest }, '[Acquiring Bank] Retrying payment from dead-letter:');
        // Re-process the payment (re-publish to main topic)
        await producer.send({
          topic: 'payment-requests',
          messages: [{ value: message.value.toString() }],
        });
      } catch (err) {
        logger.error({ err }, '[Acquiring Bank] Error retrying payment from dead-letter:');
      }
    }
  });
}

// Refund consumer
const refundConsumer = kafka.consumer({ groupId: 'acquiring-bank-refund-group' });
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
            reason: success ? undefined : 'Refund failed at acquiring bank',
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
  logger.info(`Acquiring Bank Service listening on port ${port}`);
  await startKafka();
});
