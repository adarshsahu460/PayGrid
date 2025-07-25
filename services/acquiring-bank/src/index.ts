import express from 'express';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import opossum from 'opossum';
import logger from './logger';
import { detokenize } from './tokenizationClient';

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
  let cardNumber = paymentRequest.cardNumber;
  if (!cardNumber && paymentRequest.cardToken) {
    cardNumber = await detokenize(paymentRequest.cardToken);
  }
  // Use cardNumber for any real processing logic if needed
  const isApproved = Math.random() > 0.15;
  return {
    requestId: paymentRequest.requestId,
    transactionId: paymentRequest.transactionId || paymentRequest.requestId,
    userId: paymentRequest.userId,
    status: isApproved ? 'SETTLED' : 'DECLINED',
    message: isApproved ? 'Transaction settled by acquiring bank' : 'Transaction declined by acquiring bank',
    timestamp: new Date().toISOString(),
    cardNumber: cardNumber ? '****' + cardNumber.slice(-4) : undefined 
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
        console.log("Acquiring bank sending the paymentRequest : ",paymentRequest)
        await breaker.fire(paymentRequest);
        // Publish to next stage
        await producer.send({
          topic: 'acquiring-bank-responses',
          messages: [
            {
              key: paymentRequest.requestId,
              value: JSON.stringify({
                ...paymentRequest,
                userId: paymentRequest.userId,
                status: 'PROCESSED_BY_ACQUIRING',
                message: 'Processed by acquiring bank',
                timestamp: new Date().toISOString(),
                requestId: paymentRequest.requestId // Explicitly include requestId
              })
            }
          ]
        });
      } catch (err) {
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
            userId: event.userId,
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

// --- Dead-letter consumer ---
async function startDeadLetterConsumer() {
  const deadLetterConsumer = kafka.consumer({ groupId: 'acquiring-bank-dead-letter-group' });
  await deadLetterConsumer.connect();
  await deadLetterConsumer.subscribe({ topic: 'payment-requests-dead-letter', fromBeginning: true });
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
      logger.info({ topic, value: parsed }, '[DLQ] Retrying message');
      await producer.send({
        topic: 'payment-requests',
        messages: [{ value: message.value.toString() }]
      });
    }
  });
}

app.listen(port, async () => {
  logger.info(`Acquiring Bank Service listening on port ${port}`);
  await startKafka();
  await startDeadLetterConsumer();
});
