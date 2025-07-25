import express from 'express';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import logger from './logger';
import { detokenize } from './tokenizationClient';

dotenv.config();

const app = express();
const port = process.env.PORT || 3004;


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
app.get('/health', (_, res) => {
  res.json({ status: 'healthy' });
});

// Kafka message handling
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'card-network-responses', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let cardNetworkResponse;
      try {
        cardNetworkResponse = JSON.parse(message.value.toString());
        logger.info({ cardNetworkResponse }, 'Received card network response:');
        // Detokenize if only cardToken is present
        let cardNumber = cardNetworkResponse.cardNumber;
        if (!cardNumber && cardNetworkResponse.cardToken) {
          cardNumber = await detokenize(cardNetworkResponse.cardToken);
        }
        // Simulate issuing bank processing
        const isApproved = Math.random() > 0.2; // 80% approval rate
        const response: any = {
          ...cardNetworkResponse,
          userId: cardNetworkResponse.userId,
          status: isApproved ? 'AUTHORIZED' : 'DECLINED',
          message: isApproved ? 'Transaction authorized by issuing bank' : 'Transaction declined by issuing bank',
          timestamp: new Date().toISOString(),
          cardNumber: cardNumber ? '****' + cardNumber.slice(-4) : undefined // Masked for logs
        };
        // Log the response before sending to Kafka to debug missing requestId
        logger.info({ response }, 'Issuing Bank response before sending to Kafka:');
        await producer.send({
          topic: 'issuing-bank-responses',
          messages: [
            {
              key: cardNetworkResponse.requestId,
              value: JSON.stringify(response)
            }
          ]
        });
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        logger.error({ err }, '[Issuing Bank] Error processing payment, sending to dead-letter:');
        await producer.send({
          topic: 'card-network-responses-dead-letter',
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
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
            userId: event.userId,
            status,
            reason: success ? undefined : 'Refund failed at issuing bank',
            timestamp: new Date().toISOString(),
          }) }],
        });
      }
    }
  });
}
startRefundConsumer().catch(err => logger.error({ err }, 'Error starting refund consumer'));

// Start the service

// --- Dead-letter consumer ---
async function startDeadLetterConsumer() {
  const deadLetterConsumer = kafka.consumer({ groupId: 'issuing-bank-dead-letter-group' });
  await deadLetterConsumer.connect();
  await deadLetterConsumer.subscribe({ topic: 'card-network-responses-dead-letter', fromBeginning: true });
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
        topic: 'card-network-responses',
        messages: [{ value: message.value.toString() }]
      });
    }
  });
}

app.listen(port, async () => {
  logger.info(`Issuing Bank Service listening on port ${port}`);
  await startKafka();
  await startDeadLetterConsumer();
});
