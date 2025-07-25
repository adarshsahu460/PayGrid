import express from 'express';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import logger from './logger';

dotenv.config();

const app = express();
const port = process.env.PORT || 3003;

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
app.get('/health', (_, res) => {
  res.json({ status: 'healthy' });
});


// Kafka message handling
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'acquiring-bank-responses', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let acquiringResponse;
      try {
        acquiringResponse = JSON.parse(message.value.toString()); 
        console.log("Response at the card-network :",acquiringResponse)
        await producer.send({
          topic: 'card-network-responses',
          messages: [
            {
              key: acquiringResponse.requestId,
              value: JSON.stringify({
                ...acquiringResponse,
                userId: acquiringResponse.userId,
                status: 'PROCESSED_BY_CARD_NETWORK',
                message: 'Processed by card network',
                timestamp: new Date().toISOString(),
                requestId: acquiringResponse.requestId // Explicitly include requestId
              })
            }
          ]
        });
      } catch (err) {
        logger.error({ err }, 'Error processing acquiring bank response, sending to dead-letter:');
        await producer.send({
          topic: 'acquiring-bank-responses-dead-letter',
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    }
  });
}

// Start the service

// --- Dead-letter consumer ---
async function startDeadLetterConsumer() {
  const deadLetterConsumer = kafka.consumer({ groupId: 'card-network-dead-letter-group' });
  await deadLetterConsumer.connect();
  await deadLetterConsumer.subscribe({ topic: 'acquiring-bank-responses-dead-letter', fromBeginning: true });
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
        topic: 'acquiring-bank-responses',
        messages: [{ value: message.value.toString() }]
      });
    }
  });
}

app.listen(port, async () => {
  logger.info(`Card Network Service listening on port ${port}`);
  await startKafka();
  await startDeadLetterConsumer();
});
