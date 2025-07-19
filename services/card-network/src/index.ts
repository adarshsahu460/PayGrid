import express from 'express';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';

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
        await producer.send({
          topic: 'card-network-responses',
          messages: [
            {
              key: acquiringResponse.requestId,
              value: JSON.stringify({
                ...acquiringResponse,
                status: 'PROCESSED_BY_CARD_NETWORK',
                message: 'Processed by card network',
                timestamp: new Date().toISOString()
              })
            }
          ]
        });
      } catch (err) {
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
app.listen(port, async () => {
  console.log(`Card Network Service listening on port ${port}`);
  await startKafka();
});