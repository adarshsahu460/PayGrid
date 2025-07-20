import express from 'express';
import { Kafka } from 'kafkajs';
import axios from 'axios';
import dotenv from 'dotenv';
import logger from './logger';
dotenv.config();

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});
const consumer = kafka.consumer({ groupId: 'notification-service-group' });

const webhookRegistry: Map<string, string> = new Map();

app.post('/register-webhook', (req, res) => {
  const { clientId, url } = req.body;
  if (!clientId || !url) return res.status(400).json({ error: 'clientId and url required' });
  webhookRegistry.set(clientId, url);
  return res.json({ status: 'registered' });
});

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'payment-status-changes', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      const url = webhookRegistry.get(event.clientId);
      if (url) {
        await axios.post(url, event).catch(err => {
          logger.error({ err, url, event }, 'Failed to send webhook');
        });
      }
      // TODO: Add email sending logic here
    }
  });
}
startKafka().catch(err => logger.error({ err }, 'Error starting Kafka consumer'));

const PORT = process.env.PORT || 3006;
app.listen(PORT, () => {
  logger.info(`Notification Service listening on port ${PORT}`);
});
