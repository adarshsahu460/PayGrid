import express from 'express';
import dotenv from 'dotenv';
import promClient from 'prom-client';
import { Kafka } from 'kafkajs';

dotenv.config();

const app = express();
const port = process.env.PORT || 3013;
app.use(express.json());

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();

// Kafka setup
const kafka = new Kafka({
  clientId: 'upiswitch-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});
const consumer = kafka.consumer({ groupId: 'upiswitch-service-group' });
const producer = kafka.producer();

async function startKafka() {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'upi-requests', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      // Simulate routing and response
      const response = {
        ...event,
        status: 'SUCCESS',
        switchRef: `UPIREF-${Date.now()}`,
        timestamp: new Date().toISOString()
      };
      await producer.send({
        topic: 'upi-responses',
        messages: [{ key: event.transactionId || event.requestId, value: JSON.stringify(response) }]
      });
    }
  });
}

app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

app.listen(port, async () => {
  await startKafka();
  console.log(`UPI Switch Service listening on port ${port}`);
}); 