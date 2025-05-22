import express from 'express';
import dotenv from 'dotenv';
import promClient from 'prom-client';
import { Kafka, KafkaMessage } from 'kafkajs';

dotenv.config();

const app = express();
const port = process.env.PORT || 3011;
app.use(express.json());

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();

// In-memory wallet balances (for demo)
const balances: Record<string, number> = {};

// Kafka setup
const kafka = new Kafka({
  clientId: 'wallet-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});
const consumer = kafka.consumer({ groupId: 'wallet-service-group' });

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'wallet-credit', fromBeginning: true });
  await consumer.subscribe({ topic: 'wallet-debit', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, message }: { topic: string; message: KafkaMessage }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      const { account, amount } = event;
      if (!account || typeof amount !== 'number') return;
      if (topic === 'wallet-credit') {
        balances[account] = (balances[account] || 0) + amount;
      } else if (topic === 'wallet-debit') {
        balances[account] = (balances[account] || 0) - amount;
      }
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

app.get('/wallet/:account/balance', (req, res) => {
  const { account } = req.params;
  res.json({ account, balance: balances[account] || 0 });
});

// Demo/manual credit/debit endpoints
app.post('/wallet/credit', (req, res) => {
  const { account, amount } = req.body;
  if (!account || typeof amount !== 'number') return res.status(400).json({ error: 'Invalid input' });
  balances[account] = (balances[account] || 0) + amount;
  res.json({ account, balance: balances[account] });
});

app.post('/wallet/debit', (req, res) => {
  const { account, amount } = req.body;
  if (!account || typeof amount !== 'number') return res.status(400).json({ error: 'Invalid input' });
  balances[account] = (balances[account] || 0) - amount;
  res.json({ account, balance: balances[account] });
});

app.listen(port, async () => {
  await startKafka();
  console.log(`Wallet Service listening on port ${port}`);
}); 