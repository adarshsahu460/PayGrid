import express from 'express';
import dotenv from 'dotenv';
import promClient from 'prom-client';

dotenv.config();

const app = express();
const port = process.env.PORT || 3015;

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();

app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

app.listen(port, () => {
  console.log(`Settlement Batch Processor Service listening on port ${port}`);
}); 