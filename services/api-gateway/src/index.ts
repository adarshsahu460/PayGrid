import express from 'express';
import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
dotenv.config();

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

app.get('/idempotency-key', (_req, res) => {
  const key = uuidv4();
  res.status(200).json({ idempotencyKey: key });
});

// Placeholder for payment initiation route
app.post('/payments', async (req, res) => {
  const idempotencyKey = req.headers['idempotency-key'] || req.body.idempotencyKey;
  if (!idempotencyKey) {
    return res.status(400).json({ error: 'Idempotency-Key is required' });
  }
  try {
    const response = await axios.post(
      process.env.PAYMENT_SERVICE_URL || 'http://payment-service:3001/payments',
      req.body,
      { headers: { 'Idempotency-Key': idempotencyKey } }
    );
    res.status(response.status).json(response.data);
  } catch (err: any) {
    res.status(err.response?.status || 500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API Gateway listening on port ${PORT}`);
}); 