import express from 'express';
import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import logger from './logger';
dotenv.config();

const app = express();
app.use(express.json());

function sendError(res: express.Response, status: number, message: string) {
  res.status(status).json({ error: message });
}

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
    return sendError(res, 400, 'Idempotency-Key is required');
  }
  try {
    // Step 1: Fraud Detection
    const fraudResp = await axios.post(
      process.env.FRAUD_DETECTION_URL || 'http://fraud-detection-service:3008/analyze',
      req.body
    );
    if (fraudResp.data.risk === 'high') {
      return sendError(res, 403, 'Transaction flagged as high risk');
    }

    // Step 2: Tokenization
    const tokenResp = await axios.post(
      process.env.TOKENIZATION_URL || 'http://tokenization-service:3007/tokenize',
      { cardNumber: req.body.cardNumber }
    );
    if (!tokenResp.data.token) {
      return sendError(res, 500, 'Tokenization failed');
    }
    // Replace cardNumber with token
    const paymentBody = { ...req.body, cardToken: tokenResp.data.token };
    delete paymentBody.cardNumber;

    // Step 3: Payment Service
    const response = await axios.post(
      process.env.PAYMENT_SERVICE_URL || 'http://payment-service:3001/payments',
      paymentBody,
      { headers: { 'Idempotency-Key': idempotencyKey } }
    );
    res.status(response.status).json(response.data);
  } catch (err: any) {
    sendError(res, err.response?.status || 500, err.message);
  }
});

const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  logger.info(`API Gateway listening on port ${PORT}`);
});

// Optional: Graceful shutdown
process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down...');
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down...');
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});
