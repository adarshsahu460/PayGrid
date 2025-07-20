import express from 'express';
import crypto from 'crypto';
import dotenv from 'dotenv';
import logger from './logger';
dotenv.config();

const app = express();
app.use(express.json());

const cardVault: Map<string, string> = new Map();

app.post('/tokenize', (req, res) => {
  const { cardNumber } = req.body;
  if (!cardNumber) return res.status(400).json({ error: 'cardNumber required' });
  const token = 'tok_' + crypto.randomBytes(8).toString('hex');
  cardVault.set(token, cardNumber);
  return res.json({ token });
});

app.post('/detokenize', (req, res) => {
  const { token } = req.body;
  const cardNumber = cardVault.get(token);
  if (!cardNumber) return res.status(404).json({ error: 'Token not found' });
  return res.json({ cardNumber });
});

const PORT = process.env.PORT || 3007;
app.listen(PORT, () => {
  logger.info(`Tokenization Service listening on port ${PORT}`);
});
