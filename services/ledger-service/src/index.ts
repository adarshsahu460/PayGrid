import express from 'express';
import { Kafka } from 'kafkajs';
import { z } from 'zod';
import { Pool } from 'pg';
import dotenv from 'dotenv';
import { SettlementProcessor, SettlementConfig } from './settlement';
import pino from 'pino';

export const PaymentRequestSchema = z.object({
  requestId: z.string(),
  merchantId: z.string(),
  amount: z.number().positive(),
  currency: z.string().length(3),
  cardNumber: z.string().min(13).max(19),
  expiryMonth: z.number().min(1).max(12),
  expiryYear: z.number().min(new Date().getFullYear()),
  cvv: z.string().length(3),
  description: z.string().optional(),
  metadata: z.record(z.string()).optional(),
  transactionId: z.string().optional()
});

// Payment Response Schema
export const PaymentResponseSchema = z.object({
  requestId: z.string().optional(), // Made optional
  transactionId: z.string(),
  status: z.enum(['APPROVED', 'DECLINED', 'PROCESSING', 'FAILED', 'AUTHORIZED', 'SETTLED']),
  message: z.string(),
  timestamp: z.string()
});

// Type definitions
export type PaymentRequest = z.infer<typeof PaymentRequestSchema>;
export type PaymentResponse = z.infer<typeof PaymentResponseSchema>;


dotenv.config();

const app = express();
const port = process.env.PORT || 3010;

const pool = new Pool({
  host: process.env.POSTGRES_HOST || 'postgres',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'paygrid',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres'
});

const kafka = new Kafka({
  clientId: 'ledger-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'ledger-service-group' });
const producer = kafka.producer();
const settlementProcessor = new SettlementProcessor(pool, producer);

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });

app.use(express.json());


app.use((req, _, next) => {
  logger.info({ method: req.method, url: req.url, ip: req.ip }, 'Incoming request');
  next();
});

app.get('/health', (_req, res) => {
  return res.status(200).json({ status: 'ok' });
});

async function ensureLedgerTable() {
  await pool.query(`CREATE TABLE IF NOT EXISTS ledger_entries (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(64),
    entry_type VARCHAR(16), -- debit/credit
    account VARCHAR(64),
    amount NUMERIC(18,2),
    currency VARCHAR(3),
    event_type VARCHAR(32),
    event_data JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    settled BOOLEAN DEFAULT FALSE
  )`);
}

async function ledgerEntryExists(transactionId: string, eventType: string, entryType: string, account: string) {
  const result = await pool.query(
    'SELECT 1 FROM ledger_entries WHERE transaction_id = $1 AND event_type = $2 AND entry_type = $3 AND account = $4',
    [transactionId, eventType, entryType, account]
  );
  return (result.rowCount ?? 0) > 0;
}

async function recordDoubleEntry({ transactionId, debitAccount, creditAccount, amount, currency, eventType, eventData }: {
  transactionId: string,
  debitAccount: string,
  creditAccount: string,
  amount: number,
  currency: string,
  eventType: string,
  eventData: any
}): Promise<number[]> {
  const client = await pool.connect();
  const createdEntryIds: number[] = [];
  try {
    await client.query('BEGIN');

    // Idempotency checks remain, but now they are part of a transaction
    const debitExists = await ledgerEntryExists(transactionId, eventType, 'debit', debitAccount);
    if (!debitExists) {
      const result = await client.query(
        'INSERT INTO ledger_entries (transaction_id, entry_type, account, amount, currency, event_type, event_data) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id',
        [transactionId, 'debit', debitAccount, amount, currency, eventType, eventData]
      );
      console.log(`[Ledger Debug] Created DEBIT ledger entry:`, {
        transactionId, eventType, entryType: 'debit', account: debitAccount, amount, currency
      });
      if (result.rows[0]) {
        createdEntryIds.push(result.rows[0].id);
      }
    }

    const creditExists = await ledgerEntryExists(transactionId, eventType, 'credit', creditAccount);
    if (!creditExists) {
      const result = await client.query(
        'INSERT INTO ledger_entries (transaction_id, entry_type, account, amount, currency, event_type, event_data) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id',
        [transactionId, 'credit', creditAccount, amount, currency, eventType, eventData]
      );
      console.log(`[Ledger Debug] Created CREDIT ledger entry:`, {
        transactionId, eventType, entryType: 'credit', account: creditAccount, amount, currency
      });
      if (result.rows[0]) {
        createdEntryIds.push(result.rows[0].id);
      }
    }

    await client.query('COMMIT');
    return createdEntryIds;
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error({ err: error, transactionId, eventType }, 'Failed to record double entry, transaction rolled back.');
    throw error; // Re-throw to be handled by the consumer
  } finally {
    client.release();
  }
}

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'payment-requests', fromBeginning: true });
  await consumer.subscribe({ topic: 'acquiring-bank-responses', fromBeginning: true });
  await consumer.subscribe({ topic: 'card-network-responses', fromBeginning: true });
  await consumer.subscribe({ topic: 'issuing-bank-responses', fromBeginning: true });
  await consumer.subscribe({ topic: 'refund-requests', fromBeginning: true });
  await consumer.subscribe({ topic: 'refund-completed', fromBeginning: true });
  await consumer.subscribe({ topic: 'settlement-events', fromBeginning: true });
  // Add subscription for the new topic that Payment Service will consume
  await consumer.subscribe({ topic: 'payment-settlement-updates', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const eventPayload = message.value.toString();
      try {
        const event = JSON.parse(eventPayload);
        // ... existing logic for each topic
        if (topic === 'payment-requests') {
          const parsed = PaymentRequestSchema.safeParse(event);
          if (!parsed.success) throw new Error('Invalid PaymentRequest event');
          await recordDoubleEntry({
            transactionId: event.transactionId,
            debitAccount: event.userId || event.cardNumber,
            creditAccount: event.merchantId,
            amount: event.amount,
            currency: event.currency,
            eventType: 'payment-initiated',
            eventData: event
          });
          logger.info({ transactionId: event.transactionId, eventType: 'payment-initiated' }, 'Processed payment-requests event');
        } else if (topic === 'issuing-bank-responses') { // NEW BLOCK
          // Ensure transactionId is present before parsing
          if (!event.transactionId) {
            logger.error({ event }, 'Received issuing-bank-responses with missing transactionId, sending to dead-letter:');
            await producer.send({
              topic: 'issuing-bank-responses-dead-letter',
              messages: [{ value: message.value?.toString() || '' }]
            });
            return; // Skip processing this message
          }

          const parsed = PaymentResponseSchema.safeParse(event);
          if (!parsed.success) {
            logger.error({ err: parsed.error, event }, 'Invalid IssuingBankResponse event schema, sending to dead-letter:');
            await producer.send({
              topic: 'issuing-bank-responses-dead-letter',
              messages: [{ value: message.value?.toString() || '' }]
            });
            return; // Skip processing this message
          }

          if (event.status === 'AUTHORIZED') {
            // Record an unsettled entry for authorized payments
            const createdEntryIds = await recordDoubleEntry({
              transactionId: event.transactionId,
              debitAccount: event.merchantId + '-pending', // Example account for pending funds
              creditAccount: event.merchantId + '-revenue', // Example account for revenue
              amount: event.amount, // Assuming amount is available in the response
              currency: event.currency, // Assuming currency is available
              eventType: 'payment-authorized',
              eventData: event
            });
            // Ensure the entry is marked as unsettled (settled = FALSE)
            if (createdEntryIds.length > 0) {
              await pool.query(
                `UPDATE ledger_entries SET settled = FALSE WHERE id = ANY($1::int[])`,
                [createdEntryIds]
              );
              console.log(`[Ledger Debug] Created payment-authorized ledger entry with settled=FALSE for transactionId:`, event.transactionId);
            } else {
              console.log(`[Ledger Debug] No payment-authorized ledger entry created for transactionId:`, event.transactionId);
            }
            logger.info({ transactionId: event.transactionId, eventType: 'payment-authorized' }, 'Processed issuing-bank-responses event (AUTHORIZED)');
          }
        } else if (topic === 'payment-responses') {
          const parsed = PaymentResponseSchema.safeParse(event);
          if (!parsed.success) throw new Error('Invalid PaymentResponse event');
          if (event.status === 'SETTLED') {
            await recordDoubleEntry({
              transactionId: event.transactionId,
              debitAccount: event.merchantId + '-reserve',
              creditAccount: event.merchantId,
              amount: event.amount,
              currency: event.currency,
              eventType: 'payment-settled',
              eventData: event
            });
            logger.info({ transactionId: event.transactionId, eventType: 'payment-settled' }, 'Processed payment-responses event');
          }
        } else if (topic === 'refund-requests') {
          if (!event.transactionId || !event.amount || !event.timestamp) throw new Error('Invalid RefundInitiatedEvent');
          await recordDoubleEntry({
            transactionId: event.transactionId,
            debitAccount: event.merchantId,
            creditAccount: event.userId + '-reserve',
            amount: event.amount,
            currency: event.currency,
            eventType: 'refund-initiated',
            eventData: event
          });
          logger.info({ transactionId: event.transactionId, eventType: 'refund-initiated' }, 'Processed refund-requests event');
        } else if (topic === 'refund-completed') {
          if (!event.transactionId || !event.amount || !event.status) throw new Error('Invalid RefundCompletedEvent');
          if (event.status === 'REFUNDED') {
            await recordDoubleEntry({
              transactionId: event.transactionId,
              debitAccount: event.userId + '-reserve',
              creditAccount: event.userId,
              amount: event.amount,
              currency: event.currency,
              eventType: 'refund-completed',
              eventData: event
            });
            logger.info({ transactionId: event.transactionId, eventType: 'refund-completed' }, 'Processed refund-completed event');
          }
        } else if (topic === 'settlement-events') {
          // Record settlement event in ledger
          await pool.query(
            'INSERT INTO ledger_entries (transaction_id, entry_type, account, amount, currency, event_type, event_data, settled) VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)',
            [
              event.settlementId,
              event.amount > 0 ? 'credit' : 'debit',
              event.account,
              Math.abs(event.amount),
              event.currency,
              'settlement',
              event,
            ]
          );
        }
      } catch (err) {
        const error = err as Error;
        logger.error({ err, topic, message: eventPayload }, '[Ledger] Error processing event. Sending to DLQ.');
        try {
          await producer.send({
            topic: `${topic}-dead-letter`,
            messages: [{
              key: message.key,
              value: JSON.stringify({
                original_topic: topic,
                error: error.message,
                stack: error.stack,
                payload: eventPayload,
                timestamp: new Date().toISOString()
              })
            }]
          });
        } catch (dlqError) {
          logger.error({ err: dlqError, topic }, 'FATAL: Failed to send message to DLQ.');
        }
      }
    }
  });
}

// Add new API endpoints
app.get('/api/ledger/entries', async (req, res) => {
  try {
    const {
      account,
      entryType,
      eventType,
      startDate,
      endDate,
      page = 1,
      limit = 50
    } = req.query;

    let query = 'SELECT * FROM ledger_entries WHERE 1=1';
    const params: any[] = [];
    let paramIndex = 1;

    if (account) {
      query += ` AND account = $${paramIndex}`;
      params.push(account);
      paramIndex++;
    }

    if (entryType) {
      query += ` AND entry_type = $${paramIndex}`;
      params.push(entryType);
      paramIndex++;
    }

    if (eventType) {
      query += ` AND event_type = $${paramIndex}`;
      params.push(eventType);
      paramIndex++;
    }

    if (startDate) {
      query += ` AND created_at >= $${paramIndex}`;
      params.push(startDate);
      paramIndex++;
    }

    if (endDate) {
      query += ` AND created_at <= $${paramIndex}`;
      params.push(endDate);
      paramIndex++;
    }

    // Add pagination
    const offset = (Number(page) - 1) * Number(limit);
    query += ` ORDER BY created_at DESC LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
    params.push(limit, offset);

    const result = await pool.query(query, params);
    return res.json({
      entries: result.rows,
      pagination: {
        page: Number(page),
        limit: Number(limit),
        total: result.rowCount
      }
    });
  } catch (err) {
    logger.error({ err }, '[Ledger] Error querying entries:');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/ledger/accounts/:account/balance', async (req, res) => {
  try {
    const { account } = req.params;
    const { currency } = req.query;

    let query = `
      SELECT 
        currency,
        SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE -amount END) as balance
      FROM ledger_entries 
      WHERE account = $1
      ${currency ? 'AND currency = $2' : ''}
      GROUP BY currency
    `;

    const params = currency ? [account, currency] : [account];
    const result = await pool.query(query, params);
    
    return res.json({
      account,
      balances: result.rows
    });
  } catch (err) {
    logger.error({ err }, '[Ledger] Error getting balance:');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// API key middleware
function requireApiKey(req: express.Request, res: express.Response, next: express.NextFunction) {
  const apiKey = req.headers['x-api-key'] || req.query.api_key;
  if (!apiKey || apiKey !== process.env.LEDGER_API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  return next();
}

// Add settlement endpoints with API key protection
app.post('/api/ledger/settlement', requireApiKey, async (req, res) => {
  try {
    const config: SettlementConfig = {
      settlementType: req.body.settlementType || 'manual',
      currency: req.body.currency,
      ...(req.body.startTime && { startTime: new Date(req.body.startTime) }),
      ...(req.body.endTime && { endTime: new Date(req.body.endTime) })
    };

    const result = await settlementProcessor.processSettlement(config);
    return res.json(result);
  } catch (err) {
    logger.error({ err }, '[Ledger] Error processing settlement:');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/ledger/settlement/status', requireApiKey, async (req, res) => {
  try {
    const { currency, startTime, endTime } = req.query;
    
    const query = `
      SELECT 
        event_data->>'settlementType' as settlement_type,
        COUNT(*) as entry_count,
        SUM(amount) as total_amount
      FROM ledger_entries
      WHERE event_type = 'settlement'
      ${currency ? 'AND currency = $1' : ''}
      ${startTime ? 'AND created_at >= $2' : ''}
      ${endTime ? 'AND created_at <= $3' : ''}
      GROUP BY event_data->>'settlementType'
    `;

    const params = [currency, startTime, endTime].filter(Boolean);
    const result = await pool.query(query, params);

    return res.json({
      settlements: result.rows
    });
  } catch (err) {
    logger.error({ err }, '[Ledger] Error getting settlement status:');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(port, async () => {
  await ensureLedgerTable();
  await producer.connect();
  await startKafka();
  logger.info(`Ledger Service listening on port ${port}`);
});
