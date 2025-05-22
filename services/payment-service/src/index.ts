import express from 'express';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { createClient } from 'redis';
import dotenv from 'dotenv';
import { PaymentRequest, PaymentResponse, generateTransactionId } from '@paygrid/lib';

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

// Database setup
const pool = new Pool({
  user: process.env.POSTGRES_USER || 'paygrid',
  password: process.env.POSTGRES_PASSWORD || 'paygrid',
  host: process.env.POSTGRES_HOST || 'postgres',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'paygrid'
});

// Redis setup
const redis = createClient({
  url: `redis://${process.env.REDIS_HOST || 'redis'}:${process.env.REDIS_PORT || '6379'}`
});

// Kafka setup
const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'payment-service-group' });

// Middleware
app.use(express.json());

// Routes
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

app.post('/payments', async (req, res) => {
  try {
    const paymentRequest: PaymentRequest = req.body;
    const transactionId = generateTransactionId();

    // Store payment request in Redis for idempotency
    await redis.set(`payment:${transactionId}`, JSON.stringify(paymentRequest), {
      EX: 3600 // 1 hour expiry
    });

    // Send to Kafka for processing
    await producer.send({
      topic: 'payment-requests',
      messages: [
        {
          key: transactionId,
          value: JSON.stringify({
            ...paymentRequest,
            transactionId
          })
        }
      ]
    });

    res.json({
      transactionId,
      status: 'PROCESSING',
      message: 'Payment request received'
    });
  } catch (error) {
    console.error('Error processing payment:', error);
    res.status(500).json({
      status: 'FAILED',
      message: 'Error processing payment request'
    });
  }
});

// Kafka message handling
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await redis.connect();
  
  await consumer.subscribe({ topic: 'payment-responses', fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      
      const response: PaymentResponse = JSON.parse(message.value.toString());
      console.log('Received payment response:', response);
      
      // Store response in database
      await pool.query(
        'INSERT INTO payment_transactions (transaction_id, status, message, created_at) VALUES ($1, $2, $3, NOW())',
        [response.transactionId, response.status, response.message]
      );
    }
  });
}

// Start the service
app.listen(port, async () => {
  console.log(`Payment Service listening on port ${port}`);
  await startKafka();
}); 