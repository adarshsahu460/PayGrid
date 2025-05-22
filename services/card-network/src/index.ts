import express from 'express';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import dotenv from 'dotenv';
import { PaymentRequest, PaymentResponse } from '@paygrid/lib';

dotenv.config();

const app = express();
const port = process.env.PORT || 3003;

// PostgreSQL setup
const pool = new Pool({
  host: process.env.POSTGRES_HOST || 'postgres',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'paygrid',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres'
});

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
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

// Kafka message handling
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  
  await consumer.subscribe({ topic: 'payment-requests', fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      
      const paymentRequest: PaymentRequest = JSON.parse(message.value.toString());
      console.log('Received payment request:', paymentRequest);
      
      // Simulate card network processing
      const isApproved = Math.random() > 0.1; // 90% approval rate
      
      const response: PaymentResponse = {
        requestId: paymentRequest.requestId,
        transactionId: paymentRequest.transactionId || paymentRequest.requestId,
        status: isApproved ? 'APPROVED' : 'DECLINED',
        message: isApproved ? 'Transaction approved by card network' : 'Transaction declined by card network',
        timestamp: new Date().toISOString()
      };
      
      // Send response to payment service
      await producer.send({
        topic: 'payment-responses',
        messages: [
          {
            key: paymentRequest.requestId,
            value: JSON.stringify(response)
          }
        ]
      });
    }
  });
}

// Start the service
app.listen(port, async () => {
  console.log(`Card Network Service listening on port ${port}`);
  await startKafka();
}); 