import express from 'express';
import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const port = process.env.PORT || 3004;


// Kafka setup
const kafka = new Kafka({
  clientId: 'issuing-bank',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'issuing-bank-group' });

// Middleware
app.use(express.json());

// Routes
app.get('/health', (_, res) => {
  res.json({ status: 'healthy' });
});

// Kafka message handling
async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'card-network-responses', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let cardNetworkResponse;
      try {
        cardNetworkResponse = JSON.parse(message.value.toString());
        console.log('Received card network response:', cardNetworkResponse);
        // Simulate issuing bank processing
        const isApproved = Math.random() > 0.2; // 80% approval rate
        const response: any = {
          requestId: cardNetworkResponse.requestId,
          transactionId: cardNetworkResponse.transactionId || cardNetworkResponse.requestId,
          status: isApproved ? 'AUTHORIZED' : 'DECLINED',
          message: isApproved ? 'Transaction authorized by issuing bank' : 'Transaction declined by issuing bank',
          timestamp: new Date().toISOString()
        };
        await producer.send({
          topic: 'issuing-bank-responses',
          messages: [
            {
              key: cardNetworkResponse.requestId,
              value: JSON.stringify(response)
            }
          ]
        });
      } catch (err) {
        // Dead-letter: send failed message to dead-letter topic
        console.error('[Issuing Bank] Error processing payment, sending to dead-letter:', err);
        await producer.send({
          topic: 'card-network-responses-dead-letter',
          messages: [
            { value: message.value?.toString() || '' }
          ]
        });
      }
    }
  });
}

// Refund consumer
const refundConsumer = kafka.consumer({ groupId: 'issuing-bank-refund-group' });
const refundProducer = producer;
async function startRefundConsumer() {
  await refundConsumer.connect();
  await refundConsumer.subscribe({ topic: 'refund-requests', fromBeginning: true });
  await refundConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      // Idempotency: check if already refunded
      if (event && event.transactionId) {
        // Simulate refund processing
        const success = Math.random() > 0.05; // 95% refund success
        const status = success ? 'REFUNDED' : 'REFUND_FAILED';
        // Emit RefundCompletedEvent
        await refundProducer.send({
          topic: 'refund-completed',
          messages: [{ value: JSON.stringify({
            transactionId: event.transactionId,
            status,
            reason: success ? undefined : 'Refund failed at issuing bank',
            timestamp: new Date().toISOString(),
          }) }],
        });
      }
    }
  });
}
startRefundConsumer().catch(console.error);

// Start the service
app.listen(port, async () => {
  console.log(`Issuing Bank Service listening on port ${port}`);
  await startKafka();
});