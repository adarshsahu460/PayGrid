import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const kafka = new Kafka({
  clientId: 'history-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
});

const consumer = kafka.consumer({ groupId: 'history-service-group' });

const pool = new Pool({
  user: process.env.POSTGRES_USER || 'paygrid',
  password: process.env.POSTGRES_PASSWORD || 'paygrid',
  host: process.env.POSTGRES_HOST || 'postgres',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'paygrid',
});

const main = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'postgres.public.payment_transactions', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) {
        return;
      }

      const event = JSON.parse(message.value.toString());
      const payload = event.payload;

      if (payload.op === 'u') { // It's an update
        const before = payload.before;
        const after = payload.after;

        if (before.status !== after.status) {
          await pool.query(
            'INSERT INTO payment_status_history (transaction_id, old_status, new_status, reason) VALUES ($1, $2, $3, $4)',
            [after.transaction_id, before.status, after.status, 'Status updated']
          );
        }
      }
    },
  });
};

main().catch(console.error);
