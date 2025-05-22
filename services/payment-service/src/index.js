"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const dotenv_1 = __importDefault(require("dotenv"));
const kafkajs_1 = require("kafkajs");
dotenv_1.default.config();
const app = (0, express_1.default)();
app.use(express_1.default.json());
const idempotencyStore = {};
const kafka = new kafkajs_1.Kafka({ brokers: [process.env.KAFKA_BROKER || 'localhost:9092'] });
const producer = kafka.producer();
(async () => { await producer.connect(); })();
app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
});
// Placeholder for payment processing route
app.post('/payments', async (req, res) => {
    const idempotencyKey = req.headers['idempotency-key'] || req.body.idempotencyKey;
    if (!idempotencyKey) {
        return res.status(400).json({ error: 'Idempotency-Key is required' });
    }
    if (idempotencyStore[idempotencyKey]) {
        return res.status(200).json(idempotencyStore[idempotencyKey]);
    }
    // Create pending payment record
    const payment = {
        paymentId: Date.now().toString(),
        status: 'pending',
        ...req.body,
        idempotencyKey,
        timestamp: new Date().toISOString(),
    };
    idempotencyStore[idempotencyKey] = payment;
    // Publish PaymentInitiated event
    await producer.send({
        topic: 'payments',
        messages: [{ value: JSON.stringify(payment) }],
    });
    res.status(202).json(payment);
});
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Payment Service listening on port ${PORT}`);
});
