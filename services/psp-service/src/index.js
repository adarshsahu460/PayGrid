"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const dotenv_1 = __importDefault(require("dotenv"));
const kafkajs_1 = require("kafkajs");
const lib_1 = require("@paygrid/lib");
dotenv_1.default.config();
const app = (0, express_1.default)();
app.use(express_1.default.json());
app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok' });
});
const kafka = new kafkajs_1.Kafka({ brokers: [process.env.KAFKA_BROKER || 'localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'psp-service-group' });
async function startConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'payments', fromBeginning: true });
    await consumer.run({
        eachMessage: async ({ message }) => {
            if (!message.value)
                return;
            const payment = JSON.parse(message.value.toString());
            console.log('[PSP] Received PaymentInitiated event:', payment);
            // Format ISO-8583-like message
            const isoMsg = (0, lib_1.buildISO8583Message)(payment);
            console.log('[PSP] Outgoing ISO-8583 message:', isoMsg);
            // TODO: Forward to Card Network service
        },
    });
}
startConsumer().catch(console.error);
const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
    console.log(`PSP Service listening on port ${PORT}`);
});
