# PayGrid: In-House Payment System Simulation

This project simulates a realistic, highly scalable in-house payment system, modeling every key component of a modern card and digital payment network. It is designed for educational, prototyping, and architectural exploration purposes.

## Architecture Overview

The system is composed of modular Node.js microservices, each representing a real-world payment infrastructure component:

- **API Gateway**: Entry point for all client requests, handles validation.
- **Payment Service**: Orchestrates payment flows, enforces idempotency, and emits events.
- **Card Network**: Simulates a Visa/Mastercard-like network, routes transactions, and enforces interchange rules.
- **Issuing Bank**: Simulates the customer's bank, validates cards, and checks balances.
- **Acquiring Bank**: Simulates the merchant's bank, handles settlements.
- **Ledger Service**: Maintains a distributed ledger with double-entry records for all debits/credits, listens to payment, refund, and settlement events, and exposes health and metrics endpoints on port 3010.

## Technologies Used

- **Node.js/TypeScript** for all services
- **Express.js** for REST APIs
- **Kafka** (or compatible event bus) for event-driven architecture
- **Postgres/MongoDB** for service databases (sharded/replicated)
- **Redis** for caching, idempotency, and OTP/session storage
- **Docker** for containerization

## Payment Flow

1. Client initiates payment via API Gateway.
2. Payment Service checks idempotency and emits a PaymentInitiated event.
3. Card Network routes to Issuing Bank for authorization.
5. Issuing Bank approves/declines and responds back through the network.
6. Payment Service updates status and emits events to Ledger and Wallet services.
7. Settlement and reconciliation are handled asynchronously.

## Goals

- Faithfully model real-world payment system flows
- Demonstrate event-driven, scalable, and fault-tolerant architecture
- Enable extensibility for new payment methods and services

---

See individual service directories for implementation details and API documentation. 
![System Design](./sd.png)

# PayGrid

## Overview

PayGrid is a modular payment processing platform with microservices for payment, ledger, acquiring/issuing banks, card network, and API gateway.

## Services

- api-gateway
- payment-service
- ledger-service
- acquiring-bank
- issuing-bank
- card-network

## Setup

1. Copy `.env.example` to `.env` in each service and fill in values.
2. Run `docker-compose up --build`.

## Endpoints

- `/health` - Health check for all services
- `/payments` - Payment initiation (API Gateway)
- ... (document other endpoints)

## Environment Variables

See `.env.example` in each service.

## Development

- Use `npm run dev` in each service for hot-reload.
- Use ESLint and Prettier for code quality.

## Graceful Shutdown

All services handle SIGTERM/SIGINT for clean shutdown.

## License

MIT