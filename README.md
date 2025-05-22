# PayGrid: In-House Payment System Simulation

This project simulates a realistic, highly scalable in-house payment system, modeling every key component of a modern card and digital payment network. It is designed for educational, prototyping, and architectural exploration purposes.

## Architecture Overview

The system is composed of modular Node.js microservices, each representing a real-world payment infrastructure component:

- **API Gateway**: Entry point for all client requests, handles validation and PCI DSS tokenization.
- **Payment Service**: Orchestrates payment flows, enforces idempotency, and emits events.
- **PSP Service**: Mediates between merchants and banks, formats ISO-8583 messages, and handles 3DS/UPI/Netbanking flows.
- **Card Network**: Simulates a Visa/Mastercard-like network, routes transactions, and enforces interchange rules.
- **Issuing Bank**: Simulates the customer's bank, validates cards, and checks balances.
- **Acquiring Bank**: Simulates the merchant's bank, handles settlements.
- **Ledger Service**: Maintains a distributed ledger with double-entry records for all debits/credits, listens to payment, refund, and settlement events, and exposes health and metrics endpoints on port 3010.
- **Wallet Service**: Manages user and merchant e-wallet balances.
- **3D Secure Service**: Simulates OTP-based authentication for card-not-present transactions.
- **PCI DSS Module**: Ensures secure tokenization and transmission of cardholder data.
- **NPCI/UPI Switch**: Routes UPI transactions between banks in real time.
- **Netbanking Gateway**: Simulates direct bank transfers.
- **Settlement Batch Processor**: Handles end-of-day reconciliation and net settlement between banks.

## Technologies Used

- **Node.js/TypeScript** for all services
- **Express.js** for REST APIs
- **Kafka** (or compatible event bus) for event-driven architecture
- **Postgres/MongoDB** for service databases (sharded/replicated)
- **Redis** for caching, idempotency, and OTP/session storage
- **Docker** for containerization
- **(Optional) Kubernetes** for orchestration and scaling

## Payment Flow

1. Client initiates payment via API Gateway.
2. Payment Service checks idempotency and emits a PaymentInitiated event.
3. PSP Service processes the event, formats messages, and routes to Card Network/UPI/Netbanking.
4. Card Network routes to Issuing Bank for authorization.
5. Issuing Bank approves/declines and responds back through the network.
6. Payment Service updates status and emits events to Ledger and Wallet services.
7. Settlement and reconciliation are handled asynchronously.

## Goals

- Faithfully model real-world payment system flows
- Demonstrate event-driven, scalable, and fault-tolerant architecture
- Simulate security and compliance (PCI DSS, 3DS)
- Enable extensibility for new payment methods and services

---

See individual service directories for implementation details and API documentation. 