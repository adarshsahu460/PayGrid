# PayGrid: A Microservices-Based Payment Processing System

PayGrid is a comprehensive, event-driven payment processing system built with a microservices architecture. It simulates the entire lifecycle of a credit card transaction, from the initial API request to the final settlement of funds. This project is designed to showcase a robust, scalable, and resilient system using modern technologies and best practices.

## Architecture

The system is composed of several independent microservices that communicate asynchronously using Apache Kafka. This decoupled approach ensures that the system is resilient to failures and can be scaled horizontally.

![Architecture Diagram](sd.png)

### Services

*   **API Gateway (`api-gateway`):** The single entry point for all client requests. It is responsible for request validation, authentication, and routing to the appropriate downstream services.
*   **Fraud Detection Service (`fraud-detection-service`):** Provides real-time fraud analysis based on transaction patterns and IP velocity.
*   **Tokenization Service (`tokenization-service`):** Secures sensitive cardholder data by replacing it with a non-sensitive token.
*   **Payment Service (`payment-service`):** The core orchestrator of the payment flow. It manages the state of each transaction and communicates with the banking network via Kafka.
*   **Acquiring Bank Service (`acquiring-bank`):** Simulates the merchant's bank, responsible for receiving funds from the issuing bank.
*   **Card Network Service (`card-network`):** Simulates a card network like Visa or Mastercard, responsible for routing transactions between the acquiring and issuing banks.
*   **Issuing Bank Service (`issuing-bank`):** Simulates the customer's bank, responsible for authorizing or declining the transaction.
*   **Ledger Service (`ledger-service`):** A double-entry bookkeeping system that provides an immutable, auditable record of all financial transactions. It also includes a settlement processor.
*   **Notification Service (`notification-service`):** Responsible for sending notifications to customers and merchants (e.g., via webhooks or email).

### Key Technologies

*   **Node.js & TypeScript:** For building the microservices.
*   **Docker & Docker Compose:** For containerizing and running the services in a local development environment.
*   **Apache Kafka:** As the event-driven messaging backbone for asynchronous communication between services.
*   **PostgreSQL:** As the primary data store for the Payment Service and Ledger Service.
*   **Redis:** For caching and idempotency checks in the Payment Service.
*   **Debezium:** For Change Data Capture (CDC) from the PostgreSQL database to Kafka.
*   **Pino:** For structured, production-ready logging.
*   **Opossum:** As a circuit breaker to improve the resilience of the system.

## Getting Started

### Prerequisites

*   Docker
*   Docker Compose

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/adarshsahu460/PayGrid.git
    cd PayGrid
    ```

2.  **Create `.env` files:**
    Each service uses a `.env` file for configuration. You can create these by copying the `.env.example` files:
    ```bash
    cp services/api-gateway/.env.example services/api-gateway/.env
    cp services/payment-service/.env.example services/payment-service/.env
    # ... and so on for each service
    ```
    You will need to fill in the values in the `.env` files. For local development, you can use the default values from the `docker-compose.yml` file.

3.  **Build and run the services:**
    ```bash
    docker-compose up --build
    ```

## API Usage

You can interact with the API using `curl` or a tool like Postman.

### 1. Get an Idempotency Key

To prevent duplicate transactions, each payment request requires a unique idempotency key.

```bash
curl -X GET http://localhost:3000/idempotency-key
```

**Response:**
```json
{
  "idempotencyKey": "some-unique-key"
}
```

### 2. Initiate a Payment

Use the idempotency key from the previous step in the header of your payment request.

```bash
curl -X POST http://localhost:3000/payments \
-H "Content-Type: application/json" \
-H "Idempotency-Key: some-unique-key" \
-d '{
  "merchantId": "merchant-123",
  "amount": 100.00,
  "currency": "USD",
  "cardNumber": "4242424242424242",
  "expiryMonth": 12,
  "expiryYear": 2025,
  "cvv": "123",
  "description": "Test payment"
}'
```

**Response:**
```json
{
  "transactionId": "txn_...",
  "status": "PROCESSING",
  "message": "Payment request received"
}
```

### 3. Check Payment Status

You can query the status of a payment using the `transactionId` returned in the previous step.

```bash
curl -X GET http://localhost:3001/payments/txn_.../status
```

**Response:**
```json
{
  "status": "AUTHORIZED",
  "message": "Transaction authorized by issuing bank"
}
