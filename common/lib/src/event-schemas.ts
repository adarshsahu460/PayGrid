// Placeholder for event schemas
export interface PaymentInitiatedEvent {
  paymentId: string;
  amount: number;
  currency: string;
  userId: string;
  merchantId: string;
  method: string;
  idempotencyKey: string;
  timestamp: string;
}

export interface PaymentAuthorizedEvent {
  paymentId: string;
  status: 'authorized' | 'failed';
  reason?: string;
  timestamp: string;
}

export interface RefundInitiatedEvent {
  transactionId: string;
  amount: number;
  reason: string;
  timestamp: string;
}

export interface RefundCompletedEvent {
  transactionId: string;
  status: 'REFUNDED' | 'REFUND_FAILED';
  reason?: string;
  timestamp: string;
}

// Add more event schemas as needed 