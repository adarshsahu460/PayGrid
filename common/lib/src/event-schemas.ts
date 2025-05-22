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
// Add more event schemas as needed 