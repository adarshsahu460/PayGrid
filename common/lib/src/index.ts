import { z } from 'zod';

// Payment Request Schema
export const PaymentRequestSchema = z.object({
  requestId: z.string(),
  merchantId: z.string(),
  amount: z.number().positive(),
  currency: z.string().length(3),
  cardNumber: z.string().min(13).max(19),
  expiryMonth: z.number().min(1).max(12),
  expiryYear: z.number().min(new Date().getFullYear()),
  cvv: z.string().length(3),
  description: z.string().optional(),
  metadata: z.record(z.string()).optional(),
  transactionId: z.string().optional()
});

// Payment Response Schema
export const PaymentResponseSchema = z.object({
  requestId: z.string(),
  transactionId: z.string(),
  status: z.enum(['APPROVED', 'DECLINED', 'PROCESSING', 'FAILED', 'AUTHORIZED', 'SETTLED']),
  message: z.string(),
  timestamp: z.string()
});

// Type definitions
export type PaymentRequest = z.infer<typeof PaymentRequestSchema>;
export type PaymentResponse = z.infer<typeof PaymentResponseSchema>;

// Utility functions
export function generateTransactionId(): string {
  return `txn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

export function formatAmount(amount: number, currency: string): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: currency
  }).format(amount);
}

export * from './event-schemas'; 