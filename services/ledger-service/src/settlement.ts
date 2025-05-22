import { Pool } from 'pg';
import { Kafka, Producer } from 'kafkajs';
import promClient from 'prom-client';

const settlementDuration = new promClient.Histogram({
  name: 'settlement_duration_seconds',
  help: 'Duration of settlement processing in seconds',
  labelNames: ['settlement_type']
});

const settlementAmount = new promClient.Counter({
  name: 'settlement_amount_total',
  help: 'Total amount settled',
  labelNames: ['settlement_type', 'currency']
});

export interface SettlementConfig {
  settlementType: 'daily' | 'hourly' | 'manual';
  currency: string;
  startTime?: Date;
  endTime?: Date;
}

export class SettlementProcessor {
  constructor(
    private pool: Pool,
    private producer: Producer,
    private kafka: Kafka
  ) {}

  async processSettlement(config: SettlementConfig) {
    const timer = settlementDuration.startTimer({ settlement_type: config.settlementType });
    const settlementId = `SETTLEMENT-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
    try {
      // Get all unsettled entries for the time period
      const query = `
        SELECT 
          account,
          currency,
          SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE -amount END) as net_amount
        FROM ledger_entries
        WHERE currency = $1
        AND created_at >= $2
        AND created_at <= $3
        AND settled = FALSE
        GROUP BY account, currency
        HAVING SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE -amount END) != 0
      `;

      const result = await this.pool.query(query, [
        config.currency,
        config.startTime || new Date(Date.now() - 24 * 60 * 60 * 1000), // Default to last 24 hours
        config.endTime || new Date()
      ]);

      // Mark all included entries as settled
      await this.pool.query(
        `UPDATE ledger_entries SET settled = TRUE, event_data = jsonb_set(event_data, '{settlementId}', '"${settlementId}"')
         WHERE currency = $1 AND created_at >= $2 AND created_at <= $3 AND settled = FALSE`,
        [config.currency, config.startTime || new Date(Date.now() - 24 * 60 * 60 * 1000), config.endTime || new Date()]
      );

      // Process each account's settlement
      for (const row of result.rows) {
        if (row.net_amount > 0) {
          // Credit settlement
          await this.pool.query(
            'INSERT INTO ledger_entries (transaction_id, entry_type, account, amount, currency, event_type, event_data, settled) VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)',
            [
              settlementId,
              'credit',
              row.account,
              row.net_amount,
              row.currency,
              'settlement',
              { settlementType: config.settlementType, settlementId }
            ]
          );
        } else {
          // Debit settlement
          await this.pool.query(
            'INSERT INTO ledger_entries (transaction_id, entry_type, account, amount, currency, event_type, event_data, settled) VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)',
            [
              settlementId,
              'debit',
              row.account,
              Math.abs(row.net_amount),
              row.currency,
              'settlement',
              { settlementType: config.settlementType, settlementId }
            ]
          );
        }

        // Emit settlement event
        await this.producer.send({
          topic: 'settlement-events',
          messages: [{
            key: row.account,
            value: JSON.stringify({
              account: row.account,
              currency: row.currency,
              amount: row.net_amount,
              settlementType: config.settlementType,
              settlementId,
              timestamp: new Date().toISOString()
            })
          }]
        });

        settlementAmount.inc({
          settlement_type: config.settlementType,
          currency: row.currency
        }, Math.abs(row.net_amount));
      }

      timer();
      return {
        status: 'completed',
        accountsProcessed: result.rowCount,
        currency: config.currency,
        settlementType: config.settlementType,
        settlementId
      };
    } catch (err) {
      timer();
      console.error('[Settlement] Error processing settlement:', err);
      throw err;
    }
  }
} 