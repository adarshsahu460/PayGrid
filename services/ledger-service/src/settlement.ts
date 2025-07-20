import { Pool } from 'pg';
import { Producer } from 'kafkajs';

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
  ) {}

  async processSettlement(config: SettlementConfig) {
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
      console.log(`[Settlement Debug] Unsettled ledger entries found for settlement:`, result.rows);

      // Mark all included entries as settled
      const updateResult = await this.pool.query(
        `UPDATE ledger_entries SET settled = TRUE, event_data = jsonb_set(event_data, '{settlementId}', '"${settlementId}"')
         WHERE currency = $1 AND created_at >= $2 AND created_at <= $3 AND settled = FALSE`,
        [config.currency, config.startTime || new Date(Date.now() - 24 * 60 * 60 * 1000), config.endTime || new Date()]
      );
      console.log(`[Settlement Debug] Marked entries as settled:`, updateResult.rowCount);

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

        // Find all payment-authorized entries that were just settled in this batch
        const settledAuthorizedTxs = await this.pool.query(
          `SELECT transaction_id FROM ledger_entries
           WHERE event_type = 'payment-authorized'
             AND settled = TRUE
             AND currency = $1
             AND created_at >= $2
             AND created_at <= $3`,
          [config.currency, config.startTime || new Date(Date.now() - 24 * 60 * 60 * 1000), config.endTime || new Date()]
        );

        for (const txRow of settledAuthorizedTxs.rows) {
          await this.producer.send({
            topic: 'payment-settlement-updates',
            messages: [{
              key: txRow.transaction_id,
              value: JSON.stringify({
                transactionId: txRow.transaction_id,
                status: 'SETTLED',
                settlementId: settlementId,
                timestamp: new Date().toISOString()
              })
            }]
          });
        }
      }

      return {
        status: 'completed',
        accountsProcessed: result.rowCount,
        currency: config.currency,
        settlementType: config.settlementType,
        settlementId
      };
    } catch (err) {
      console.error('[Settlement] Error processing settlement:', err);
      throw err;
    }
  }
}
