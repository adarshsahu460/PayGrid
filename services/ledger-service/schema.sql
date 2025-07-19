CREATE TABLE IF NOT EXISTS ledger_entries (
  id SERIAL PRIMARY KEY,
  transaction_id VARCHAR(64),
  entry_type VARCHAR(16),
  account VARCHAR(64),
  amount NUMERIC(18,2),
  currency VARCHAR(3),
  event_type VARCHAR(32),
  event_data JSONB,
  created_at TIMESTAMP DEFAULT NOW(),
  settled BOOLEAN DEFAULT FALSE
);
