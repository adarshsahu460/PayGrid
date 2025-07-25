CREATE TABLE IF NOT EXISTS payment_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) DEFAULT 'PENDING',
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_status_history (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    old_status VARCHAR(32),
    new_status VARCHAR(32),
    changed_at TIMESTAMP DEFAULT NOW(),
    reason TEXT
);