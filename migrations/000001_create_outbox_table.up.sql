CREATE TYPE outbox_status AS ENUM ('pending', 'processing', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status outbox_status NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    last_error TEXT,
    locked_by VARCHAR(255),
    locked_until TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fetching pending and retryable failed work
CREATE INDEX IF NOT EXISTS idx_outbox_processing ON outbox_events (status, next_retry_at, created_at) 
WHERE status IN ('pending', 'failed');

-- Index for background worker lease recovery
CREATE INDEX IF NOT EXISTS idx_outbox_stuck_processing ON outbox_events (status, locked_until) 
WHERE status = 'processing';