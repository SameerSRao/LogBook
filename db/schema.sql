CREATE TABLE IF NOT EXISTS logs (
    id          BIGSERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL,
    service     TEXT NOT NULL,
    level       TEXT NOT NULL CHECK (level IN ('DEBUG', 'INFO', 'WARN', 'ERROR')),
    message     TEXT NOT NULL,
    context     JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_logs_service_timestamp
    ON logs (service, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_level_timestamp
    ON logs (level, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_context
    ON logs USING GIN (context);
