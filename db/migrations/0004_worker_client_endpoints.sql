CREATE TABLE IF NOT EXISTS worker_client_endpoints (
    worker_id TEXT PRIMARY KEY,
    flight_uri TEXT NOT NULL,
    source TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS worker_client_endpoints_fresh_idx
    ON worker_client_endpoints (expires_at DESC);
