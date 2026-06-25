CREATE TABLE IF NOT EXISTS coordinator_query_registry (
    query_id TEXT PRIMARY KEY,
    query_type TEXT NOT NULL,
    status TEXT NOT NULL,
    descriptor_json JSONB NOT NULL,
    target_table TEXT,
    submitted_sql TEXT,
    trino_user TEXT,
    trino_query_id TEXT,
    trino_info_uri TEXT,
    trino_next_uri TEXT,
    trino_stats_json JSONB,
    progress DOUBLE PRECISION,
    error_message TEXT,
    result_flight_info_json JSONB,
    result_tickets_json JSONB,
    result_files_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS coordinator_query_registry_status_idx
    ON coordinator_query_registry (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS coordinator_query_registry_expiry_idx
    ON coordinator_query_registry (expires_at);

CREATE INDEX IF NOT EXISTS coordinator_query_registry_trino_query_idx
    ON coordinator_query_registry (trino_query_id);
