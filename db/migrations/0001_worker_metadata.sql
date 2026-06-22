CREATE TABLE IF NOT EXISTS worker_put_streams (
    attempt_id TEXT PRIMARY KEY,
    upload_id TEXT,
    stream_id TEXT,
    worker_id TEXT NOT NULL,
    object_key TEXT NOT NULL,
    status TEXT NOT NULL,
    mode TEXT,
    staging_prefix TEXT,
    target_file_size BIGINT,
    client_input_file_bytes BIGINT,
    stream_budget_bytes BIGINT,
    global_put_stream_limit INTEGER NOT NULL,
    upload_put_stream_limit INTEGER,
    active_put_streams_at_admit INTEGER,
    upload_active_streams_at_admit INTEGER,
    compression TEXT NOT NULL,
    multipart_part_size BIGINT NOT NULL,
    multipart_max_concurrency INTEGER NOT NULL,
    rows BIGINT NOT NULL DEFAULT 0,
    batches BIGINT NOT NULL DEFAULT 0,
    parts INTEGER NOT NULL DEFAULT 0,
    flight_stream_bytes BIGINT NOT NULL DEFAULT 0,
    parquet_object_bytes BIGINT,
    elapsed_ms BIGINT,
    error_message TEXT,
    put_result_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS worker_put_streams_upload_status_idx
    ON worker_put_streams (upload_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS worker_put_streams_upload_stream_idx
    ON worker_put_streams (upload_id, stream_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS worker_put_streams_status_idx
    ON worker_put_streams (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS worker_put_files (
    attempt_id TEXT NOT NULL REFERENCES worker_put_streams(attempt_id) ON DELETE CASCADE,
    upload_id TEXT,
    stream_id TEXT,
    worker_id TEXT NOT NULL,
    logical_key TEXT NOT NULL,
    part_index INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    rows BIGINT NOT NULL,
    batches BIGINT NOT NULL,
    flight_stream_bytes BIGINT NOT NULL,
    parquet_object_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (attempt_id, part_index)
);

CREATE INDEX IF NOT EXISTS worker_put_files_upload_idx
    ON worker_put_files (upload_id, stream_id, part_index);

CREATE INDEX IF NOT EXISTS worker_put_files_path_idx
    ON worker_put_files (file_path);

CREATE TABLE IF NOT EXISTS worker_registry (
    worker_id TEXT PRIMARY KEY,
    flight_uri TEXT NOT NULL,
    state TEXT NOT NULL,
    draining BOOLEAN NOT NULL,
    put_limit INTEGER NOT NULL,
    active_put_streams INTEGER NOT NULL,
    available_put_streams INTEGER NOT NULL,
    read_limit INTEGER NOT NULL,
    active_read_streams INTEGER NOT NULL,
    available_read_streams INTEGER NOT NULL,
    heartbeat_interval_ms BIGINT NOT NULL,
    registry_ttl_ms BIGINT NOT NULL,
    status_json JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS worker_registry_state_heartbeat_idx
    ON worker_registry (state, last_heartbeat_at DESC);

