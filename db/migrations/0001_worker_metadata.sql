CREATE TABLE IF NOT EXISTS worker_put_streams (
    attempt_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    rows BIGINT NOT NULL DEFAULT 0,
    batches BIGINT NOT NULL DEFAULT 0,
    parts INTEGER NOT NULL DEFAULT 0,
    flight_stream_bytes BIGINT NOT NULL DEFAULT 0,
    parquet_object_bytes BIGINT,
    elapsed_ms BIGINT,
    error_message TEXT,
    arrow_schema_json JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS worker_put_files (
    attempt_id TEXT NOT NULL REFERENCES worker_put_streams(attempt_id) ON DELETE CASCADE,
    part_index INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    rows BIGINT NOT NULL,
    batches BIGINT NOT NULL,
    flight_stream_bytes BIGINT NOT NULL,
    parquet_object_bytes BIGINT NOT NULL,
    PRIMARY KEY (attempt_id, part_index)
);

CREATE TABLE IF NOT EXISTS worker_registry (
    worker_id TEXT PRIMARY KEY,
    flight_uri TEXT NOT NULL,
    state TEXT NOT NULL,
    draining BOOLEAN NOT NULL,
    put_recommended_streams INTEGER NOT NULL,
    put_selection_score BIGINT NOT NULL,
    put_utilization_per_mille INTEGER NOT NULL,
    put_admission_wait_ms_ewma BIGINT NOT NULL,
    put_throughput_bytes_per_sec_ewma BIGINT NOT NULL,
    read_recommended_streams INTEGER NOT NULL,
    read_selection_score BIGINT NOT NULL,
    read_utilization_per_mille INTEGER NOT NULL,
    read_admission_wait_ms_ewma BIGINT NOT NULL,
    read_throughput_bytes_per_sec_ewma BIGINT NOT NULL,
    registry_ttl_ms BIGINT NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS worker_registry_put_scheduler_idx
    ON worker_registry (state, draining, put_recommended_streams DESC, put_selection_score DESC, last_heartbeat_at DESC);

CREATE INDEX IF NOT EXISTS worker_registry_read_scheduler_idx
    ON worker_registry (state, draining, read_recommended_streams DESC, read_selection_score DESC, last_heartbeat_at DESC);

CREATE TABLE IF NOT EXISTS coordinator_upload_sessions (
    upload_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    table_name TEXT,
    status TEXT NOT NULL,
    expected_streams INTEGER NOT NULL,
    staging_prefix TEXT NOT NULL,
    target_file_size BIGINT NOT NULL,
    max_stream_bytes BIGINT,
    max_record_batch_bytes BIGINT,
    create_table_sql TEXT,
    commit_mode TEXT,
    commit_table_name TEXT,
    commit_snapshot_id BIGINT,
    commit_summary_json JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    committed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS coordinator_upload_sessions_status_idx
    ON coordinator_upload_sessions (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS coordinator_upload_streams (
    upload_id TEXT NOT NULL REFERENCES coordinator_upload_sessions(upload_id) ON DELETE CASCADE,
    stream_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL UNIQUE,
    worker_id TEXT NOT NULL,
    flight_uri TEXT NOT NULL,
    descriptor_path TEXT NOT NULL,
    PRIMARY KEY (upload_id, stream_id)
);

CREATE INDEX IF NOT EXISTS coordinator_upload_streams_attempt_idx
    ON coordinator_upload_streams (attempt_id);
