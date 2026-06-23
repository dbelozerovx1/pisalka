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
    arrow_schema_json JSONB,
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
    worker_zone TEXT,
    state TEXT NOT NULL,
    draining BOOLEAN NOT NULL,
    put_limit INTEGER NOT NULL,
    active_put_streams INTEGER NOT NULL,
    available_put_streams INTEGER NOT NULL,
    put_recommended_streams INTEGER NOT NULL DEFAULT 0,
    put_soft_available_slots INTEGER NOT NULL DEFAULT 0,
    put_memory_available_streams INTEGER NOT NULL DEFAULT 0,
    put_utilization_per_mille INTEGER NOT NULL DEFAULT 0,
    put_selection_score BIGINT NOT NULL DEFAULT 0,
    put_admission_wait_ms_ewma BIGINT NOT NULL DEFAULT 0,
    put_throughput_bytes_per_sec_ewma BIGINT NOT NULL DEFAULT 0,
    put_succeeded_total BIGINT NOT NULL DEFAULT 0,
    put_failed_total BIGINT NOT NULL DEFAULT 0,
    put_rejected_total BIGINT NOT NULL DEFAULT 0,
    read_limit INTEGER NOT NULL,
    active_read_streams INTEGER NOT NULL,
    available_read_streams INTEGER NOT NULL,
    read_recommended_streams INTEGER NOT NULL DEFAULT 0,
    read_soft_available_slots INTEGER NOT NULL DEFAULT 0,
    read_memory_available_streams INTEGER NOT NULL DEFAULT 0,
    read_utilization_per_mille INTEGER NOT NULL DEFAULT 0,
    read_selection_score BIGINT NOT NULL DEFAULT 0,
    read_admission_wait_ms_ewma BIGINT NOT NULL DEFAULT 0,
    read_throughput_bytes_per_sec_ewma BIGINT NOT NULL DEFAULT 0,
    read_succeeded_total BIGINT NOT NULL DEFAULT 0,
    read_failed_total BIGINT NOT NULL DEFAULT 0,
    read_rejected_total BIGINT NOT NULL DEFAULT 0,
    read_cancelled_total BIGINT NOT NULL DEFAULT 0,
    worker_memory_bytes BIGINT NOT NULL DEFAULT 0,
    reserved_memory_bytes BIGINT NOT NULL DEFAULT 0,
    put_memory_limit_bytes BIGINT NOT NULL DEFAULT 0,
    put_memory_active_bytes BIGINT NOT NULL DEFAULT 0,
    put_memory_available_bytes BIGINT NOT NULL DEFAULT 0,
    put_max_stream_memory_bytes BIGINT NOT NULL DEFAULT 0,
    put_max_record_batch_bytes BIGINT NOT NULL DEFAULT 0,
    read_memory_limit_bytes BIGINT NOT NULL DEFAULT 0,
    read_memory_active_bytes BIGINT NOT NULL DEFAULT 0,
    read_memory_available_bytes BIGINT NOT NULL DEFAULT 0,
    read_max_stream_memory_bytes BIGINT NOT NULL DEFAULT 0,
    read_max_record_batch_bytes BIGINT NOT NULL DEFAULT 0,
    read_max_batch_rows INTEGER NOT NULL DEFAULT 0,
    heartbeat_interval_ms BIGINT NOT NULL,
    registry_ttl_ms BIGINT NOT NULL,
    status_json JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS worker_registry_state_heartbeat_idx
    ON worker_registry (state, last_heartbeat_at DESC);

ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS worker_zone TEXT;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_recommended_streams INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_soft_available_slots INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_memory_available_streams INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_utilization_per_mille INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_selection_score BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_admission_wait_ms_ewma BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_throughput_bytes_per_sec_ewma BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_succeeded_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_failed_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_rejected_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_recommended_streams INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_soft_available_slots INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_memory_available_streams INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_utilization_per_mille INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_selection_score BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_admission_wait_ms_ewma BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_throughput_bytes_per_sec_ewma BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_succeeded_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_failed_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_rejected_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_cancelled_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS worker_memory_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS reserved_memory_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_memory_limit_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_memory_active_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_memory_available_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_max_stream_memory_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS put_max_record_batch_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_memory_limit_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_memory_active_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_memory_available_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_max_stream_memory_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_max_record_batch_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS read_max_batch_rows INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_put_streams ADD COLUMN IF NOT EXISTS arrow_schema_json JSONB;

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
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
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
    status TEXT NOT NULL DEFAULT 'PLANNED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (upload_id, stream_id)
);

CREATE INDEX IF NOT EXISTS coordinator_upload_streams_attempt_idx
    ON coordinator_upload_streams (attempt_id);

CREATE INDEX IF NOT EXISTS coordinator_upload_streams_upload_status_idx
    ON coordinator_upload_streams (upload_id, status, updated_at DESC);
