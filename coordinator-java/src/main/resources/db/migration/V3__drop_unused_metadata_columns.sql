DROP INDEX IF EXISTS worker_put_streams_upload_status_idx;
DROP INDEX IF EXISTS worker_put_streams_upload_stream_idx;
DROP INDEX IF EXISTS worker_put_streams_status_idx;
DROP INDEX IF EXISTS worker_put_files_upload_idx;
DROP INDEX IF EXISTS worker_put_files_path_idx;
DROP INDEX IF EXISTS worker_registry_state_heartbeat_idx;
DROP INDEX IF EXISTS coordinator_upload_streams_upload_status_idx;
DROP INDEX IF EXISTS coordinator_upload_sessions_commit_idx;

ALTER TABLE worker_put_streams
    DROP COLUMN IF EXISTS upload_id,
    DROP COLUMN IF EXISTS stream_id,
    DROP COLUMN IF EXISTS worker_id,
    DROP COLUMN IF EXISTS object_key,
    DROP COLUMN IF EXISTS mode,
    DROP COLUMN IF EXISTS staging_prefix,
    DROP COLUMN IF EXISTS target_file_size,
    DROP COLUMN IF EXISTS client_input_file_bytes,
    DROP COLUMN IF EXISTS stream_budget_bytes,
    DROP COLUMN IF EXISTS global_put_stream_limit,
    DROP COLUMN IF EXISTS upload_put_stream_limit,
    DROP COLUMN IF EXISTS active_put_streams_at_admit,
    DROP COLUMN IF EXISTS upload_active_streams_at_admit,
    DROP COLUMN IF EXISTS compression,
    DROP COLUMN IF EXISTS multipart_part_size,
    DROP COLUMN IF EXISTS multipart_max_concurrency,
    DROP COLUMN IF EXISTS manifest_key,
    DROP COLUMN IF EXISTS manifest_object_bytes,
    DROP COLUMN IF EXISTS put_result_json,
    DROP COLUMN IF EXISTS created_at;

ALTER TABLE worker_put_files
    DROP COLUMN IF EXISTS upload_id,
    DROP COLUMN IF EXISTS stream_id,
    DROP COLUMN IF EXISTS worker_id,
    DROP COLUMN IF EXISTS logical_key,
    DROP COLUMN IF EXISTS created_at;

ALTER TABLE worker_registry
    DROP COLUMN IF EXISTS worker_zone,
    DROP COLUMN IF EXISTS put_limit,
    DROP COLUMN IF EXISTS active_put_streams,
    DROP COLUMN IF EXISTS available_put_streams,
    DROP COLUMN IF EXISTS put_soft_available_slots,
    DROP COLUMN IF EXISTS put_memory_available_streams,
    DROP COLUMN IF EXISTS put_succeeded_total,
    DROP COLUMN IF EXISTS put_failed_total,
    DROP COLUMN IF EXISTS put_rejected_total,
    DROP COLUMN IF EXISTS read_limit,
    DROP COLUMN IF EXISTS active_read_streams,
    DROP COLUMN IF EXISTS available_read_streams,
    DROP COLUMN IF EXISTS read_soft_available_slots,
    DROP COLUMN IF EXISTS read_memory_available_streams,
    DROP COLUMN IF EXISTS read_succeeded_total,
    DROP COLUMN IF EXISTS read_failed_total,
    DROP COLUMN IF EXISTS read_rejected_total,
    DROP COLUMN IF EXISTS read_cancelled_total,
    DROP COLUMN IF EXISTS worker_memory_bytes,
    DROP COLUMN IF EXISTS reserved_memory_bytes,
    DROP COLUMN IF EXISTS put_memory_limit_bytes,
    DROP COLUMN IF EXISTS put_memory_active_bytes,
    DROP COLUMN IF EXISTS put_memory_available_bytes,
    DROP COLUMN IF EXISTS put_max_stream_memory_bytes,
    DROP COLUMN IF EXISTS put_max_record_batch_bytes,
    DROP COLUMN IF EXISTS read_memory_limit_bytes,
    DROP COLUMN IF EXISTS read_memory_active_bytes,
    DROP COLUMN IF EXISTS read_memory_available_bytes,
    DROP COLUMN IF EXISTS read_max_stream_memory_bytes,
    DROP COLUMN IF EXISTS read_max_record_batch_bytes,
    DROP COLUMN IF EXISTS read_max_batch_rows,
    DROP COLUMN IF EXISTS heartbeat_interval_ms,
    DROP COLUMN IF EXISTS status_json,
    DROP COLUMN IF EXISTS first_seen_at;

ALTER TABLE coordinator_upload_streams
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS created_at,
    DROP COLUMN IF EXISTS updated_at;

ALTER TABLE coordinator_query_registry
    DROP COLUMN IF EXISTS descriptor_json;
