ALTER TABLE coordinator_upload_sessions
    ADD COLUMN IF NOT EXISTS commit_mode TEXT,
    ADD COLUMN IF NOT EXISTS commit_table_name TEXT,
    ADD COLUMN IF NOT EXISTS commit_snapshot_id BIGINT,
    ADD COLUMN IF NOT EXISTS commit_summary_json JSONB,
    ADD COLUMN IF NOT EXISTS committed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS coordinator_upload_sessions_commit_idx
    ON coordinator_upload_sessions (status, committed_at DESC);
