ALTER TABLE coordinator_upload_sessions
    ADD COLUMN IF NOT EXISTS upload_bucket TEXT;

CREATE INDEX IF NOT EXISTS coordinator_upload_sessions_expiry_idx
    ON coordinator_upload_sessions (expires_at, status);
