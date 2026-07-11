ALTER TABLE worker_registry
    ADD COLUMN IF NOT EXISTS put_capacity_streams INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS put_available_streams INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS put_max_streams_per_upload INTEGER NOT NULL DEFAULT 1;

ALTER TABLE coordinator_upload_sessions
    ADD COLUMN IF NOT EXISTS upload_flavor TEXT NOT NULL DEFAULT 'small';

ALTER TABLE coordinator_upload_streams
    ADD COLUMN IF NOT EXISTS reservation_expires_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS coordinator_upload_streams_reservation_idx
    ON coordinator_upload_streams (worker_id, reservation_expires_at);
