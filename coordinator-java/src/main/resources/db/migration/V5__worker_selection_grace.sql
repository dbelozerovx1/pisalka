ALTER TABLE worker_registry
    ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ;

UPDATE worker_registry
SET first_seen_at = now() - interval '1 day'
WHERE first_seen_at IS NULL;

ALTER TABLE worker_registry
    ALTER COLUMN first_seen_at SET DEFAULT now(),
    ALTER COLUMN first_seen_at SET NOT NULL;

ALTER TABLE worker_client_endpoints
    ADD COLUMN IF NOT EXISTS first_observed_at TIMESTAMPTZ;

UPDATE worker_client_endpoints
SET first_observed_at = now() - interval '1 day'
WHERE first_observed_at IS NULL;

ALTER TABLE worker_client_endpoints
    ALTER COLUMN first_observed_at SET DEFAULT now(),
    ALTER COLUMN first_observed_at SET NOT NULL;
