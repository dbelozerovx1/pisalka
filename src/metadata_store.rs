use anyhow::{Context, Result};
use serde_json::Value;
use tokio_postgres::{Client, NoTls};
use tracing::error;

use crate::config::MetadataConfig;

pub struct MetadataStore {
    client: Client,
}

#[derive(Debug)]
pub struct PutStreamStartRecord {
    pub attempt_id: String,
    pub upload_id: Option<String>,
    pub stream_id: Option<String>,
    pub worker_id: String,
    pub key: String,
    pub mode: Option<String>,
    pub staging_prefix: Option<String>,
    pub target_file_size: Option<i64>,
    pub client_input_file_bytes: Option<i64>,
    pub stream_budget_bytes: Option<i64>,
    pub global_put_stream_limit: i32,
    pub upload_put_stream_limit: Option<i32>,
    pub active_put_streams_at_admit: Option<i32>,
    pub upload_active_streams_at_admit: Option<i32>,
    pub compression: String,
    pub multipart_part_size: i64,
    pub multipart_max_concurrency: i32,
}

#[derive(Debug)]
pub struct PutStreamCompleteRecord {
    pub attempt_id: String,
    pub mode: String,
    pub rows: i64,
    pub batches: i64,
    pub parts: i32,
    pub flight_stream_bytes: i64,
    pub parquet_object_bytes: Option<i64>,
    pub elapsed_ms: i64,
    pub put_result_json: Value,
    pub files: Vec<PutFileRecord>,
}

#[derive(Debug)]
pub struct PutFileRecord {
    pub attempt_id: String,
    pub upload_id: Option<String>,
    pub stream_id: Option<String>,
    pub worker_id: String,
    pub logical_key: String,
    pub part_index: i32,
    pub file_path: String,
    pub rows: i64,
    pub batches: i64,
    pub flight_stream_bytes: i64,
    pub parquet_object_bytes: i64,
}

impl MetadataStore {
    pub async fn connect(config: &MetadataConfig) -> Result<Option<Self>> {
        let Some(database_url) = config.database_url.as_deref() else {
            return Ok(None);
        };

        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .context("failed to connect metadata database")?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                error!(error = %error, "metadata database connection failed");
            }
        });

        let store = Self { client };
        if config.auto_migrate {
            store.migrate().await?;
        }

        Ok(Some(store))
    }

    pub async fn migrate(&self) -> Result<()> {
        self.client
            .batch_execute(
                r#"
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
"#,
            )
            .await
            .context("failed to migrate metadata database")?;

        Ok(())
    }

    pub async fn record_admitted(&self, record: &PutStreamStartRecord) -> Result<()> {
        self.client
            .execute(
                r#"
INSERT INTO worker_put_streams (
    attempt_id,
    upload_id,
    stream_id,
    worker_id,
    object_key,
    status,
    mode,
    staging_prefix,
    target_file_size,
    client_input_file_bytes,
    stream_budget_bytes,
    global_put_stream_limit,
    upload_put_stream_limit,
    active_put_streams_at_admit,
    upload_active_streams_at_admit,
    compression,
    multipart_part_size,
    multipart_max_concurrency,
    error_message,
    put_result_json,
    updated_at,
    completed_at
) VALUES (
    $1, $2, $3, $4, $5, 'ADMITTED', $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
    NULL, NULL, now(), NULL
)
ON CONFLICT (attempt_id) DO UPDATE SET
    upload_id = EXCLUDED.upload_id,
    stream_id = EXCLUDED.stream_id,
    worker_id = EXCLUDED.worker_id,
    object_key = EXCLUDED.object_key,
    status = 'ADMITTED',
    mode = EXCLUDED.mode,
    staging_prefix = EXCLUDED.staging_prefix,
    target_file_size = EXCLUDED.target_file_size,
    client_input_file_bytes = EXCLUDED.client_input_file_bytes,
    stream_budget_bytes = EXCLUDED.stream_budget_bytes,
    global_put_stream_limit = EXCLUDED.global_put_stream_limit,
    upload_put_stream_limit = EXCLUDED.upload_put_stream_limit,
    active_put_streams_at_admit = EXCLUDED.active_put_streams_at_admit,
    upload_active_streams_at_admit = EXCLUDED.upload_active_streams_at_admit,
    compression = EXCLUDED.compression,
    multipart_part_size = EXCLUDED.multipart_part_size,
    multipart_max_concurrency = EXCLUDED.multipart_max_concurrency,
    error_message = NULL,
    put_result_json = NULL,
    updated_at = now(),
    completed_at = NULL
"#,
                &[
                    &record.attempt_id,
                    &record.upload_id,
                    &record.stream_id,
                    &record.worker_id,
                    &record.key,
                    &record.mode,
                    &record.staging_prefix,
                    &record.target_file_size,
                    &record.client_input_file_bytes,
                    &record.stream_budget_bytes,
                    &record.global_put_stream_limit,
                    &record.upload_put_stream_limit,
                    &record.active_put_streams_at_admit,
                    &record.upload_active_streams_at_admit,
                    &record.compression,
                    &record.multipart_part_size,
                    &record.multipart_max_concurrency,
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to record admitted DoPut attempt {}",
                    record.attempt_id
                )
            })?;

        Ok(())
    }

    pub async fn record_rejected(
        &self,
        record: &PutStreamStartRecord,
        error_message: &str,
    ) -> Result<()> {
        self.client
            .execute(
                r#"
INSERT INTO worker_put_streams (
    attempt_id,
    upload_id,
    stream_id,
    worker_id,
    object_key,
    status,
    mode,
    staging_prefix,
    target_file_size,
    client_input_file_bytes,
    stream_budget_bytes,
    global_put_stream_limit,
    upload_put_stream_limit,
    active_put_streams_at_admit,
    upload_active_streams_at_admit,
    compression,
    multipart_part_size,
    multipart_max_concurrency,
    error_message,
    updated_at,
    completed_at
) VALUES (
    $1, $2, $3, $4, $5, 'REJECTED', $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
    $18, now(), now()
)
ON CONFLICT (attempt_id) DO UPDATE SET
    status = 'REJECTED',
    error_message = EXCLUDED.error_message,
    updated_at = now(),
    completed_at = now()
"#,
                &[
                    &record.attempt_id,
                    &record.upload_id,
                    &record.stream_id,
                    &record.worker_id,
                    &record.key,
                    &record.mode,
                    &record.staging_prefix,
                    &record.target_file_size,
                    &record.client_input_file_bytes,
                    &record.stream_budget_bytes,
                    &record.global_put_stream_limit,
                    &record.upload_put_stream_limit,
                    &record.active_put_streams_at_admit,
                    &record.upload_active_streams_at_admit,
                    &record.compression,
                    &record.multipart_part_size,
                    &record.multipart_max_concurrency,
                    &error_message,
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to record rejected DoPut attempt {}",
                    record.attempt_id
                )
            })?;

        Ok(())
    }

    pub async fn record_writing(&self, attempt_id: &str) -> Result<()> {
        self.client
            .execute(
                r#"
UPDATE worker_put_streams
SET status = 'WRITING',
    updated_at = now()
WHERE attempt_id = $1
"#,
                &[&attempt_id],
            )
            .await
            .with_context(|| format!("failed to record writing DoPut attempt {attempt_id}"))?;

        Ok(())
    }

    pub async fn record_completed(&self, record: &PutStreamCompleteRecord) -> Result<()> {
        self.client
            .execute(
                "DELETE FROM worker_put_files WHERE attempt_id = $1",
                &[&record.attempt_id],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to clear completed DoPut file rows for attempt {}",
                    record.attempt_id
                )
            })?;

        for file in &record.files {
            self.client
                .execute(
                    r#"
INSERT INTO worker_put_files (
    attempt_id,
    upload_id,
    stream_id,
    worker_id,
    logical_key,
    part_index,
    file_path,
    rows,
    batches,
    flight_stream_bytes,
    parquet_object_bytes
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (attempt_id, part_index) DO UPDATE SET
    upload_id = EXCLUDED.upload_id,
    stream_id = EXCLUDED.stream_id,
    worker_id = EXCLUDED.worker_id,
    logical_key = EXCLUDED.logical_key,
    file_path = EXCLUDED.file_path,
    rows = EXCLUDED.rows,
    batches = EXCLUDED.batches,
    flight_stream_bytes = EXCLUDED.flight_stream_bytes,
    parquet_object_bytes = EXCLUDED.parquet_object_bytes
"#,
                    &[
                        &file.attempt_id,
                        &file.upload_id,
                        &file.stream_id,
                        &file.worker_id,
                        &file.logical_key,
                        &file.part_index,
                        &file.file_path,
                        &file.rows,
                        &file.batches,
                        &file.flight_stream_bytes,
                        &file.parquet_object_bytes,
                    ],
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to record completed DoPut file {} for attempt {}",
                        file.file_path, file.attempt_id
                    )
                })?;
        }

        self.client
            .execute(
                r#"
UPDATE worker_put_streams
SET status = 'SUCCEEDED',
    mode = $2,
    rows = $3,
    batches = $4,
    parts = $5,
    flight_stream_bytes = $6,
    parquet_object_bytes = $7,
    elapsed_ms = $8,
    error_message = NULL,
    put_result_json = $9,
    updated_at = now(),
    completed_at = now()
WHERE attempt_id = $1
"#,
                &[
                    &record.attempt_id,
                    &record.mode,
                    &record.rows,
                    &record.batches,
                    &record.parts,
                    &record.flight_stream_bytes,
                    &record.parquet_object_bytes,
                    &record.elapsed_ms,
                    &record.put_result_json,
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to record completed DoPut attempt {}",
                    record.attempt_id
                )
            })?;

        Ok(())
    }

    pub async fn record_failed(&self, attempt_id: &str, error_message: &str) -> Result<()> {
        self.client
            .execute(
                r#"
UPDATE worker_put_streams
SET status = 'FAILED',
    error_message = $2,
    updated_at = now(),
    completed_at = now()
WHERE attempt_id = $1
"#,
                &[&attempt_id, &error_message],
            )
            .await
            .with_context(|| format!("failed to record failed DoPut attempt {attempt_id}"))?;

        Ok(())
    }

    pub async fn record_failed_if_status(
        &self,
        attempt_id: &str,
        expected_status: &str,
        error_message: &str,
    ) -> Result<u64> {
        let updated = self
            .client
            .execute(
                r#"
UPDATE worker_put_streams
SET status = 'FAILED',
    error_message = $3,
    updated_at = now(),
    completed_at = now()
WHERE attempt_id = $1
  AND status = $2
  AND completed_at IS NULL
"#,
                &[&attempt_id, &expected_status, &error_message],
            )
            .await
            .with_context(|| format!("failed to conditionally fail DoPut attempt {attempt_id}"))?;

        Ok(updated)
    }
}
