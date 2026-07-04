use anyhow::{Context, Result};
use serde_json::Value;
use tokio_postgres::{Client, NoTls};
use tracing::error;

use crate::{config::MetadataConfig, worker_status::WorkerStatus};

const WORKER_METADATA_MIGRATION: &str = include_str!("../db/migrations/0001_worker_metadata.sql");

pub struct MetadataStore {
    client: Client,
}

#[derive(Debug)]
pub struct PutStreamStartRecord {
    pub attempt_id: String,
}

#[derive(Debug)]
pub struct PutStreamCompleteRecord {
    pub attempt_id: String,
    pub rows: i64,
    pub batches: i64,
    pub parts: i32,
    pub flight_stream_bytes: i64,
    pub parquet_object_bytes: Option<i64>,
    pub elapsed_ms: i64,
    pub files: Vec<PutFileRecord>,
}

#[derive(Debug)]
pub struct PutFileRecord {
    pub attempt_id: String,
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
            .batch_execute(WORKER_METADATA_MIGRATION)
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
    status,
    error_message,
    updated_at,
    completed_at
) VALUES (
    $1, 'ADMITTED', NULL, now(), NULL
)
ON CONFLICT (attempt_id) DO UPDATE SET
    status = 'ADMITTED',
    error_message = NULL,
    updated_at = now(),
    completed_at = NULL
"#,
                &[&record.attempt_id],
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
    status,
    error_message,
    updated_at,
    completed_at
) VALUES (
    $1, 'REJECTED', $2, now(), now()
)
ON CONFLICT (attempt_id) DO UPDATE SET
    status = 'REJECTED',
    error_message = EXCLUDED.error_message,
    updated_at = now(),
    completed_at = now()
"#,
                &[&record.attempt_id, &error_message],
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

    pub async fn record_schema(&self, attempt_id: &str, arrow_schema_json: &Value) -> Result<()> {
        self.client
            .execute(
                r#"
UPDATE worker_put_streams
SET arrow_schema_json = $2,
    updated_at = now()
WHERE attempt_id = $1
"#,
                &[&attempt_id, arrow_schema_json],
            )
            .await
            .with_context(|| {
                format!("failed to record Arrow schema for DoPut attempt {attempt_id}")
            })?;

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
    part_index,
    file_path,
    rows,
    batches,
    flight_stream_bytes,
    parquet_object_bytes
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
ON CONFLICT (attempt_id, part_index) DO UPDATE SET
    file_path = EXCLUDED.file_path,
    rows = EXCLUDED.rows,
    batches = EXCLUDED.batches,
    flight_stream_bytes = EXCLUDED.flight_stream_bytes,
    parquet_object_bytes = EXCLUDED.parquet_object_bytes
"#,
                    &[
                        &file.attempt_id,
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
    rows = $2,
    batches = $3,
    parts = $4,
    flight_stream_bytes = $5,
    parquet_object_bytes = $6,
    elapsed_ms = $7,
    error_message = NULL,
    updated_at = now(),
    completed_at = now()
WHERE attempt_id = $1
"#,
                &[
                    &record.attempt_id,
                    &record.rows,
                    &record.batches,
                    &record.parts,
                    &record.flight_stream_bytes,
                    &record.parquet_object_bytes,
                    &record.elapsed_ms,
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

    pub async fn record_worker_heartbeat(&self, status: &WorkerStatus) -> Result<()> {
        self.client
            .execute(
                r#"
INSERT INTO worker_registry (
    worker_id,
    flight_uri,
    state,
    draining,
    put_recommended_streams,
    put_utilization_per_mille,
    put_selection_score,
    put_admission_wait_ms_ewma,
    put_throughput_bytes_per_sec_ewma,
    read_recommended_streams,
    read_utilization_per_mille,
    read_selection_score,
    read_admission_wait_ms_ewma,
    read_throughput_bytes_per_sec_ewma,
    registry_ttl_ms,
    last_heartbeat_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now()
)
ON CONFLICT (worker_id) DO UPDATE SET
    flight_uri = EXCLUDED.flight_uri,
    state = EXCLUDED.state,
    draining = EXCLUDED.draining,
    put_recommended_streams = EXCLUDED.put_recommended_streams,
    put_utilization_per_mille = EXCLUDED.put_utilization_per_mille,
    put_selection_score = EXCLUDED.put_selection_score,
    put_admission_wait_ms_ewma = EXCLUDED.put_admission_wait_ms_ewma,
    put_throughput_bytes_per_sec_ewma = EXCLUDED.put_throughput_bytes_per_sec_ewma,
    read_recommended_streams = EXCLUDED.read_recommended_streams,
    read_utilization_per_mille = EXCLUDED.read_utilization_per_mille,
    read_selection_score = EXCLUDED.read_selection_score,
    read_admission_wait_ms_ewma = EXCLUDED.read_admission_wait_ms_ewma,
    read_throughput_bytes_per_sec_ewma = EXCLUDED.read_throughput_bytes_per_sec_ewma,
    registry_ttl_ms = EXCLUDED.registry_ttl_ms,
    first_seen_at = CASE
        WHEN worker_registry.flight_uri IS DISTINCT FROM EXCLUDED.flight_uri
          OR extract(epoch FROM (now() - worker_registry.last_heartbeat_at)) * 1000 > worker_registry.registry_ttl_ms
        THEN now()
        ELSE worker_registry.first_seen_at
    END,
    last_heartbeat_at = now()
"#,
                &[
                    &status.worker_id,
                    &status.flight_uri,
                    &status.state.as_str(),
                    &status.draining,
                    &usize_to_i32(status.scheduler.put.recommended_streams),
                    &i32::from(status.scheduler.put.utilization_per_mille),
                    &u64_to_i64(status.scheduler.put.selection_score),
                    &u64_to_i64(status.runtime.put.admission_wait_ms_ewma),
                    &u64_to_i64(status.runtime.put.throughput_bytes_per_sec_ewma),
                    &usize_to_i32(status.scheduler.read.recommended_streams),
                    &i32::from(status.scheduler.read.utilization_per_mille),
                    &u64_to_i64(status.scheduler.read.selection_score),
                    &u64_to_i64(status.runtime.read.admission_wait_ms_ewma),
                    &u64_to_i64(status.runtime.read.throughput_bytes_per_sec_ewma),
                    &u64_to_i64(status.registry_ttl_ms),
                ],
            )
            .await
            .with_context(|| {
                format!("failed to record worker heartbeat for {}", status.worker_id)
            })?;

        Ok(())
    }
}

fn usize_to_i32(value: usize) -> i32 {
    value.min(i32::MAX as usize) as i32
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}
