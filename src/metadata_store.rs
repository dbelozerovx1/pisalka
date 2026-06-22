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

    pub async fn record_worker_heartbeat(&self, status: &WorkerStatus) -> Result<()> {
        let status_json =
            serde_json::to_value(status).context("failed to serialize worker heartbeat")?;

        self.client
            .execute(
                r#"
INSERT INTO worker_registry (
    worker_id,
    flight_uri,
    worker_zone,
    state,
    draining,
    put_limit,
    active_put_streams,
    available_put_streams,
    put_recommended_streams,
    put_soft_available_slots,
    put_memory_available_streams,
    put_utilization_per_mille,
    put_selection_score,
    put_admission_wait_ms_ewma,
    put_throughput_bytes_per_sec_ewma,
    put_succeeded_total,
    put_failed_total,
    put_rejected_total,
    read_limit,
    active_read_streams,
    available_read_streams,
    read_recommended_streams,
    read_soft_available_slots,
    read_memory_available_streams,
    read_utilization_per_mille,
    read_selection_score,
    read_admission_wait_ms_ewma,
    read_throughput_bytes_per_sec_ewma,
    read_succeeded_total,
    read_failed_total,
    read_rejected_total,
    read_cancelled_total,
    worker_memory_bytes,
    reserved_memory_bytes,
    put_memory_limit_bytes,
    put_memory_active_bytes,
    put_memory_available_bytes,
    put_max_stream_memory_bytes,
    put_max_record_batch_bytes,
    read_memory_limit_bytes,
    read_memory_active_bytes,
    read_memory_available_bytes,
    read_max_stream_memory_bytes,
    read_max_record_batch_bytes,
    read_max_batch_rows,
    heartbeat_interval_ms,
    registry_ttl_ms,
    status_json,
    last_heartbeat_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
    $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
    $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48,
    now()
)
ON CONFLICT (worker_id) DO UPDATE SET
    flight_uri = EXCLUDED.flight_uri,
    worker_zone = EXCLUDED.worker_zone,
    state = EXCLUDED.state,
    draining = EXCLUDED.draining,
    put_limit = EXCLUDED.put_limit,
    active_put_streams = EXCLUDED.active_put_streams,
    available_put_streams = EXCLUDED.available_put_streams,
    put_recommended_streams = EXCLUDED.put_recommended_streams,
    put_soft_available_slots = EXCLUDED.put_soft_available_slots,
    put_memory_available_streams = EXCLUDED.put_memory_available_streams,
    put_utilization_per_mille = EXCLUDED.put_utilization_per_mille,
    put_selection_score = EXCLUDED.put_selection_score,
    put_admission_wait_ms_ewma = EXCLUDED.put_admission_wait_ms_ewma,
    put_throughput_bytes_per_sec_ewma = EXCLUDED.put_throughput_bytes_per_sec_ewma,
    put_succeeded_total = EXCLUDED.put_succeeded_total,
    put_failed_total = EXCLUDED.put_failed_total,
    put_rejected_total = EXCLUDED.put_rejected_total,
    read_limit = EXCLUDED.read_limit,
    active_read_streams = EXCLUDED.active_read_streams,
    available_read_streams = EXCLUDED.available_read_streams,
    read_recommended_streams = EXCLUDED.read_recommended_streams,
    read_soft_available_slots = EXCLUDED.read_soft_available_slots,
    read_memory_available_streams = EXCLUDED.read_memory_available_streams,
    read_utilization_per_mille = EXCLUDED.read_utilization_per_mille,
    read_selection_score = EXCLUDED.read_selection_score,
    read_admission_wait_ms_ewma = EXCLUDED.read_admission_wait_ms_ewma,
    read_throughput_bytes_per_sec_ewma = EXCLUDED.read_throughput_bytes_per_sec_ewma,
    read_succeeded_total = EXCLUDED.read_succeeded_total,
    read_failed_total = EXCLUDED.read_failed_total,
    read_rejected_total = EXCLUDED.read_rejected_total,
    read_cancelled_total = EXCLUDED.read_cancelled_total,
    worker_memory_bytes = EXCLUDED.worker_memory_bytes,
    reserved_memory_bytes = EXCLUDED.reserved_memory_bytes,
    put_memory_limit_bytes = EXCLUDED.put_memory_limit_bytes,
    put_memory_active_bytes = EXCLUDED.put_memory_active_bytes,
    put_memory_available_bytes = EXCLUDED.put_memory_available_bytes,
    put_max_stream_memory_bytes = EXCLUDED.put_max_stream_memory_bytes,
    put_max_record_batch_bytes = EXCLUDED.put_max_record_batch_bytes,
    read_memory_limit_bytes = EXCLUDED.read_memory_limit_bytes,
    read_memory_active_bytes = EXCLUDED.read_memory_active_bytes,
    read_memory_available_bytes = EXCLUDED.read_memory_available_bytes,
    read_max_stream_memory_bytes = EXCLUDED.read_max_stream_memory_bytes,
    read_max_record_batch_bytes = EXCLUDED.read_max_record_batch_bytes,
    read_max_batch_rows = EXCLUDED.read_max_batch_rows,
    heartbeat_interval_ms = EXCLUDED.heartbeat_interval_ms,
    registry_ttl_ms = EXCLUDED.registry_ttl_ms,
    status_json = EXCLUDED.status_json,
    last_heartbeat_at = now()
"#,
                &[
                    &status.worker_id,
                    &status.flight_uri,
                    &status.locality.zone,
                    &status.state.as_str(),
                    &status.draining,
                    &usize_to_i32(status.put.limit),
                    &usize_to_i32(status.put.active),
                    &usize_to_i32(status.put.available),
                    &usize_to_i32(status.scheduler.put.recommended_streams),
                    &usize_to_i32(status.scheduler.put.soft_available_slots),
                    &usize_to_i32(status.scheduler.put.memory_available_streams),
                    &i32::from(status.scheduler.put.utilization_per_mille),
                    &u64_to_i64(status.scheduler.put.selection_score),
                    &u64_to_i64(status.runtime.put.admission_wait_ms_ewma),
                    &u64_to_i64(status.runtime.put.throughput_bytes_per_sec_ewma),
                    &u64_to_i64(status.runtime.put.succeeded_total),
                    &u64_to_i64(status.runtime.put.failed_total),
                    &u64_to_i64(status.runtime.put.rejected_total),
                    &usize_to_i32(status.read.limit),
                    &usize_to_i32(status.read.active),
                    &usize_to_i32(status.read.available),
                    &usize_to_i32(status.scheduler.read.recommended_streams),
                    &usize_to_i32(status.scheduler.read.soft_available_slots),
                    &usize_to_i32(status.scheduler.read.memory_available_streams),
                    &i32::from(status.scheduler.read.utilization_per_mille),
                    &u64_to_i64(status.scheduler.read.selection_score),
                    &u64_to_i64(status.runtime.read.admission_wait_ms_ewma),
                    &u64_to_i64(status.runtime.read.throughput_bytes_per_sec_ewma),
                    &u64_to_i64(status.runtime.read.succeeded_total),
                    &u64_to_i64(status.runtime.read.failed_total),
                    &u64_to_i64(status.runtime.read.rejected_total),
                    &u64_to_i64(status.runtime.read.cancelled_total),
                    &u64_to_i64(status.resources.worker_memory_bytes),
                    &u64_to_i64(status.resources.reserved_memory_bytes),
                    &u64_to_i64(status.resources.put.limit_bytes),
                    &u64_to_i64(status.resources.put.active_bytes),
                    &u64_to_i64(status.resources.put.available_bytes),
                    &u64_to_i64(status.resources.put.max_stream_memory_bytes),
                    &u64_to_i64(status.resources.put.max_record_batch_bytes),
                    &u64_to_i64(status.resources.read.limit_bytes),
                    &u64_to_i64(status.resources.read.active_bytes),
                    &u64_to_i64(status.resources.read.available_bytes),
                    &u64_to_i64(status.resources.read.max_stream_memory_bytes),
                    &u64_to_i64(status.resources.read.max_record_batch_bytes),
                    &usize_to_i32(status.resources.read.max_batch_rows.unwrap_or_default()),
                    &u64_to_i64(status.heartbeat_interval_ms),
                    &u64_to_i64(status.registry_ttl_ms),
                    &status_json,
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
