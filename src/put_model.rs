use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize)]
pub(crate) struct PutSummary {
    pub(crate) key: String,
    pub(crate) worker: WorkerPutSummary,
    pub(crate) mode: String,
    pub(crate) rows: usize,
    pub(crate) batches: usize,
    pub(crate) parts: usize,
    pub(crate) put_parallelism: usize,
    pub(crate) client_input_file_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) flight_data_messages: Option<u64>,
    pub(crate) flight_stream_bytes: u64,
    pub(crate) parquet_object_bytes: Option<u64>,
    pub(crate) files: Vec<PutFileSummary>,
    pub(crate) arrow_schema: serde_json::Value,
    pub(crate) target_file_size: Option<usize>,
    pub(crate) elapsed_ms: u128,
    pub(crate) compression: String,
    pub(crate) multipart_part_size: usize,
    pub(crate) multipart_max_concurrency: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<PutProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) part_profiles: Option<Vec<PartProfileSummary>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PutFileSummary {
    pub(crate) key: String,
    pub(crate) part_index: usize,
    pub(crate) rows: usize,
    pub(crate) batches: usize,
    pub(crate) flight_stream_bytes: u64,
    pub(crate) parquet_object_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkerPutSummary {
    pub(crate) worker_id: String,
    pub(crate) operation_id: Option<String>,
    pub(crate) attempt_id: String,
    pub(crate) upload_id: Option<String>,
    pub(crate) stream_id: Option<String>,
    pub(crate) staging_prefix: Option<String>,
    pub(crate) admission_wait_ms: u128,
    pub(crate) global_put_stream_limit: usize,
    pub(crate) upload_put_stream_limit: Option<usize>,
    pub(crate) active_put_streams_at_admit: usize,
    pub(crate) upload_active_streams_at_admit: Option<usize>,
    pub(crate) stream_budget_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct PutProfile {
    pub(crate) total_server_ms: u128,
    pub(crate) first_flight_data_message_ms: u128,
    pub(crate) first_batch_receive_decode_ms: u128,
    pub(crate) receive_decode_ms: u128,
    pub(crate) enqueue_wait_ms: u128,
    pub(crate) collect_writer_wait_ms: u128,
    pub(crate) object_head_ms: u128,
    pub(crate) writer_task_elapsed_ms_sum: u128,
    pub(crate) writer_task_elapsed_ms_max: u128,
    pub(crate) writer_task_idle_wait_ms_sum: u128,
    pub(crate) writer_task_write_ms_sum: u128,
    pub(crate) writer_task_write_ms_max: u128,
    pub(crate) writer_task_flush_ms_sum: u128,
    pub(crate) writer_task_close_ms_sum: u128,
    pub(crate) writer_task_close_ms_max: u128,
    pub(crate) writer_task_head_ms_sum: u128,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PartProfile {
    pub(crate) elapsed_ms: u128,
    pub(crate) idle_wait_ms: u128,
    pub(crate) write_ms: u128,
    pub(crate) flush_ms: u128,
    pub(crate) close_ms: u128,
    pub(crate) head_ms: u128,
}

impl PartProfile {
    pub(crate) fn is_empty(&self) -> bool {
        self.elapsed_ms == 0
            && self.idle_wait_ms == 0
            && self.write_ms == 0
            && self.flush_ms == 0
            && self.close_ms == 0
            && self.head_ms == 0
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PartProfileSummary {
    pub(crate) key: String,
    pub(crate) part_index: usize,
    pub(crate) rows: usize,
    pub(crate) batches: usize,
    pub(crate) flight_stream_bytes: u64,
    pub(crate) parquet_object_bytes: u64,
    pub(crate) profile: PartProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DatasetPart {
    pub(crate) key: String,
    #[serde(alias = "worker")]
    pub(crate) part_index: usize,
    pub(crate) rows: usize,
    pub(crate) batches: usize,
    #[serde(default)]
    pub(crate) flight_stream_bytes: u64,
    pub(crate) parquet_object_bytes: u64,
    #[serde(default)]
    #[serde(skip_serializing_if = "PartProfile::is_empty")]
    pub(crate) profile: PartProfile,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub(crate) struct PutOptions {
    #[serde(default)]
    pub(crate) capability: Option<Value>,
    pub(crate) attempt_id: Option<String>,
    pub(crate) upload_id: Option<String>,
    pub(crate) stream_id: Option<String>,
    pub(crate) staging_prefix: Option<String>,
    pub(crate) max_upload_streams: Option<usize>,
    pub(crate) max_stream_bytes: Option<u64>,
    pub(crate) target_file_size: Option<usize>,
    pub(crate) input_file_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) profile: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PutContext {
    pub(crate) operation_id: Option<String>,
    pub(crate) attempt_id: String,
    pub(crate) upload_id: Option<String>,
    pub(crate) stream_id: Option<String>,
    pub(crate) staging_prefix: Option<String>,
    pub(crate) target_file_size: Option<usize>,
    pub(crate) upload_stream_limit: Option<usize>,
    pub(crate) stream_budget_bytes: Option<u64>,
    pub(crate) max_record_batch_bytes: u64,
}

pub(crate) struct PartBatch {
    pub(crate) batch: RecordBatch,
    pub(crate) flight_stream_bytes: u64,
}
