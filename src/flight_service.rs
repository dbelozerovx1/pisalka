use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::{Array, RecordBatch};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    error::FlightError, flight_service_server::FlightService,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt, buffered::BufWriter, path::Path};
use parquet::{
    arrow::{
        async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
        async_writer::{AsyncArrowWriter, ParquetObjectWriter},
    },
    file::properties::WriterProperties,
};
use serde_json::{Value, json};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
    time::{Instant as TokioInstant, sleep, timeout},
};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::{
    admission::{GuardedResponseStream, PutAdmission, ReadAdmission},
    capability::{CAPABILITY_VERSION, parse_capability_envelope, verify_put_capability},
    config::{AppConfig, ParquetTuning},
    metadata_store::{MetadataStore, PutFileRecord, PutStreamCompleteRecord, PutStreamStartRecord},
    metrics::{MeasuredReadStream, WorkerMetrics},
    put_model::{
        DatasetPart, PartBatch, PartProfile, PartProfileSummary, PutContext, PutFileSummary,
        PutOptions, PutProfile, PutSummary, WorkerPutSummary,
    },
    resource::ResourceLimiter,
    ticket::parse_read_ticket,
    util::{descriptor_to_object_key, path_from_key},
    worker_status::{
        WorkerCapabilities, WorkerCapacity, WorkerLocality, WorkerResourcePool,
        WorkerResourceStatus, WorkerSchedulerStatus, WorkerSchedulingPolicy,
        WorkerSchedulingSignal, WorkerSchedulingTelemetry, WorkerState, WorkerStatus,
    },
};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct WorkerFlightService {
    config: Arc<AppConfig>,
    store: Arc<dyn ObjectStore>,
    metadata_store: Option<Arc<MetadataStore>>,
    put_slots: Arc<Semaphore>,
    read_slots: Arc<Semaphore>,
    put_memory: ResourceLimiter,
    read_memory: ResourceLimiter,
    upload_streams: Arc<Mutex<HashMap<String, usize>>>,
    active_put_streams: Arc<AtomicUsize>,
    active_read_streams: Arc<AtomicUsize>,
    draining: Arc<AtomicBool>,
    metrics: Arc<WorkerMetrics>,
}

impl WorkerFlightService {
    pub fn new(
        config: AppConfig,
        store: Arc<dyn ObjectStore>,
        metadata_store: Option<Arc<MetadataStore>>,
        metrics: Arc<WorkerMetrics>,
    ) -> Self {
        let put_slots = Arc::new(Semaphore::new(config.worker.max_active_put_streams.max(1)));
        let read_slots = Arc::new(Semaphore::new(config.worker.max_active_read_streams.max(1)));
        let put_memory = ResourceLimiter::new(config.resources.put_memory_bytes);
        let read_memory = ResourceLimiter::new(config.resources.read_memory_bytes);
        let draining = Arc::new(AtomicBool::new(config.worker.draining));
        Self {
            config: Arc::new(config),
            store,
            metadata_store,
            put_slots,
            read_slots,
            put_memory,
            read_memory,
            upload_streams: Arc::new(Mutex::new(HashMap::new())),
            active_put_streams: Arc::new(AtomicUsize::new(0)),
            active_read_streams: Arc::new(AtomicUsize::new(0)),
            draining,
            metrics,
        }
    }

    pub fn worker_status(&self) -> WorkerStatus {
        let active_put = self.active_put_streams.load(Ordering::Relaxed);
        let active_read = self.active_read_streams.load(Ordering::Relaxed);
        let draining = self.draining.load(Ordering::Relaxed);
        let put = WorkerCapacity {
            limit: self.config.worker.max_active_put_streams,
            active: active_put,
            available: self
                .config
                .worker
                .max_active_put_streams
                .saturating_sub(active_put),
            slot_wait_ms: self.config.worker.put_slot_wait_ms,
        };
        let read = WorkerCapacity {
            limit: self.config.worker.max_active_read_streams,
            active: active_read,
            available: self
                .config
                .worker
                .max_active_read_streams
                .saturating_sub(active_read),
            slot_wait_ms: self.config.worker.read_slot_wait_ms,
        };
        let runtime = self.metrics.runtime_status();
        let resources = WorkerResourceStatus {
            worker_memory_bytes: self.config.resources.worker_memory_bytes,
            reserved_memory_bytes: self.config.resources.reserved_memory_bytes,
            put: WorkerResourcePool {
                limit_bytes: self.put_memory.total_bytes(),
                active_bytes: self.put_memory.active_bytes(),
                available_bytes: self.put_memory.available_bytes(),
                max_stream_memory_bytes: self.config.resources.put_max_stream_memory_bytes,
                max_record_batch_bytes: self.config.resources.put_max_record_batch_bytes,
                max_batch_rows: None,
            },
            read: WorkerResourcePool {
                limit_bytes: self.read_memory.total_bytes(),
                active_bytes: self.read_memory.active_bytes(),
                available_bytes: self.read_memory.available_bytes(),
                max_stream_memory_bytes: self.config.resources.read_max_stream_memory_bytes,
                max_record_batch_bytes: self.config.resources.read_max_record_batch_bytes,
                max_batch_rows: Some(self.config.resources.read_max_batch_rows),
            },
        };
        let scheduler = WorkerSchedulerStatus {
            put: WorkerSchedulingSignal::from_capacity(
                &put,
                WorkerSchedulingPolicy {
                    max_streams_per_operation: self.config.worker.max_put_streams_per_upload,
                    reserved_slots: self.config.worker.put_scheduler_reserved_slots,
                    memory_available_streams: Some(
                        self.put_memory
                            .available_streams(self.config.resources.put_max_stream_memory_bytes),
                    ),
                },
                WorkerSchedulingTelemetry {
                    succeeded_total: runtime.put.succeeded_total,
                    failed_total: runtime.put.failed_total,
                    rejected_total: runtime.put.rejected_total,
                    cancelled_total: 0,
                    admission_wait_ms_ewma: runtime.put.admission_wait_ms_ewma,
                },
                draining,
            ),
            read: WorkerSchedulingSignal::from_capacity(
                &read,
                WorkerSchedulingPolicy {
                    max_streams_per_operation: self.config.worker.max_read_streams_per_operation,
                    reserved_slots: self.config.worker.read_scheduler_reserved_slots,
                    memory_available_streams: Some(
                        self.read_memory
                            .available_streams(self.config.resources.read_max_stream_memory_bytes),
                    ),
                },
                WorkerSchedulingTelemetry {
                    succeeded_total: runtime.read.succeeded_total,
                    failed_total: runtime.read.failed_total,
                    rejected_total: runtime.read.rejected_total,
                    cancelled_total: runtime.read.cancelled_total,
                    admission_wait_ms_ewma: runtime.read.admission_wait_ms_ewma,
                },
                draining,
            ),
        };

        WorkerStatus {
            worker_id: self.config.worker.worker_id.clone(),
            flight_uri: self.config.worker.flight_uri.clone(),
            locality: WorkerLocality {
                zone: self.config.worker.zone.clone(),
            },
            state: if draining {
                WorkerState::Draining
            } else {
                WorkerState::Active
            },
            draining,
            put,
            read,
            resources,
            scheduler,
            runtime,
            capabilities: WorkerCapabilities {
                put_parallelism: self.config.parquet.put_parallelism,
                put_queue_depth: self.config.parquet.put_queue_depth,
                max_put_streams_per_upload: self.config.worker.max_put_streams_per_upload,
                max_put_stream_bytes: self.config.worker.max_put_stream_bytes,
                put_max_stream_memory_bytes: self.config.resources.put_max_stream_memory_bytes,
                put_max_record_batch_bytes: self.config.resources.put_max_record_batch_bytes,
                read_max_streams_per_operation: self.config.worker.max_read_streams_per_operation,
                read_batch_size: self.config.read_batch_size,
                read_max_stream_memory_bytes: self.config.resources.read_max_stream_memory_bytes,
                read_max_record_batch_bytes: self.config.resources.read_max_record_batch_bytes,
                read_max_batch_rows: self.config.resources.read_max_batch_rows,
                flight_data_chunk_size: self.config.flight_data_chunk_size,
                capability_version: CAPABILITY_VERSION,
                signed_capabilities_required: self.config.security.require_signed_capabilities,
                capability_worker_binding_required: self
                    .config
                    .security
                    .require_capability_worker_binding,
            },
            heartbeat_interval_ms: self.config.worker.registry_heartbeat_interval_ms,
            registry_ttl_ms: self.config.worker.registry_ttl_ms,
        }
    }

    fn build_put_context(&self, key: &str, options: &PutOptions) -> Result<PutContext, Status> {
        let capability = match options.capability.clone() {
            Some(value) => Some(verify_put_capability(
                value,
                &self.config.worker,
                &self.config.security,
            )?),
            None if self.config.security.require_signed_capabilities => {
                return Err(Status::permission_denied(
                    "worker requires signed DoPut capability metadata",
                ));
            }
            None => None,
        };

        let option_attempt_id = validate_worker_id("attempt_id", options.attempt_id.as_deref())?;
        let option_upload_id = validate_worker_id("upload_id", options.upload_id.as_deref())?;
        let option_stream_id = validate_worker_id("stream_id", options.stream_id.as_deref())?;
        let option_staging_prefix = options
            .staging_prefix
            .as_deref()
            .map(normalize_staging_prefix)
            .transpose()?;
        let attempt_id = reconcile_optional_value(
            "attempt_id",
            option_attempt_id,
            capability
                .as_ref()
                .and_then(|capability| capability.attempt_id.clone()),
        )?
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let upload_id = reconcile_optional_value(
            "upload_id",
            option_upload_id,
            capability
                .as_ref()
                .and_then(|capability| capability.upload_id.clone()),
        )?;
        let stream_id = reconcile_optional_value(
            "stream_id",
            option_stream_id,
            capability
                .as_ref()
                .and_then(|capability| capability.stream_id.clone()),
        )?;
        let allowed_output_prefix = capability
            .as_ref()
            .map(|capability| capability.allowed_output_prefix.clone());
        let staging_prefix = reconcile_optional_value(
            "staging_prefix",
            option_staging_prefix,
            capability
                .as_ref()
                .and_then(|capability| capability.staging_prefix.clone())
                .or_else(|| allowed_output_prefix.clone()),
        )?;

        if self.config.worker.require_staging_prefix && staging_prefix.is_none() {
            return Err(Status::permission_denied(
                "DoPut requires staging_prefix metadata",
            ));
        }

        if let Some(staging_prefix) = staging_prefix.as_ref() {
            if !key.starts_with(staging_prefix) {
                return Err(Status::permission_denied(format!(
                    "DoPut key {key:?} is outside staging_prefix {staging_prefix:?}"
                )));
            }
        }
        if let Some(allowed_output_prefix) = allowed_output_prefix.as_ref() {
            if !key.starts_with(allowed_output_prefix) {
                return Err(Status::permission_denied(format!(
                    "DoPut key {key:?} is outside signed allowed_output_prefix {allowed_output_prefix:?}"
                )));
            }
        }

        let capability_target_file_size = capability
            .as_ref()
            .and_then(|capability| capability.target_file_size);
        if let Some(capability_target_file_size) = capability_target_file_size {
            if options
                .target_file_size
                .is_some_and(|target_file_size| target_file_size != capability_target_file_size)
            {
                return Err(Status::permission_denied(format!(
                    "DoPut target_file_size {:?} does not match signed target_file_size {capability_target_file_size}",
                    options.target_file_size
                )));
            }
        }
        let target_file_size = options.target_file_size.or(capability_target_file_size);

        let upload_stream_limit = upload_id.as_ref().map(|_| {
            min_optional_usize(
                options.max_upload_streams,
                capability
                    .as_ref()
                    .and_then(|capability| capability.max_upload_streams),
            )
            .unwrap_or(self.config.worker.max_put_streams_per_upload)
            .min(self.config.worker.max_put_streams_per_upload)
            .max(1)
        });
        let stream_budget_bytes = min_optional_u64(
            min_optional_u64(
                options.max_stream_bytes.filter(|bytes| *bytes > 0),
                capability
                    .as_ref()
                    .and_then(|capability| capability.max_stream_bytes),
            ),
            self.config.worker.max_put_stream_bytes,
        );
        let max_record_batch_bytes = min_optional_u64(
            Some(self.config.resources.put_max_record_batch_bytes),
            capability
                .as_ref()
                .and_then(|capability| capability.max_record_batch_bytes),
        )
        .unwrap_or(self.config.resources.put_max_record_batch_bytes)
        .max(1);

        if let (Some(input_file_bytes), Some(stream_budget_bytes)) =
            (options.input_file_bytes, stream_budget_bytes)
        {
            if input_file_bytes > stream_budget_bytes {
                return Err(Status::resource_exhausted(format!(
                    "DoPut input_file_bytes {input_file_bytes} exceeds stream budget {stream_budget_bytes}"
                )));
            }
        }

        Ok(PutContext {
            operation_id: capability.and_then(|capability| capability.operation_id),
            attempt_id,
            upload_id,
            stream_id,
            staging_prefix,
            target_file_size,
            upload_stream_limit,
            stream_budget_bytes,
            max_record_batch_bytes,
        })
    }

    async fn admit_put(
        &self,
        context: &PutContext,
    ) -> Result<(PutAdmission, WorkerPutSummary), Status> {
        if self.draining.load(Ordering::Relaxed) {
            self.metrics.record_put_rejected();
            return Err(Status::unavailable(
                "worker is draining and rejects new DoPut streams",
            ));
        }

        let wait_started = Instant::now();
        let permit = if self.config.worker.put_slot_wait_ms == 0 {
            match self.put_slots.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.metrics.record_put_rejected();
                    return Err(Status::resource_exhausted(
                        "DoPut worker has no free upload slots",
                    ));
                }
            }
        } else {
            match timeout(
                Duration::from_millis(self.config.worker.put_slot_wait_ms),
                self.put_slots.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(err)) => return Err(Status::internal(err.to_string())),
                Err(_) => {
                    self.metrics.record_put_rejected();
                    return Err(Status::resource_exhausted(
                        "timed out waiting for DoPut upload slot",
                    ));
                }
            }
        };
        let admission_wait_ms = wait_started.elapsed().as_millis();
        let memory = match self
            .put_memory
            .reserve(
                self.config.resources.put_max_stream_memory_bytes,
                self.config.worker.put_slot_wait_ms,
                "DoPut",
            )
            .await
        {
            Ok(memory) => memory,
            Err(status) => {
                self.metrics.record_put_rejected();
                return Err(status);
            }
        };

        let upload_active_streams_at_admit = if let (Some(upload_id), Some(upload_stream_limit)) =
            (context.upload_id.as_ref(), context.upload_stream_limit)
        {
            let mut upload_streams = self
                .upload_streams
                .lock()
                .map_err(|_| Status::internal("upload stream slot tracker mutex poisoned"))?;
            let active_streams = upload_streams.get(upload_id).copied().unwrap_or_default();
            if active_streams >= upload_stream_limit {
                self.metrics.record_put_rejected();
                return Err(Status::resource_exhausted(format!(
                    "upload {upload_id:?} has no free stream slots; active={active_streams}, limit={upload_stream_limit}"
                )));
            }

            let admitted_streams = active_streams + 1;
            upload_streams.insert(upload_id.clone(), admitted_streams);
            Some(admitted_streams)
        } else {
            None
        };

        let active_put_streams_at_admit =
            self.active_put_streams.fetch_add(1, Ordering::Relaxed) + 1;
        self.metrics
            .record_put_admission_wait(u128_to_u64(admission_wait_ms));
        let summary = WorkerPutSummary {
            worker_id: self.config.worker.worker_id.clone(),
            operation_id: context.operation_id.clone(),
            attempt_id: context.attempt_id.clone(),
            upload_id: context.upload_id.clone(),
            stream_id: context.stream_id.clone(),
            staging_prefix: context.staging_prefix.clone(),
            admission_wait_ms,
            global_put_stream_limit: self.config.worker.max_active_put_streams,
            upload_put_stream_limit: context.upload_stream_limit,
            active_put_streams_at_admit,
            upload_active_streams_at_admit,
            stream_budget_bytes: context.stream_budget_bytes,
        };
        let admission = PutAdmission {
            _permit: permit,
            _memory: memory,
            upload_id: context.upload_id.clone(),
            upload_streams: self.upload_streams.clone(),
            active_put_streams: self.active_put_streams.clone(),
        };

        Ok((admission, summary))
    }

    async fn admit_read(&self) -> Result<ReadAdmission, Status> {
        if self.draining.load(Ordering::Relaxed) {
            self.metrics.record_get_rejected();
            return Err(Status::unavailable(
                "worker is draining and rejects new DoGet streams",
            ));
        }

        let wait_started = Instant::now();
        let permit = if self.config.worker.read_slot_wait_ms == 0 {
            match self.read_slots.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.metrics.record_get_rejected();
                    return Err(Status::resource_exhausted(
                        "DoGet worker has no free read slots",
                    ));
                }
            }
        } else {
            match timeout(
                Duration::from_millis(self.config.worker.read_slot_wait_ms),
                self.read_slots.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(err)) => return Err(Status::internal(err.to_string())),
                Err(_) => {
                    self.metrics.record_get_rejected();
                    return Err(Status::resource_exhausted(
                        "timed out waiting for DoGet read slot",
                    ));
                }
            }
        };
        let memory = match self
            .read_memory
            .reserve(
                self.config.resources.read_max_stream_memory_bytes,
                self.config.worker.read_slot_wait_ms,
                "DoGet",
            )
            .await
        {
            Ok(memory) => memory,
            Err(status) => {
                self.metrics.record_get_rejected();
                return Err(status);
            }
        };

        let active_read_streams_at_admit =
            self.active_read_streams.fetch_add(1, Ordering::Relaxed) + 1;
        self.metrics
            .record_get_admission_wait(u128_to_u64(wait_started.elapsed().as_millis()));

        Ok(ReadAdmission {
            _permit: permit,
            _memory: memory,
            active_read_streams: self.active_read_streams.clone(),
            active_read_streams_at_admit,
        })
    }

    fn put_start_record(
        &self,
        key: &str,
        context: &PutContext,
        worker_summary: Option<&WorkerPutSummary>,
        options: &PutOptions,
    ) -> PutStreamStartRecord {
        PutStreamStartRecord {
            attempt_id: context.attempt_id.clone(),
            upload_id: context.upload_id.clone(),
            stream_id: context.stream_id.clone(),
            worker_id: self.config.worker.worker_id.clone(),
            key: key.to_owned(),
            mode: Some(put_mode(context).to_owned()),
            staging_prefix: context.staging_prefix.clone(),
            target_file_size: context.target_file_size.map(usize_to_i64),
            client_input_file_bytes: options.input_file_bytes.map(u64_to_i64),
            stream_budget_bytes: context.stream_budget_bytes.map(u64_to_i64),
            global_put_stream_limit: usize_to_i32(self.config.worker.max_active_put_streams),
            upload_put_stream_limit: context.upload_stream_limit.map(usize_to_i32),
            active_put_streams_at_admit: worker_summary
                .map(|summary| usize_to_i32(summary.active_put_streams_at_admit)),
            upload_active_streams_at_admit: worker_summary
                .and_then(|summary| summary.upload_active_streams_at_admit)
                .map(usize_to_i32),
            compression: self.config.parquet.compression_name.clone(),
            multipart_part_size: usize_to_i64(self.config.parquet.multipart_part_size),
            multipart_max_concurrency: usize_to_i32(self.config.parquet.multipart_max_concurrency),
        }
    }

    async fn record_put_admitted(&self, record: &PutStreamStartRecord) -> Result<(), Status> {
        let Some(metadata_store) = self.metadata_store.as_ref() else {
            return Ok(());
        };
        metadata_store
            .record_admitted(record)
            .await
            .map_err(status_from_context)?;

        let timeout_ms = self.config.worker.put_first_batch_timeout_ms;
        if timeout_ms > 0 {
            let metadata_store = metadata_store.clone();
            let attempt_id = record.attempt_id.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(timeout_ms)).await;
                let error_message =
                    format!("timed out waiting {timeout_ms}ms for first DoPut record batch");
                match metadata_store
                    .record_failed_if_status(&attempt_id, "ADMITTED", &error_message)
                    .await
                {
                    Ok(1) => info!(
                        attempt_id = %attempt_id,
                        "marked stale admitted DoPut stream as failed"
                    ),
                    Ok(_) => {}
                    Err(error) => error!(
                        attempt_id = %attempt_id,
                        error = %error,
                        "failed to mark stale admitted DoPut stream as failed"
                    ),
                }
            });
        }

        Ok(())
    }

    async fn record_put_rejected(
        &self,
        record: &PutStreamStartRecord,
        status: &Status,
    ) -> Result<(), Status> {
        let Some(metadata_store) = self.metadata_store.as_ref() else {
            return Ok(());
        };
        metadata_store
            .record_rejected(record, &status.to_string())
            .await
            .map_err(status_from_context)
    }

    async fn record_put_writing(&self, attempt_id: &str) -> Result<(), Status> {
        let Some(metadata_store) = self.metadata_store.as_ref() else {
            return Ok(());
        };
        metadata_store
            .record_writing(attempt_id)
            .await
            .map_err(status_from_context)
    }

    async fn record_put_schema(
        &self,
        attempt_id: &str,
        arrow_schema_json: &Value,
    ) -> Result<(), Status> {
        let Some(metadata_store) = self.metadata_store.as_ref() else {
            return Ok(());
        };
        metadata_store
            .record_schema(attempt_id, arrow_schema_json)
            .await
            .map_err(status_from_context)
    }

    async fn record_put_completed(&self, summary: &PutSummary) -> Result<(), Status> {
        let Some(metadata_store) = self.metadata_store.as_ref() else {
            return Ok(());
        };
        let record = PutStreamCompleteRecord {
            attempt_id: summary.worker.attempt_id.clone(),
            mode: summary.mode.clone(),
            rows: usize_to_i64(summary.rows),
            batches: usize_to_i64(summary.batches),
            parts: usize_to_i32(summary.parts),
            flight_stream_bytes: u64_to_i64(summary.flight_stream_bytes),
            parquet_object_bytes: summary.parquet_object_bytes.map(u64_to_i64),
            elapsed_ms: u128_to_i64(summary.elapsed_ms),
            put_result_json: serde_json::to_value(summary).map_err(status_from_anyhow)?,
            files: put_file_records(summary),
        };
        metadata_store
            .record_completed(&record)
            .await
            .map_err(status_from_context)
    }

    async fn record_put_failed(&self, attempt_id: &str, status: &Status) {
        let Some(metadata_store) = self.metadata_store.as_ref() else {
            return;
        };
        if let Err(error) = metadata_store
            .record_failed(attempt_id, &status.to_string())
            .await
        {
            error!(
                attempt_id = %attempt_id,
                error = %error,
                "failed to persist DoPut failure status"
            );
        }
    }

    async fn write_put_stream(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<PutSummary, Status> {
        let request_started = Instant::now();
        let mut incoming = request.into_inner();
        let first_message_started = Some(Instant::now());
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("DoPut stream was empty"))?;
        let first_flight_data_message_ms = first_message_started
            .map(|started| started.elapsed().as_millis())
            .unwrap_or_default();

        let key = descriptor_to_object_key(first.flight_descriptor.as_ref());
        let put_options = parse_put_options(&first.app_metadata)?;
        let put_context = self.build_put_context(&key, &put_options)?;
        let (admission_guard, worker_summary) = match self.admit_put(&put_context).await {
            Ok(admission) => admission,
            Err(status) => {
                let record = self.put_start_record(&key, &put_context, None, &put_options);
                self.record_put_rejected(&record, &status).await?;
                return Err(status);
            }
        };
        let start_record =
            self.put_start_record(&key, &put_context, Some(&worker_summary), &put_options);
        self.record_put_admitted(&start_record).await?;

        let attempt_id = worker_summary.attempt_id.clone();
        let result = async {
            let profile_enabled = put_options.profile;
            let path = path_from_key(&key);
            let flight_stream_bytes = Arc::new(AtomicU64::new(0));
            let flight_data_messages = Arc::new(AtomicU64::new(0));

            let first_stream = stream::once(async move { Ok(first) });
            let stream_bytes = flight_stream_bytes.clone();
            let stream_messages = flight_data_messages.clone();
            let flight_stream = first_stream
                .chain(incoming.map(|item| item.map_err(FlightError::from)))
                .map(move |item| {
                    item.map(|data| {
                        if profile_enabled {
                            stream_messages.fetch_add(1, Ordering::Relaxed);
                        }
                        stream_bytes.fetch_add(flight_data_size(&data), Ordering::Relaxed);
                        data
                    })
                });
            let mut batches =
                arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(flight_stream);

            let first_batch_started = profile_enabled.then(Instant::now);
            let first_batch =
                read_first_batch(&mut batches, self.config.worker.put_first_batch_timeout_ms)
                    .await?
                    .ok_or_else(|| {
                        Status::invalid_argument("DoPut stream did not contain record batches")
                    })?;
            enforce_record_batch_budget("DoPut", &first_batch, put_context.max_record_batch_bytes)?;
            enforce_stream_budget(&put_context, flight_stream_bytes.load(Ordering::Relaxed))?;
            self.record_put_writing(&attempt_id).await?;
            let first_batch_receive_decode_ms = first_batch_started
                .map(|started| started.elapsed().as_millis())
                .unwrap_or_default();
            let receive_decode_ms = first_batch_receive_decode_ms;
            let first_batch_flight_bytes = flight_stream_bytes.load(Ordering::Relaxed);
            let arrow_schema_json = arrow_schema_to_json(first_batch.schema().as_ref());
            self.record_put_schema(&attempt_id, &arrow_schema_json)
                .await?;

            if let Some(target_file_size) = put_context.target_file_size {
                return self
                    .write_sized_dataset(
                        key,
                        target_file_size,
                        put_options.input_file_bytes,
                        first_batch,
                        first_batch_flight_bytes,
                        &mut batches,
                        &flight_stream_bytes,
                        &flight_data_messages,
                        request_started,
                        first_flight_data_message_ms,
                        first_batch_receive_decode_ms,
                        receive_decode_ms,
                        arrow_schema_json,
                        profile_enabled,
                        put_context,
                        worker_summary,
                    )
                    .await;
            }

            self.write_single_file(
                key,
                path,
                put_options.input_file_bytes,
                first_batch,
                &mut batches,
                &flight_stream_bytes,
                &flight_data_messages,
                request_started,
                first_flight_data_message_ms,
                first_batch_receive_decode_ms,
                receive_decode_ms,
                arrow_schema_json,
                profile_enabled,
                put_context,
                worker_summary,
            )
            .await
        }
        .await;
        drop(admission_guard);

        match result {
            Ok(summary) => {
                self.record_put_completed(&summary).await?;
                Ok(summary)
            }
            Err(status) => {
                self.record_put_failed(&attempt_id, &status).await;
                Err(status)
            }
        }
    }

    async fn write_single_file<S>(
        &self,
        key: String,
        path: Path,
        client_input_file_bytes: Option<u64>,
        first_batch: RecordBatch,
        batches: &mut S,
        flight_stream_bytes: &AtomicU64,
        flight_data_messages: &AtomicU64,
        request_started: Instant,
        first_flight_data_message_ms: u128,
        first_batch_receive_decode_ms: u128,
        mut receive_decode_ms: u128,
        arrow_schema: Value,
        profile_enabled: bool,
        put_context: PutContext,
        worker_summary: WorkerPutSummary,
    ) -> Result<PutSummary, Status>
    where
        S: Stream<Item = Result<RecordBatch, FlightError>> + Unpin,
    {
        let props = writer_properties(&self.config.parquet);
        let object_writer =
            parquet_object_writer(self.store.clone(), path.clone(), &self.config.parquet);
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, first_batch.schema(), Some(props))
                .map_err(status_from_anyhow)?;

        let writer_started = profile_enabled.then(Instant::now);
        let mut writer_profile = PartProfile::default();
        let mut batches_written = 0usize;
        let mut rows_written = 0usize;

        write_batch(
            &mut writer,
            &first_batch,
            &self.config.parquet,
            &mut batches_written,
            &mut rows_written,
            &mut writer_profile,
            profile_enabled,
        )
        .await?;

        loop {
            let receive_started = profile_enabled.then(Instant::now);
            let next_batch = batches.try_next().await.map_err(status_from_flight_error)?;
            if let Some(started) = receive_started {
                receive_decode_ms += started.elapsed().as_millis();
            }
            let Some(batch) = next_batch else {
                break;
            };
            enforce_stream_budget(&put_context, flight_stream_bytes.load(Ordering::Relaxed))?;
            enforce_record_batch_budget("DoPut", &batch, put_context.max_record_batch_bytes)?;

            write_batch(
                &mut writer,
                &batch,
                &self.config.parquet,
                &mut batches_written,
                &mut rows_written,
                &mut writer_profile,
                profile_enabled,
            )
            .await?;
        }

        let close_started = profile_enabled.then(Instant::now);
        writer.close().await.map_err(status_from_anyhow)?;
        if let Some(started) = close_started {
            writer_profile.close_ms += started.elapsed().as_millis();
        }
        let head_started = profile_enabled.then(Instant::now);
        let object_meta = self.store.head(&path).await.ok();
        if let Some(started) = head_started {
            writer_profile.head_ms += started.elapsed().as_millis();
        }
        if let Some(started) = writer_started {
            writer_profile.elapsed_ms = started.elapsed().as_millis();
        }
        let flight_stream_bytes = flight_stream_bytes.load(Ordering::Relaxed);
        let flight_data_messages = flight_data_messages.load(Ordering::Relaxed);
        let parquet_object_bytes = object_meta.as_ref().map(|meta| meta.size);
        let files = vec![PutFileSummary {
            key: path.to_string(),
            part_index: 0,
            rows: rows_written,
            batches: batches_written,
            flight_stream_bytes,
            parquet_object_bytes: parquet_object_bytes.unwrap_or_default(),
        }];
        let elapsed_ms = request_started.elapsed().as_millis();
        let profile = profile_enabled.then(|| {
            profile_from_parts(
                elapsed_ms,
                first_flight_data_message_ms,
                first_batch_receive_decode_ms,
                receive_decode_ms,
                0,
                0,
                std::slice::from_ref(&writer_profile),
            )
        });
        let part_profiles = profile_enabled.then(|| {
            vec![PartProfileSummary {
                key: path.to_string(),
                part_index: 0,
                rows: rows_written,
                batches: batches_written,
                flight_stream_bytes,
                parquet_object_bytes: parquet_object_bytes.unwrap_or_default(),
                profile: writer_profile.clone(),
            }]
        });
        let summary = PutSummary {
            key,
            worker: worker_summary,
            mode: "single".to_owned(),
            rows: rows_written,
            batches: batches_written,
            parts: 1,
            put_parallelism: 1,
            client_input_file_bytes,
            flight_data_messages: profile_enabled.then_some(flight_data_messages),
            flight_stream_bytes,
            parquet_object_bytes,
            files,
            arrow_schema,
            target_file_size: None,
            elapsed_ms,
            compression: self.config.parquet.compression_name.clone(),
            multipart_part_size: self.config.parquet.multipart_part_size,
            multipart_max_concurrency: self.config.parquet.multipart_max_concurrency,
            profile,
            part_profiles,
        };

        info!(
            key = %summary.key,
            worker_id = %summary.worker.worker_id,
            attempt_id = %summary.worker.attempt_id,
            upload_id = ?summary.worker.upload_id,
            stream_id = ?summary.worker.stream_id,
            admission_wait_ms = summary.worker.admission_wait_ms,
            active_put_streams_at_admit = summary.worker.active_put_streams_at_admit,
            mode = %summary.mode,
            rows = summary.rows,
            batches = summary.batches,
            parquet_object_bytes = ?summary.parquet_object_bytes,
            elapsed_ms = summary.elapsed_ms,
            receive_decode_ms = ?summary.profile.as_ref().map(|profile| profile.receive_decode_ms),
            writer_write_ms = ?summary.profile.as_ref().map(|profile| profile.writer_task_write_ms_sum),
            writer_flush_ms = ?summary.profile.as_ref().map(|profile| profile.writer_task_flush_ms_sum),
            writer_close_ms = ?summary.profile.as_ref().map(|profile| profile.writer_task_close_ms_sum),
            "DoPut persisted parquet object"
        );

        Ok(summary)
    }

    async fn write_sized_dataset<S>(
        &self,
        key: String,
        target_file_size: usize,
        client_input_file_bytes: Option<u64>,
        first_batch: RecordBatch,
        first_batch_flight_bytes: u64,
        batches: &mut S,
        flight_stream_bytes: &AtomicU64,
        flight_data_messages: &AtomicU64,
        request_started: Instant,
        first_flight_data_message_ms: u128,
        first_batch_receive_decode_ms: u128,
        mut receive_decode_ms: u128,
        arrow_schema: Value,
        profile_enabled: bool,
        put_context: PutContext,
        worker_summary: WorkerPutSummary,
    ) -> Result<PutSummary, Status>
    where
        S: Stream<Item = Result<RecordBatch, FlightError>> + Unpin,
    {
        let max_part_writers = self.config.parquet.put_parallelism.max(1);
        let mut active_writers = VecDeque::new();
        let mut parts = Vec::new();
        let mut current_sender = None;
        let mut current_part_flight_bytes = 0u64;
        let mut next_part = 0usize;
        let mut last_seen_flight_bytes = first_batch_flight_bytes;
        let mut enqueue_wait_ms = 0u128;
        let mut collect_writer_wait_ms = 0u128;

        let ensure_started = profile_enabled.then(Instant::now);
        ensure_part_writer(
            &mut current_sender,
            &mut active_writers,
            &mut parts,
            max_part_writers,
            self.store.clone(),
            self.config.parquet.clone(),
            &key,
            &mut next_part,
            profile_enabled,
        )
        .await?;
        if let Some(started) = ensure_started {
            collect_writer_wait_ms += started.elapsed().as_millis();
        }

        let enqueue_started = profile_enabled.then(Instant::now);
        send_part_batch(
            current_sender.as_ref(),
            PartBatch {
                batch: first_batch,
                flight_stream_bytes: first_batch_flight_bytes,
            },
        )
        .await?;
        if let Some(started) = enqueue_started {
            enqueue_wait_ms += started.elapsed().as_millis();
        }
        current_part_flight_bytes += first_batch_flight_bytes;

        if current_part_flight_bytes >= target_file_size as u64 {
            current_sender.take();
            current_part_flight_bytes = 0;
        }

        loop {
            let receive_started = profile_enabled.then(Instant::now);
            let next_batch = batches.try_next().await.map_err(status_from_flight_error)?;
            if let Some(started) = receive_started {
                receive_decode_ms += started.elapsed().as_millis();
            }
            let Some(batch) = next_batch else {
                break;
            };
            enforce_stream_budget(&put_context, flight_stream_bytes.load(Ordering::Relaxed))?;
            enforce_record_batch_budget("DoPut", &batch, put_context.max_record_batch_bytes)?;

            let seen = flight_stream_bytes.load(Ordering::Relaxed);
            let batch_flight_bytes = seen.saturating_sub(last_seen_flight_bytes);
            last_seen_flight_bytes = seen;

            let ensure_started = profile_enabled.then(Instant::now);
            ensure_part_writer(
                &mut current_sender,
                &mut active_writers,
                &mut parts,
                max_part_writers,
                self.store.clone(),
                self.config.parquet.clone(),
                &key,
                &mut next_part,
                profile_enabled,
            )
            .await?;
            if let Some(started) = ensure_started {
                collect_writer_wait_ms += started.elapsed().as_millis();
            }

            let enqueue_started = profile_enabled.then(Instant::now);
            send_part_batch(
                current_sender.as_ref(),
                PartBatch {
                    batch,
                    flight_stream_bytes: batch_flight_bytes,
                },
            )
            .await?;
            if let Some(started) = enqueue_started {
                enqueue_wait_ms += started.elapsed().as_millis();
            }
            current_part_flight_bytes += batch_flight_bytes;

            if current_part_flight_bytes >= target_file_size as u64 {
                current_sender.take();
                current_part_flight_bytes = 0;
            }
        }

        drop(current_sender);

        while !active_writers.is_empty() {
            let collect_started = profile_enabled.then(Instant::now);
            collect_next_part(&mut active_writers, &mut parts).await?;
            if let Some(started) = collect_started {
                collect_writer_wait_ms += started.elapsed().as_millis();
            }
        }

        if parts.is_empty() {
            return Err(Status::invalid_argument(
                "DoPut did not contain record batches",
            ));
        }

        parts.sort_by_key(|part| part.part_index);
        let rows_written = parts.iter().map(|part| part.rows).sum();
        let batches_written = parts.iter().map(|part| part.batches).sum();
        let parquet_object_bytes = parts.iter().map(|part| part.parquet_object_bytes).sum();
        let total_flight_stream_bytes = flight_stream_bytes.load(Ordering::Relaxed);
        let flight_data_messages = flight_data_messages.load(Ordering::Relaxed);
        let part_profiles = profile_enabled.then(|| part_profile_summaries(&parts));
        let files = put_file_summaries_from_parts(&parts);
        let elapsed_ms = request_started.elapsed().as_millis();
        let profile = profile_enabled.then(|| {
            profile_from_dataset_parts(
                elapsed_ms,
                first_flight_data_message_ms,
                first_batch_receive_decode_ms,
                receive_decode_ms,
                enqueue_wait_ms,
                collect_writer_wait_ms,
                &parts,
            )
        });

        let summary = PutSummary {
            key,
            worker: worker_summary,
            mode: "sized_dataset".to_owned(),
            rows: rows_written,
            batches: batches_written,
            parts: parts.len(),
            put_parallelism: max_part_writers,
            client_input_file_bytes,
            flight_data_messages: profile_enabled.then_some(flight_data_messages),
            flight_stream_bytes: total_flight_stream_bytes,
            parquet_object_bytes: Some(parquet_object_bytes),
            files,
            arrow_schema,
            target_file_size: Some(target_file_size),
            elapsed_ms,
            compression: self.config.parquet.compression_name.clone(),
            multipart_part_size: self.config.parquet.multipart_part_size,
            multipart_max_concurrency: self.config.parquet.multipart_max_concurrency,
            profile,
            part_profiles,
        };

        info!(
            key = %summary.key,
            worker_id = %summary.worker.worker_id,
            attempt_id = %summary.worker.attempt_id,
            upload_id = ?summary.worker.upload_id,
            stream_id = ?summary.worker.stream_id,
            admission_wait_ms = summary.worker.admission_wait_ms,
            active_put_streams_at_admit = summary.worker.active_put_streams_at_admit,
            mode = %summary.mode,
            rows = summary.rows,
            batches = summary.batches,
            parts = summary.parts,
            target_file_size = target_file_size,
            parquet_object_bytes = ?summary.parquet_object_bytes,
            elapsed_ms = summary.elapsed_ms,
            receive_decode_ms = ?summary.profile.as_ref().map(|profile| profile.receive_decode_ms),
            enqueue_wait_ms = ?summary.profile.as_ref().map(|profile| profile.enqueue_wait_ms),
            collect_writer_wait_ms = ?summary.profile.as_ref().map(|profile| profile.collect_writer_wait_ms),
            writer_write_ms_sum = ?summary.profile.as_ref().map(|profile| profile.writer_task_write_ms_sum),
            writer_write_ms_max = ?summary.profile.as_ref().map(|profile| profile.writer_task_write_ms_max),
            writer_close_ms_max = ?summary.profile.as_ref().map(|profile| profile.writer_task_close_ms_max),
            "DoPut persisted size-split parquet dataset"
        );

        Ok(summary)
    }
}

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    type HandshakeStream = ResponseStream<HandshakeResponse>;
    type ListFlightsStream = ResponseStream<FlightInfo>;
    type DoGetStream = ResponseStream<FlightData>;
    type DoPutStream = ResponseStream<PutResult>;
    type DoExchangeStream = ResponseStream<FlightData>;
    type DoActionStream = ResponseStream<arrow_flight::Result>;
    type ListActionsStream = ResponseStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "GetFlightInfo is owned by the Java coordinator; worker only accepts direct DoPut/DoGet tickets",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo is not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("GetSchema is not implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let started = Instant::now();
        self.metrics.record_get_started();

        let result = async {
            let ticket = parse_read_ticket(
                &request.into_inner().ticket,
                &self.config.worker,
                &self.config.security,
            )?;
            let key = ticket.key;

            let path = path_from_key(&key);
            let read_admission = self.admit_read().await?;
            let meta = self
                .store
                .head(&path)
                .await
                .map_err(|err| Status::not_found(err.to_string()))?;

            let reader =
                ParquetObjectReader::new(self.store.clone(), path).with_file_size(meta.size);
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(status_from_anyhow)?;
            let schema = builder.schema().clone();
            let read_batch_size = self
                .config
                .read_batch_size
                .min(self.config.resources.read_max_batch_rows)
                .min(ticket.max_batch_rows.unwrap_or(usize::MAX))
                .max(1);
            let max_record_batch_bytes = min_optional_u64(
                Some(self.config.resources.read_max_record_batch_bytes),
                ticket.max_record_batch_bytes,
            )
            .unwrap_or(self.config.resources.read_max_record_batch_bytes)
            .max(1);
            let parquet_stream = builder
                .with_batch_size(read_batch_size)
                .build()
                .map_err(status_from_anyhow)?
                .map(move |result| match result {
                    Ok(batch) => {
                        enforce_record_batch_budget("DoGet", &batch, max_record_batch_bytes)
                            .map(|_| batch)
                            .map_err(status_into_flight_error)
                    }
                    Err(err) => Err(FlightError::ExternalError(Box::new(err))),
                });

            let flight_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .with_max_flight_data_size(self.config.flight_data_chunk_size)
                .build(parquet_stream)
                .map(|result| result.map_err(status_from_flight_error));
            let measured_stream =
                MeasuredReadStream::new(flight_stream, self.metrics.clone(), started, meta.size);

            info!(
                key = %key,
                operation_id = ?ticket.operation_id,
                bytes = meta.size,
                active_read_streams_at_admit = read_admission.active_read_streams_at_admit,
                "DoGet streaming parquet object"
            );
            Ok(Response::new(Box::pin(GuardedResponseStream {
                inner: Box::pin(measured_stream),
                _read_admission: read_admission,
            }) as Self::DoGetStream))
        }
        .await;

        if result.is_err() {
            self.metrics.record_get_failed(started.elapsed());
        }

        result
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let started = TokioInstant::now();
        self.metrics.record_put_started();
        match self.write_put_stream(request).await {
            Ok(summary) => {
                self.metrics
                    .record_put_succeeded(started.elapsed(), &summary);
                let metadata = serde_json::to_vec(&summary)
                    .map_err(|err| Status::internal(err.to_string()))?;
                let result = PutResult {
                    app_metadata: Bytes::from(metadata),
                };

                Ok(Response::new(Box::pin(stream::once(
                    async move { Ok(result) },
                ))))
            }
            Err(status) => {
                self.metrics.record_put_failed(started.elapsed());
                error!(
                    elapsed_ms = started.elapsed().as_millis(),
                    error = %status,
                    "DoPut failed"
                );
                Err(status)
            }
        }
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange is not implemented"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "worker.status" | "worker.scheduler.v1" => {
                let body = serde_json::to_vec(&self.worker_status())
                    .map_err(|err| Status::internal(err.to_string()))?;
                Ok(Response::new(Box::pin(stream::once(async move {
                    Ok(arrow_flight::Result {
                        body: Bytes::from(body),
                    })
                }))))
            }
            other => Err(Status::unimplemented(format!(
                "unsupported worker action {other:?}"
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(stream::iter(vec![
            Ok(ActionType {
                r#type: "worker.status".to_owned(),
                description: "Return raw Parquet worker readiness and capacity snapshot as JSON"
                    .to_owned(),
            }),
            Ok(ActionType {
                r#type: "worker.scheduler.v1".to_owned(),
                description: "Return coordinator-facing scheduling, runtime, and capacity signals"
                    .to_owned(),
            }),
        ]))))
    }
}

fn parse_put_options(app_metadata: &Bytes) -> Result<PutOptions, Status> {
    if app_metadata.is_empty() {
        return Ok(PutOptions::default());
    }

    if let Some(value) = parse_capability_envelope(app_metadata) {
        return Ok(PutOptions {
            capability: Some(value),
            ..PutOptions::default()
        });
    }

    serde_json::from_slice(app_metadata).map_err(|err| {
        Status::invalid_argument(format!("invalid DoPut app_metadata options: {err}"))
    })
}

fn put_mode(context: &PutContext) -> &'static str {
    if context.target_file_size.is_some() {
        "sized_dataset"
    } else {
        "single"
    }
}

fn usize_to_i32(value: usize) -> i32 {
    value.min(i32::MAX as usize) as i32
}

fn usize_to_i64(value: usize) -> i64 {
    value.min(i64::MAX as usize) as i64
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn u128_to_i64(value: u128) -> i64 {
    value.min(i64::MAX as u128) as i64
}

fn u128_to_u64(value: u128) -> u64 {
    value.min(u64::MAX as u128) as u64
}

fn validate_worker_id(name: &str, value: Option<&str>) -> Result<Option<String>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{name} must not be empty"
        )));
    }
    if value.len() > 128 {
        return Err(Status::invalid_argument(format!(
            "{name} must be at most 128 bytes"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Status::invalid_argument(format!(
            "{name} may only contain ASCII letters, digits, '-', '_', '.', and ':'"
        )));
    }

    Ok(Some(value.to_owned()))
}

fn normalize_staging_prefix(raw: &str) -> Result<String, Status> {
    let prefix = raw
        .replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .collect::<Vec<_>>()
        .join("/");

    if prefix.is_empty() {
        return Err(Status::invalid_argument("staging_prefix must not be empty"));
    }

    Ok(format!("{prefix}/"))
}

fn min_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn min_optional_usize(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn reconcile_optional_value(
    name: &str,
    request_value: Option<String>,
    capability_value: Option<String>,
) -> Result<Option<String>, Status> {
    match (request_value, capability_value) {
        (Some(request_value), Some(capability_value)) if request_value != capability_value => {
            Err(Status::permission_denied(format!(
                "{name} {request_value:?} does not match signed {name} {capability_value:?}"
            )))
        }
        (Some(value), Some(_)) | (Some(value), None) | (None, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn enforce_stream_budget(context: &PutContext, observed_bytes: u64) -> Result<(), Status> {
    let Some(stream_budget_bytes) = context.stream_budget_bytes else {
        return Ok(());
    };

    if observed_bytes > stream_budget_bytes {
        return Err(Status::resource_exhausted(format!(
            "DoPut stream exceeded byte budget; observed={observed_bytes}, budget={stream_budget_bytes}"
        )));
    }

    Ok(())
}

fn enforce_record_batch_budget(
    operation: &str,
    batch: &RecordBatch,
    max_bytes: u64,
) -> Result<u64, Status> {
    let bytes = record_batch_memory_bytes(batch);
    if bytes > max_bytes {
        return Err(Status::resource_exhausted(format!(
            "{operation} record batch memory bytes {bytes} exceeds worker max {max_bytes}; reduce client batch size or increase worker flavor budget"
        )));
    }

    Ok(bytes)
}

fn record_batch_memory_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|array| array.get_array_memory_size() as u64)
        .sum()
}

fn arrow_schema_to_json(schema: &Schema) -> Value {
    json!({
        "format": "arrow_schema_v1",
        "fields": schema
            .fields()
            .iter()
            .map(|field| arrow_field_to_json(field.as_ref()))
            .collect::<Vec<_>>(),
        "metadata": schema.metadata(),
    })
}

fn arrow_field_to_json(field: &Field) -> Value {
    json!({
        "name": field.name(),
        "nullable": field.is_nullable(),
        "type": arrow_data_type_to_json(field.data_type()),
        "metadata": field.metadata(),
    })
}

fn arrow_data_type_to_json(data_type: &DataType) -> Value {
    match data_type {
        DataType::Null => json!({"kind": "Null"}),
        DataType::Boolean => json!({"kind": "Boolean"}),
        DataType::Int8 => json!({"kind": "Int8"}),
        DataType::Int16 => json!({"kind": "Int16"}),
        DataType::Int32 => json!({"kind": "Int32"}),
        DataType::Int64 => json!({"kind": "Int64"}),
        DataType::UInt8 => json!({"kind": "UInt8"}),
        DataType::UInt16 => json!({"kind": "UInt16"}),
        DataType::UInt32 => json!({"kind": "UInt32"}),
        DataType::UInt64 => json!({"kind": "UInt64"}),
        DataType::Float16 => json!({"kind": "Float16"}),
        DataType::Float32 => json!({"kind": "Float32"}),
        DataType::Float64 => json!({"kind": "Float64"}),
        DataType::Timestamp(unit, timezone) => json!({
            "kind": "Timestamp",
            "unit": time_unit_name(*unit),
            "timezone": timezone.as_ref().map(|value| value.to_string()),
        }),
        DataType::Date32 => json!({"kind": "Date32"}),
        DataType::Date64 => json!({"kind": "Date64"}),
        DataType::Time32(unit) => json!({"kind": "Time32", "unit": time_unit_name(*unit)}),
        DataType::Time64(unit) => json!({"kind": "Time64", "unit": time_unit_name(*unit)}),
        DataType::Duration(unit) => json!({"kind": "Duration", "unit": time_unit_name(*unit)}),
        DataType::Interval(unit) => json!({"kind": "Interval", "unit": format!("{unit:?}")}),
        DataType::Binary => json!({"kind": "Binary"}),
        DataType::FixedSizeBinary(size) => json!({"kind": "FixedSizeBinary", "byte_width": size}),
        DataType::LargeBinary => json!({"kind": "LargeBinary"}),
        DataType::BinaryView => json!({"kind": "BinaryView"}),
        DataType::Utf8 => json!({"kind": "Utf8"}),
        DataType::LargeUtf8 => json!({"kind": "LargeUtf8"}),
        DataType::Utf8View => json!({"kind": "Utf8View"}),
        DataType::List(field) => {
            json!({"kind": "List", "field": arrow_field_to_json(field.as_ref())})
        }
        DataType::ListView(field) => {
            json!({"kind": "ListView", "field": arrow_field_to_json(field.as_ref())})
        }
        DataType::FixedSizeList(field, size) => json!({
            "kind": "FixedSizeList",
            "field": arrow_field_to_json(field.as_ref()),
            "length": size,
        }),
        DataType::LargeList(field) => {
            json!({"kind": "LargeList", "field": arrow_field_to_json(field.as_ref())})
        }
        DataType::LargeListView(field) => {
            json!({"kind": "LargeListView", "field": arrow_field_to_json(field.as_ref())})
        }
        DataType::Struct(fields) => json!({
            "kind": "Struct",
            "fields": fields
                .iter()
                .map(|field| arrow_field_to_json(field.as_ref()))
                .collect::<Vec<_>>(),
        }),
        DataType::Union(_, _) => {
            json!({"kind": "Unsupported", "arrow_debug": format!("{data_type:?}")})
        }
        DataType::Dictionary(key_type, value_type) => json!({
            "kind": "Dictionary",
            "key": arrow_data_type_to_json(key_type),
            "value": arrow_data_type_to_json(value_type),
        }),
        DataType::Decimal32(precision, scale) => {
            json!({"kind": "Decimal32", "precision": precision, "scale": scale})
        }
        DataType::Decimal64(precision, scale) => {
            json!({"kind": "Decimal64", "precision": precision, "scale": scale})
        }
        DataType::Decimal128(precision, scale) => {
            json!({"kind": "Decimal128", "precision": precision, "scale": scale})
        }
        DataType::Decimal256(precision, scale) => {
            json!({"kind": "Decimal256", "precision": precision, "scale": scale})
        }
        DataType::Map(field, sorted) => json!({
            "kind": "Map",
            "field": arrow_field_to_json(field.as_ref()),
            "keys_sorted": sorted,
        }),
        DataType::RunEndEncoded(run_ends, values) => json!({
            "kind": "RunEndEncoded",
            "run_ends": arrow_field_to_json(run_ends.as_ref()),
            "values": arrow_field_to_json(values.as_ref()),
        }),
    }
}

fn time_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "second",
        TimeUnit::Millisecond => "millisecond",
        TimeUnit::Microsecond => "microsecond",
        TimeUnit::Nanosecond => "nanosecond",
    }
}

fn status_into_flight_error(status: Status) -> FlightError {
    FlightError::ExternalError(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        status.to_string(),
    )))
}

fn flight_data_size(data: &FlightData) -> u64 {
    let descriptor_bytes = data
        .flight_descriptor
        .as_ref()
        .map(|descriptor| {
            descriptor.cmd.len() + descriptor.path.iter().map(|part| part.len()).sum::<usize>()
        })
        .unwrap_or_default();

    (data.app_metadata.len() + data.data_body.len() + data.data_header.len() + descriptor_bytes)
        as u64
}

fn dataset_part_key(key: &str, part_index: usize) -> String {
    let stem = key.strip_suffix(".parquet").unwrap_or(key);
    format!("{stem}.parts/part-{part_index:05}.parquet")
}

fn part_profile_summaries(parts: &[DatasetPart]) -> Vec<PartProfileSummary> {
    parts
        .iter()
        .map(|part| PartProfileSummary {
            key: part.key.clone(),
            part_index: part.part_index,
            rows: part.rows,
            batches: part.batches,
            flight_stream_bytes: part.flight_stream_bytes,
            parquet_object_bytes: part.parquet_object_bytes,
            profile: part.profile.clone(),
        })
        .collect()
}

fn put_file_summaries_from_parts(parts: &[DatasetPart]) -> Vec<PutFileSummary> {
    parts
        .iter()
        .map(|part| PutFileSummary {
            key: part.key.clone(),
            part_index: part.part_index,
            rows: part.rows,
            batches: part.batches,
            flight_stream_bytes: part.flight_stream_bytes,
            parquet_object_bytes: part.parquet_object_bytes,
        })
        .collect()
}

fn put_file_records(summary: &PutSummary) -> Vec<PutFileRecord> {
    summary
        .files
        .iter()
        .map(|file| PutFileRecord {
            attempt_id: summary.worker.attempt_id.clone(),
            upload_id: summary.worker.upload_id.clone(),
            stream_id: summary.worker.stream_id.clone(),
            worker_id: summary.worker.worker_id.clone(),
            logical_key: summary.key.clone(),
            part_index: usize_to_i32(file.part_index),
            file_path: file.key.clone(),
            rows: usize_to_i64(file.rows),
            batches: usize_to_i64(file.batches),
            flight_stream_bytes: u64_to_i64(file.flight_stream_bytes),
            parquet_object_bytes: u64_to_i64(file.parquet_object_bytes),
        })
        .collect()
}

fn profile_from_dataset_parts(
    total_server_ms: u128,
    first_flight_data_message_ms: u128,
    first_batch_receive_decode_ms: u128,
    receive_decode_ms: u128,
    enqueue_wait_ms: u128,
    collect_writer_wait_ms: u128,
    parts: &[DatasetPart],
) -> PutProfile {
    let profiles = parts
        .iter()
        .map(|part| part.profile.clone())
        .collect::<Vec<_>>();
    profile_from_parts(
        total_server_ms,
        first_flight_data_message_ms,
        first_batch_receive_decode_ms,
        receive_decode_ms,
        enqueue_wait_ms,
        collect_writer_wait_ms,
        &profiles,
    )
}

fn profile_from_parts(
    total_server_ms: u128,
    first_flight_data_message_ms: u128,
    first_batch_receive_decode_ms: u128,
    receive_decode_ms: u128,
    enqueue_wait_ms: u128,
    collect_writer_wait_ms: u128,
    parts: &[PartProfile],
) -> PutProfile {
    PutProfile {
        total_server_ms,
        first_flight_data_message_ms,
        first_batch_receive_decode_ms,
        receive_decode_ms,
        enqueue_wait_ms,
        collect_writer_wait_ms,
        object_head_ms: parts.iter().map(|part| part.head_ms).sum(),
        writer_task_elapsed_ms_sum: parts.iter().map(|part| part.elapsed_ms).sum(),
        writer_task_elapsed_ms_max: parts
            .iter()
            .map(|part| part.elapsed_ms)
            .max()
            .unwrap_or_default(),
        writer_task_idle_wait_ms_sum: parts.iter().map(|part| part.idle_wait_ms).sum(),
        writer_task_write_ms_sum: parts.iter().map(|part| part.write_ms).sum(),
        writer_task_write_ms_max: parts
            .iter()
            .map(|part| part.write_ms)
            .max()
            .unwrap_or_default(),
        writer_task_flush_ms_sum: parts.iter().map(|part| part.flush_ms).sum(),
        writer_task_close_ms_sum: parts.iter().map(|part| part.close_ms).sum(),
        writer_task_close_ms_max: parts
            .iter()
            .map(|part| part.close_ms)
            .max()
            .unwrap_or_default(),
        writer_task_head_ms_sum: parts.iter().map(|part| part.head_ms).sum(),
    }
}

fn parquet_object_writer(
    store: Arc<dyn ObjectStore>,
    path: Path,
    tuning: &ParquetTuning,
) -> ParquetObjectWriter {
    let writer = BufWriter::with_capacity(store, path, tuning.multipart_part_size)
        .with_max_concurrency(tuning.multipart_max_concurrency);
    ParquetObjectWriter::from_buf_writer(writer)
}

async fn write_dataset_part(
    store: Arc<dyn ObjectStore>,
    tuning: ParquetTuning,
    key: String,
    part_index: usize,
    mut receiver: mpsc::Receiver<PartBatch>,
    profile_enabled: bool,
) -> Result<Option<DatasetPart>, String> {
    let started = profile_enabled.then(Instant::now);
    let mut profile = PartProfile::default();
    let wait_started = profile_enabled.then(Instant::now);
    let Some(first) = receiver.recv().await else {
        return Ok(None);
    };
    if let Some(started) = wait_started {
        profile.idle_wait_ms += started.elapsed().as_millis();
    }

    let path = path_from_key(&key);
    let props = writer_properties(&tuning);
    let object_writer = parquet_object_writer(store.clone(), path.clone(), &tuning);
    let mut writer = AsyncArrowWriter::try_new(object_writer, first.batch.schema(), Some(props))
        .map_err(|err| err.to_string())?;

    let mut batches_written = 0usize;
    let mut rows_written = 0usize;
    let mut flight_stream_bytes = 0u64;

    write_batch(
        &mut writer,
        &first.batch,
        &tuning,
        &mut batches_written,
        &mut rows_written,
        &mut profile,
        profile_enabled,
    )
    .await
    .map_err(|err| err.to_string())?;
    flight_stream_bytes += first.flight_stream_bytes;

    loop {
        let wait_started = profile_enabled.then(Instant::now);
        let Some(part_batch) = receiver.recv().await else {
            if let Some(started) = wait_started {
                profile.idle_wait_ms += started.elapsed().as_millis();
            }
            break;
        };
        if let Some(started) = wait_started {
            profile.idle_wait_ms += started.elapsed().as_millis();
        }

        write_batch(
            &mut writer,
            &part_batch.batch,
            &tuning,
            &mut batches_written,
            &mut rows_written,
            &mut profile,
            profile_enabled,
        )
        .await
        .map_err(|err| err.to_string())?;
        flight_stream_bytes += part_batch.flight_stream_bytes;
    }

    let close_started = profile_enabled.then(Instant::now);
    writer.close().await.map_err(|err| err.to_string())?;
    if let Some(started) = close_started {
        profile.close_ms += started.elapsed().as_millis();
    }
    let head_started = profile_enabled.then(Instant::now);
    let object_meta = store.head(&path).await.map_err(|err| err.to_string())?;
    if let Some(started) = head_started {
        profile.head_ms += started.elapsed().as_millis();
    }
    if let Some(started) = started {
        profile.elapsed_ms = started.elapsed().as_millis();
    }

    Ok(Some(DatasetPart {
        key,
        part_index,
        rows: rows_written,
        batches: batches_written,
        flight_stream_bytes,
        parquet_object_bytes: object_meta.size,
        profile,
    }))
}

fn spawn_dataset_part_writer(
    store: Arc<dyn ObjectStore>,
    tuning: ParquetTuning,
    key: &str,
    part_index: usize,
    profile_enabled: bool,
) -> (
    mpsc::Sender<PartBatch>,
    JoinHandle<Result<Option<DatasetPart>, String>>,
) {
    let (sender, receiver) = mpsc::channel(tuning.put_queue_depth);
    let part_key = dataset_part_key(key, part_index);
    let handle = tokio::spawn(async move {
        write_dataset_part(
            store,
            tuning,
            part_key,
            part_index,
            receiver,
            profile_enabled,
        )
        .await
    });

    (sender, handle)
}

async fn ensure_part_writer(
    sender: &mut Option<mpsc::Sender<PartBatch>>,
    active_writers: &mut VecDeque<JoinHandle<Result<Option<DatasetPart>, String>>>,
    parts: &mut Vec<DatasetPart>,
    max_part_writers: usize,
    store: Arc<dyn ObjectStore>,
    tuning: ParquetTuning,
    key: &str,
    next_part: &mut usize,
    profile_enabled: bool,
) -> Result<(), Status> {
    if sender.is_some() {
        return Ok(());
    }

    while active_writers.len() >= max_part_writers {
        collect_next_part(active_writers, parts).await?;
    }

    let (next_sender, handle) =
        spawn_dataset_part_writer(store, tuning, key, *next_part, profile_enabled);
    *next_part += 1;
    active_writers.push_back(handle);
    *sender = Some(next_sender);
    Ok(())
}

async fn send_part_batch(
    sender: Option<&mpsc::Sender<PartBatch>>,
    part_batch: PartBatch,
) -> Result<(), Status> {
    let sender = sender.ok_or_else(|| Status::internal("dataset writer was not initialized"))?;
    sender
        .send(part_batch)
        .await
        .map_err(|_| Status::internal("dataset writer task stopped during DoPut"))
}

async fn read_first_batch<S>(
    batches: &mut S,
    timeout_ms: u64,
) -> Result<Option<RecordBatch>, Status>
where
    S: Stream<Item = Result<RecordBatch, FlightError>> + Unpin,
{
    if timeout_ms == 0 {
        return batches.try_next().await.map_err(status_from_flight_error);
    }

    timeout(Duration::from_millis(timeout_ms), batches.try_next())
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "timed out waiting {timeout_ms}ms for first DoPut record batch after admission"
            ))
        })?
        .map_err(status_from_flight_error)
}

async fn collect_next_part(
    active_writers: &mut VecDeque<JoinHandle<Result<Option<DatasetPart>, String>>>,
    parts: &mut Vec<DatasetPart>,
) -> Result<(), Status> {
    let handle = active_writers
        .pop_front()
        .ok_or_else(|| Status::internal("no active dataset writer to collect"))?;

    match handle.await {
        Ok(Ok(Some(part))) => {
            parts.push(part);
            Ok(())
        }
        Ok(Ok(None)) => Ok(()),
        Ok(Err(error)) => Err(Status::internal(error)),
        Err(error) => Err(Status::internal(error.to_string())),
    }
}

fn writer_properties(tuning: &ParquetTuning) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(tuning.compression)
        .set_dictionary_enabled(tuning.dictionary_enabled)
        .set_max_row_group_row_count(Some(tuning.max_row_group_rows))
        .set_write_batch_size(tuning.write_batch_size)
        .set_data_page_size_limit(tuning.data_page_size_limit)
        .build()
}

async fn write_batch<W>(
    writer: &mut AsyncArrowWriter<W>,
    batch: &RecordBatch,
    tuning: &ParquetTuning,
    batches_written: &mut usize,
    rows_written: &mut usize,
    profile: &mut PartProfile,
    profile_enabled: bool,
) -> Result<(), Status>
where
    W: parquet::arrow::async_writer::AsyncFileWriter + Unpin + Send,
{
    let write_started = profile_enabled.then(Instant::now);
    writer.write(batch).await.map_err(status_from_anyhow)?;
    if let Some(started) = write_started {
        profile.write_ms += started.elapsed().as_millis();
    }
    *batches_written += 1;
    *rows_written += batch.num_rows();

    if writer.in_progress_size() >= tuning.flush_threshold_bytes {
        let flush_started = profile_enabled.then(Instant::now);
        writer.flush().await.map_err(status_from_anyhow)?;
        if let Some(started) = flush_started {
            profile.flush_ms += started.elapsed().as_millis();
        }
    }

    Ok(())
}

fn status_from_flight_error(error: FlightError) -> Status {
    Status::from(error)
}

fn status_from_anyhow(error: impl std::error::Error) -> Status {
    Status::internal(error.to_string())
}

pub fn status_from_context(error: anyhow::Error) -> Status {
    Status::internal(format!("{error:#}"))
}

pub fn map_context<T>(result: anyhow::Result<T>) -> Result<T, Status> {
    result.map_err(status_from_context)
}
