use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use arrow_array::RecordBatch;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Location, PollInfo, PutResult, SchemaResult, Ticket,
    error::FlightError, flight_service_server::FlightService,
};
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
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::{sync::mpsc, time::Instant as TokioInstant};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::{
    config::{AppConfig, ParquetTuning},
    util::{batch_memory_size, descriptor_to_object_key, normalize_object_key, path_from_key},
};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static>>;

const DATASET_MANIFEST_FORMAT: &str = "arrow-flight-s3-mvp.dataset.v1";

#[derive(Clone)]
pub struct S3FlightService {
    config: Arc<AppConfig>,
    store: Arc<dyn ObjectStore>,
}

#[derive(Debug, Serialize)]
struct PutSummary {
    key: String,
    mode: String,
    rows: usize,
    batches: usize,
    parts: usize,
    put_parallelism: usize,
    flight_stream_bytes: u64,
    arrow_memory_bytes_estimate: u64,
    parquet_object_bytes: Option<u64>,
    manifest_key: Option<String>,
    manifest_object_bytes: Option<u64>,
    target_file_size: Option<usize>,
    elapsed_ms: u128,
    compression: String,
    multipart_part_size: usize,
    multipart_max_concurrency: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatasetPart {
    key: String,
    #[serde(alias = "worker")]
    part_index: usize,
    rows: usize,
    batches: usize,
    #[serde(default)]
    flight_stream_bytes: u64,
    #[serde(alias = "arrow_memory_bytes")]
    arrow_memory_bytes_estimate: u64,
    parquet_object_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatasetManifest {
    format: String,
    logical_key: String,
    ordered: bool,
    compression: String,
    #[serde(default)]
    target_file_size: Option<usize>,
    rows: usize,
    batches: usize,
    #[serde(default)]
    flight_stream_bytes: u64,
    #[serde(alias = "arrow_memory_bytes")]
    arrow_memory_bytes_estimate: u64,
    parquet_object_bytes: u64,
    parts: Vec<DatasetPart>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct PutOptions {
    target_file_size: Option<usize>,
    input_file_bytes: Option<u64>,
}

struct PartBatch {
    batch: RecordBatch,
    flight_stream_bytes: u64,
}

impl S3FlightService {
    pub fn new(config: AppConfig, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config: Arc::new(config),
            store,
        }
    }

    async fn write_put_stream(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<PutSummary, Status> {
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("DoPut stream was empty"))?;

        let key = descriptor_to_object_key(first.flight_descriptor.as_ref());
        let put_options = parse_put_options(&first.app_metadata)?;
        let path = path_from_key(&key);
        let flight_stream_bytes = Arc::new(AtomicU64::new(0));

        let first_stream = stream::once(async move { Ok(first) });
        let stream_bytes = flight_stream_bytes.clone();
        let flight_stream = first_stream
            .chain(incoming.map(|item| item.map_err(FlightError::from)))
            .map(move |item| {
                item.map(|data| {
                    stream_bytes.fetch_add(flight_data_size(&data), Ordering::Relaxed);
                    data
                })
            });
        let mut batches =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(flight_stream);

        let first_batch = batches
            .try_next()
            .await
            .map_err(status_from_flight_error)?
            .ok_or_else(|| {
                Status::invalid_argument("DoPut stream did not contain record batches")
            })?;
        let first_batch_flight_bytes = flight_stream_bytes.load(Ordering::Relaxed);

        if let Some(target_file_size) = put_options.target_file_size {
            return self
                .write_sized_dataset(
                    key,
                    target_file_size,
                    first_batch,
                    first_batch_flight_bytes,
                    &mut batches,
                    &flight_stream_bytes,
                )
                .await;
        }

        self.write_single_file(key, path, first_batch, &mut batches, &flight_stream_bytes)
            .await
    }

    async fn write_single_file<S>(
        &self,
        key: String,
        path: Path,
        first_batch: RecordBatch,
        batches: &mut S,
        flight_stream_bytes: &AtomicU64,
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

        let start = Instant::now();
        let mut batches_written = 0usize;
        let mut rows_written = 0usize;
        let mut arrow_memory_bytes_estimate = 0u64;

        write_batch(
            &mut writer,
            &first_batch,
            &self.config.parquet,
            &mut batches_written,
            &mut rows_written,
            &mut arrow_memory_bytes_estimate,
        )
        .await?;

        while let Some(batch) = batches.try_next().await.map_err(status_from_flight_error)? {
            write_batch(
                &mut writer,
                &batch,
                &self.config.parquet,
                &mut batches_written,
                &mut rows_written,
                &mut arrow_memory_bytes_estimate,
            )
            .await?;
        }

        writer.close().await.map_err(status_from_anyhow)?;
        let object_meta = self.store.head(&path).await.ok();
        let flight_stream_bytes = flight_stream_bytes.load(Ordering::Relaxed);
        let summary = PutSummary {
            key,
            mode: "single".to_owned(),
            rows: rows_written,
            batches: batches_written,
            parts: 1,
            put_parallelism: 1,
            flight_stream_bytes,
            arrow_memory_bytes_estimate,
            parquet_object_bytes: object_meta.map(|meta| meta.size),
            manifest_key: None,
            manifest_object_bytes: None,
            target_file_size: None,
            elapsed_ms: start.elapsed().as_millis(),
            compression: self.config.parquet.compression_name.clone(),
            multipart_part_size: self.config.parquet.multipart_part_size,
            multipart_max_concurrency: self.config.parquet.multipart_max_concurrency,
        };

        info!(
            key = %summary.key,
            mode = %summary.mode,
            rows = summary.rows,
            batches = summary.batches,
            parquet_object_bytes = ?summary.parquet_object_bytes,
            elapsed_ms = summary.elapsed_ms,
            "DoPut persisted parquet object"
        );

        Ok(summary)
    }

    async fn write_sized_dataset<S>(
        &self,
        key: String,
        target_file_size: usize,
        first_batch: RecordBatch,
        first_batch_flight_bytes: u64,
        batches: &mut S,
        flight_stream_bytes: &AtomicU64,
    ) -> Result<PutSummary, Status>
    where
        S: Stream<Item = Result<RecordBatch, FlightError>> + Unpin,
    {
        let start = Instant::now();
        let max_part_writers = self.config.parquet.put_parallelism.max(1);
        let mut active_writers = VecDeque::new();
        let mut parts = Vec::new();
        let mut current_sender = None;
        let mut current_part_flight_bytes = 0u64;
        let mut next_part = 0usize;
        let mut last_seen_flight_bytes = first_batch_flight_bytes;

        ensure_part_writer(
            &mut current_sender,
            &mut active_writers,
            &mut parts,
            max_part_writers,
            self.store.clone(),
            self.config.parquet.clone(),
            &key,
            &mut next_part,
        )
        .await?;

        send_part_batch(
            current_sender.as_ref(),
            PartBatch {
                batch: first_batch,
                flight_stream_bytes: first_batch_flight_bytes,
            },
        )
        .await?;
        current_part_flight_bytes += first_batch_flight_bytes;

        if current_part_flight_bytes >= target_file_size as u64 {
            current_sender.take();
            current_part_flight_bytes = 0;
        }

        while let Some(batch) = batches.try_next().await.map_err(status_from_flight_error)? {
            let seen = flight_stream_bytes.load(Ordering::Relaxed);
            let batch_flight_bytes = seen.saturating_sub(last_seen_flight_bytes);
            last_seen_flight_bytes = seen;

            ensure_part_writer(
                &mut current_sender,
                &mut active_writers,
                &mut parts,
                max_part_writers,
                self.store.clone(),
                self.config.parquet.clone(),
                &key,
                &mut next_part,
            )
            .await?;

            send_part_batch(
                current_sender.as_ref(),
                PartBatch {
                    batch,
                    flight_stream_bytes: batch_flight_bytes,
                },
            )
            .await?;
            current_part_flight_bytes += batch_flight_bytes;

            if current_part_flight_bytes >= target_file_size as u64 {
                current_sender.take();
                current_part_flight_bytes = 0;
            }
        }

        drop(current_sender);

        while !active_writers.is_empty() {
            collect_next_part(&mut active_writers, &mut parts).await?;
        }

        if parts.is_empty() {
            return Err(Status::invalid_argument(
                "DoPut did not contain record batches",
            ));
        }

        parts.sort_by_key(|part| part.part_index);
        let rows_written = parts.iter().map(|part| part.rows).sum();
        let batches_written = parts.iter().map(|part| part.batches).sum();
        let assigned_flight_stream_bytes = parts.iter().map(|part| part.flight_stream_bytes).sum();
        let arrow_memory_bytes_estimate = parts
            .iter()
            .map(|part| part.arrow_memory_bytes_estimate)
            .sum();
        let parquet_object_bytes = parts.iter().map(|part| part.parquet_object_bytes).sum();
        let total_flight_stream_bytes = flight_stream_bytes.load(Ordering::Relaxed);
        let manifest_key = dataset_manifest_key(&key);
        let manifest = DatasetManifest {
            format: DATASET_MANIFEST_FORMAT.to_owned(),
            logical_key: key.clone(),
            ordered: true,
            compression: self.config.parquet.compression_name.clone(),
            target_file_size: Some(target_file_size),
            rows: rows_written,
            batches: batches_written,
            flight_stream_bytes: assigned_flight_stream_bytes,
            arrow_memory_bytes_estimate,
            parquet_object_bytes,
            parts,
        };

        let manifest_payload = Bytes::from(
            serde_json::to_vec(&manifest).map_err(|err| Status::internal(err.to_string()))?,
        );
        let manifest_path = Path::from(manifest_key.clone());
        self.store
            .put(&manifest_path, manifest_payload.into())
            .await
            .map_err(status_from_anyhow)?;
        let manifest_meta = self.store.head(&manifest_path).await.ok();

        let summary = PutSummary {
            key,
            mode: "sized_dataset".to_owned(),
            rows: rows_written,
            batches: batches_written,
            parts: manifest.parts.len(),
            put_parallelism: max_part_writers,
            flight_stream_bytes: total_flight_stream_bytes,
            arrow_memory_bytes_estimate,
            parquet_object_bytes: Some(parquet_object_bytes),
            manifest_key: Some(manifest_key),
            manifest_object_bytes: manifest_meta.map(|meta| meta.size),
            target_file_size: Some(target_file_size),
            elapsed_ms: start.elapsed().as_millis(),
            compression: self.config.parquet.compression_name.clone(),
            multipart_part_size: self.config.parquet.multipart_part_size,
            multipart_max_concurrency: self.config.parquet.multipart_max_concurrency,
        };

        info!(
            key = %summary.key,
            mode = %summary.mode,
            rows = summary.rows,
            batches = summary.batches,
            parts = summary.parts,
            target_file_size = target_file_size,
            parquet_object_bytes = ?summary.parquet_object_bytes,
            elapsed_ms = summary.elapsed_ms,
            "DoPut persisted size-split parquet dataset"
        );

        Ok(summary)
    }

    async fn load_dataset_manifest(&self, key: &str) -> Result<Option<DatasetManifest>, Status> {
        let manifest_path = Path::from(dataset_manifest_key(key));
        let result = match self.store.get(&manifest_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(status_from_anyhow(error)),
        };

        let bytes = result.bytes().await.map_err(status_from_anyhow)?;
        let manifest: DatasetManifest =
            serde_json::from_slice(&bytes).map_err(|err| Status::internal(err.to_string()))?;

        if manifest.format != DATASET_MANIFEST_FORMAT {
            return Err(Status::internal(format!(
                "unsupported dataset manifest format: {}",
                manifest.format
            )));
        }

        Ok(Some(manifest))
    }
}

#[tonic::async_trait]
impl FlightService for S3FlightService {
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
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let key = descriptor_to_object_key(Some(&descriptor));
        let total_bytes = if let Some(manifest) = self.load_dataset_manifest(&key).await? {
            manifest.parquet_object_bytes as i64
        } else {
            let path = path_from_key(&key);
            let meta = self
                .store
                .head(&path)
                .await
                .map_err(|err| Status::not_found(err.to_string()))?;
            meta.size as i64
        };

        let ticket = Ticket {
            ticket: Bytes::from(key),
        };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![Location {
                uri: "grpc+tcp://0.0.0.0:50051".to_owned(),
            }],
            expiration_time: None,
            app_metadata: Bytes::new(),
        };

        Ok(Response::new(FlightInfo {
            schema: Bytes::new(),
            flight_descriptor: Some(descriptor),
            endpoint: vec![endpoint],
            total_records: -1,
            total_bytes,
            ordered: false,
            app_metadata: Bytes::new(),
        }))
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
        let key = normalize_object_key(
            std::str::from_utf8(&request.into_inner().ticket)
                .map_err(|err| Status::invalid_argument(err.to_string()))?,
        );

        if let Some(manifest) = self.load_dataset_manifest(&key).await? {
            let first_part = manifest
                .parts
                .first()
                .ok_or_else(|| Status::not_found("dataset manifest did not contain parts"))?;
            let first_reader =
                ParquetObjectReader::new(self.store.clone(), path_from_key(&first_part.key))
                    .with_file_size(first_part.parquet_object_bytes);
            let first_builder = ParquetRecordBatchStreamBuilder::new(first_reader)
                .await
                .map_err(status_from_anyhow)?;
            let schema = first_builder.schema().clone();
            let part_count = manifest.parts.len();
            let total_bytes = manifest.parquet_object_bytes;
            let parquet_stream = dataset_batch_stream(
                self.store.clone(),
                manifest.parts,
                self.config.read_batch_size,
            );

            let flight_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .with_max_flight_data_size(self.config.flight_data_chunk_size)
                .build(parquet_stream)
                .map(|result| result.map_err(status_from_flight_error));

            info!(
                key = %key,
                parts = part_count,
                bytes = total_bytes,
                "DoGet streaming parallel parquet dataset"
            );
            return Ok(Response::new(Box::pin(flight_stream)));
        }

        let path = path_from_key(&key);
        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|err| Status::not_found(err.to_string()))?;

        let reader = ParquetObjectReader::new(self.store.clone(), path).with_file_size(meta.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(status_from_anyhow)?;
        let schema = builder.schema().clone();
        let parquet_stream = builder
            .with_batch_size(self.config.read_batch_size)
            .build()
            .map_err(status_from_anyhow)?
            .map_err(|err| FlightError::ExternalError(Box::new(err)));

        let flight_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_max_flight_data_size(self.config.flight_data_chunk_size)
            .build(parquet_stream)
            .map(|result| result.map_err(status_from_flight_error));

        info!(key = %key, bytes = meta.size, "DoGet streaming parquet object");
        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let started = TokioInstant::now();
        match self.write_put_stream(request).await {
            Ok(summary) => {
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
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("DoAction is not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }
}

fn dataset_manifest_key(key: &str) -> String {
    format!("{key}.manifest.json")
}

fn parse_put_options(app_metadata: &Bytes) -> Result<PutOptions, Status> {
    if app_metadata.is_empty() {
        return Ok(PutOptions::default());
    }

    serde_json::from_slice(app_metadata).map_err(|err| {
        Status::invalid_argument(format!("invalid DoPut app_metadata options: {err}"))
    })
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
) -> Result<Option<DatasetPart>, String> {
    let Some(first) = receiver.recv().await else {
        return Ok(None);
    };

    let path = path_from_key(&key);
    let props = writer_properties(&tuning);
    let object_writer = parquet_object_writer(store.clone(), path.clone(), &tuning);
    let mut writer = AsyncArrowWriter::try_new(object_writer, first.batch.schema(), Some(props))
        .map_err(|err| err.to_string())?;

    let mut batches_written = 0usize;
    let mut rows_written = 0usize;
    let mut flight_stream_bytes = 0u64;
    let mut arrow_memory_bytes_estimate = 0u64;

    write_batch(
        &mut writer,
        &first.batch,
        &tuning,
        &mut batches_written,
        &mut rows_written,
        &mut arrow_memory_bytes_estimate,
    )
    .await
    .map_err(|err| err.to_string())?;
    flight_stream_bytes += first.flight_stream_bytes;

    while let Some(part_batch) = receiver.recv().await {
        write_batch(
            &mut writer,
            &part_batch.batch,
            &tuning,
            &mut batches_written,
            &mut rows_written,
            &mut arrow_memory_bytes_estimate,
        )
        .await
        .map_err(|err| err.to_string())?;
        flight_stream_bytes += part_batch.flight_stream_bytes;
    }

    writer.close().await.map_err(|err| err.to_string())?;
    let object_meta = store.head(&path).await.map_err(|err| err.to_string())?;

    Ok(Some(DatasetPart {
        key,
        part_index,
        rows: rows_written,
        batches: batches_written,
        flight_stream_bytes,
        arrow_memory_bytes_estimate,
        parquet_object_bytes: object_meta.size,
    }))
}

fn spawn_dataset_part_writer(
    store: Arc<dyn ObjectStore>,
    tuning: ParquetTuning,
    key: &str,
    part_index: usize,
) -> (
    mpsc::Sender<PartBatch>,
    JoinHandle<Result<Option<DatasetPart>, String>>,
) {
    let (sender, receiver) = mpsc::channel(tuning.put_queue_depth);
    let part_key = dataset_part_key(key, part_index);
    let handle = tokio::spawn(async move {
        write_dataset_part(store, tuning, part_key, part_index, receiver).await
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
) -> Result<(), Status> {
    if sender.is_some() {
        return Ok(());
    }

    while active_writers.len() >= max_part_writers {
        collect_next_part(active_writers, parts).await?;
    }

    let (next_sender, handle) = spawn_dataset_part_writer(store, tuning, key, *next_part);
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

struct DatasetReadState {
    store: Arc<dyn ObjectStore>,
    parts: Vec<DatasetPart>,
    next_part: usize,
    current: Option<BatchStream>,
    batch_size: usize,
}

fn dataset_batch_stream(
    store: Arc<dyn ObjectStore>,
    parts: Vec<DatasetPart>,
    batch_size: usize,
) -> BatchStream {
    Box::pin(stream::unfold(
        DatasetReadState {
            store,
            parts,
            next_part: 0,
            current: None,
            batch_size,
        },
        |mut state| async move {
            loop {
                if let Some(current) = state.current.as_mut() {
                    match current.next().await {
                        Some(Ok(batch)) => return Some((Ok(batch), state)),
                        Some(Err(error)) => return Some((Err(error), state)),
                        None => {
                            state.current = None;
                            continue;
                        }
                    }
                }

                let Some(part) = state.parts.get(state.next_part).cloned() else {
                    return None;
                };
                state.next_part += 1;

                let reader =
                    ParquetObjectReader::new(state.store.clone(), path_from_key(&part.key))
                        .with_file_size(part.parquet_object_bytes);
                let builder = match ParquetRecordBatchStreamBuilder::new(reader).await {
                    Ok(builder) => builder,
                    Err(error) => {
                        return Some((Err(FlightError::ExternalError(Box::new(error))), state));
                    }
                };
                let stream = match builder.with_batch_size(state.batch_size).build() {
                    Ok(stream) => stream.map_err(|err| FlightError::ExternalError(Box::new(err))),
                    Err(error) => {
                        return Some((Err(FlightError::ExternalError(Box::new(error))), state));
                    }
                };

                state.current = Some(Box::pin(stream));
            }
        },
    ))
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
    batch: &arrow_array::RecordBatch,
    tuning: &ParquetTuning,
    batches_written: &mut usize,
    rows_written: &mut usize,
    arrow_memory_bytes_estimate: &mut u64,
) -> Result<(), Status>
where
    W: parquet::arrow::async_writer::AsyncFileWriter + Unpin + Send,
{
    writer.write(batch).await.map_err(status_from_anyhow)?;
    *batches_written += 1;
    *rows_written += batch.num_rows();
    *arrow_memory_bytes_estimate += batch_memory_size(batch);

    if writer.in_progress_size() >= tuning.flush_threshold_bytes {
        writer.flush().await.map_err(status_from_anyhow)?;
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
