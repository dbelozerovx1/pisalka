use std::{
    fs::File,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::{
    Action, FlightClient, FlightDescriptor, encode::FlightDataEncoderBuilder, error::FlightError,
};
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use futures::{TryStreamExt, stream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use tokio::{sync::mpsc, task::JoinHandle};
use tonic::transport::Channel;

use arrow_flight_s3_mvp::util::{parse_size, pretty_bytes, throughput};

#[path = "common/client_config.rs"]
mod client_config;
#[path = "common/flight_uri.rs"]
mod flight_uri;

use client_config::{E2eClientConfig, env_size};
use flight_uri::tonic_uri;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,

    #[arg(long, env = "COORDINATOR_URI", default_value = "http://127.0.0.1:8088")]
    coordinator_uri: String,

    #[arg(long, env = "COORDINATOR_ADMIN_TOKEN")]
    coordinator_admin_token: Option<String>,

    #[arg(
        long,
        env = "COORDINATOR_CONNECT_TIMEOUT_SECONDS",
        default_value_t = 30
    )]
    coordinator_connect_timeout_seconds: u64,

    #[arg(long, env = "COORDINATOR_OPERATION_ID")]
    operation_id: Option<String>,

    #[arg(long, env = "COORDINATOR_UPLOAD_ID")]
    upload_id: Option<String>,

    #[arg(long, alias = "table", env = "COORDINATOR_TABLE_NAME")]
    table_name: Option<String>,

    #[arg(long, env = "COORDINATOR_SCHEMA")]
    schema: Option<String>,

    #[arg(
        long,
        env = "COORDINATOR_COMMIT_MODE",
        value_enum,
        default_value = "overwrite"
    )]
    commit_mode: CommitMode,

    #[arg(long, env = "TRINO_USER")]
    trino_user: Option<String>,

    #[arg(long, env = "TRINO_AUTHORIZATION")]
    trino_authorization: Option<String>,

    #[arg(long, env = "UPLOAD_FLAVOR", value_enum, default_value = "small")]
    flavor: UploadFlavor,

    #[arg(
        long = "file-size",
        alias = "target-file-size",
        env = "TARGET_FILE_SIZE"
    )]
    file_size: Option<String>,

    #[arg(long, env = "PUT_CLIENT_QUEUE_DEPTH", default_value_t = 2)]
    client_queue_depth: usize,

    #[arg(long, env = "PUT_MAX_STREAM_BYTES")]
    max_stream_bytes: Option<String>,

    #[arg(long, env = "PUT_MAX_RECORD_BATCH_BYTES")]
    max_record_batch_bytes: Option<String>,

    #[arg(long, env = "COORDINATOR_UPLOAD_TTL_MS")]
    upload_ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
enum CommitMode {
    Append,
    Overwrite,
}

#[derive(Debug, Clone, ValueEnum)]
enum UploadFlavor {
    Small,
    Medium,
    Large,
}

impl UploadFlavor {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

struct CoordinatorClient {
    admin_token: Option<String>,
    client: FlightClient,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateUploadResponse {
    upload_id: String,
    operation_id: String,
    #[serde(default)]
    table_name: Option<String>,
    status: String,
    requested_flavor: String,
    granted_streams: usize,
    expected_streams: usize,
    staging_prefix: String,
    target_file_size_bytes: u64,
    tickets: Vec<UploadTicket>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadTicket {
    worker_id: String,
    flight_uri: String,
    descriptor_path: String,
    attempt_id: String,
    upload_id: String,
    stream_id: String,
    app_metadata: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommitUploadResponse {
    upload_id: String,
    status: String,
    table_name: String,
    mode: String,
    snapshot_id: u64,
    #[serde(default)]
    record_count: u64,
    #[serde(default)]
    parquet_object_bytes: u64,
    #[serde(default)]
    commit_summary: Value,
    #[serde(default)]
    files: Vec<WrittenFile>,
    #[serde(default)]
    already_committed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct WrittenFile {
    stream_id: String,
    part_index: i32,
    file_path: String,
    rows: u64,
    batches: u64,
    parquet_object_bytes: u64,
}

#[derive(Debug)]
struct UploadRunSummary {
    elapsed_ms: u128,
    batches_sent: usize,
    stream_results: Vec<StreamResult>,
}

#[derive(Debug)]
struct StreamResult {
    stream_id: String,
    worker_id: String,
    flight_uri: String,
    key: String,
    elapsed_ms: u128,
    put_results: Vec<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client_config = E2eClientConfig::from_env()?;
    let flight_data_chunk_size = env_size("FLIGHT_DATA_CHUNK_SIZE", 16 * 1024 * 1024)?;
    let mut coordinator = CoordinatorClient::connect(
        args.coordinator_uri.clone(),
        args.coordinator_admin_token.clone(),
        args.coordinator_connect_timeout_seconds,
        &client_config,
    )
    .await?;

    let operation_id = args
        .operation_id
        .clone()
        .unwrap_or_else(|| format!("coordinator-{}", uuid::Uuid::new_v4()));
    let input_bytes = std::fs::metadata(&args.input)
        .with_context(|| format!("failed to stat {}", args.input.display()))?
        .len();
    let target_file_size = parse_optional_size("--file-size", args.file_size.as_deref())?;
    let max_stream_bytes =
        parse_optional_size("--max-stream-bytes", args.max_stream_bytes.as_deref())?;
    let max_record_batch_bytes = parse_optional_size(
        "--max-record-batch-bytes",
        args.max_record_batch_bytes.as_deref(),
    )?;
    let create_upload = coordinator
        .create_upload(CreateUploadRequest {
            operation_id: operation_id.clone(),
            upload_id: args.upload_id.clone(),
            table_name: args.table_name.clone(),
            schema: args.schema.clone(),
            commit_mode: Some(args.commit_mode.clone()),
            trino_user: args.trino_user.clone(),
            trino_authorization: args.trino_authorization.clone(),
            flavor: args.flavor.as_str().to_owned(),
            target_file_size,
            max_stream_bytes,
            max_record_batch_bytes,
            ttl_ms: args.upload_ttl_ms,
        })
        .await?;

    anyhow::ensure!(
        create_upload.granted_streams == create_upload.tickets.len(),
        "coordinator returned grantedStreams={} but tickets={}",
        create_upload.granted_streams,
        create_upload.tickets.len()
    );
    anyhow::ensure!(
        create_upload.expected_streams == create_upload.tickets.len(),
        "coordinator returned expectedStreams={} but tickets={}",
        create_upload.expected_streams,
        create_upload.tickets.len()
    );

    println!("coordinator_uri={}", args.coordinator_uri);
    println!("input={}", args.input.display());
    println!("input_bytes={input_bytes}");
    println!("input_size={}", pretty_bytes(input_bytes));
    println!("operation_id={}", create_upload.operation_id);
    println!("upload_id={}", create_upload.upload_id);
    println!("upload_status={}", create_upload.status);
    println!("requested_flavor={}", create_upload.requested_flavor);
    println!("granted_streams={}", create_upload.granted_streams);
    println!("expected_streams={}", create_upload.expected_streams);
    println!("staging_prefix={}", create_upload.staging_prefix);
    println!(
        "target_file_size_bytes={}",
        create_upload.target_file_size_bytes
    );
    println!(
        "target_file_size={}",
        pretty_bytes(create_upload.target_file_size_bytes)
    );
    for ticket in &create_upload.tickets {
        println!(
            "upload_ticket={} worker={} uri={} path={}",
            ticket.stream_id, ticket.worker_id, ticket.flight_uri, ticket.descriptor_path
        );
    }

    let upload_result = run_upload(
        &args.input,
        input_bytes,
        create_upload.tickets.clone(),
        &client_config,
        flight_data_chunk_size,
        args.client_queue_depth.max(1),
    )
    .await;
    let upload_summary = match upload_result {
        Ok(summary) => summary,
        Err(error) => {
            let _ = coordinator
                .abort_upload(
                    &create_upload.upload_id,
                    &format!("client upload failed: {error}"),
                )
                .await;
            return Err(error);
        }
    };

    println!("upload_elapsed_ms={}", upload_summary.elapsed_ms);
    println!(
        "upload_aggregate_throughput={}",
        throughput(
            input_bytes,
            std::time::Duration::from_millis(upload_summary.elapsed_ms as u64)
        )
    );
    println!("upload_batches_sent={}", upload_summary.batches_sent);
    for result in &upload_summary.stream_results {
        println!("stream_id={}", result.stream_id);
        println!("stream_worker_id={}", result.worker_id);
        println!("stream_flight_uri={}", result.flight_uri);
        println!("stream_key={}", result.key);
        println!("stream_elapsed_ms={}", result.elapsed_ms);
        for put_result in &result.put_results {
            println!("stream_put_result={put_result}");
        }
    }

    let commit = coordinator
        .commit_upload(
            &create_upload.upload_id,
            &args.commit_mode,
            create_upload.table_name.clone().or(args.table_name.clone()),
            args.trino_user.clone(),
            args.trino_authorization.clone(),
        )
        .await?;
    println!("commit_upload_id={}", commit.upload_id);
    println!("commit_status={}", commit.status);
    println!("commit_table_name={}", commit.table_name);
    println!("commit_mode={}", commit.mode);
    println!("commit_snapshot_id={}", commit.snapshot_id);
    println!("commit_record_count={}", commit.record_count);
    println!(
        "commit_parquet_object_bytes={}",
        commit.parquet_object_bytes
    );
    println!("commit_already_committed={}", commit.already_committed);
    println!(
        "commit_summary={}",
        serde_json::to_string(&commit.commit_summary)?
    );
    let written_files = commit.files;

    println!("written_files={}", written_files.len());
    for file in &written_files {
        println!(
            "written_file={} stream={} part={} rows={} batches={} parquet_bytes={}",
            file.file_path,
            file.stream_id,
            file.part_index,
            file.rows,
            file.batches,
            file.parquet_object_bytes
        );
    }

    Ok(())
}

impl CoordinatorClient {
    async fn connect(
        uri: String,
        admin_token: Option<String>,
        timeout_seconds: u64,
        config: &E2eClientConfig,
    ) -> Result<Self> {
        let channel = tokio::time::timeout(
            Duration::from_secs(timeout_seconds.max(1)),
            Channel::from_shared(tonic_uri(&uri)?)?.connect(),
        )
        .await
        .with_context(|| format!("timed out connecting to coordinator {uri}"))??;
        let client = FlightClient::new_from_inner(
            arrow_flight::flight_service_client::FlightServiceClient::new(channel)
                .max_decoding_message_size(config.max_message_size)
                .max_encoding_message_size(config.max_message_size),
        );
        Ok(Self {
            admin_token,
            client,
        })
    }

    async fn create_upload(
        &mut self,
        request: CreateUploadRequest,
    ) -> Result<CreateUploadResponse> {
        let mut body = Map::new();
        body.insert(
            "operationId".to_owned(),
            Value::String(request.operation_id),
        );
        body.insert("flavor".to_owned(), Value::String(request.flavor));
        insert_string(&mut body, "uploadId", request.upload_id);
        insert_string(&mut body, "tableName", request.table_name);
        insert_string(&mut body, "schema", request.schema);
        if let Some(mode) = request.commit_mode {
            body.insert(
                "mode".to_owned(),
                Value::String(
                    match mode {
                        CommitMode::Append => "append",
                        CommitMode::Overwrite => "overwrite",
                    }
                    .to_owned(),
                ),
            );
        }
        insert_string(&mut body, "user", request.trino_user);
        insert_string(&mut body, "authorization", request.trino_authorization);
        insert_u64(&mut body, "targetFileSizeBytes", request.target_file_size);
        insert_u64(&mut body, "maxStreamBytes", request.max_stream_bytes);
        insert_u64(
            &mut body,
            "maxRecordBatchBytes",
            request.max_record_batch_bytes,
        );
        insert_u64(&mut body, "ttlMs", request.ttl_ms);

        self.action_json("coordinator.create-upload", body).await
    }

    async fn commit_upload(
        &mut self,
        upload_id: &str,
        mode: &CommitMode,
        table_name: Option<String>,
        user: Option<String>,
        authorization: Option<String>,
    ) -> Result<CommitUploadResponse> {
        let mode = match mode {
            CommitMode::Append => "append",
            CommitMode::Overwrite => "overwrite",
        };
        let mut body = Map::new();
        body.insert("uploadId".to_owned(), Value::String(upload_id.to_owned()));
        body.insert("mode".to_owned(), Value::String(mode.to_owned()));
        insert_string(&mut body, "tableName", table_name);
        insert_string(&mut body, "user", user);
        insert_string(&mut body, "authorization", authorization);
        self.action_json("coordinator.commit-upload", body).await
    }

    async fn abort_upload(&mut self, upload_id: &str, reason: &str) -> Result<Value> {
        let mut body = Map::new();
        body.insert("uploadId".to_owned(), Value::String(upload_id.to_owned()));
        body.insert("reason".to_owned(), Value::String(reason.to_owned()));
        self.action_json("coordinator.abort-upload", body).await
    }

    async fn action_json<T: DeserializeOwned>(
        &mut self,
        action_type: &str,
        mut body: Map<String, Value>,
    ) -> Result<T> {
        if let Some(token) = self.admin_token.as_deref() {
            body.insert("adminToken".to_owned(), Value::String(token.to_owned()));
        }
        let action = Action {
            r#type: action_type.to_owned(),
            body: Bytes::from(json_bytes(Value::Object(body))),
        };
        let mut stream = self
            .client
            .do_action(action)
            .await
            .with_context(|| format!("coordinator action {action_type} failed"))?;
        let response = stream
            .try_next()
            .await?
            .with_context(|| format!("coordinator action {action_type} returned no result"))?;
        serde_json::from_slice(&response)
            .with_context(|| format!("failed to parse coordinator action {action_type} response"))
    }
}

struct CreateUploadRequest {
    operation_id: String,
    upload_id: Option<String>,
    table_name: Option<String>,
    schema: Option<String>,
    commit_mode: Option<CommitMode>,
    trino_user: Option<String>,
    trino_authorization: Option<String>,
    flavor: String,
    target_file_size: Option<u64>,
    max_stream_bytes: Option<u64>,
    max_record_batch_bytes: Option<u64>,
    ttl_ms: Option<u64>,
}

async fn run_upload(
    input: &PathBuf,
    input_bytes: u64,
    tickets: Vec<UploadTicket>,
    config: &E2eClientConfig,
    flight_data_chunk_size: usize,
    client_queue_depth: usize,
) -> Result<UploadRunSummary> {
    anyhow::ensure!(
        !tickets.is_empty(),
        "coordinator did not return upload tickets"
    );
    let requested_streams = tickets.len();
    let mut reader = StreamReader::try_new(
        File::open(input).with_context(|| format!("failed to open {}", input.display()))?,
        None,
    )?;
    let mut first_batches = Vec::with_capacity(requested_streams);
    for stream_index in 0..requested_streams {
        let Some(batch) = reader.next().transpose()? else {
            anyhow::bail!(
                "coordinator returned {requested_streams} upload tickets but input only has {stream_index} Arrow batches; use a smaller upload flavor or generate smaller batches"
            );
        };
        first_batches.push(batch);
    }

    let stream_config = Arc::new(StreamUploadConfig {
        max_message_size: config.max_message_size,
        flight_data_chunk_size,
    });

    let mut senders = Vec::with_capacity(requested_streams);
    let mut handles: Vec<JoinHandle<Result<StreamResult>>> = Vec::with_capacity(requested_streams);
    for (stream_index, ticket) in tickets.into_iter().enumerate() {
        let (sender, receiver) = mpsc::channel(client_queue_depth);
        senders.push(sender);
        handles.push(tokio::spawn(run_put_stream(
            stream_config.clone(),
            stream_index,
            ticket,
            receiver,
        )));
    }

    let started = Instant::now();
    let mut batches_sent = 0usize;
    for (index, batch) in first_batches.into_iter().enumerate() {
        senders[index]
            .send(batch)
            .await
            .with_context(|| format!("stream {index} stopped before first batch"))?;
        batches_sent += 1;
    }

    let mut next_stream = 0usize;
    while let Some(batch) = reader.next().transpose()? {
        senders[next_stream]
            .send(batch)
            .await
            .with_context(|| format!("stream {next_stream} stopped during upload"))?;
        batches_sent += 1;
        next_stream = (next_stream + 1) % requested_streams;
    }
    drop(senders);

    let mut stream_results = Vec::with_capacity(requested_streams);
    for handle in handles {
        stream_results.push(handle.await??);
    }
    stream_results.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));

    let elapsed = started.elapsed();
    println!(
        "upload_input_throughput={}",
        throughput(input_bytes, elapsed)
    );

    Ok(UploadRunSummary {
        elapsed_ms: elapsed.as_millis(),
        batches_sent,
        stream_results,
    })
}

#[derive(Debug, Clone)]
struct StreamUploadConfig {
    max_message_size: usize,
    flight_data_chunk_size: usize,
}

async fn run_put_stream(
    config: Arc<StreamUploadConfig>,
    stream_index: usize,
    ticket: UploadTicket,
    receiver: mpsc::Receiver<RecordBatch>,
) -> Result<StreamResult> {
    let batch_stream = stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|batch| (Ok::<RecordBatch, FlightError>(batch), receiver))
    });
    let flight_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![
            ticket.descriptor_path.clone(),
        ])))
        .with_max_flight_data_size(config.flight_data_chunk_size)
        .with_metadata(Bytes::from(ticket.app_metadata.clone()))
        .build(batch_stream);

    let channel = Channel::from_shared(tonic_uri(&ticket.flight_uri)?)?
        .connect()
        .await?;
    let mut client = FlightClient::new_from_inner(
        arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size),
    );

    let started = Instant::now();
    let put_context = format!(
        "uploadId={} streamId={} attemptId={} worker={} uri={} path={}",
        ticket.upload_id,
        ticket.stream_id,
        ticket.attempt_id,
        ticket.worker_id,
        ticket.flight_uri,
        ticket.descriptor_path
    );
    let mut response = client
        .do_put(flight_stream)
        .await
        .with_context(|| format!("DoPut stream {stream_index} failed to start; {put_context}"))?;
    let mut put_results = Vec::new();
    while let Some(result) = response
        .try_next()
        .await
        .with_context(|| format!("DoPut stream {stream_index} failed; {put_context}"))?
    {
        put_results.push(String::from_utf8_lossy(&result.app_metadata).into_owned());
    }

    Ok(StreamResult {
        stream_id: ticket.stream_id,
        worker_id: ticket.worker_id,
        flight_uri: ticket.flight_uri,
        key: ticket.descriptor_path,
        elapsed_ms: started.elapsed().as_millis(),
        put_results,
    })
}

fn parse_optional_size(name: &str, value: Option<&str>) -> Result<Option<u64>> {
    value
        .map(parse_size)
        .transpose()
        .with_context(|| format!("failed to parse {name}"))
        .map(|value| value.map(|bytes| bytes as u64))
}

fn insert_string(body: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.is_blank()) {
        body.insert(key.to_owned(), Value::String(value));
    }
}

fn insert_u64(body: &mut Map<String, Value>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        body.insert(
            key.to_owned(),
            Value::Number(serde_json::Number::from(value)),
        );
    }
}

fn json_bytes(value: Value) -> Vec<u8> {
    serde_json::to_vec(&value).expect("JSON serialization should not fail")
}

trait BlankString {
    fn is_blank(&self) -> bool;
}

impl BlankString for String {
    fn is_blank(&self) -> bool {
        self.trim().is_empty()
    }
}
