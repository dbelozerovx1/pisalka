use std::{fs::File, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::{
    FlightClient, FlightDescriptor, encode::FlightDataEncoderBuilder, error::FlightError,
};
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use clap::Parser;
use futures::{TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{sync::mpsc, task::JoinHandle};
use tonic::transport::Channel;

use arrow_flight_s3_mvp::{
    config::BenchConfig,
    util::{parse_size, pretty_bytes, throughput},
};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,

    #[arg(long, env = "FLIGHT_URI")]
    uri: Option<String>,

    #[arg(long, env = "PUT_STREAMS", default_value_t = 4)]
    streams: usize,

    #[arg(long, env = "PUT_CLIENT_QUEUE_DEPTH", default_value_t = 2)]
    client_queue_depth: usize,

    #[arg(
        long = "file-size",
        alias = "target-file-size",
        env = "TARGET_FILE_SIZE"
    )]
    file_size: Option<String>,

    #[arg(long, env = "PUT_UPLOAD_ID")]
    upload_id: Option<String>,

    #[arg(long, env = "PUT_STAGING_PREFIX")]
    staging_prefix: Option<String>,

    #[arg(long, env = "PUT_MAX_UPLOAD_STREAMS")]
    max_upload_streams: Option<usize>,

    #[arg(long, env = "PUT_MAX_STREAM_BYTES")]
    max_stream_bytes: Option<String>,

    #[arg(long, env = "PUT_PROFILE", default_value_t = false)]
    profile: bool,

    #[arg(long, env = "PUT_TICKETS_JSON")]
    tickets_json: Option<String>,

    #[arg(long, env = "PUT_TICKETS_FILE")]
    tickets_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct StreamUploadConfig {
    max_message_size: usize,
    flight_data_chunk_size: usize,
    target_file_size: Option<usize>,
    upload_id: String,
    staging_prefix: String,
    max_upload_streams: usize,
    max_stream_bytes: Option<u64>,
    profile: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TicketSpec {
    stream_id: String,
    descriptor_path: String,
    flight_uri: String,
    app_metadata: String,
}

#[derive(Debug, Serialize)]
struct PutOptions {
    upload_id: String,
    stream_id: String,
    staging_prefix: String,
    max_upload_streams: usize,
    max_stream_bytes: Option<u64>,
    target_file_size: Option<usize>,
    input_file_bytes: Option<u64>,
    profile: bool,
}

#[derive(Debug)]
struct StreamResult {
    stream_id: String,
    flight_uri: String,
    key: String,
    elapsed_ms: u128,
    put_results: Vec<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = BenchConfig::from_env()?;
    let uri = args.uri.unwrap_or(config.uri);
    let ticket_specs = read_ticket_specs(&args.tickets_json, &args.tickets_file)?;
    let requested_streams = ticket_specs
        .as_ref()
        .map(|tickets| tickets.len())
        .unwrap_or_else(|| args.streams.max(1))
        .max(1);
    let client_queue_depth = args.client_queue_depth.max(1);
    let input_bytes = std::fs::metadata(&args.input)
        .with_context(|| format!("failed to stat {}", args.input.display()))?
        .len();
    let target_file_size = args
        .file_size
        .as_deref()
        .map(parse_size)
        .transpose()
        .context("failed to parse --file-size")?;
    let max_stream_bytes = args
        .max_stream_bytes
        .as_deref()
        .map(parse_size)
        .transpose()
        .context("failed to parse --max-stream-bytes")?
        .map(|value| value as u64);
    let upload_id = args
        .upload_id
        .unwrap_or_else(|| format!("upload-{}", uuid::Uuid::new_v4()));
    let staging_prefix = args
        .staging_prefix
        .unwrap_or_else(|| format!("staging/{upload_id}"))
        .trim_matches('/')
        .to_owned();
    let max_upload_streams = args.max_upload_streams.unwrap_or(requested_streams).max(1);

    let mut reader = StreamReader::try_new(
        File::open(&args.input)
            .with_context(|| format!("failed to open {}", args.input.display()))?,
        None,
    )?;
    let mut first_batches = Vec::new();
    for _ in 0..requested_streams {
        let Some(batch) = reader.next().transpose()? else {
            break;
        };
        first_batches.push(batch);
    }
    anyhow::ensure!(
        !first_batches.is_empty(),
        "input did not contain any Arrow batches"
    );

    let active_streams = first_batches.len();
    let stream_config = Arc::new(StreamUploadConfig {
        max_message_size: config.max_message_size,
        flight_data_chunk_size: config.flight_data_chunk_size,
        target_file_size,
        upload_id: upload_id.clone(),
        staging_prefix: staging_prefix.clone(),
        max_upload_streams,
        max_stream_bytes,
        profile: args.profile,
    });

    let mut senders = Vec::with_capacity(active_streams);
    let mut handles: Vec<JoinHandle<Result<StreamResult>>> = Vec::with_capacity(active_streams);

    for stream_index in 0..active_streams {
        let (sender, receiver) = mpsc::channel(client_queue_depth);
        senders.push(sender);
        let ticket_spec = ticket_specs
            .as_ref()
            .and_then(|tickets| tickets.get(stream_index))
            .cloned();
        handles.push(tokio::spawn(run_put_stream(
            uri.clone(),
            stream_config.clone(),
            stream_index,
            ticket_spec,
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
        next_stream = (next_stream + 1) % active_streams;
    }
    drop(senders);

    let mut results = Vec::with_capacity(active_streams);
    for handle in handles {
        results.push(handle.await??);
    }
    let elapsed = started.elapsed();

    println!("uri={uri}");
    println!("input={}", args.input.display());
    println!("input_bytes={input_bytes}");
    println!("input_size={}", pretty_bytes(input_bytes));
    println!("upload_id={upload_id}");
    println!("staging_prefix={staging_prefix}");
    println!("requested_streams={requested_streams}");
    println!("active_streams={active_streams}");
    if ticket_specs.is_some() {
        println!("tickets=provided");
    }
    println!("client_queue_depth={client_queue_depth}");
    println!("batches_sent={batches_sent}");
    if let Some(target_file_size) = target_file_size {
        println!("target_file_size_bytes={target_file_size}");
        println!("target_file_size={}", pretty_bytes(target_file_size as u64));
    }
    println!("max_upload_streams={max_upload_streams}");
    if let Some(max_stream_bytes) = max_stream_bytes {
        println!("max_stream_bytes={max_stream_bytes}");
        println!("max_stream_size={}", pretty_bytes(max_stream_bytes));
    }
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("aggregate_throughput={}", throughput(input_bytes, elapsed));

    results.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    for result in results {
        println!("stream_id={}", result.stream_id);
        println!("stream_flight_uri={}", result.flight_uri);
        println!("stream_key={}", result.key);
        println!("stream_elapsed_ms={}", result.elapsed_ms);
        for put_result in result.put_results {
            println!("stream_put_result={put_result}");
        }
    }

    Ok(())
}

async fn run_put_stream(
    default_uri: String,
    config: Arc<StreamUploadConfig>,
    stream_index: usize,
    ticket_spec: Option<TicketSpec>,
    receiver: mpsc::Receiver<RecordBatch>,
) -> Result<StreamResult> {
    let stream_id = ticket_spec
        .as_ref()
        .map(|ticket| ticket.stream_id.clone())
        .unwrap_or_else(|| format!("stream-{stream_index:05}"));
    let key = ticket_spec
        .as_ref()
        .map(|ticket| ticket.descriptor_path.clone())
        .unwrap_or_else(|| format!("{}/{}.parquet", config.staging_prefix, stream_id));
    let flight_uri = ticket_spec
        .as_ref()
        .map(|ticket| ticket.flight_uri.clone())
        .unwrap_or(default_uri);
    let batch_stream = stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|batch| (Ok::<RecordBatch, FlightError>(batch), receiver))
    });
    let metadata = match ticket_spec {
        Some(ticket) => ticket.app_metadata.into_bytes(),
        None => serde_json::to_vec(&PutOptions {
            upload_id: config.upload_id.clone(),
            stream_id: stream_id.clone(),
            staging_prefix: config.staging_prefix.clone(),
            max_upload_streams: config.max_upload_streams,
            max_stream_bytes: config.max_stream_bytes,
            target_file_size: config.target_file_size,
            input_file_bytes: None,
            profile: config.profile,
        })?,
    };
    let flight_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![key.clone()])))
        .with_max_flight_data_size(config.flight_data_chunk_size)
        .with_metadata(Bytes::from(metadata))
        .build(batch_stream);

    let channel = Channel::from_shared(flight_uri.clone())?.connect().await?;
    let mut client = FlightClient::new_from_inner(
        arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size),
    );

    let started = Instant::now();
    let mut response = client.do_put(flight_stream).await?;
    let mut put_results = Vec::new();
    while let Some(result) = response.try_next().await? {
        put_results.push(String::from_utf8_lossy(&result.app_metadata).into_owned());
    }

    Ok(StreamResult {
        stream_id,
        flight_uri,
        key,
        elapsed_ms: started.elapsed().as_millis(),
        put_results,
    })
}

fn read_ticket_specs(
    inline: &Option<String>,
    file: &Option<PathBuf>,
) -> Result<Option<Vec<TicketSpec>>> {
    let Some(raw) = read_optional_text(inline, file)? else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(&raw).context("failed to parse PUT_TICKETS_JSON")?;
    let tickets_value = value.get("tickets").cloned().unwrap_or(value);
    let tickets: Vec<TicketSpec> =
        serde_json::from_value(tickets_value).context("failed to parse coordinator tickets")?;
    anyhow::ensure!(!tickets.is_empty(), "coordinator ticket list is empty");
    Ok(Some(tickets))
}

fn read_optional_text(inline: &Option<String>, file: &Option<PathBuf>) -> Result<Option<String>> {
    match (inline, file) {
        (Some(_), Some(_)) => {
            anyhow::bail!("use only one of --tickets-json or --tickets-file")
        }
        (Some(value), None) => Ok(Some(value.clone())),
        (None, Some(path)) => std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))
            .map(Some),
        (None, None) => Ok(None),
    }
}
