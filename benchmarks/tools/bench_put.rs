use std::{fs::File, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use arrow_flight::{
    FlightClient, FlightDescriptor, encode::FlightDataEncoderBuilder, error::FlightError,
};
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use clap::Parser;
use futures::{TryStreamExt, stream};
use serde::Serialize;
use tonic::transport::Channel;

use arrow_flight_s3_mvp::{
    config::BenchConfig,
    util::{parse_size, pretty_bytes, throughput},
};

mod common;

use common::profile::{ClientSourceProfile, print_server_profile};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    path: String,

    #[arg(long, env = "FLIGHT_URI")]
    uri: Option<String>,

    #[arg(
        long = "file-size",
        alias = "target-file-size",
        env = "TARGET_FILE_SIZE"
    )]
    file_size: Option<String>,

    #[arg(long, env = "PUT_PROFILE", default_value_t = false)]
    profile: bool,

    #[arg(long, env = "PUT_UPLOAD_ID")]
    upload_id: Option<String>,

    #[arg(long, env = "PUT_STREAM_ID")]
    stream_id: Option<String>,

    #[arg(long, env = "PUT_STAGING_PREFIX")]
    staging_prefix: Option<String>,

    #[arg(long, env = "PUT_MAX_UPLOAD_STREAMS")]
    max_upload_streams: Option<usize>,

    #[arg(long, env = "PUT_MAX_STREAM_BYTES")]
    max_stream_bytes: Option<String>,

    #[arg(long, env = "PUT_APP_METADATA_JSON")]
    app_metadata_json: Option<String>,

    #[arg(long, env = "PUT_APP_METADATA_FILE")]
    app_metadata_file: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct PutOptions {
    upload_id: Option<String>,
    stream_id: Option<String>,
    staging_prefix: Option<String>,
    max_upload_streams: Option<usize>,
    max_stream_bytes: Option<u64>,
    target_file_size: Option<usize>,
    input_file_bytes: u64,
    profile: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = BenchConfig::from_env()?;
    let uri = args.uri.unwrap_or(config.uri);

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
    let reader = StreamReader::try_new(
        File::open(&args.input)
            .with_context(|| format!("failed to open {}", args.input.display()))?,
        None,
    )?;

    let source_profile = Arc::new(ClientSourceProfile::new(args.profile));
    let batch_stream = stream::unfold(
        (reader, source_profile.clone()),
        |(mut reader, source_profile)| async move {
            let started = source_profile.enabled().then(Instant::now);
            let next_batch = reader.next()?;

            if let (Some(started), Ok(batch)) = (started, &next_batch) {
                source_profile.record_ipc_read(started.elapsed(), batch);
            }

            Some((
                next_batch.map_err(FlightError::from),
                (reader, source_profile),
            ))
        },
    );
    let mut encoder = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![args.path.clone()])))
        .with_max_flight_data_size(config.flight_data_chunk_size);

    let metadata = match read_optional_text(&args.app_metadata_json, &args.app_metadata_file)? {
        Some(metadata) => metadata.into_bytes(),
        None => serde_json::to_vec(&PutOptions {
            upload_id: args.upload_id.clone(),
            stream_id: args.stream_id.clone(),
            staging_prefix: args.staging_prefix.clone(),
            max_upload_streams: args.max_upload_streams,
            max_stream_bytes,
            target_file_size,
            input_file_bytes: input_bytes,
            profile: args.profile,
        })?,
    };
    encoder = encoder.with_metadata(Bytes::from(metadata));

    let flight_stream = encoder.build(batch_stream);

    let channel = Channel::from_shared(uri.clone())?.connect().await?;
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
    let elapsed = started.elapsed();

    println!("uri={uri}");
    println!("input={}", args.input.display());
    println!("input_bytes={input_bytes}");
    println!("input_size={}", pretty_bytes(input_bytes));
    println!("path={}", args.path);
    if let Some(target_file_size) = target_file_size {
        println!("target_file_size_bytes={target_file_size}");
        println!("target_file_size={}", pretty_bytes(target_file_size as u64));
    }
    if let Some(upload_id) = args.upload_id.as_deref() {
        println!("upload_id={upload_id}");
    }
    if let Some(stream_id) = args.stream_id.as_deref() {
        println!("stream_id={stream_id}");
    }
    if let Some(staging_prefix) = args.staging_prefix.as_deref() {
        println!("staging_prefix={staging_prefix}");
    }
    if let Some(max_upload_streams) = args.max_upload_streams {
        println!("max_upload_streams={max_upload_streams}");
    }
    if let Some(max_stream_bytes) = max_stream_bytes {
        println!("max_stream_bytes={max_stream_bytes}");
        println!("max_stream_size={}", pretty_bytes(max_stream_bytes));
    }
    if args.app_metadata_json.is_some() || args.app_metadata_file.is_some() {
        println!("app_metadata=provided");
    }
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("throughput={}", throughput(input_bytes, elapsed));
    source_profile.print();
    for result in put_results {
        println!("put_result={result}");
        if args.profile {
            print_server_profile(&result);
        }
    }

    Ok(())
}

fn read_optional_text(inline: &Option<String>, file: &Option<PathBuf>) -> Result<Option<String>> {
    match (inline, file) {
        (Some(_), Some(_)) => {
            anyhow::bail!("use only one of --app-metadata-json or --app-metadata-file")
        }
        (Some(value), None) => Ok(Some(value.clone())),
        (None, Some(path)) => std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))
            .map(Some),
        (None, None) => Ok(None),
    }
}
