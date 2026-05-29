use std::{fs::File, path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use arrow_flight::{
    FlightClient, FlightDescriptor, encode::FlightDataEncoderBuilder, error::FlightError,
};
use arrow_ipc::reader::StreamReader;
use clap::Parser;
use futures::{TryStreamExt, stream};
use tonic::transport::Channel;

use arrow_flight_s3_mvp::{
    config::BenchConfig,
    util::{pretty_bytes, throughput},
};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    path: String,

    #[arg(long, env = "FLIGHT_URI")]
    uri: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = BenchConfig::from_env()?;
    let uri = args.uri.unwrap_or(config.uri);

    let input_bytes = std::fs::metadata(&args.input)
        .with_context(|| format!("failed to stat {}", args.input.display()))?
        .len();
    let reader = StreamReader::try_new(
        File::open(&args.input)
            .with_context(|| format!("failed to open {}", args.input.display()))?,
        None,
    )?;

    let batch_stream = stream::iter(reader.map(|batch| batch.map_err(FlightError::from)));
    let flight_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![args.path.clone()])))
        .with_max_flight_data_size(config.flight_data_chunk_size)
        .build(batch_stream);

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
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("throughput={}", throughput(input_bytes, elapsed));
    for result in put_results {
        println!("put_result={result}");
    }

    Ok(())
}
