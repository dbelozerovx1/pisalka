use std::time::Instant;

use anyhow::Result;
use arrow_flight::{FlightClient, Ticket};
use bytes::Bytes;
use clap::Parser;
use futures::TryStreamExt;
use tonic::transport::Channel;

use arrow_flight_s3_mvp::{
    config::BenchConfig,
    util::{batch_memory_size, pretty_bytes, throughput},
};

#[derive(Debug, Parser)]
struct Args {
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

    let channel = Channel::from_shared(uri.clone())?.connect().await?;
    let mut client = FlightClient::new_from_inner(
        arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size),
    );

    let ticket = Ticket {
        ticket: Bytes::from(args.path.clone()),
    };
    let started = Instant::now();
    let mut stream = client.do_get(ticket).await?;

    let mut rows = 0usize;
    let mut batches = 0usize;
    let mut arrow_memory_bytes_estimate = 0u64;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        batches += 1;
        arrow_memory_bytes_estimate += batch_memory_size(&batch);
    }
    let elapsed = started.elapsed();

    println!("uri={uri}");
    println!("path={}", args.path);
    println!("rows={rows}");
    println!("batches={batches}");
    println!("arrow_memory_bytes_estimate={arrow_memory_bytes_estimate}");
    println!(
        "arrow_memory_size_estimate={}",
        pretty_bytes(arrow_memory_bytes_estimate)
    );
    println!("elapsed_ms={}", elapsed.as_millis());
    println!(
        "throughput={}",
        throughput(arrow_memory_bytes_estimate, elapsed)
    );

    Ok(())
}
