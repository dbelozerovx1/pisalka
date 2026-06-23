use std::{path::PathBuf, time::Instant};

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

    #[arg(long, env = "GET_TICKET_JSON")]
    ticket_json: Option<String>,

    #[arg(long, env = "GET_TICKET_FILE")]
    ticket_file: Option<PathBuf>,
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

    let ticket_body = read_optional_text(&args.ticket_json, &args.ticket_file)?
        .unwrap_or_else(|| args.path.clone());
    let ticket = Ticket {
        ticket: Bytes::from(ticket_body),
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
    if args.ticket_json.is_some() || args.ticket_file.is_some() {
        println!("ticket=provided");
    }
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

fn read_optional_text(inline: &Option<String>, file: &Option<PathBuf>) -> Result<Option<String>> {
    match (inline, file) {
        (Some(_), Some(_)) => anyhow::bail!("use only one of --ticket-json or --ticket-file"),
        (Some(value), None) => Ok(Some(value.clone())),
        (None, Some(path)) => std::fs::read_to_string(path).map(Some).map_err(Into::into),
        (None, None) => Ok(None),
    }
}
