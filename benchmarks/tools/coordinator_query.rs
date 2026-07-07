use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use arrow_flight::{Action, FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use bytes::Bytes;
use clap::Parser;
use futures::TryStreamExt;
use serde_json::{Map, Value};
use tokio::time::sleep;
use tonic::transport::Channel;

use arrow_flight_s3_mvp::{
    config::BenchConfig,
    util::{batch_memory_size, pretty_bytes, throughput},
};

#[path = "common/flight_uri.rs"]
mod flight_uri;

use flight_uri::tonic_uri;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "COORDINATOR_URI", default_value = "http://127.0.0.1:8088")]
    coordinator_uri: String,

    #[arg(long, env = "COORDINATOR_QUERY_SQL")]
    sql: String,

    #[arg(long, env = "COORDINATOR_TARGET_TABLE")]
    target_table: Option<String>,

    #[arg(long, env = "COORDINATOR_SCHEMA")]
    schema: Option<String>,

    #[arg(long, env = "TRINO_USER", default_value = "local")]
    user: String,

    #[arg(long, env = "TRINO_AUTHORIZATION")]
    authorization: Option<String>,

    #[arg(long, env = "COORDINATOR_POLL_INTERVAL_MS", default_value_t = 250)]
    poll_interval_ms: u64,

    #[arg(long, env = "COORDINATOR_MAX_POLLS", default_value_t = 120)]
    max_polls: usize,

    #[arg(long, env = "COORDINATOR_READ_RESULTS", default_value_t = false)]
    read_results: bool,

    #[arg(long, env = "COORDINATOR_READ_MAX_ENDPOINTS")]
    read_max_endpoints: Option<usize>,

    #[arg(long, env = "COORDINATOR_PREVIEW_ROWS", default_value_t = 20)]
    preview_rows: usize,

    #[arg(long, env = "COORDINATOR_DROP_TEMP", default_value_t = false)]
    drop_temp: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let bench_config = BenchConfig::from_env()?;
    let channel = Channel::from_shared(tonic_uri(&args.coordinator_uri)?)?
        .connect()
        .await
        .with_context(|| format!("failed to connect to coordinator {}", args.coordinator_uri))?;
    let mut client = FlightClient::new_from_inner(
        arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(bench_config.max_message_size)
            .max_encoding_message_size(bench_config.max_message_size),
    );

    let mut body = Map::new();
    body.insert("type".to_owned(), Value::String("ctas".to_owned()));
    body.insert("sql".to_owned(), Value::String(args.sql.clone()));
    body.insert("user".to_owned(), Value::String(args.user.clone()));
    insert_string(&mut body, "targetTable", args.target_table.clone());
    insert_string(&mut body, "schema", args.schema.clone());
    insert_string(&mut body, "authorization", args.authorization.clone());

    let flight_info = client
        .get_flight_info(FlightDescriptor::new_cmd(json_bytes(Value::Object(body))))
        .await
        .context("coordinator GetFlightInfo failed")?;
    let query_id = query_id(&flight_info)?;
    println!("query_id={query_id}");
    print_flight_info("get_flight_info", &flight_info)?;

    let mut descriptor = FlightDescriptor::new_cmd(json_bytes(serde_json::json!({
        "type": "poll",
        "queryId": query_id
    })));
    for poll_index in 0..args.max_polls {
        let poll = client
            .poll_flight_info(descriptor)
            .await
            .with_context(|| format!("coordinator PollFlightInfo failed at poll {poll_index}"))?;
        println!(
            "poll={} progress={:?} complete={}",
            poll_index,
            poll.progress,
            poll.flight_descriptor.is_none()
        );
        let info = poll
            .info
            .as_ref()
            .context("PollFlightInfo response did not include FlightInfo")?;
        print_flight_info("poll_flight_info", info)?;
        let metadata = metadata_json(info)?;
        let status = metadata
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        if status == "FAILED" {
            let message = metadata
                .get("errorMessage")
                .and_then(Value::as_str)
                .unwrap_or("unknown coordinator query failure");
            anyhow::bail!("query {query_id} failed: {message}");
        }
        if let Some(next_descriptor) = poll.flight_descriptor {
            descriptor = next_descriptor;
            sleep(Duration::from_millis(args.poll_interval_ms)).await;
            continue;
        }
        anyhow::ensure!(
            status == "SUCCEEDED",
            "query {query_id} completed with unexpected status {status}"
        );
        let read_result = if args.read_results {
            read_results(info, &args, &bench_config).await
        } else {
            Ok(())
        };
        if args.drop_temp {
            drop_temp(&mut client, &query_id, &args).await?;
        }
        read_result?;
        return Ok(());
    }

    anyhow::bail!(
        "query {query_id} did not complete after {} polls",
        args.max_polls
    );
}

async fn drop_temp(client: &mut FlightClient, query_id: &str, args: &Args) -> Result<()> {
    let mut body = Map::new();
    body.insert("queryId".to_owned(), Value::String(query_id.to_owned()));
    body.insert("user".to_owned(), Value::String(args.user.clone()));
    insert_string(&mut body, "authorization", args.authorization.clone());
    let action = Action {
        r#type: "coordinator.drop-temp".to_owned(),
        body: Bytes::from(json_bytes(Value::Object(body))),
    };
    let mut stream = client
        .do_action(action)
        .await
        .context("coordinator drop-temp action failed")?;
    let response = stream
        .try_next()
        .await?
        .context("coordinator drop-temp action returned no result")?;
    let value: Value =
        serde_json::from_slice(&response).context("invalid drop-temp JSON response")?;
    println!("drop_temp_result={}", serde_json::to_string(&value)?);
    Ok(())
}

async fn read_results(info: &FlightInfo, args: &Args, config: &BenchConfig) -> Result<()> {
    let endpoint_limit = args
        .read_max_endpoints
        .unwrap_or(info.endpoint.len())
        .min(info.endpoint.len());
    println!("read_results=true");
    println!("read_endpoint_count={}", info.endpoint.len());
    println!("read_endpoint_limit={endpoint_limit}");
    if endpoint_limit == 0 {
        return Ok(());
    }

    let started = std::time::Instant::now();
    let mut total_rows = 0usize;
    let mut total_batches = 0usize;
    let mut total_arrow_bytes = 0u64;
    let mut preview_remaining = args.preview_rows;
    let mut preview_batches = Vec::new();

    for (index, endpoint) in info.endpoint.iter().take(endpoint_limit).enumerate() {
        let result = read_endpoint(endpoint, index, config, preview_remaining).await?;
        preview_remaining = preview_remaining.saturating_sub(result.preview_rows);
        total_rows += result.rows;
        total_batches += result.batches;
        total_arrow_bytes += result.arrow_bytes;
        preview_batches.extend(result.preview_batches);
        println!(
            "read_endpoint={} uri={} path={} rows={} batches={} arrow_bytes={} elapsed_ms={}",
            index,
            result.flight_uri,
            result.path,
            result.rows,
            result.batches,
            result.arrow_bytes,
            result.elapsed_ms
        );
    }

    let elapsed = started.elapsed();
    println!("read_rows={total_rows}");
    println!("read_batches={total_batches}");
    println!("read_arrow_memory_bytes_estimate={total_arrow_bytes}");
    println!(
        "read_arrow_memory_size_estimate={}",
        pretty_bytes(total_arrow_bytes)
    );
    println!("read_elapsed_ms={}", elapsed.as_millis());
    println!(
        "read_aggregate_throughput={}",
        throughput(total_arrow_bytes, elapsed)
    );
    if !preview_batches.is_empty() {
        println!("preview_rows={}", args.preview_rows - preview_remaining);
        println!("{}", pretty_format_batches(&preview_batches)?);
    }
    Ok(())
}

async fn read_endpoint(
    endpoint: &FlightEndpoint,
    index: usize,
    config: &BenchConfig,
    preview_limit: usize,
) -> Result<EndpointReadResult> {
    let ticket = endpoint
        .ticket
        .as_ref()
        .with_context(|| format!("endpoint {index} did not include a ticket"))?;
    let flight_uri = endpoint
        .location
        .first()
        .map(|location| location.uri.clone())
        .with_context(|| format!("endpoint {index} did not include a worker location"))?;
    let metadata = endpoint_metadata(endpoint)?;
    let path = metadata
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();

    let channel = Channel::from_shared(tonic_uri(&flight_uri)?)?
        .connect()
        .await
        .with_context(|| format!("failed to connect to worker {flight_uri}"))?;
    let mut client = FlightClient::new_from_inner(
        arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size),
    );

    let started = std::time::Instant::now();
    let mut stream = client
        .do_get(Ticket {
            ticket: Bytes::copy_from_slice(&ticket.ticket),
        })
        .await
        .with_context(|| format!("DoGet failed to start for {path}"))?;

    let mut rows = 0usize;
    let mut batches = 0usize;
    let mut arrow_bytes = 0u64;
    let mut preview_rows = 0usize;
    let mut preview_batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .with_context(|| format!("DoGet stream failed for {path}"))?
    {
        rows += batch.num_rows();
        batches += 1;
        arrow_bytes += batch_memory_size(&batch);
        if preview_rows < preview_limit {
            let take = (preview_limit - preview_rows).min(batch.num_rows());
            preview_batches.push(batch.slice(0, take));
            preview_rows += take;
        }
    }

    Ok(EndpointReadResult {
        flight_uri,
        path,
        rows,
        batches,
        arrow_bytes,
        preview_rows,
        preview_batches,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

fn endpoint_metadata(endpoint: &FlightEndpoint) -> Result<Value> {
    if endpoint.app_metadata.is_empty() {
        Ok(Value::Object(Map::new()))
    } else {
        serde_json::from_slice(&endpoint.app_metadata)
            .context("FlightEndpoint app_metadata was not JSON")
    }
}

struct EndpointReadResult {
    flight_uri: String,
    path: String,
    rows: usize,
    batches: usize,
    arrow_bytes: u64,
    preview_rows: usize,
    preview_batches: Vec<RecordBatch>,
    elapsed_ms: u128,
}

fn print_flight_info(prefix: &str, info: &FlightInfo) -> Result<()> {
    let metadata = metadata_json(info)?;
    println!("{prefix}.metadata={metadata}");
    println!("{prefix}.endpoints={}", info.endpoint.len());
    println!("{prefix}.total_records={}", info.total_records);
    println!("{prefix}.total_bytes={}", info.total_bytes);
    Ok(())
}

fn metadata_json(info: &FlightInfo) -> Result<Value> {
    if info.app_metadata.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    serde_json::from_slice(&info.app_metadata).context("FlightInfo app_metadata was not JSON")
}

fn query_id(info: &FlightInfo) -> Result<String> {
    metadata_json(info)?
        .get("queryId")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("FlightInfo metadata did not include queryId")
}

fn insert_string(body: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        body.insert(key.to_owned(), Value::String(value));
    }
}

fn json_bytes(value: Value) -> Vec<u8> {
    serde_json::to_vec(&value).expect("JSON serialization should not fail")
}
