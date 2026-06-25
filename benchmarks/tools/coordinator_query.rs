use std::time::Duration;

use anyhow::{Context, Result};
use arrow_flight::{FlightClient, FlightDescriptor, FlightInfo};
use clap::Parser;
use serde_json::{Map, Value};
use tokio::time::sleep;
use tonic::transport::Channel;

use arrow_flight_s3_mvp::config::BenchConfig;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "COORDINATOR_URI", default_value = "http://127.0.0.1:8088")]
    coordinator_uri: String,

    #[arg(long, env = "COORDINATOR_QUERY_SQL")]
    sql: String,

    #[arg(long, env = "COORDINATOR_TARGET_TABLE")]
    target_table: Option<String>,

    #[arg(long, env = "TRINO_USER", default_value = "local")]
    user: String,

    #[arg(long, env = "TRINO_AUTHORIZATION")]
    authorization: Option<String>,

    #[arg(long, env = "COORDINATOR_POLL_INTERVAL_MS", default_value_t = 250)]
    poll_interval_ms: u64,

    #[arg(long, env = "COORDINATOR_MAX_POLLS", default_value_t = 120)]
    max_polls: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let bench_config = BenchConfig::from_env()?;
    let channel = Channel::from_shared(args.coordinator_uri.clone())?
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
    body.insert("sql".to_owned(), Value::String(args.sql));
    body.insert("user".to_owned(), Value::String(args.user));
    insert_string(&mut body, "targetTable", args.target_table);
    insert_string(&mut body, "authorization", args.authorization);

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
        return Ok(());
    }

    anyhow::bail!(
        "query {query_id} did not complete after {} polls",
        args.max_polls
    );
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
