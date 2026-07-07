use anyhow::{Context, Result};
use arrow_flight::{Action, FlightClient};
use bytes::Bytes;
use clap::Parser;
use futures::TryStreamExt;
use serde_json::{Map, Value};
use tonic::transport::Channel;

use arrow_flight_s3_mvp::config::BenchConfig;

#[path = "common/flight_uri.rs"]
mod flight_uri;

use flight_uri::tonic_uri;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "COORDINATOR_URI", default_value = "http://127.0.0.1:8088")]
    coordinator_uri: String,

    #[arg(long, env = "COORDINATOR_ACTION")]
    action: String,

    #[arg(long, env = "COORDINATOR_ACTION_BODY", default_value = "{}")]
    body: String,

    #[arg(long, env = "TRINO_USER")]
    user: Option<String>,

    #[arg(long, env = "TRINO_AUTHORIZATION")]
    authorization: Option<String>,

    #[arg(long, env = "COORDINATOR_ADMIN_TOKEN")]
    admin_token: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let bench_config = BenchConfig::from_env()?;
    let mut body = parse_body(&args.body)?;
    insert_string(&mut body, "user", args.user);
    insert_string(&mut body, "authorization", args.authorization);
    insert_string(&mut body, "adminToken", args.admin_token);

    let channel = Channel::from_shared(tonic_uri(&args.coordinator_uri)?)?
        .connect()
        .await
        .with_context(|| format!("failed to connect to coordinator {}", args.coordinator_uri))?;
    let mut client = FlightClient::new_from_inner(
        arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(bench_config.max_message_size)
            .max_encoding_message_size(bench_config.max_message_size),
    );

    let action = Action {
        r#type: args.action.clone(),
        body: Bytes::from(serde_json::to_vec(&Value::Object(body))?),
    };
    let mut stream = client
        .do_action(action)
        .await
        .with_context(|| format!("coordinator action {} failed", args.action))?;
    let mut count = 0usize;
    while let Some(result) = stream.try_next().await? {
        count += 1;
        let value: Value =
            serde_json::from_slice(&result).context("invalid JSON action response")?;
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    anyhow::ensure!(count > 0, "coordinator action returned no results");
    Ok(())
}

fn parse_body(raw: &str) -> Result<Map<String, Value>> {
    let value: Value = serde_json::from_str(raw).context("--body must be a JSON object")?;
    match value {
        Value::Object(map) => Ok(map),
        _ => anyhow::bail!("--body must be a JSON object"),
    }
}

fn insert_string(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        map.insert(key.to_owned(), Value::String(value));
    }
}
