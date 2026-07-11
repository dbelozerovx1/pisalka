use anyhow::{Context, Result};
use arrow_flight::{Action, FlightClient};
use bytes::Bytes;
use clap::Parser;
use futures::TryStreamExt;
use serde_json::{Map, Value};
use tonic::transport::Channel;

#[path = "common/client_config.rs"]
mod client_config;
#[path = "common/flight_uri.rs"]
mod flight_uri;

use client_config::E2eClientConfig;
use flight_uri::tonic_uri;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "COORDINATOR_URI", default_value = "http://127.0.0.1:8088")]
    coordinator_uri: String,

    #[arg(long)]
    schema_name: String,

    #[arg(long)]
    location: Option<String>,

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
    let client_config = E2eClientConfig::from_env()?;
    let mut body = Map::new();
    body.insert("schemaName".to_owned(), Value::String(args.schema_name));
    insert_string(&mut body, "location", args.location);
    insert_string(&mut body, "user", args.user);
    insert_string(&mut body, "authorization", args.authorization);
    insert_string(&mut body, "adminToken", args.admin_token);

    let channel = Channel::from_shared(tonic_uri(&args.coordinator_uri)?)?
        .connect()
        .await
        .with_context(|| format!("failed to connect to coordinator {}", args.coordinator_uri))?;
    let mut client = FlightClient::new_from_inner(
        arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(client_config.max_message_size)
            .max_encoding_message_size(client_config.max_message_size),
    );

    let action = Action {
        r#type: "coordinator.create-schema".to_owned(),
        body: Bytes::from(serde_json::to_vec(&Value::Object(body))?),
    };
    let mut stream = client
        .do_action(action)
        .await
        .context("coordinator create-schema action failed")?;
    let mut count = 0usize;
    while let Some(result) = stream.try_next().await? {
        count += 1;
        let value: Value =
            serde_json::from_slice(&result).context("invalid JSON action response")?;
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    anyhow::ensure!(count > 0, "coordinator create-schema returned no result");
    Ok(())
}

fn insert_string(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        map.insert(key.to_owned(), Value::String(value));
    }
}
