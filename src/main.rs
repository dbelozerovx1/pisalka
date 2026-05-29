use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight_s3_mvp::{
    config::AppConfig, flight_service::S3FlightService, util::build_object_store,
};
use tonic::transport::Server;
use tracing::info;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::from_env()?;
    let store = build_object_store(&config.s3)?;
    let service = S3FlightService::new(config.clone(), store);
    let flight_service = FlightServiceServer::new(service)
        .max_decoding_message_size(config.flight_max_message_size)
        .max_encoding_message_size(config.flight_max_message_size);

    info!(
        addr = %config.flight_addr,
        s3_endpoint = %config.s3.endpoint,
        bucket = %config.s3.bucket,
        compression = %config.parquet.compression_name,
        dictionary = config.parquet.dictionary_enabled,
        "starting Arrow Flight S3 MVP server"
    );

    Server::builder()
        .add_service(flight_service)
        .serve_with_shutdown(config.flight_addr, shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
