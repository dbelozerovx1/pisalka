use std::{sync::Arc, time::Duration};

use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight_s3_mvp::{
    config::AppConfig,
    flight_service::WorkerFlightService,
    metadata_store::MetadataStore,
    metrics::{WorkerMetrics, spawn_metrics_server},
    util::build_object_store,
};
use tokio::time::sleep;
use tonic::transport::Server;
use tracing::{error, info};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::from_env()?;
    let store = build_object_store(&config.s3)?;
    let metadata_store = MetadataStore::connect(&config.metadata)
        .await?
        .map(Arc::new);
    let metrics = Arc::new(WorkerMetrics::new());
    let service = WorkerFlightService::new(
        config.clone(),
        store,
        metadata_store.clone(),
        metrics.clone(),
    );
    spawn_worker_registry_heartbeat(config.clone(), service.clone(), metadata_store.clone());
    spawn_metrics_server(config.metrics.clone(), metrics, {
        let service = service.clone();
        move || service.worker_status()
    });
    let flight_service = FlightServiceServer::new(service)
        .max_decoding_message_size(config.flight_max_message_size)
        .max_encoding_message_size(config.flight_max_message_size);

    info!(
        addr = %config.flight_addr,
        s3_endpoint = %config.s3.endpoint,
        bucket = %config.s3.bucket,
        metadata_db = config.metadata.database_url.is_some(),
        worker_id = %config.worker.worker_id,
        worker_flight_uri = %config.worker.flight_uri,
        metrics_enabled = config.metrics.enabled,
        metrics_addr = %config.metrics.addr,
        read_slots = config.worker.max_active_read_streams,
        put_slots = config.worker.max_active_put_streams,
        signed_capabilities_required = config.security.require_signed_capabilities,
        capability_worker_binding_required = config.security.require_capability_worker_binding,
        compression = %config.parquet.compression_name,
        dictionary = config.parquet.dictionary_enabled,
        "starting raw Parquet data-plane worker"
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

fn spawn_worker_registry_heartbeat(
    config: AppConfig,
    service: WorkerFlightService,
    metadata_store: Option<Arc<MetadataStore>>,
) {
    let Some(metadata_store) = metadata_store else {
        return;
    };
    if config.worker.registry_heartbeat_interval_ms == 0 {
        return;
    }

    let interval = Duration::from_millis(config.worker.registry_heartbeat_interval_ms);
    tokio::spawn(async move {
        loop {
            let status = service.worker_status();
            if let Err(error) = metadata_store.record_worker_heartbeat(&status).await {
                error!(
                    worker_id = %status.worker_id,
                    error = %error,
                    "failed to publish worker heartbeat"
                );
            }
            sleep(interval).await;
        }
    });
}
