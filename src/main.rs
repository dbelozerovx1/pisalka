use std::{fs, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight_s3_mvp::{
    config::{AppConfig, FlightTlsConfig},
    flight_service::WorkerFlightService,
    logging::init_tracing,
    metadata_store::MetadataStore,
    metrics::{WorkerMetrics, spawn_metrics_server},
    util::ObjectStoreRegistry,
};
use tokio::time::sleep;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{error, info};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    init_tracing("worker");

    let config = AppConfig::from_env()?;
    let stores = Arc::new(ObjectStoreRegistry::new(config.s3.clone())?);
    let metadata_store = MetadataStore::connect(&config.metadata)
        .await?
        .map(Arc::new);
    let metrics = Arc::new(WorkerMetrics::new());
    let service = WorkerFlightService::new(
        config.clone(),
        stores,
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
        s3_presigned_bucket = %config.s3.presigned_bucket,
        s3_tmp_bucket = %config.s3.tmp_bucket,
        legacy_default_bucket = config.s3.legacy_default_bucket.as_deref().unwrap_or(""),
        metadata_db = config.metadata.database_url.is_some(),
        worker_id = %config.worker.worker_id,
        worker_flight_uri = %config.worker.flight_uri,
        flight_tls_enabled = config.flight_tls.enabled,
        flight_tls_cert_path = config.flight_tls.cert_path.as_deref().unwrap_or(""),
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

    let mut server = Server::builder();
    if let Some(identity) = load_tls_identity(&config.flight_tls)? {
        server = server
            .tls_config(ServerTlsConfig::new().identity(identity))
            .context("failed to configure Flight server TLS")?;
    }

    server
        .add_service(flight_service)
        .serve_with_shutdown(config.flight_addr, shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn load_tls_identity(config: &FlightTlsConfig) -> Result<Option<Identity>> {
    if !config.enabled {
        return Ok(None);
    }

    let cert_path = config
        .cert_path
        .as_deref()
        .context("FLIGHT_TLS_CERT_PATH must be set when Flight TLS is enabled")?;
    let key_path = config
        .key_path
        .as_deref()
        .context("FLIGHT_TLS_KEY_PATH must be set when Flight TLS is enabled")?;
    let cert = fs::read(cert_path)
        .with_context(|| format!("failed to read Flight TLS certificate from {cert_path}"))?;
    let key = fs::read(key_path)
        .with_context(|| format!("failed to read Flight TLS private key from {key_path}"))?;

    Ok(Some(Identity::from_pem(cert, key)))
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
