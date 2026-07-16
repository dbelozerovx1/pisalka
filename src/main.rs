use std::{fs, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight_s3_mvp::{
    config::{AppConfig, FlightTlsConfig},
    flight_service::WorkerFlightService,
    logging::init_tracing,
    metadata_store::MetadataStore,
    metrics::{WorkerMetrics, spawn_metrics_server},
    util::ObjectStoreRegistry,
};
use tokio::{
    sync::oneshot,
    time::{Instant, sleep, sleep_until},
};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Copy)]
struct ShutdownContext {
    signal: &'static str,
    started: Instant,
}

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
    let flight_service = FlightServiceServer::new(service.clone())
        .max_decoding_message_size(config.flight_max_message_size)
        .max_encoding_message_size(config.flight_max_message_size);

    info!(
        event = "worker_starting",
        addr = %config.flight_addr,
        s3_endpoint = %config.s3.endpoint,
        s3_presigned_bucket = %config.s3.presigned_bucket,
        s3_tmp_bucket = %config.s3.tmp_bucket,
        legacy_default_bucket = config.s3.legacy_default_bucket.as_deref().unwrap_or(""),
        metadata_db = config.metadata.database_url.is_some(),
        workerId = %config.worker.worker_id,
        worker_flight_uri = %config.worker.flight_uri,
        flight_tls_enabled = config.flight_tls.enabled,
        flight_tls_cert_path = config.flight_tls.cert_path.as_deref().unwrap_or(""),
        metrics_enabled = config.metrics.enabled,
        metrics_addr = %config.metrics.addr,
        read_slots = config.worker.max_active_read_streams,
        put_slots = config.worker.max_active_put_streams,
        put_parallelism = config.parquet.put_parallelism,
        put_writer_slots = config.parquet.max_active_put_writers,
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

    let (shutdown_started_tx, shutdown_started_rx) = oneshot::channel();
    let serve = server.add_service(flight_service).serve_with_shutdown(
        config.flight_addr,
        shutdown_signal(
            service.clone(),
            metadata_store,
            config.worker.shutdown_grace_ms,
            shutdown_started_tx,
        ),
    );
    tokio::pin!(serve);

    let shutdown = tokio::select! {
        result = &mut serve => {
            result.context("Flight server stopped before receiving a shutdown signal")?;
            warn!(
                event = "worker_server_stopped",
                phase = "serve",
                outcome = "unexpected",
                workerId = %config.worker.worker_id,
                "Flight server stopped without a process shutdown signal"
            );
            return Ok(());
        }
        result = shutdown_started_rx => {
            result.context("worker shutdown signal monitor stopped unexpectedly")?
        }
    };

    let deadline =
        sleep_until(shutdown.started + Duration::from_millis(config.worker.shutdown_grace_ms));
    tokio::pin!(deadline);

    tokio::select! {
        result = &mut serve => {
            result.context("Flight server failed during graceful shutdown")?;
            let status = service.worker_status();
            info!(
                event = "worker_shutdown_completed",
                phase = "stopped",
                outcome = "success",
                signal = shutdown.signal,
                workerId = %status.worker_id,
                activePutStreams = status.put.active,
                activeReadStreams = status.read.active,
                elapsedMs = elapsed_millis(shutdown.started),
                "worker graceful shutdown completed"
            );
        }
        _ = &mut deadline => {
            let status = service.worker_status();
            error!(
                event = "worker_shutdown_timed_out",
                phase = "drain",
                outcome = "timeout",
                signal = shutdown.signal,
                workerId = %status.worker_id,
                activePutStreams = status.put.active,
                activeReadStreams = status.read.active,
                graceMs = config.worker.shutdown_grace_ms,
                elapsedMs = elapsed_millis(shutdown.started),
                "worker graceful shutdown timed out"
            );
            return Err(anyhow!(
                "worker shutdown exceeded {}ms grace period with {} active DoPut and {} active DoGet streams",
                config.worker.shutdown_grace_ms,
                status.put.active,
                status.read.active
            ));
        }
    }

    Ok(())
}

async fn shutdown_signal(
    service: WorkerFlightService,
    metadata_store: Option<Arc<MetadataStore>>,
    grace_ms: u64,
    started_tx: oneshot::Sender<ShutdownContext>,
) {
    let signal = wait_for_shutdown_signal().await;
    let started = Instant::now();
    service.begin_draining();
    let status = service.worker_status();

    info!(
        event = "worker_shutdown_started",
        phase = "draining",
        outcome = "in_progress",
        signal,
        workerId = %status.worker_id,
        activePutStreams = status.put.active,
        activeReadStreams = status.read.active,
        graceMs = grace_ms,
        "worker received shutdown signal and started draining"
    );

    let _ = started_tx.send(ShutdownContext { signal, started });

    if let Some(metadata_store) = metadata_store {
        match metadata_store.record_worker_heartbeat(&status).await {
            Ok(()) => info!(
                event = "worker_shutdown_state_published",
                phase = "registry",
                workerId = %status.worker_id,
                signal,
                "published draining worker state"
            ),
            Err(error) => error!(
                event = "worker_shutdown_state_publish_failed",
                phase = "registry",
                workerId = %status.worker_id,
                signal,
                error = %error,
                "failed to publish draining worker state"
            ),
        }
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            error!(
                event = "worker_signal_registration_failed",
                phase = "startup",
                signal = "SIGTERM",
                error = %error,
                "failed to register SIGTERM handler"
            );
            let _ = tokio::signal::ctrl_c().await;
            return "sigint";
        }
    };

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                error!(
                    event = "worker_signal_wait_failed",
                    phase = "serve",
                    signal = "SIGINT",
                    error = %error,
                    "failed while waiting for SIGINT"
                );
            }
            "sigint"
        }
        _ = terminate.recv() => "sigterm",
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> &'static str {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(
            event = "worker_signal_wait_failed",
            phase = "serve",
            signal = "CTRL_C",
            error = %error,
            "failed while waiting for Ctrl+C"
        );
    }
    "ctrl_c"
}

fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
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
                    event = "worker_heartbeat_failed",
                    phase = "registry_heartbeat",
                    workerId = %status.worker_id,
                    error = %error,
                    "failed to publish worker heartbeat"
                );
            }
            sleep(interval).await;
        }
    });
}
