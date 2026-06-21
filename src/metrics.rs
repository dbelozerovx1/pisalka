use std::{
    fmt::Write as _,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::Stream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tonic::Status;
use tracing::{error, info};

use crate::{
    config::MetricsConfig,
    put_model::PutSummary,
    worker_status::{WorkerState, WorkerStatus},
};

const DURATION_BUCKET_MS: [u64; 15] = [
    5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000,
];

#[derive(Debug)]
pub struct WorkerMetrics {
    started_at_unix_seconds: u64,
    put_started: AtomicU64,
    put_succeeded: AtomicU64,
    put_failed: AtomicU64,
    put_rows: AtomicU64,
    put_batches: AtomicU64,
    put_files: AtomicU64,
    put_flight_stream_bytes: AtomicU64,
    put_parquet_object_bytes: AtomicU64,
    put_duration: DurationHistogram,
    get_started: AtomicU64,
    get_succeeded: AtomicU64,
    get_failed: AtomicU64,
    get_cancelled: AtomicU64,
    get_object_bytes: AtomicU64,
    get_duration: DurationHistogram,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        Self {
            started_at_unix_seconds: unix_seconds(),
            put_started: AtomicU64::new(0),
            put_succeeded: AtomicU64::new(0),
            put_failed: AtomicU64::new(0),
            put_rows: AtomicU64::new(0),
            put_batches: AtomicU64::new(0),
            put_files: AtomicU64::new(0),
            put_flight_stream_bytes: AtomicU64::new(0),
            put_parquet_object_bytes: AtomicU64::new(0),
            put_duration: DurationHistogram::new(),
            get_started: AtomicU64::new(0),
            get_succeeded: AtomicU64::new(0),
            get_failed: AtomicU64::new(0),
            get_cancelled: AtomicU64::new(0),
            get_object_bytes: AtomicU64::new(0),
            get_duration: DurationHistogram::new(),
        }
    }

    pub(crate) fn record_put_started(&self) {
        self.put_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_put_succeeded(&self, elapsed: Duration, summary: &PutSummary) {
        self.put_succeeded.fetch_add(1, Ordering::Relaxed);
        self.put_rows
            .fetch_add(summary.rows as u64, Ordering::Relaxed);
        self.put_batches
            .fetch_add(summary.batches as u64, Ordering::Relaxed);
        self.put_files
            .fetch_add(summary.parts as u64, Ordering::Relaxed);
        self.put_flight_stream_bytes
            .fetch_add(summary.flight_stream_bytes, Ordering::Relaxed);
        if let Some(bytes) = summary.parquet_object_bytes {
            self.put_parquet_object_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
        self.put_duration.observe(elapsed);
    }

    pub(crate) fn record_put_failed(&self, elapsed: Duration) {
        self.put_failed.fetch_add(1, Ordering::Relaxed);
        self.put_duration.observe(elapsed);
    }

    pub(crate) fn record_get_started(&self) {
        self.get_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_get_failed(&self, elapsed: Duration) {
        self.get_failed.fetch_add(1, Ordering::Relaxed);
        self.get_duration.observe(elapsed);
    }

    fn record_get_succeeded(&self, elapsed: Duration, object_bytes: u64) {
        self.get_succeeded.fetch_add(1, Ordering::Relaxed);
        self.get_object_bytes
            .fetch_add(object_bytes, Ordering::Relaxed);
        self.get_duration.observe(elapsed);
    }

    fn record_get_cancelled(&self, elapsed: Duration) {
        self.get_cancelled.fetch_add(1, Ordering::Relaxed);
        self.get_duration.observe(elapsed);
    }

    pub fn render_prometheus(&self, status: &WorkerStatus) -> String {
        let mut out = String::with_capacity(8192);
        let uptime = unix_seconds().saturating_sub(self.started_at_unix_seconds);

        metric_help(&mut out, "worker_info", "Worker identity.");
        metric_type(&mut out, "worker_info", "gauge");
        let _ = writeln!(
            out,
            "worker_info{{worker_id=\"{}\",flight_uri=\"{}\"}} 1",
            escape_label_value(&status.worker_id),
            escape_label_value(&status.flight_uri)
        );

        metric_help(&mut out, "worker_up", "Worker process is running.");
        metric_type(&mut out, "worker_up", "gauge");
        metric(&mut out, "worker_up", 1);

        metric_help(
            &mut out,
            "worker_uptime_seconds",
            "Worker process uptime in seconds.",
        );
        metric_type(&mut out, "worker_uptime_seconds", "gauge");
        metric(&mut out, "worker_uptime_seconds", uptime);

        metric_help(
            &mut out,
            "worker_draining",
            "Whether worker rejects new work.",
        );
        metric_type(&mut out, "worker_draining", "gauge");
        metric(&mut out, "worker_draining", u64::from(status.draining));

        metric_help(
            &mut out,
            "worker_state_active",
            "Whether worker state is ACTIVE.",
        );
        metric_type(&mut out, "worker_state_active", "gauge");
        metric(
            &mut out,
            "worker_state_active",
            u64::from(matches!(status.state, WorkerState::Active)),
        );

        gauge(&mut out, "worker_put_slots_limit", status.put.limit);
        gauge(&mut out, "worker_put_slots_active", status.put.active);
        gauge(&mut out, "worker_put_slots_available", status.put.available);
        gauge_u64(
            &mut out,
            "worker_put_slot_wait_milliseconds",
            status.put.slot_wait_ms,
        );
        gauge(&mut out, "worker_read_slots_limit", status.read.limit);
        gauge(&mut out, "worker_read_slots_active", status.read.active);
        gauge(
            &mut out,
            "worker_read_slots_available",
            status.read.available,
        );
        gauge_u64(
            &mut out,
            "worker_read_slot_wait_milliseconds",
            status.read.slot_wait_ms,
        );
        gauge_u64(
            &mut out,
            "worker_heartbeat_interval_milliseconds",
            status.heartbeat_interval_ms,
        );
        gauge_u64(
            &mut out,
            "worker_registry_ttl_milliseconds",
            status.registry_ttl_ms,
        );

        counter(&mut out, "worker_put_started_total", &self.put_started);
        counter(&mut out, "worker_put_succeeded_total", &self.put_succeeded);
        counter(&mut out, "worker_put_failed_total", &self.put_failed);
        counter(&mut out, "worker_put_rows_total", &self.put_rows);
        counter(&mut out, "worker_put_batches_total", &self.put_batches);
        counter(&mut out, "worker_put_files_total", &self.put_files);
        counter(
            &mut out,
            "worker_put_flight_stream_bytes_total",
            &self.put_flight_stream_bytes,
        );
        counter(
            &mut out,
            "worker_put_parquet_object_bytes_total",
            &self.put_parquet_object_bytes,
        );
        self.put_duration
            .render(&mut out, "worker_put_duration_seconds");

        counter(&mut out, "worker_get_started_total", &self.get_started);
        counter(&mut out, "worker_get_succeeded_total", &self.get_succeeded);
        counter(&mut out, "worker_get_failed_total", &self.get_failed);
        counter(&mut out, "worker_get_cancelled_total", &self.get_cancelled);
        counter(
            &mut out,
            "worker_get_object_bytes_total",
            &self.get_object_bytes,
        );
        self.get_duration
            .render(&mut out, "worker_get_duration_seconds");

        out
    }
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct DurationHistogram {
    buckets: [AtomicU64; DURATION_BUCKET_MS.len()],
    count: AtomicU64,
    sum_ms: AtomicU64,
}

impl DurationHistogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_ms: AtomicU64::new(0),
        }
    }

    fn observe(&self, elapsed: Duration) {
        let ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ms.fetch_add(ms, Ordering::Relaxed);

        if let Some(index) = DURATION_BUCKET_MS.iter().position(|bucket| ms <= *bucket) {
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn render(&self, out: &mut String, name: &str) {
        metric_type(out, name, "histogram");
        let mut cumulative = 0u64;
        for (index, bucket_ms) in DURATION_BUCKET_MS.iter().enumerate() {
            cumulative += self.buckets[index].load(Ordering::Relaxed);
            let le = *bucket_ms as f64 / 1000.0;
            let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let sum_seconds = self.sum_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(out, "{name}_sum {sum_seconds}");
        let _ = writeln!(out, "{name}_count {count}");
    }
}

pub struct MeasuredReadStream<S> {
    inner: S,
    metrics: Arc<WorkerMetrics>,
    started: Instant,
    object_bytes: u64,
    completed: bool,
}

impl<S> MeasuredReadStream<S> {
    pub fn new(inner: S, metrics: Arc<WorkerMetrics>, started: Instant, object_bytes: u64) -> Self {
        Self {
            inner,
            metrics,
            started,
            object_bytes,
            completed: false,
        }
    }
}

impl<S, T> Stream for MeasuredReadStream<S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(None) => {
                if !self.completed {
                    self.completed = true;
                    self.metrics
                        .record_get_succeeded(self.started.elapsed(), self.object_bytes);
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(status))) => {
                if !self.completed {
                    self.completed = true;
                    self.metrics.record_get_failed(self.started.elapsed());
                }
                Poll::Ready(Some(Err(status)))
            }
            other => other,
        }
    }
}

impl<S> Drop for MeasuredReadStream<S> {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.record_get_cancelled(self.started.elapsed());
        }
    }
}

pub fn spawn_metrics_server(
    config: MetricsConfig,
    metrics: Arc<WorkerMetrics>,
    status_provider: impl Fn() -> WorkerStatus + Send + Sync + 'static,
) {
    if !config.enabled {
        return;
    }

    let status_provider = Arc::new(status_provider);
    tokio::spawn(async move {
        let listener = match TcpListener::bind(config.addr).await {
            Ok(listener) => listener,
            Err(error) => {
                error!(addr = %config.addr, error = %error, "failed to bind metrics endpoint");
                return;
            }
        };
        info!(addr = %config.addr, "metrics endpoint listening");

        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    error!(error = %error, "failed to accept metrics connection");
                    continue;
                }
            };
            let metrics = metrics.clone();
            let status_provider = status_provider.clone();
            tokio::spawn(async move {
                let mut buffer = [0u8; 1024];
                let read = socket.read(&mut buffer).await.unwrap_or_default();
                let request = String::from_utf8_lossy(&buffer[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");

                let (status, body) = if path == "/metrics" {
                    (
                        "200 OK",
                        metrics.render_prometheus(&status_provider()).into_bytes(),
                    )
                } else if path == "/healthz" {
                    ("200 OK", b"ok\n".to_vec())
                } else {
                    ("404 Not Found", b"not found\n".to_vec())
                };

                let header = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.shutdown().await;
            });
        }
    });
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn metric_help(out: &mut String, name: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
}

fn metric_type(out: &mut String, name: &str, metric_type: &str) {
    let _ = writeln!(out, "# TYPE {name} {metric_type}");
}

fn metric(out: &mut String, name: &str, value: u64) {
    let _ = writeln!(out, "{name} {value}");
}

fn gauge(out: &mut String, name: &str, value: usize) {
    metric_type(out, name, "gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn gauge_u64(out: &mut String, name: &str, value: u64) {
    metric_type(out, name, "gauge");
    metric(out, name, value);
}

fn counter(out: &mut String, name: &str, value: &AtomicU64) {
    metric_type(out, name, "counter");
    metric(out, name, value.load(Ordering::Relaxed));
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}
