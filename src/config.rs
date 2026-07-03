use std::{env, fs, net::SocketAddr};

use anyhow::{Context, Result};
use parquet::basic::Compression;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub flight_addr: SocketAddr,
    pub flight_tls: FlightTlsConfig,
    pub s3: S3Config,
    pub parquet: ParquetTuning,
    pub worker: WorkerConfig,
    pub resources: ResourceConfig,
    pub security: SecurityConfig,
    pub metadata: MetadataConfig,
    pub metrics: MetricsConfig,
    pub flight_max_message_size: usize,
    pub flight_data_chunk_size: usize,
    pub read_batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub uri: String,
    pub max_message_size: usize,
    pub flight_data_chunk_size: usize,
}

#[derive(Debug, Clone)]
pub struct FlightTlsConfig {
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub allow_http: bool,
}

#[derive(Debug, Clone)]
pub struct ParquetTuning {
    pub compression_name: String,
    pub compression: Compression,
    pub dictionary_enabled: bool,
    pub max_row_group_rows: usize,
    pub write_batch_size: usize,
    pub data_page_size_limit: usize,
    pub flush_threshold_bytes: usize,
    pub multipart_part_size: usize,
    pub multipart_max_concurrency: usize,
    pub put_parallelism: usize,
    pub put_queue_depth: usize,
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub flight_uri: String,
    pub zone: Option<String>,
    pub draining: bool,
    pub max_active_put_streams: usize,
    pub max_put_streams_per_upload: usize,
    pub put_scheduler_reserved_slots: usize,
    pub put_slot_wait_ms: u64,
    pub put_first_batch_timeout_ms: u64,
    pub max_put_stream_bytes: Option<u64>,
    pub require_staging_prefix: bool,
    pub max_active_read_streams: usize,
    pub max_read_streams_per_operation: usize,
    pub read_scheduler_reserved_slots: usize,
    pub read_slot_wait_ms: u64,
    pub require_structured_tickets: bool,
    pub registry_heartbeat_interval_ms: u64,
    pub registry_ttl_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SecurityConfig {
    pub require_signed_capabilities: bool,
    pub capability_secret: Option<String>,
    pub require_capability_worker_binding: bool,
    pub max_capability_ttl_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ResourceConfig {
    pub worker_cpu_millicores: u64,
    pub worker_memory_bytes: u64,
    pub reserved_memory_bytes: u64,
    pub put_memory_bytes: u64,
    pub read_memory_bytes: u64,
    pub put_max_stream_memory_bytes: u64,
    pub put_max_record_batch_bytes: u64,
    pub read_max_stream_memory_bytes: u64,
    pub read_max_record_batch_bytes: u64,
    pub read_max_batch_rows: usize,
}

#[derive(Debug, Clone)]
pub struct MetadataConfig {
    pub database_url: Option<String>,
    pub auto_migrate: bool,
}

#[derive(Debug, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub addr: SocketAddr,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let flight_addr = env_string("FLIGHT_ADDR", "0.0.0.0:50051")
            .parse::<SocketAddr>()
            .context("FLIGHT_ADDR must be a socket address such as 0.0.0.0:50051")?;
        let flavor = WorkerFlavor::from_env()?;
        let worker = WorkerConfig::from_env(&flavor)?;
        let read_batch_size = env_usize("READ_BATCH_SIZE", 65_536)?;
        let resources = ResourceConfig::from_env(&worker, read_batch_size, &flavor)?;
        let security = SecurityConfig::from_env()?;

        Ok(Self {
            flight_addr,
            flight_tls: FlightTlsConfig::from_env()?,
            s3: S3Config::from_env(),
            parquet: ParquetTuning::from_env()?,
            worker,
            resources,
            security,
            metadata: MetadataConfig::from_env(),
            metrics: MetricsConfig::from_env()?,
            flight_max_message_size: env_usize("FLIGHT_MAX_MESSAGE_SIZE", 256 * 1024 * 1024)?,
            flight_data_chunk_size: env_usize("FLIGHT_DATA_CHUNK_SIZE", 16 * 1024 * 1024)?,
            read_batch_size,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct WorkerFlavor {
    cpu_millicores: u64,
    memory_bytes: u64,
}

impl WorkerFlavor {
    fn from_env() -> Result<Self> {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * MIB;

        let cpu_millicores = env_optional_cpu_millicores("WORKER_CPU_MILLICORES")?
            .or_else(detect_worker_cpu_millicores)
            .unwrap_or_else(default_cpu_millicores)
            .max(100);
        let memory_bytes = env_optional_u64("WORKER_MEMORY_BYTES")?
            .or_else(detect_worker_memory_bytes)
            .unwrap_or(16 * GIB)
            .max(512 * MIB);

        Ok(Self {
            cpu_millicores,
            memory_bytes,
        })
    }
}

impl BenchConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            uri: env_string("FLIGHT_URI", "grpc+tcp://127.0.0.1:50051"),
            max_message_size: env_usize("FLIGHT_MAX_MESSAGE_SIZE", 256 * 1024 * 1024)?,
            flight_data_chunk_size: env_usize("FLIGHT_DATA_CHUNK_SIZE", 16 * 1024 * 1024)?,
        })
    }
}

impl FlightTlsConfig {
    fn from_env() -> Result<Self> {
        let cert_path = env_optional_string("FLIGHT_TLS_CERT_PATH")
            .or_else(|| env_optional_string("WORKER_TLS_CERT_PATH"));
        let key_path = env_optional_string("FLIGHT_TLS_KEY_PATH")
            .or_else(|| env_optional_string("WORKER_TLS_KEY_PATH"));
        let enabled = env_optional_bool("FLIGHT_TLS_ENABLED")?
            .unwrap_or_else(|| cert_path.is_some() || key_path.is_some());

        if enabled && cert_path.is_none() {
            anyhow::bail!(
                "FLIGHT_TLS_CERT_PATH must be set when FLIGHT_TLS_ENABLED=true or TLS key/cert paths are provided"
            );
        }
        if enabled && key_path.is_none() {
            anyhow::bail!(
                "FLIGHT_TLS_KEY_PATH must be set when FLIGHT_TLS_ENABLED=true or TLS key/cert paths are provided"
            );
        }

        Ok(Self {
            enabled,
            cert_path,
            key_path,
        })
    }
}

impl S3Config {
    pub fn from_env() -> Self {
        Self {
            endpoint: env_string("S3_ENDPOINT", "http://127.0.0.1:9000"),
            bucket: env_string("S3_BUCKET", "arrow-flight"),
            region: env_string("S3_REGION", "us-east-1"),
            access_key_id: env_string("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret_access_key: env_string("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            allow_http: env_bool("AWS_ALLOW_HTTP", true),
        }
    }
}

impl ParquetTuning {
    pub fn from_env() -> Result<Self> {
        let compression_name = env_string("PARQUET_COMPRESSION", "uncompressed");

        Ok(Self {
            compression: parse_compression(&compression_name)?,
            compression_name,
            dictionary_enabled: env_bool("PARQUET_DICTIONARY", false),
            max_row_group_rows: env_usize("PARQUET_MAX_ROW_GROUP_ROWS", 1_048_576)?,
            write_batch_size: env_usize("PARQUET_WRITE_BATCH_SIZE", 65_536)?,
            data_page_size_limit: env_usize("PARQUET_DATA_PAGE_SIZE_LIMIT", 1024 * 1024)?,
            flush_threshold_bytes: env_usize("PARQUET_FLUSH_THRESHOLD_BYTES", 256 * 1024 * 1024)?,
            multipart_part_size: env_usize("S3_MULTIPART_PART_SIZE", 64 * 1024 * 1024)?
                .max(5 * 1024 * 1024),
            multipart_max_concurrency: env_usize("S3_MULTIPART_MAX_CONCURRENCY", 16)?.max(1),
            put_parallelism: env_usize("PUT_PARALLELISM", 4)?.max(1),
            put_queue_depth: env_usize("PUT_QUEUE_DEPTH", 2)?.max(1),
        })
    }
}

impl WorkerConfig {
    fn from_env(flavor: &WorkerFlavor) -> Result<Self> {
        let auto_put_streams = auto_put_streams(flavor.cpu_millicores);
        let auto_read_streams = auto_read_streams(flavor.cpu_millicores);
        Ok(Self {
            worker_id: env_string("WORKER_ID", "local-worker"),
            flight_uri: env_string("WORKER_FLIGHT_URI", "grpc+tcp://127.0.0.1:50051"),
            zone: env_optional_string("WORKER_ZONE"),
            draining: env_bool("WORKER_DRAINING", false),
            max_active_put_streams: env_count_auto("PUT_MAX_ACTIVE_STREAMS", auto_put_streams)?
                .max(1),
            max_put_streams_per_upload: env_count_auto(
                "PUT_MAX_STREAMS_PER_UPLOAD",
                auto_put_streams.min(4),
            )?
            .max(1),
            put_scheduler_reserved_slots: env_count_auto(
                "PUT_SCHEDULER_RESERVED_SLOTS",
                auto_reserved_slots(auto_put_streams),
            )?,
            put_slot_wait_ms: env_count("PUT_SLOT_WAIT_MS", 30_000)? as u64,
            put_first_batch_timeout_ms: env_count("PUT_FIRST_BATCH_TIMEOUT_MS", 10_000)? as u64,
            max_put_stream_bytes: env_optional_u64("PUT_MAX_STREAM_BYTES")?,
            require_staging_prefix: env_bool("PUT_REQUIRE_STAGING_PREFIX", false),
            max_active_read_streams: env_count_auto("READ_MAX_ACTIVE_STREAMS", auto_read_streams)?
                .max(1),
            max_read_streams_per_operation: env_count_auto(
                "READ_MAX_STREAMS_PER_OPERATION",
                auto_read_streams.min(8),
            )?
            .max(1),
            read_scheduler_reserved_slots: env_count_auto(
                "READ_SCHEDULER_RESERVED_SLOTS",
                auto_reserved_slots(auto_read_streams),
            )?,
            read_slot_wait_ms: env_count("READ_SLOT_WAIT_MS", 30_000)? as u64,
            require_structured_tickets: env_bool("WORKER_REQUIRE_STRUCTURED_TICKETS", false),
            registry_heartbeat_interval_ms: env_count("WORKER_HEARTBEAT_INTERVAL_MS", 5_000)?
                as u64,
            registry_ttl_ms: env_count("WORKER_REGISTRY_TTL_MS", 15_000)? as u64,
        })
    }
}

impl MetadataConfig {
    pub fn from_env() -> Self {
        Self {
            database_url: env_optional_string("METADATA_DATABASE_URL"),
            auto_migrate: env_bool("METADATA_DB_AUTO_MIGRATE", false),
        }
    }
}

impl SecurityConfig {
    pub fn from_env() -> Result<Self> {
        let require_signed_capabilities = env_bool("WORKER_REQUIRE_SIGNED_CAPABILITIES", false);
        let capability_secret = env_optional_string("WORKER_CAPABILITY_SECRET");
        if require_signed_capabilities && capability_secret.is_none() {
            anyhow::bail!(
                "WORKER_CAPABILITY_SECRET must be set when WORKER_REQUIRE_SIGNED_CAPABILITIES=true"
            );
        }

        Ok(Self {
            require_signed_capabilities,
            capability_secret,
            require_capability_worker_binding: env_bool(
                "WORKER_REQUIRE_CAPABILITY_WORKER_ID",
                require_signed_capabilities,
            ),
            max_capability_ttl_ms: env_usize("WORKER_CAPABILITY_MAX_TTL_MS", 60 * 60 * 1000)?
                as u64,
        })
    }
}

impl ResourceConfig {
    fn from_env(
        worker: &WorkerConfig,
        read_batch_size: usize,
        flavor: &WorkerFlavor,
    ) -> Result<Self> {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * MIB;

        let worker_memory_bytes = flavor.memory_bytes;
        let default_reserved = (worker_memory_bytes / 5)
            .max(512 * MIB)
            .min(worker_memory_bytes / 2);
        let reserved_memory_bytes = env_optional_u64("WORKER_RESERVED_MEMORY_BYTES")?
            .unwrap_or(default_reserved)
            .min(worker_memory_bytes.saturating_sub(256 * MIB));
        let usable_memory_bytes = worker_memory_bytes
            .saturating_sub(reserved_memory_bytes)
            .max(256 * MIB);

        let put_percent = env_percent("PUT_MEMORY_PERCENT", 55)?;
        let read_percent = env_percent("READ_MEMORY_PERCENT", 30)?;
        let default_put_memory = percent_of(usable_memory_bytes, put_percent);
        let default_read_memory = percent_of(usable_memory_bytes, read_percent);
        let put_memory_bytes = env_optional_u64("PUT_MEMORY_BUDGET_BYTES")?
            .unwrap_or(default_put_memory)
            .min(usable_memory_bytes)
            .max(64 * MIB);
        let read_memory_bytes = env_optional_u64("READ_MEMORY_BUDGET_BYTES")?
            .unwrap_or(default_read_memory)
            .min(usable_memory_bytes)
            .max(64 * MIB);

        let put_default_stream = default_stream_memory(
            put_memory_bytes,
            worker.max_active_put_streams,
            worker.max_put_streams_per_upload,
            256 * MIB,
            2 * GIB,
        );
        let put_max_stream_memory_bytes =
            env_optional_u64("PUT_MAX_STREAM_MEMORY_BYTES")?.unwrap_or(put_default_stream);
        let put_default_batch = default_batch_memory(put_max_stream_memory_bytes, 512 * MIB);
        let put_max_record_batch_bytes =
            env_optional_u64("PUT_MAX_RECORD_BATCH_BYTES")?.unwrap_or(put_default_batch);

        let read_default_stream = default_stream_memory(
            read_memory_bytes,
            worker.max_active_read_streams,
            worker.max_read_streams_per_operation,
            128 * MIB,
            GIB,
        );
        let read_max_stream_memory_bytes =
            env_optional_u64("READ_MAX_STREAM_MEMORY_BYTES")?.unwrap_or(read_default_stream);
        let read_default_batch = default_batch_memory(read_max_stream_memory_bytes, 256 * MIB);
        let read_max_record_batch_bytes =
            env_optional_u64("READ_MAX_RECORD_BATCH_BYTES")?.unwrap_or(read_default_batch);
        let read_max_batch_rows =
            env_usize("READ_MAX_BATCH_ROWS", read_batch_size.max(1_048_576))?.max(1);

        Ok(Self {
            worker_cpu_millicores: flavor.cpu_millicores,
            worker_memory_bytes,
            reserved_memory_bytes,
            put_memory_bytes,
            read_memory_bytes,
            put_max_stream_memory_bytes: put_max_stream_memory_bytes.max(1),
            put_max_record_batch_bytes: put_max_record_batch_bytes
                .min(put_max_stream_memory_bytes)
                .max(1),
            read_max_stream_memory_bytes: read_max_stream_memory_bytes.max(1),
            read_max_record_batch_bytes: read_max_record_batch_bytes
                .min(read_max_stream_memory_bytes)
                .max(1),
            read_max_batch_rows,
        })
    }
}

impl MetricsConfig {
    pub fn from_env() -> Result<Self> {
        let addr = env_string("METRICS_ADDR", "0.0.0.0:9090")
            .parse::<SocketAddr>()
            .context("METRICS_ADDR must be a socket address such as 0.0.0.0:9090")?;

        Ok(Self {
            enabled: env_bool("METRICS_ENABLED", true),
            addr,
        })
    }
}

pub fn parse_compression(value: &str) -> Result<Compression> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "lz4_raw" | "lz4raw" => Ok(Compression::LZ4_RAW),
        other => anyhow::bail!(
            "unsupported PARQUET_COMPRESSION={other}; use uncompressed, snappy, or lz4_raw"
        ),
    }
}

fn env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn env_optional_string(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    })
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_optional_bool(key: &str) -> Result<Option<bool>> {
    let Ok(value) = env::var(key) else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => anyhow::bail!("{key} must be a boolean value"),
    }
}

fn env_usize(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(value) => crate::util::parse_size(&value)
            .with_context(|| format!("{key} must be a byte count or size like 64mb")),
        Err(_) => Ok(default),
    }
}

fn env_count(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(value) => value
            .trim()
            .parse::<usize>()
            .with_context(|| format!("{key} must be an integer")),
        Err(_) => Ok(default),
    }
}

fn env_count_auto(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(value) if value.trim().is_empty() || value.trim().eq_ignore_ascii_case("auto") => {
            Ok(default)
        }
        Ok(value) => value
            .trim()
            .parse::<usize>()
            .with_context(|| format!("{key} must be auto or an integer")),
        Err(_) => Ok(default),
    }
}

fn env_percent(key: &str, default: u64) -> Result<u64> {
    match env::var(key) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .with_context(|| format!("{key} must be an integer percent"))
            .map(|value| value.min(100)),
        Err(_) => Ok(default.min(100)),
    }
}

fn env_optional_cpu_millicores(key: &str) -> Result<Option<u64>> {
    match env::var(key) {
        Ok(value) if value.trim().is_empty() || value.trim().eq_ignore_ascii_case("auto") => {
            Ok(None)
        }
        Ok(value) => parse_cpu_millicores(&value)
            .with_context(|| format!("{key} must be auto, 1000m, or a CPU count like 1 or 0.5"))
            .map(Some),
        Err(_) => Ok(None),
    }
}

fn env_optional_u64(key: &str) -> Result<Option<u64>> {
    match env::var(key) {
        Ok(value) if value.trim().is_empty() || value.trim() == "0" => Ok(None),
        Ok(value) => Ok(Some(
            crate::util::parse_size(&value)
                .with_context(|| format!("{key} must be 0, a byte count, or a size like 64mb"))?
                as u64,
        )),
        Err(_) => Ok(None),
    }
}

fn parse_cpu_millicores(value: &str) -> Result<u64> {
    let value = value.trim();
    if let Some(millis) = value.strip_suffix('m') {
        let parsed = millis
            .trim()
            .parse::<u64>()
            .context("millicore value must be an integer")?;
        return Ok(parsed.max(1));
    }

    let cores = value
        .parse::<f64>()
        .context("CPU value must be a number of cores")?;
    if !cores.is_finite() || cores <= 0.0 {
        anyhow::bail!("CPU value must be positive");
    }
    Ok((cores * 1000.0).ceil().max(1.0) as u64)
}

fn detect_worker_memory_bytes() -> Option<u64> {
    read_cgroup_memory_limit("/sys/fs/cgroup/memory.max")
        .or_else(|| read_cgroup_memory_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
}

fn detect_worker_cpu_millicores() -> Option<u64> {
    read_cgroup_cpu_max("/sys/fs/cgroup/cpu.max")
        .or_else(|| {
            read_cgroup_cpu_quota_period(
                "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
                "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
            )
        })
        .or_else(default_cpu_millicores_opt)
}

fn read_cgroup_memory_limit(path: &str) -> Option<u64> {
    let raw = fs::read_to_string(path).ok()?;
    let value = raw.trim();
    if value.is_empty() || value == "max" {
        return None;
    }

    let bytes = value.parse::<u64>().ok()?;
    (bytes > 0 && bytes < i64::MAX as u64).then_some(bytes)
}

fn read_cgroup_cpu_max(path: &str) -> Option<u64> {
    let raw = fs::read_to_string(path).ok()?;
    let mut parts = raw.split_whitespace();
    let quota = parts.next()?;
    let period = parts.next()?.parse::<u64>().ok()?;
    if quota == "max" || period == 0 {
        return None;
    }
    let quota = quota.parse::<u64>().ok()?;
    cpu_quota_to_millicores(quota, period)
}

fn read_cgroup_cpu_quota_period(quota_path: &str, period_path: &str) -> Option<u64> {
    let quota = fs::read_to_string(quota_path)
        .ok()?
        .trim()
        .parse::<i64>()
        .ok()?;
    if quota <= 0 {
        return None;
    }
    let period = fs::read_to_string(period_path)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    cpu_quota_to_millicores(quota as u64, period)
}

fn cpu_quota_to_millicores(quota: u64, period: u64) -> Option<u64> {
    if quota == 0 || period == 0 {
        return None;
    }
    Some(
        (((quota as u128).saturating_mul(1000) + period as u128 - 1) / period as u128)
            .min(u64::MAX as u128) as u64,
    )
}

fn default_cpu_millicores_opt() -> Option<u64> {
    std::thread::available_parallelism()
        .ok()
        .map(|parallelism| parallelism.get() as u64 * 1000)
}

fn default_cpu_millicores() -> u64 {
    default_cpu_millicores_opt().unwrap_or(1000)
}

fn auto_put_streams(cpu_millicores: u64) -> usize {
    (((cpu_millicores as u128).saturating_mul(2) + 999) / 1000).clamp(1, 16) as usize
}

fn auto_read_streams(cpu_millicores: u64) -> usize {
    (((cpu_millicores as u128).saturating_mul(4) + 999) / 1000).clamp(1, 32) as usize
}

fn auto_reserved_slots(streams: usize) -> usize {
    usize::from(streams >= 4)
}

fn percent_of(value: u64, percent: u64) -> u64 {
    ((value as u128).saturating_mul(percent as u128) / 100).min(u64::MAX as u128) as u64
}

fn default_stream_memory(
    budget_bytes: u64,
    max_active_streams: usize,
    max_streams_per_operation: usize,
    floor_bytes: u64,
    cap_bytes: u64,
) -> u64 {
    let planning_streams = max_active_streams
        .min(max_streams_per_operation.max(1))
        .max(1);
    let fair_share = budget_bytes / planning_streams as u64;
    fair_share.max(floor_bytes).min(cap_bytes).min(budget_bytes)
}

fn default_batch_memory(stream_memory_bytes: u64, cap_bytes: u64) -> u64 {
    (stream_memory_bytes / 2)
        .max(16 * 1024 * 1024)
        .min(cap_bytes)
        .min(stream_memory_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kubernetes_cpu_values() {
        assert_eq!(parse_cpu_millicores("1000m").unwrap(), 1000);
        assert_eq!(parse_cpu_millicores("500m").unwrap(), 500);
        assert_eq!(parse_cpu_millicores("1").unwrap(), 1000);
        assert_eq!(parse_cpu_millicores("0.5").unwrap(), 500);
    }

    #[test]
    fn detects_cpu_quota_as_millicores() {
        assert_eq!(cpu_quota_to_millicores(100_000, 100_000), Some(1000));
        assert_eq!(cpu_quota_to_millicores(150_000, 100_000), Some(1500));
        assert_eq!(cpu_quota_to_millicores(50_000, 100_000), Some(500));
    }

    #[test]
    fn auto_slots_follow_cpu_budget() {
        assert_eq!(auto_put_streams(500), 1);
        assert_eq!(auto_put_streams(1000), 2);
        assert_eq!(auto_put_streams(4000), 8);
        assert_eq!(auto_put_streams(16_000), 16);

        assert_eq!(auto_read_streams(500), 2);
        assert_eq!(auto_read_streams(1000), 4);
        assert_eq!(auto_read_streams(4000), 16);
        assert_eq!(auto_read_streams(16_000), 32);
    }
}
