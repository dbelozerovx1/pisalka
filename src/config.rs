use std::{env, net::SocketAddr};

use anyhow::{Context, Result};
use parquet::basic::Compression;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub flight_addr: SocketAddr,
    pub s3: S3Config,
    pub parquet: ParquetTuning,
    pub worker: WorkerConfig,
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
    pub draining: bool,
    pub max_active_put_streams: usize,
    pub max_put_streams_per_upload: usize,
    pub put_slot_wait_ms: u64,
    pub put_first_batch_timeout_ms: u64,
    pub max_put_stream_bytes: Option<u64>,
    pub require_staging_prefix: bool,
    pub max_active_read_streams: usize,
    pub read_slot_wait_ms: u64,
    pub require_structured_tickets: bool,
    pub registry_heartbeat_interval_ms: u64,
    pub registry_ttl_ms: u64,
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

        Ok(Self {
            flight_addr,
            s3: S3Config::from_env(),
            parquet: ParquetTuning::from_env()?,
            worker: WorkerConfig::from_env()?,
            metadata: MetadataConfig::from_env(),
            metrics: MetricsConfig::from_env()?,
            flight_max_message_size: env_usize("FLIGHT_MAX_MESSAGE_SIZE", 256 * 1024 * 1024)?,
            flight_data_chunk_size: env_usize("FLIGHT_DATA_CHUNK_SIZE", 16 * 1024 * 1024)?,
            read_batch_size: env_usize("READ_BATCH_SIZE", 65_536)?,
        })
    }
}

impl BenchConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            uri: env_string("FLIGHT_URI", "http://127.0.0.1:50051"),
            max_message_size: env_usize("FLIGHT_MAX_MESSAGE_SIZE", 256 * 1024 * 1024)?,
            flight_data_chunk_size: env_usize("FLIGHT_DATA_CHUNK_SIZE", 16 * 1024 * 1024)?,
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
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            worker_id: env_string("WORKER_ID", "local-worker"),
            flight_uri: env_string("WORKER_FLIGHT_URI", "http://127.0.0.1:50051"),
            draining: env_bool("WORKER_DRAINING", false),
            max_active_put_streams: env_usize("PUT_MAX_ACTIVE_STREAMS", 16)?.max(1),
            max_put_streams_per_upload: env_usize("PUT_MAX_STREAMS_PER_UPLOAD", 8)?.max(1),
            put_slot_wait_ms: env_usize("PUT_SLOT_WAIT_MS", 30_000)? as u64,
            put_first_batch_timeout_ms: env_usize("PUT_FIRST_BATCH_TIMEOUT_MS", 10_000)? as u64,
            max_put_stream_bytes: env_optional_u64("PUT_MAX_STREAM_BYTES")?,
            require_staging_prefix: env_bool("PUT_REQUIRE_STAGING_PREFIX", false),
            max_active_read_streams: env_usize("READ_MAX_ACTIVE_STREAMS", 16)?.max(1),
            read_slot_wait_ms: env_usize("READ_SLOT_WAIT_MS", 30_000)? as u64,
            require_structured_tickets: env_bool("WORKER_REQUIRE_STRUCTURED_TICKETS", false),
            registry_heartbeat_interval_ms: env_usize("WORKER_HEARTBEAT_INTERVAL_MS", 5_000)?
                as u64,
            registry_ttl_ms: env_usize("WORKER_REGISTRY_TTL_MS", 15_000)? as u64,
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

fn env_usize(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(value) => crate::util::parse_size(&value)
            .with_context(|| format!("{key} must be a byte count or size like 64mb")),
        Err(_) => Ok(default),
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
