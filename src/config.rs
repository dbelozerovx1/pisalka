use std::{env, net::SocketAddr};

use anyhow::{Context, Result};
use parquet::basic::Compression;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub flight_addr: SocketAddr,
    pub s3: S3Config,
    pub parquet: ParquetTuning,
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

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let flight_addr = env_string("FLIGHT_ADDR", "0.0.0.0:50051")
            .parse::<SocketAddr>()
            .context("FLIGHT_ADDR must be a socket address such as 0.0.0.0:50051")?;

        Ok(Self {
            flight_addr,
            s3: S3Config::from_env(),
            parquet: ParquetTuning::from_env()?,
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
            put_parallelism: env_usize("PUT_PARALLELISM", 1)?.max(1),
            put_queue_depth: env_usize("PUT_QUEUE_DEPTH", 2)?.max(1),
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
