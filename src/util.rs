use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::FlightDescriptor;
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path};
use std::sync::Arc;

use crate::S3Config;

pub fn build_object_store(config: &S3Config) -> Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(&config.endpoint)
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(false)
        .build()
        .context("failed to build S3 object store")?;

    Ok(Arc::new(store))
}

pub fn descriptor_to_object_key(descriptor: Option<&FlightDescriptor>) -> String {
    let raw = descriptor
        .and_then(|descriptor| {
            if !descriptor.path.is_empty() {
                Some(descriptor.path.join("/"))
            } else if !descriptor.cmd.is_empty() {
                Some(String::from_utf8_lossy(&descriptor.cmd).into_owned())
            } else {
                None
            }
        })
        .unwrap_or_else(|| format!("uploads/{}.parquet", uuid::Uuid::new_v4()));

    normalize_object_key(&raw)
}

pub fn normalize_object_key(raw: &str) -> String {
    let key = raw
        .replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .collect::<Vec<_>>()
        .join("/");

    let key = if key.is_empty() {
        format!("uploads/{}.parquet", uuid::Uuid::new_v4())
    } else {
        key
    };

    if key.ends_with(".parquet") {
        key
    } else {
        format!("{key}.parquet")
    }
}

pub fn path_from_key(key: &str) -> Path {
    Path::from(normalize_object_key(key))
}

pub fn parse_size(input: &str) -> Result<usize> {
    let value = input.trim().to_ascii_lowercase().replace('_', "");
    let split = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);

    let base = number
        .parse::<usize>()
        .with_context(|| format!("invalid size number in {input:?}"))?;

    let multiplier = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024usize.pow(4),
        other => anyhow::bail!("unsupported size suffix {other:?} in {input:?}"),
    };

    base.checked_mul(multiplier)
        .with_context(|| format!("size {input:?} overflowed usize"))
}

pub fn pretty_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;

    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

pub fn throughput(bytes: u64, elapsed: Duration) -> String {
    if elapsed.is_zero() {
        return "inf".to_owned();
    }

    let bytes_per_second = bytes as f64 / elapsed.as_secs_f64();
    format!("{}/s", pretty_bytes(bytes_per_second as u64))
}

pub fn batch_memory_size(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|array| array.get_array_memory_size() as u64)
        .sum()
}
