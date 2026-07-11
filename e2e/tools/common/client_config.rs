use std::env;

use anyhow::{Context, Result};
use arrow_flight_s3_mvp::util::parse_size;

pub(crate) struct E2eClientConfig {
    pub(crate) max_message_size: usize,
}

impl E2eClientConfig {
    pub(crate) fn from_env() -> Result<Self> {
        Ok(Self {
            max_message_size: env_size("FLIGHT_MAX_MESSAGE_SIZE", 256 * 1024 * 1024)?,
        })
    }
}

pub(crate) fn env_size(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(value) => parse_size(&value).with_context(|| format!("invalid {key}")),
        Err(_) => Ok(default),
    }
}
