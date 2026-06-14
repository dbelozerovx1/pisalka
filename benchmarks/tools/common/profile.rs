use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use arrow_array::RecordBatch;
use serde_json::Value;

#[derive(Debug, Default)]
pub struct ClientSourceProfile {
    enabled: bool,
    ipc_read_ms: AtomicU64,
    rows: AtomicU64,
    batches: AtomicU64,
}

impl ClientSourceProfile {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Self::default()
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn record_ipc_read(&self, elapsed: Duration, batch: &RecordBatch) {
        if !self.enabled {
            return;
        }

        self.ipc_read_ms
            .fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);
        self.rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn print(&self) {
        if !self.enabled {
            return;
        }

        println!(
            "client_source.ipc_read_ms={}",
            self.ipc_read_ms.load(Ordering::Relaxed)
        );
        println!("client_source.rows={}", self.rows.load(Ordering::Relaxed));
        println!(
            "client_source.batches={}",
            self.batches.load(Ordering::Relaxed)
        );
    }
}

pub fn print_server_profile(result: &str) {
    let Ok(value) = serde_json::from_str::<Value>(result) else {
        return;
    };

    if let Some(elapsed_ms) = value
        .get("profile")
        .and_then(|profile| profile.get("total_server_ms"))
        .and_then(Value::as_u64)
    {
        println!("server_profile.total_server_ms={elapsed_ms}");
    }

    if let Some(messages) = value.get("flight_data_messages").and_then(Value::as_u64) {
        println!("server_profile.flight_data_messages={messages}");
    }

    let Some(profile) = value.get("profile") else {
        return;
    };
    let total_ms = profile
        .get("total_server_ms")
        .and_then(Value::as_u64)
        .unwrap_or_default();

    for key in [
        "first_flight_data_message_ms",
        "first_batch_receive_decode_ms",
        "receive_decode_ms",
        "enqueue_wait_ms",
        "collect_writer_wait_ms",
        "manifest_put_ms",
        "manifest_head_ms",
        "object_head_ms",
        "writer_task_elapsed_ms_max",
        "writer_task_write_ms_max",
        "writer_task_close_ms_max",
        "writer_task_write_ms_sum",
        "writer_task_flush_ms_sum",
        "writer_task_close_ms_sum",
        "writer_task_idle_wait_ms_sum",
    ] {
        print_profile_ms(profile, key, total_ms);
    }

    print_part_profiles(&value);
}

fn print_profile_ms(profile: &Value, key: &str, total_ms: u64) {
    let Some(ms) = profile.get(key).and_then(Value::as_u64) else {
        return;
    };

    if total_ms == 0 {
        println!("server_profile.{key}={ms}");
        return;
    }

    let pct = (ms as f64 / total_ms as f64) * 100.0;
    println!("server_profile.{key}={ms} ({pct:.1}%)");
}

fn print_part_profiles(value: &Value) {
    let Some(parts) = value.get("part_profiles").and_then(Value::as_array) else {
        return;
    };

    let limit = std::env::var("PROFILE_PART_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(16);

    for part in parts.iter().take(limit) {
        let part_index = part
            .get("part_index")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let rows = part.get("rows").and_then(Value::as_u64).unwrap_or_default();
        let batches = part
            .get("batches")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let parquet_bytes = part
            .get("parquet_object_bytes")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let profile = part.get("profile").unwrap_or(&Value::Null);
        let elapsed_ms = profile
            .get("elapsed_ms")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let write_ms = profile
            .get("write_ms")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let flush_ms = profile
            .get("flush_ms")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let close_ms = profile
            .get("close_ms")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let idle_wait_ms = profile
            .get("idle_wait_ms")
            .and_then(Value::as_u64)
            .unwrap_or_default();

        println!(
            "server_part.{part_index}=rows={rows} batches={batches} parquet_bytes={parquet_bytes} elapsed_ms={elapsed_ms} write_ms={write_ms} flush_ms={flush_ms} close_ms={close_ms} idle_wait_ms={idle_wait_ms}"
        );
    }

    if parts.len() > limit {
        println!("server_part.omitted={}", parts.len() - limit);
    }
}
