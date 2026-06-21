use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct WorkerStatus {
    pub worker_id: String,
    pub flight_uri: String,
    pub state: WorkerState,
    pub draining: bool,
    pub put: WorkerCapacity,
    pub read: WorkerCapacity,
    pub heartbeat_interval_ms: u64,
    pub registry_ttl_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerState {
    Active,
    Draining,
}

impl WorkerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Draining => "DRAINING",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerCapacity {
    pub limit: usize,
    pub active: usize,
    pub available: usize,
    pub slot_wait_ms: u64,
}
