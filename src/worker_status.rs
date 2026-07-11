use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct WorkerStatus {
    pub worker_id: String,
    pub flight_uri: String,
    pub locality: WorkerLocality,
    pub state: WorkerState,
    pub draining: bool,
    pub put: WorkerCapacity,
    pub put_writers: WorkerWriterCapacity,
    pub read: WorkerCapacity,
    pub resources: WorkerResourceStatus,
    pub scheduler: WorkerSchedulerStatus,
    pub runtime: WorkerRuntimeStatus,
    pub capabilities: WorkerCapabilities,
    pub heartbeat_interval_ms: u64,
    pub registry_ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerLocality {
    pub zone: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
pub struct WorkerWriterCapacity {
    pub limit: usize,
    pub active: usize,
    pub available: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerResourceStatus {
    pub worker_cpu_millicores: u64,
    pub worker_memory_bytes: u64,
    pub reserved_memory_bytes: u64,
    pub put: WorkerResourcePool,
    pub read: WorkerResourcePool,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerResourcePool {
    pub limit_bytes: u64,
    pub active_bytes: u64,
    pub available_bytes: u64,
    pub max_stream_memory_bytes: u64,
    pub max_record_batch_bytes: u64,
    pub max_batch_rows: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerSchedulerStatus {
    pub put: WorkerSchedulingSignal,
    pub read: WorkerSchedulingSignal,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerSchedulingSignal {
    pub allowed: bool,
    pub saturation: WorkerSaturation,
    pub recommended_streams: usize,
    pub max_streams_per_operation: usize,
    pub reserved_slots: usize,
    pub soft_available_slots: usize,
    pub memory_available_streams: usize,
    pub utilization_per_mille: u16,
    pub selection_score: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerSaturation {
    Disabled,
    Saturated,
    Busy,
    Available,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerRuntimeStatus {
    pub put: PutRuntimeStatus,
    pub read: ReadRuntimeStatus,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PutRuntimeStatus {
    pub started_total: u64,
    pub succeeded_total: u64,
    pub failed_total: u64,
    pub rejected_total: u64,
    pub rows_total: u64,
    pub batches_total: u64,
    pub files_total: u64,
    pub flight_stream_bytes_total: u64,
    pub parquet_object_bytes_total: u64,
    pub admission_wait_ms_ewma: u64,
    pub duration_ms_ewma: u64,
    pub throughput_bytes_per_sec_ewma: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ReadRuntimeStatus {
    pub started_total: u64,
    pub succeeded_total: u64,
    pub failed_total: u64,
    pub rejected_total: u64,
    pub cancelled_total: u64,
    pub object_bytes_total: u64,
    pub admission_wait_ms_ewma: u64,
    pub duration_ms_ewma: u64,
    pub throughput_bytes_per_sec_ewma: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerCapabilities {
    pub put_parallelism: usize,
    pub max_active_put_writers: usize,
    pub put_queue_depth: usize,
    pub max_put_streams_per_upload: usize,
    pub max_put_stream_bytes: Option<u64>,
    pub put_max_stream_memory_bytes: u64,
    pub put_max_record_batch_bytes: u64,
    pub read_max_streams_per_operation: usize,
    pub read_batch_size: usize,
    pub read_max_stream_memory_bytes: u64,
    pub read_max_record_batch_bytes: u64,
    pub read_max_batch_rows: usize,
    pub flight_data_chunk_size: usize,
    pub capability_version: u16,
    pub signed_capabilities_required: bool,
    pub capability_worker_binding_required: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerSchedulingPolicy {
    pub max_streams_per_operation: usize,
    pub reserved_slots: usize,
    pub memory_available_streams: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WorkerSchedulingTelemetry {
    pub succeeded_total: u64,
    pub failed_total: u64,
    pub rejected_total: u64,
    pub cancelled_total: u64,
    pub admission_wait_ms_ewma: u64,
}

impl WorkerSchedulingSignal {
    pub fn from_capacity(
        capacity: &WorkerCapacity,
        policy: WorkerSchedulingPolicy,
        telemetry: WorkerSchedulingTelemetry,
        draining: bool,
    ) -> Self {
        let utilization_per_mille = utilization_per_mille(capacity.active, capacity.limit);
        let slot_soft_available = capacity.available.saturating_sub(policy.reserved_slots);
        let memory_available_streams = policy.memory_available_streams.unwrap_or(usize::MAX);
        let soft_available_slots = slot_soft_available.min(memory_available_streams);
        let max_streams_per_operation = policy.max_streams_per_operation.max(1);
        let recommended_streams = if draining {
            0
        } else {
            soft_available_slots.min(max_streams_per_operation)
        };
        let allowed = recommended_streams > 0;
        let saturation = if draining {
            WorkerSaturation::Disabled
        } else if soft_available_slots == 0 {
            WorkerSaturation::Saturated
        } else if utilization_per_mille >= 800 {
            WorkerSaturation::Busy
        } else {
            WorkerSaturation::Available
        };

        Self {
            allowed,
            saturation,
            recommended_streams,
            max_streams_per_operation,
            reserved_slots: policy.reserved_slots,
            soft_available_slots,
            memory_available_streams,
            utilization_per_mille,
            selection_score: selection_score(
                capacity,
                soft_available_slots,
                recommended_streams,
                utilization_per_mille,
                telemetry,
            ),
        }
    }
}

fn utilization_per_mille(active: usize, limit: usize) -> u16 {
    if limit == 0 {
        return 1_000;
    }

    ((active.saturating_mul(1_000) / limit).min(1_000)) as u16
}

fn selection_score(
    capacity: &WorkerCapacity,
    soft_available_slots: usize,
    recommended_streams: usize,
    utilization_per_mille: u16,
    telemetry: WorkerSchedulingTelemetry,
) -> u64 {
    if recommended_streams == 0 || capacity.limit == 0 {
        return 0;
    }

    let availability_score =
        (soft_available_slots as u64).saturating_mul(10_000) / (capacity.limit as u64).max(1);
    let wait_penalty = telemetry.admission_wait_ms_ewma.min(30_000) / 10;
    let completed = telemetry
        .succeeded_total
        .saturating_add(telemetry.failed_total)
        .saturating_add(telemetry.rejected_total)
        .saturating_add(telemetry.cancelled_total);
    let failure_rate_per_mille = if completed == 0 {
        0
    } else {
        telemetry
            .failed_total
            .saturating_add(telemetry.rejected_total)
            .saturating_add(telemetry.cancelled_total)
            .saturating_mul(1_000)
            / completed
    };
    let failure_penalty = failure_rate_per_mille.saturating_mul(2);
    let utilization_penalty = u64::from(utilization_per_mille);

    availability_score
        .saturating_sub(wait_penalty)
        .saturating_sub(failure_penalty)
        .saturating_sub(utilization_penalty)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capacity(active: usize, limit: usize) -> WorkerCapacity {
        WorkerCapacity {
            limit,
            active,
            available: limit.saturating_sub(active),
            slot_wait_ms: 30_000,
        }
    }

    #[test]
    fn recommends_streams_from_available_slots_and_operation_cap() {
        let signal = WorkerSchedulingSignal::from_capacity(
            &capacity(2, 16),
            WorkerSchedulingPolicy {
                max_streams_per_operation: 6,
                reserved_slots: 0,
                memory_available_streams: None,
            },
            WorkerSchedulingTelemetry::default(),
            false,
        );

        assert_eq!(signal.recommended_streams, 6);
        assert_eq!(signal.soft_available_slots, 14);
        assert_eq!(signal.saturation, WorkerSaturation::Available);
        assert!(signal.selection_score > 0);
    }

    #[test]
    fn keeps_reserved_slots_out_of_coordinator_grants() {
        let signal = WorkerSchedulingSignal::from_capacity(
            &capacity(13, 16),
            WorkerSchedulingPolicy {
                max_streams_per_operation: 8,
                reserved_slots: 2,
                memory_available_streams: None,
            },
            WorkerSchedulingTelemetry::default(),
            false,
        );

        assert_eq!(signal.recommended_streams, 1);
        assert_eq!(signal.soft_available_slots, 1);
        assert_eq!(signal.saturation, WorkerSaturation::Busy);
    }

    #[test]
    fn stops_grants_when_draining_or_saturated() {
        let draining = WorkerSchedulingSignal::from_capacity(
            &capacity(0, 16),
            WorkerSchedulingPolicy {
                max_streams_per_operation: 8,
                reserved_slots: 0,
                memory_available_streams: None,
            },
            WorkerSchedulingTelemetry::default(),
            true,
        );
        let saturated = WorkerSchedulingSignal::from_capacity(
            &capacity(16, 16),
            WorkerSchedulingPolicy {
                max_streams_per_operation: 8,
                reserved_slots: 0,
                memory_available_streams: None,
            },
            WorkerSchedulingTelemetry::default(),
            false,
        );

        assert_eq!(draining.recommended_streams, 0);
        assert_eq!(draining.saturation, WorkerSaturation::Disabled);
        assert_eq!(saturated.recommended_streams, 0);
        assert_eq!(saturated.saturation, WorkerSaturation::Saturated);
    }

    #[test]
    fn limits_recommendation_by_available_memory() {
        let signal = WorkerSchedulingSignal::from_capacity(
            &capacity(1, 16),
            WorkerSchedulingPolicy {
                max_streams_per_operation: 8,
                reserved_slots: 0,
                memory_available_streams: Some(3),
            },
            WorkerSchedulingTelemetry::default(),
            false,
        );

        assert_eq!(signal.recommended_streams, 3);
        assert_eq!(signal.soft_available_slots, 3);
        assert_eq!(signal.memory_available_streams, 3);
        assert_eq!(signal.saturation, WorkerSaturation::Available);
    }
}
