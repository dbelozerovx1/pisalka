use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Duration, timeout},
};
use tonic::Status;

const MEMORY_UNIT_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ResourceLimiter {
    semaphore: Arc<Semaphore>,
    active_units: Arc<AtomicU64>,
    unit_bytes: u64,
    total_units: u32,
}

pub(crate) struct ResourcePermit {
    _permit: OwnedSemaphorePermit,
    active_units: Arc<AtomicU64>,
    units: u32,
}

impl ResourceLimiter {
    pub(crate) fn new(total_bytes: u64) -> Self {
        let total_units = units_for(total_bytes).max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(total_units as usize)),
            active_units: Arc::new(AtomicU64::new(0)),
            unit_bytes: MEMORY_UNIT_BYTES,
            total_units,
        }
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        u64::from(self.total_units).saturating_mul(self.unit_bytes)
    }

    pub(crate) fn active_bytes(&self) -> u64 {
        self.active_units()
            .saturating_mul(self.unit_bytes)
            .min(self.total_bytes())
    }

    pub(crate) fn available_bytes(&self) -> u64 {
        self.total_bytes().saturating_sub(self.active_bytes())
    }

    pub(crate) fn available_streams(&self, stream_memory_bytes: u64) -> usize {
        let stream_memory_bytes = stream_memory_bytes.max(1);
        (self.available_bytes() / stream_memory_bytes).min(usize::MAX as u64) as usize
    }

    pub(crate) async fn reserve(
        &self,
        bytes: u64,
        wait_ms: u64,
        operation: &str,
    ) -> Result<ResourcePermit, Status> {
        let units = units_for(bytes).min(self.total_units).max(1);
        let permit = if wait_ms == 0 {
            self.semaphore
                .clone()
                .try_acquire_many_owned(units)
                .map_err(|_| {
                    Status::resource_exhausted(format!(
                        "{operation} worker has no free memory budget; requested_bytes={}",
                        u64::from(units).saturating_mul(self.unit_bytes)
                    ))
                })?
        } else {
            match timeout(
                Duration::from_millis(wait_ms),
                self.semaphore.clone().acquire_many_owned(units),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(err)) => return Err(Status::internal(err.to_string())),
                Err(_) => {
                    return Err(Status::resource_exhausted(format!(
                        "timed out waiting for {operation} memory budget"
                    )));
                }
            }
        };

        self.active_units
            .fetch_add(u64::from(units), Ordering::Relaxed);
        Ok(ResourcePermit {
            _permit: permit,
            active_units: self.active_units.clone(),
            units,
        })
    }

    fn active_units(&self) -> u64 {
        self.active_units.load(Ordering::Relaxed)
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.active_units
            .fetch_sub(u64::from(self.units), Ordering::Relaxed);
    }
}

fn units_for(bytes: u64) -> u32 {
    bytes
        .saturating_add(MEMORY_UNIT_BYTES - 1)
        .saturating_div(MEMORY_UNIT_BYTES)
        .min(u64::from(u32::MAX))
        .max(1) as u32
}
