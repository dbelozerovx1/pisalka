use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
};

use futures::Stream;
use tokio::sync::OwnedSemaphorePermit;
use tonic::Status;

use crate::resource::ResourcePermit;

pub(crate) struct PutAdmission {
    pub(crate) _permit: OwnedSemaphorePermit,
    pub(crate) _memory: ResourcePermit,
    pub(crate) upload_id: Option<String>,
    pub(crate) upload_streams: Arc<Mutex<HashMap<String, usize>>>,
    pub(crate) active_put_streams: Arc<AtomicUsize>,
}

impl Drop for PutAdmission {
    fn drop(&mut self) {
        self.active_put_streams.fetch_sub(1, Ordering::Relaxed);

        let Some(upload_id) = self.upload_id.as_ref() else {
            return;
        };

        let mut upload_streams = self
            .upload_streams
            .lock()
            .expect("upload stream slot tracker mutex poisoned");
        match upload_streams.get_mut(upload_id) {
            Some(active) if *active > 1 => *active -= 1,
            Some(_) => {
                upload_streams.remove(upload_id);
            }
            None => {}
        }
    }
}

pub(crate) struct ReadAdmission {
    pub(crate) _permit: OwnedSemaphorePermit,
    pub(crate) _memory: ResourcePermit,
    pub(crate) active_read_streams: Arc<AtomicUsize>,
    pub(crate) active_read_streams_at_admit: usize,
}

impl Drop for ReadAdmission {
    fn drop(&mut self) {
        self.active_read_streams.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) struct GuardedResponseStream<T> {
    pub(crate) inner: Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>,
    pub(crate) _read_admission: ReadAdmission,
}

impl<T> Unpin for GuardedResponseStream<T> {}

impl<T> Stream for GuardedResponseStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
