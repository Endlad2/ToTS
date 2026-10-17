//! Кэш TURN-креденшелов, сгруппированный по `streams_per_cache` потокам.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use crate::TurnCredentials;

#[derive(Debug)]
pub struct CredentialSlot {
    inner: RwLock<Option<TurnCredentials>>,
    pub error_count: AtomicI32,
    pub last_error_unix: AtomicI64,
}

impl CredentialSlot {
    fn new() -> Self {
        Self {
            inner: RwLock::new(None),
            error_count: AtomicI32::new(0),
            last_error_unix: AtomicI64::new(0),
        }
    }

    pub fn get_valid(&self, margin: Duration) -> Option<TurnCredentials> {
        self.inner
            .read()
            .as_ref()
            .filter(|c| c.is_valid(margin))
            .cloned()
    }

    pub fn store(&self, creds: TurnCredentials) {
        *self.inner.write() = Some(creds);
    }

    pub fn invalidate(&self) -> bool {
        let had = self.inner.write().take().is_some();
        self.error_count.store(0, Ordering::Relaxed);
        self.last_error_unix.store(0, Ordering::Relaxed);
        had
    }
}

pub struct CredentialStore {
    slots: RwLock<HashMap<u32, Arc<CredentialSlot>>>,
    streams_per_cache: u32,
}

impl CredentialStore {
    pub fn new(streams_per_cache: u32) -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
            streams_per_cache: streams_per_cache.max(1),
        }
    }

    pub fn slot_id(&self, stream_id: u32) -> u32 {
        if stream_id < 1 {
            return 0;
        }
        (stream_id - 1) / self.streams_per_cache
    }

    pub fn slot(&self, stream_id: u32) -> Arc<CredentialSlot> {
        let id = self.slot_id(stream_id);
        self.slots
            .write()
            .entry(id)
            .or_insert_with(|| Arc::new(CredentialSlot::new()))
            .clone()
    }
}
