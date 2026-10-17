//! Счётчики трафика.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Stats {
    tx: AtomicU64,
    rx: AtomicU64,
    wire_rx: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_tx(&self, n: u64) {
        self.tx.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_rx(&self, n: u64) {
        self.rx.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_wire_rx(&self, n: u64) {
        self.wire_rx.fetch_add(n, Ordering::Relaxed);
    }

    pub fn counters(&self) -> (u64, u64) {
        (
            self.tx.load(Ordering::Relaxed),
            self.rx.load(Ordering::Relaxed),
        )
    }

    pub fn liveness_rx(&self) -> u64 {
        self.rx.load(Ordering::Relaxed) + self.wire_rx.load(Ordering::Relaxed)
    }
}

pub fn format_byte_count(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
