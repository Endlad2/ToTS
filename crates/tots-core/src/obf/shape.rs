//! Пейсинг пакетов с джиттером и всплесками — мимикрия под аудио.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rand::Rng;

pub struct Shaper {
    interval: Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    last_send: Instant,
    burst: u32,
}

impl Shaper {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            inner: Mutex::new(Inner {
                last_send: Instant::now(),
                burst: 0,
            }),
        }
    }

    pub async fn wait(&self) {
        if self.interval.is_zero() {
            return;
        }

        let sleep_for = {
            let mut inner = self.inner.lock().unwrap();
            if inner.burst > 0 {
                inner.burst -= 1;
                inner.last_send = Instant::now();
                return;
            }

            let mut rng = rand::thread_rng();
            if rng.gen_range(0..100) < 30 {
                inner.burst = rng.gen_range(0..3);
                inner.last_send = Instant::now();
                return;
            }

            let now = Instant::now();
            let next_slot = inner.last_send + self.interval;
            let wait = next_slot.saturating_duration_since(now);

            let jitter_span = (self.interval.as_nanos() as f64 * 0.10) as u64;
            let jitter = if jitter_span > 0 {
                Duration::from_nanos(
                    rng.gen_range(0..=jitter_span * 2)
                        .saturating_sub(jitter_span),
                )
            } else {
                Duration::ZERO
            };

            inner.last_send = now + wait + jitter;
            wait + jitter
        };

        if !sleep_for.is_zero() {
            tokio::time::sleep(sleep_for).await;
        }
    }
}
