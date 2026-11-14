use serde_json::Value;
use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tots_core::{
    providers::{Provider, ProviderBootstrap},
    Error, Result,
};

// Space VK API logins rather than issuing a burst that triggers anonymous flood control.
// One fetched credential generation is leased to a small worker cohort; each worker still
// creates an independent UDP socket/TURN allocation, matching the safe cohort model used by MRTT.
const FETCH_INTERVAL: Duration = Duration::from_secs(3);
/// One VK bootstrap/credential generation can back several independent TURN allocations.
/// Keeping this equal to startup parallelism avoids 20 sequential VK API logins while each
/// worker still owns its own UDP socket and TURN allocation. MRTT uses the same cohort idea.
pub const VK_WORKERS_PER_BOOTSTRAP: usize = 7;
const RETRY_DELAY: Duration = Duration::from_secs(2);
const FLOOD_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_OWNED_GENERATIONS: usize = 256;

struct OwnedBootstrap {
    data: Value,
    bootstrap: ProviderBootstrap,
}

#[derive(Clone)]
struct SharedBootstrap {
    bootstrap: ProviderBootstrap,
    leases_left: usize,
}

struct State {
    owned: Vec<OwnedBootstrap>,
    next_link: usize,
    not_before: Instant,
    failed: bool,
    flood_streak: u32,
    closed: bool,
    credential_fingerprints: HashSet<u64>,
    shared: Option<SharedBootstrap>,
}

/// Serializes VK bootstrap and owns created calls for one immutable configuration.
pub struct BootstrapPool {
    state: Mutex<State>,
    stopped: std::sync::atomic::AtomicBool,
    credentials_seen: AtomicUsize,
}

impl BootstrapPool {
    pub fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn credential_count(&self) -> usize {
        self.credentials_seen.load(Ordering::Relaxed)
    }
    pub fn new() -> Self {
        Self {
            stopped: std::sync::atomic::AtomicBool::new(false),
            credentials_seen: AtomicUsize::new(0),
            state: Mutex::new(State {
                owned: Vec::new(),
                next_link: 0,
                not_before: Instant::now(),
                failed: false,
                flood_streak: 0,
                closed: false,
                credential_fingerprints: HashSet::new(),
                shared: None,
            }),
        }
    }

    pub async fn acquire(
        &self,
        provider: &dyn Provider,
        data: &Value,
    ) -> Result<ProviderBootstrap> {
        // The lock makes credential fetches single-flight, including failed fetches.
        let mut state = self.state.lock().await;
        if state.closed || self.stopped.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Provider("VK bootstrap pool is closed".into()));
        }
        if state.failed && Instant::now() < state.not_before {
            return Err(Error::Provider("VK bootstrap retry is delayed".into()));
        }
        if let Some(shared) = state.shared.as_mut() {
            if shared.leases_left > 0 {
                shared.leases_left -= 1;
                return Ok(shared.bootstrap.clone());
            }
            state.shared = None;
        }
        // Old calls may still carry live workers; never evict them by force-finishing.
        if state.owned.len() >= MAX_OWNED_GENERATIONS {
            return Err(Error::Provider(
                "VK owned-call limit reached; restart the client to renew".into(),
            ));
        }
        if Instant::now() < state.not_before {
            tokio::time::sleep_until(state.not_before).await;
        }
        if self.stopped.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Provider("VK bootstrap pool is closed".into()));
        }
        let mut selected = data.clone();
        if let Some(links) = data
            .get("vk_call_links")
            .and_then(Value::as_array)
            .filter(|links| !links.is_empty())
        {
            let link = links[state.next_link % links.len()].clone();
            selected["vk_call_links"] = Value::Array(vec![link]);
            state.next_link = state.next_link.wrapping_add(1);
        }
        match provider.bootstrap(&selected).await {
            Ok(bootstrap) => {
                for credential in &bootstrap.turn {
                    if state.credential_fingerprints.len() < 256 {
                        let mut fingerprint = DefaultHasher::new();
                        credential.username.hash(&mut fingerprint);
                        credential.password.hash(&mut fingerprint);
                        state.credential_fingerprints.insert(fingerprint.finish());
                    }
                }
                self.credentials_seen
                    .store(state.credential_fingerprints.len(), Ordering::Relaxed);
                if bootstrap
                    .signalling
                    .get("created_call_ids")
                    .and_then(Value::as_array)
                    .is_some_and(|ids| !ids.is_empty())
                {
                    state.owned.push(OwnedBootstrap {
                        data: selected,
                        bootstrap: bootstrap.clone(),
                    });
                }
                state.failed = false;
                state.flood_streak = 0;
                state.not_before = Instant::now() + FETCH_INTERVAL;
                state.shared = Some(SharedBootstrap {
                    bootstrap: bootstrap.clone(),
                    leases_left: VK_WORKERS_PER_BOOTSTRAP.saturating_sub(1),
                });
                Ok(bootstrap)
            }
            Err(error) => {
                let message = error.to_string().to_ascii_lowercase();
                let compact: String = message.chars().filter(|ch| !ch.is_whitespace()).collect();
                let flood = ["flood", "rate limit", "too many", "429"]
                    .iter()
                    .any(|marker| message.contains(marker))
                    || ["\"error_code\":6", "\"error_code\":9", "\"error_code\":29"]
                        .iter()
                        .any(|marker| compact.contains(marker));
                state.failed = true;
                let delay = if flood {
                    state.flood_streak = state.flood_streak.saturating_add(1);
                    FLOOD_RETRY_DELAY
                        .saturating_mul(1 << state.flood_streak.saturating_sub(1).min(4))
                        .min(Duration::from_secs(300))
                } else {
                    state.flood_streak = 0;
                    RETRY_DELAY
                };
                state.not_before = Instant::now() + delay;
                // Provider errors may contain response bodies, URLs or credentials.
                Err(Error::Provider(if flood {
                    format!("VK flood control; retry after {} seconds", delay.as_secs())
                } else {
                    "VK bootstrap failed; retry after 2 seconds".into()
                }))
            }
        }
    }

    pub async fn cleanup(&self, provider: &dyn Provider) -> Result<()> {
        self.stop();
        let owned = {
            let mut state = self.state.lock().await;
            state.closed = true;
            std::mem::take(&mut state.owned)
        };
        let mut failed = false;
        for owner in owned {
            failed |= provider
                .cleanup(&owner.data, &owner.bootstrap)
                .await
                .is_err();
        }
        if failed {
            Err(Error::Provider("VK owned-call cleanup failed".into()))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    #[derive(Default)]
    struct FakeProvider {
        calls: AtomicUsize,
        cleaned: AtomicUsize,
        inputs: Mutex<Vec<Value>>,
        fetch_times: Mutex<Vec<Instant>>,
        fail: bool,
        owned: bool,
    }

    #[tokio::test]
    async fn shutdown_stops_a_queued_credential_fetch() {
        let pool = Arc::new(BootstrapPool::new());
        let provider = Arc::new(FakeProvider::default());
        let held = pool.state.lock().await;
        let task = {
            let pool = pool.clone();
            let provider = provider.clone();
            tokio::spawn(async move { pool.acquire(provider.as_ref(), &json!({})).await })
        };
        tokio::task::yield_now().await;
        pool.stop();
        drop(held);
        assert!(task.await.unwrap().is_err());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }

    impl Provider for FakeProvider {
        fn bootstrap<'a, 'b, 'c>(
            &'a self,
            data: &'b Value,
        ) -> Pin<Box<dyn Future<Output = Result<ProviderBootstrap>> + Send + 'c>>
        where
            'a: 'c,
            'b: 'c,
            Self: 'c,
        {
            Box::pin(async move {
                let generation = self.calls.fetch_add(1, Ordering::SeqCst);
                self.inputs.lock().await.push(data.clone());
                self.fetch_times.lock().await.push(Instant::now());
                tokio::task::yield_now().await;
                if self.fail {
                    return Err(tots_core::Error::Provider("VK flood control".into()));
                }
                Ok(ProviderBootstrap {
                    turn: vec![tots_core::transport::turn::TurnCredentials {
                        urls: vec!["turn:127.0.0.1:3478".into()],
                        username: format!("test-user-{generation}"),
                        password: "test-credential".into(),
                    }],
                    signalling: if self.owned {
                        json!({"created_call_ids": [format!("call-{generation}")]})
                    } else {
                        json!({"created_call_ids": []})
                    },
                    ..Default::default()
                })
            })
        }

        fn cleanup<'a, 'b, 'c, 'd>(
            &'a self,
            _data: &'b Value,
            _bootstrap: &'c ProviderBootstrap,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'd>>
        where
            'a: 'd,
            'b: 'd,
            'c: 'd,
            Self: 'd,
        {
            Box::pin(async move {
                self.cleaned.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn thirty_workers_share_paced_bootstraps_in_seven_worker_cohorts() {
        let provider = Arc::new(FakeProvider::default());
        let pool = Arc::new(BootstrapPool::new());
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..30 {
            let provider = provider.clone();
            let pool = pool.clone();
            workers.spawn(async move {
                let bootstrap = pool
                    .acquire(
                        provider.as_ref(),
                        &json!({"vk_call_links":["call-a","call-b","call-c","call-d"]}),
                    )
                    .await
                    .unwrap();
                bootstrap.turn[0].username.clone()
            });
        }
        let mut leases = std::collections::BTreeMap::new();
        while let Some(result) = workers.join_next().await {
            *leases.entry(result.unwrap()).or_insert(0usize) += 1;
        }
        let generations = 30usize.div_ceil(VK_WORKERS_PER_BOOTSTRAP);
        assert_eq!(provider.calls.load(Ordering::SeqCst), generations);
        assert_eq!(pool.credential_count(), generations);
        assert_eq!(leases.len(), generations);
        assert!(leases.values().all(|count| *count <= VK_WORKERS_PER_BOOTSTRAP));
        assert_eq!(leases.values().sum::<usize>(), 30);
        let times = provider.fetch_times.lock().await;
        assert!(times
            .windows(2)
            .all(|pair| pair[1] - pair[0] >= Duration::from_secs(3)));
        let inputs = provider.inputs.lock().await;
        let selected: Vec<_> = inputs
            .iter()
            .map(|v| v["vk_call_links"][0].as_str().unwrap())
            .collect();
        let expected: Vec<_> = (0..generations)
            .map(|i| ["call-a", "call-b", "call-c", "call-d"][i % 4])
            .collect();
        assert_eq!(selected, expected);
        assert!(inputs
            .iter()
            .all(|v| v["vk_call_links"].as_array().unwrap().len() == 1));
    }

    #[tokio::test]
    async fn simultaneous_failures_share_the_provider_cooldown() {
        let provider = Arc::new(FakeProvider {
            fail: true,
            ..Default::default()
        });
        let pool = Arc::new(BootstrapPool::new());
        let error = pool
            .acquire(provider.as_ref(), &json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("flood") && error.contains("30"));
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..40 {
            let provider = provider.clone();
            let pool = pool.clone();
            workers.spawn(async move {
                assert!(pool.acquire(provider.as_ref(), &json!({})).await.is_err());
            });
        }
        while let Some(result) = workers.join_next().await {
            result.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(pool.acquire(provider.as_ref(), &json!({})).await.is_err());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn repeated_flood_responses_extend_the_shared_cooldown() {
        let provider = FakeProvider {
            fail: true,
            ..Default::default()
        };
        let pool = BootstrapPool::new();
        for expected_seconds in [30, 60, 120, 240, 300, 300] {
            // Model expiration of the prior cooldown without issuing requests early.
            pool.state.lock().await.not_before = Instant::now();
            let error = pool.acquire(&provider, &json!({})).await.unwrap_err();
            assert!(error
                .to_string()
                .contains(&format!("{expected_seconds} seconds")));
            let wait = pool.state.lock().await.not_before - Instant::now();
            assert!(wait >= Duration::from_secs(expected_seconds - 1));
            assert!(pool.acquire(&provider, &json!({})).await.is_err());
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cleans_owned_calls_once_after_all_leases() {
        let provider = FakeProvider {
            owned: true,
            ..Default::default()
        };
        let pool = BootstrapPool::new();
        for _ in 0..3 {
            pool.acquire(&provider, &json!({})).await.unwrap();
        }
        assert_eq!(provider.cleaned.load(Ordering::SeqCst), 0);
        pool.cleanup(&provider).await.unwrap();
        pool.cleanup(&provider).await.unwrap();
        assert_eq!(provider.cleaned.load(Ordering::SeqCst), 1);
        assert!(pool.acquire(&provider, &json!({})).await.is_err());
    }
}
