# Worker scaling and throughput acceptance

Goal: encrypted VK and Jazz IPv4 VPN egress from this Windows machine through 201.58.191.113; at least 40 Mbit/s with 20–50 workers. Do not count direct UDP fallback or a unit benchmark as provider evidence.

Observed input: supplied speedtest 0.88 Mbit/s down, 1.53 up, 99 ms; supplied 20-worker log has VK API flood control, progressively rising Jazz probe RTT, partial pool startup.

1. Reproduce partial-readiness failure and fix full-pool startup before installing routes.
2. Share one Argon2-derived device key across independent replay/sequence sessions. Admit authenticated server sessions asynchronously under bounded concurrency.
3. Share and pace VK credential fetches in bounded cohorts, using one TURN allocation attempt per lease.
4. Move server congestion/pacing waits into bounded per-session queues so independent allocations run concurrently.
5. Correct recovery epochs so the same acknowledgement cannot exhaust retransmission retries.
6. Provision distinct Jazz rooms/bridges per worker, with bounded concurrent setup, persistence and complete rollback/revoke/expiry cleanup.
7. Verify targeted regressions, workspace tests and release builds. Deploy atomically after backing up binary/database.
8. Verify every requested worker, public IPv4 egress and repeated HTTPS speed measurements. Compare provider-only runs and retain machine-readable evidence. Package source, binaries, licenses and exact measured limitations.

Reference MRTT is studied for behavior only. Its PolyForm Noncommercial license is incompatible with direct source transplantation into Apache-2.0 ToTS; no source is copied.
