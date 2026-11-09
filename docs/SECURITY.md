# Security notes

- Config passwords are high-value secrets; the server DB currently stores them because it must derive packet keys. Protect `/var/lib/tots` and consider OS-backed secret encryption for production.
- XChaCha20-Poly1305 nonces are random 192-bit values; sequence numbers additionally provide replay rejection.
- The management API binds to loopback by default. Set `TOTS_ADMIN_TOKEN` if it is reachable by anything other than root-local tooling.
- Provider tokens and TURN credentials are never persisted by `tots-core`; they live in memory for path setup.
- The provider APIs are external dependencies and may change. Treat returned URLs/credentials as untrusted input.
- The project has not been independently audited.
