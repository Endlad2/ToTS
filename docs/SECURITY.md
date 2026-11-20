# Security notes

- Config passwords are high-value secrets; the server DB currently stores them because it must derive packet keys. Protect `/var/lib/tots` and consider OS-backed secret encryption for production.
- XChaCha20-Poly1305 nonces are random 192-bit values; sequence numbers additionally provide replay rejection.
- The management API binds to loopback by default. Set `TOTS_ADMIN_TOKEN` if it is reachable by anything other than root-local tooling.
- MAX `access_token` is stored inside `provider_data` in the server config database so the client can restore the provider after restart. Protect `/var/lib/tots` and treat the returned `tots://` URI as a bearer secret. The derived MAX call token, call `session_key`, WebRTC endpoint token and TURN credentials remain in process memory and are not logged or persisted by the provider.
- The provider APIs are external dependencies and may change. Treat returned URLs/credentials as untrusted input.
- The project has not been independently audited.
