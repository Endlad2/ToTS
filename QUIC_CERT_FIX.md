# Nested QUIC certificate fix

The previous SOCKS5-over-QUIC build used the same self-signed certificate both as
a root trust anchor and as the server end-entity certificate. That certificate had
`Basic Constraints: CA=TRUE`, so rustls/webpki rejected it as a TLS server leaf
with `CaUsedAsEndEntity`.

This build separates the roles:

- `tots-core/src/quic_ca_cert.der`: embedded root CA trusted by the client.
- `tots-server/src/quic_cert.der`: `CA=FALSE` server leaf for
  `tots-quic.internal`.
- `tots-server/src/quic_key.der`: PKCS#8 private key matching that leaf.
- The leaf has SAN `DNS:tots-quic.internal` and EKU `serverAuth`.

The outer VK/WebRTC carrier is unchanged. Update both `tots-client` and
`tots-server` from this build because the embedded nested-QUIC trust material
changed.
