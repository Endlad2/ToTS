# ToTS wire protocol v1

Every UDP datagram is:

`MAGIC(4) | VERSION(1) | DEVICE_UUID(16) | SESSION_ID(8) | SEQUENCE(8) | NONCE(24) | XCHACHA20POLY1305(frame)`

All integer fields are network byte order. The bytes before `NONCE` are AEAD associated data. Replay protection uses a 128-packet sliding sequence window per active session.

Encrypted frame types:

- `0x01 Data`: association id, packet id, fragment count/index, original length, fragment length, bytes.
- `0x02 Ack`: packet id + receiver timing field.
- `0x03 Heartbeat`: monotonically increasing counter.
- `0x04 Probe`: random token + sender timestamp.
- `0x05 ProbeAck`: reflected probe fields.
- `0x06 Dissociate`: association id.

The protocol is not TUIC-compatible. It independently uses association/packet IDs and fragmentation because those concepts fit a high-throughput datagram tunnel.
