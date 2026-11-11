# ToTS wire protocol v1

Every UDP datagram is:

`MAGIC(4) | VERSION(1) | DEVICE_UUID(16) | SESSION_ID(8) | SEQUENCE(8) | NONCE(24) | XCHACHA20POLY1305(frame)`

All integer fields are network byte order. The bytes before `NONCE` are AEAD associated data. Replay protection uses a 128-packet sliding sequence window per active worker session. Workers of one device intentionally use different `session_id` values.

Encrypted frame types:

- `0x01 Data`: association id, packet id, fragment count/index, original length, fragment length, bytes.
- `0x02 Ack`: legacy single packet ACK, retained for compatibility.
- `0x03 Heartbeat`: counter.
- `0x04 Probe`: token + sender timestamp.
- `0x05 ProbeAck`: reflected probe fields.
- `0x06 Dissociate`: association id.
- `0x07 AckRanges`: up to 32 inclusive packet-id ranges.

## Transport engine

ToTS is not QUIC- or TUIC-wire-compatible. It implements the transport mechanisms needed by this tunnel independently above `DatagramPath`:

- stable IPv4 five-tuple associations; small UDP is worker-pinned for low jitter while TCP/large or high-rate UDP is striped across workers at bounded flowlet boundaries;
- ACK ranges and packet-number RTT samples;
- congestion window and pacing;
- time-based loss detection;
- bounded retransmission for `Reliable` packets;
- `Unreliable` mode for latency-sensitive/inner-reliable traffic;
- duplicate packet-id suppression after a recovered packet is delivered;
- fresh encryption sequence numbers on every retransmission, preserving replay protection;
- authenticated path migration: a worker keeps its SessionEngine while rebuilding or re-probing a provider path, so the first packet on the new path rebinds the server route without a separate handshake.

Inner TCP and UDP default to `Unreliable` ToTS recovery to avoid double retransmission and head-of-line amplification. Control/other IP protocols default to bounded `Reliable` recovery.

The encrypted ToTS datagram budget is capped at 1200 bytes before provider envelopes. TURN adds RTP/channel-data outside that budget. This is deliberately more conservative than forcing a 1420-byte media payload and avoids outer IP fragmentation on common paths.

### Allocation scheduling estimate

The multi-worker scheduler uses 247,000 bytes/s (about 2 Mbit/s) as the nominal capacity weight of one provider allocation. It is not a hard limiter. Each worker has a bounded token/deficit budget and an adaptive weight that is temporarily reduced when its queue/path is congested. Client and server apply the same flowlet policy so both upload and download can aggregate several independent sessions without per-packet round-robin reordering.
