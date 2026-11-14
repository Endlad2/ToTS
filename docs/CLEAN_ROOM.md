# Clean-room note

The supplied reference archives were used to understand desired behavior and project requirements, not as source material to copy. The archive identified MRTT under PolyForm Noncommercial 1.0.0 and the supplied TUIC implementation under GPLv3. ToTS therefore defines its own wire format and code structure, and does not vendor or transplant those sources into the Apache-2.0 tree.

## OpenFlux integration

The provided OpenFlux reference archive is GPLv3. ToTS does not copy its native Yandex/Cups/Mail.ru transport implementations into the Apache-2.0 crates. The integration boundary is process-level: a separately licensed OpenFlux executable exposes SOCKS5 UDP ASSOCIATE on localhost, while ToTS independently implements the SOCKS5 client adapter and continues to use its own encrypted datagram/session protocol. Carrier names, documented command-line behavior and public interoperability requirements are treated as interface facts; OpenFlux source files are not vendored into this repository.
