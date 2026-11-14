# ADR 0001: HQFBP Protocol, Encoding Pipelines, and KISS Framing Design

- **Status**: Accepted
- **Date**: 2026-11-14

## Context

Amateur radio VHF/HF packet links (1200/9600 bps AFSK/GMSK and half-duplex TNCs) suffer from high packet loss, lack of end-to-end return channels in broadcast modes, and narrow KISS serial/TCP interfaces such as DireWolf. Hardy BPv7 bundles frequently exceed single-frame MTUs and require forward error correction (FEC), chunking, and strict channel pacing.

## Decision

1. **HQFBP Convergence Layer (`HqfbpCla` / `Driver`)**: Wrap `hqfbp-rs` (`PDUGenerator`, `Deframer`, `KissDecoder`, `encode_kiss_frame`) behind a Hardy `Cla` actor so BPv7 bundles can be segmented, CBOR-headered, protected by composable encoding pipelines (`ContentEncoding`: `Chunk`, `H`, `Crc32`, `Deflate`, `Brotli`, `ReedSolomon`, `RaptorQ`, `LT`, `Repeat`), and dispatched over:
   - `TransportMode::Udp` for IP packet radio / emulator testing,
   - `TransportMode::KissTcp` for software TNCs such as DireWolf, and
   - `TransportMode::KissSerial` (optional `serial` feature) for hardware TNCs.
2. **Hailing & Dynamic Per-Peer Encoding Resolution**: Transmit a hailing header (`hailing_encodings`) alongside post-`H` FEC payloads, register `__hailing__` and `__default__` encoding lists consistently across all three transports (including `SetDefaultEncodings` in `run_kiss_tcp`), and support `DynamicEncodingSelector` overrides per destination callsign or lane.
3. **KISS Parameter Escaping & Pacing**: Escape `FEND` (`0xC0`) and `FESC` (`0xDB`) in both the command byte and value byte inside `make_kiss_param_frame` so parameter values like `0xC0` or `0xDB` cannot prematurely terminate KISS frames, and enforce byte-level token-bucket pacing (`LeakyBucket`) on KISS writes.
4. **Resilient KISS TCP Reconnect**: Treat initial `TcpSocket::connect` refusal identically to mid-session disconnects by reporting a bound placeholder address to BPA registration and continuing exponential backoff retries until the TNC becomes reachable.

## Consequences

- DireWolf and hardware KISS TNCs can be started in any order relative to `dtnpm-cla-hqfbp-kiss`.
- All three transports share one `Driver::handle_control_command` path, eliminating drift in default/peer encoding registration and peer unregistration (removing both `ClaAddress::Private` and `ClaAddress::Tcp` entries).
- Pure-Rust YAML configuration parsing uses `yaml_serde` (`RUSTSEC-2024-0449` eliminated).
