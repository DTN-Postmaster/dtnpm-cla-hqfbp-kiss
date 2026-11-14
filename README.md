# dtnpm-cla-hqfbp-kiss

Hamradio Quick File Broadcasting Protocol (HQFBP) Convergence Layer Adapter (CLA) for Hardy BPv7 and **DTN Postmaster** (`dtnpm`).

## Overview

This workspace provides an RFC 9171 CLA implementing HQFBP transport (`hqfbp-rs`) over amateur radio physical channels including KISS over TCP (Direwolf / Soundmodem), KISS over Serial (`serial` feature), and UDP datagrams.

## Features

- **Amateur Radio Addressing**: Translates BPv7 `ClaAddress` and amateur callsigns/SSIDs with native `"CQ"` broadcast support.
- **Multiple Transports**:
  - **KISS over TCP**: Connects to Direwolf, Soundmodem, or networked TNCs with hardware framing parameters (`TxDelay`, `Persistence`, `SlotTime`).
  - **KISS over Serial / TTY**: RS-232 / USB TNC support via `tokio-serial` (`serial` feature).
  - **Direct UDP**: Low-overhead datagram encapsulation for SDR / flowgraph links and loopback tests.
- **Dynamic Content Encoding Pipeline**: Configurable hailing/bundle pipelines (Gzip, chunking, Reed-Solomon, RaptorQ) and runtime per-peer/per-bundle overrides.
- **Standalone Daemon**: Can run embedded in `dtnpm` or standalone (`dtnpm-cla-hqfbp-kiss` with default `cli` feature) connected to a BPA node via gRPC.

## Configuration

In `dtnpm` configuration files under `clas`:

```yaml
clas:
  - name: kiss-tnc-vhf
    type: hqfbp
    callsign: "F4JXQ-1"
    transport:
      type: tcp
      address: "127.0.0.1:8001"
```

## Build and Test

Requires sibling checkouts of `../dtnpm-core`, `../../hardy/*`, and `../../hqfbp-rs` as referenced in `Cargo.toml`.

```bash
cargo test --workspace --exclude dtnpm-cla-hqfbp-kiss-fuzz
```

## License

Dual-licensed under the Apache License, Version 2.0 or the MIT License. See [LICENSE.md](LICENSE.md).
