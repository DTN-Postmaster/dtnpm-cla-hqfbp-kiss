# Security Policy — `dtnpm-cla-hqfbp-kiss`

## Supported Versions

| Version | Supported | Security Updates |
|---------|-----------|------------------|
| 0.2.x   | Yes       | Active           |
| < 0.2   | No        | End of Life      |

## Security Architecture & EU Cyber Resilience Act (CRA) Alignment

`dtnpm-cla-hqfbp-kiss` implements the HQFBP/KISS Convergence Layer Adapter for Hardy BPv7 (`dtn-postmaster`) over UDP, KISS-over-TCP (e.g., DireWolf), and KISS Serial TNCs:

- **Memory Safety & Panic-Free Production Targets**: Written in Rust (`edition = "2024"`) and verified in CI with `-D warnings -D clippy::unwrap_used -D clippy::panic`.
- **Bounded Defragmentation & Fair-Share Limits (`src/session.rs`)**:
  - `RxSessionManager` bounds total in-flight memory (`max_in_flight_bytes`, default 16 MiB), concurrent senders (`max_concurrent_senders`, default 64), and per-sender in-flight memory (`max_in_flight_bytes / 2`) so a single transmitter cannot starve other stations.
  - TTL-expired sessions (`session_ttl`) are swept before evicting active senders, and LRU eviction uses `self.session_ttl` rather than `Duration::ZERO` so active sessions of other stations are preserved.
  - Duplicate chunk arrivals deduct prior chunk accounting (`chunk_bytes`) and duplicate payload hashes still release allocated session accounting so `buffered_bytes()` cannot drift or leak.
- **RF Identification Policy (`src/config.rs`, `src/main.rs`)**: Missing callsigns and placeholder `N0CALL` log a warning by default to preserve receive-only and local test compatibility; strict compliance enforcement is available via `require-callsign: true` in config or `--require-callsign` (and explicit opt-out via `--allow-unidentified-rf`).
- **KISS Framing Integrity & Resilient Reconnect (`src/driver.rs`)**: KISS parameter frames (`TXDELAY`, `PERSISTENCE`, `SLOTTIME`, `TXTAIL`) escape `FEND`/`FESC` octets before framing, and initial KISS TCP failures retry with exponential backoff.
- **Secret Handling**: Standalone CLI authentication supports `--token-file <PATH>` (`DTN_HDY_TOKEN_FILE`) to avoid passing Bearer tokens on process command lines, alongside Unix Domain Socket transport (`--socket` / `DTN_HDY_SOCKET`).
- **Supply Chain & VEX**: Replaced `serde_yaml` (`unsafe-libyaml`, `RUSTSEC-2024-0449`) with pure-Rust `yaml_serde`. `RUSTSEC-2026-0459` (`libsqlite3-sys` SQLite `session` extension) is documented as VEX Not Affected in `.cargo/audit.toml` and `deny.toml` because transitive `dtnpm-core` enables only `["array", "bundled", "time"]` without the SQLite `session` extension.
- **SBOM**: Generated on every CI run in CycloneDX JSON format via `anchore/sbom-action`.

## Reporting a Vulnerability

Do **not** open a public GitHub issue for security vulnerabilities.
Report vulnerabilities privately via GitHub Security Advisories for `dtn-postmaster/dtnpm-cla-hqfbp-kiss` or by contacting the maintainers directly. Please include:

- Component version and commit hash
- Transport mode (`udp`, `kiss-tcp`, `kiss-serial`) and relevant config (`require-callsign`, `max-in-flight-bytes`, etc.)
- Minimal proof-of-concept frames or reproduction steps

We aim to acknowledge reports within 72 hours and coordinate a fix before public disclosure.
