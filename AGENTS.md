# Agent Guide — `dtnpm-cla-hqfbp-kiss`

## 🎯 Mission
Provide the Convergence Layer Adapter (CLA) for the Hamradio Quick File Broadcasting Protocol (HQFBP) over amateur radio channels (KISS TCP for Direwolf/Soundmodem, KISS Serial/TTY, and UDP for SDR), with Forward Error Correction support (Reed-Solomon and RaptorQ FEC).

---

## 🛡️ Security Frameworks and Requirements

### 1. Cyber Resilience Act (CRA) & Amateur Radio Security
- **Amateur Radio Identity Validation**:
  - Callsigns are normalized and strictly validated syntactically (`is_valid_callsign`: 3 to 10 ASCII alphanumeric characters, at least one letter, optional SSID `-0` to `-15`, strict rejection of `\r`, `\n`, `\0`).
- **Explicit Opt-In for Lab Testing**:
  - Operating with an unidentified callsign (blank or `N0CALL`) emits a warning by default and is only permitted for transmission upon explicit request via `--allow-unidentified-rf` or `require_callsign: false`.
  - The `--require-callsign` flag enforces a strictly valid callsign before any RF transmission.
- **Supply-Chain & VEX**:
  - Dependencies audited with `cargo-audit` and `cargo-deny`; VEX `not_affected` statement for `RUSTSEC-2026-0459`.
  - Uses `yaml_serde` (without libyaml).
  - GitHub CI actions pinned by full 40-character SHA-1.
- **Security Policy**: Documented in [SECURITY.md](SECURITY.md).

### 2. Protocol Robustness & Session Isolation
- **Cross-Station DoS Protection (`RxSessionManager`)**:
  - Evicting an old sender (`evict_oldest_sender`) checks the configured TTL (`self.session_ttl`) and must NEVER reset active sessions of other radio stations to zero.
  - Preventive cleanup of expired sessions (`sweep_expired`) before any capacity shedding or LRU eviction.
- **RF-Noise-Resilient Memory Accounting**:
  - Per-chunk accounting (`chunk_bytes: HashMap<(u32, u32), usize>`) on `SenderState` with deduplication deduction to prevent memory quota leaks during duplicated or noisy frames.
  - Enforced per-sender cap (`per_sender_byte_cap = max_in_flight_bytes / 2`).
- **KISS Protocol Escaping**:
  - Systematic escaping of control bytes `FEND` (`0xC0`) and `FESC` (`0xDB`) in `make_kiss_param_frame`.
- **Network Resilience**:
  - Automatic reconnection with exponential backoff (`reconnect_initial` to `reconnect_max`) upon TCP link failure with the TNC.

---

## 🦀 Rust Engineering Standards

1. **Zero Panic / Zero Unwrap Rule**:
   - All code under `src/` must be 100% free of `.unwrap()`, `.expect()`, and the `panic!` macro.
   - Strict linter: `-D warnings -D clippy::unwrap_used -D clippy::panic`.
2. **Mandatory ADRs**:
   - Any HQFBP/KISS architectural decision must be recorded under `adr/` and indexed in [adr/README.md](adr/README.md).
3. **Documentation Language**:
   - English must be used as the language for all documentation, unless the file is suffixed with `.fr.md` or another language is explicitly requested in the prompt.

---

## ⚡ Critical Verification Commands

- `cargo fmt --all -- --check`
- `cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic`
- `cargo test --offline --all-features`
- `./scripts/check-security.sh --strict`
- `./scripts/check-coverage.sh --summary-only`
