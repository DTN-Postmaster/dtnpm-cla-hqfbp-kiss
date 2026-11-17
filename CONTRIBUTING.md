# Contributing Guide — `dtnpm-cla-hqfbp-kiss`

This guide outlines the development rules and security requirements for the HQFBP and KISS CLA in `dtnpm-cla-hqfbp-kiss`.

---

## 1. Core Principles

- **Zero Panic / Zero Unwrap**: No `.unwrap()` or `.expect()` calls in production code.
- **RF Session Security and Bounds**:
  - Multi-sender session management must isolate stations so that interference on one link does not affect others.
  - Defragmentation memory must be strictly bounded and released when replacing duplicate packets.
- **Architectural Traceability (ADR)**:
  - Any protocol addition or adjustment must be documented in an ADR file under `adr/` and indexed in `adr/README.md`.

---

## 2. Local Verification

```bash
cargo fmt --all -- --check
cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic
cargo test --offline --all-features
./scripts/check-security.sh --strict
```
