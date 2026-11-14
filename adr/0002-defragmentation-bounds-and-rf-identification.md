# ADR 0002: Bounded Defragmentation Memory Accounting and RF Identification Policy

- **Status**: Accepted
- **Date**: 2026-11-14

## Context

Two security/reliability risks arise in an RF convergence layer accepting unauthenticated broadcast frames:

1. **Reassembly State Exhaustion & Accounting Drift**: Without per-sender fairness limits, exact duplicate-chunk accounting, and selective TTL expiration, a single noisy or malicious station can consume 100% of reassembly memory, inflate `buffered_bytes` via duplicate chunks/payloads, or trigger LRU eviction that clears live deframer sessions belonging to other transmitters (`cleanup_expired(Duration::ZERO)`).
2. **Unidentified RF Transmission**: Amateur radio regulations require station identification, yet integration tests, receive-only monitors, and bench labs frequently default to `None` or `N0CALL`.

## Decision

1. **Selective TTL Purge & Fair-Share Caps (`RxSessionManager`)**:
   - Enforce a per-sender byte cap equal to `max_in_flight_bytes / 2` (`per_sender_byte_cap()`) alongside `max_concurrent_senders` and `max_in_flight_bytes`.
   - When sender count, per-sender memory, or global memory limits are reached, sweep expired sessions using `self.session_ttl` first and run LRU eviction only if still over capacity.
   - Inside `evict_oldest_sender`, pass `self.session_ttl` (instead of `Duration::ZERO`) to `Deframer::cleanup_expired` so live sessions of remaining active stations stay intact.
   - Track per-chunk byte counts (`SenderState::chunk_bytes`) so retransmitted chunks deduct prior chunk bytes before re-accounting, and always deduct finished session bytes before skipping duplicate payload hashes.
2. **Warn-by-Default RF Callsign Policy with Strict Opt-In**:
   - Default `Config::require_callsign` to `false`: missing or `N0CALL` callsigns emit a `tracing::warn!` while allowing existing bench setups and tests to run.
   - Support strict rejection (`Error::InvalidCallsign`) via `require-callsign: true` or CLI `--require-callsign`, and explicit override via `--allow-unidentified-rf`.
   - Support `--token-file <PATH>` (`DTN_HDY_TOKEN_FILE`) for passing gRPC Bearer tokens without exposing secrets in process arguments.

## Consequences

- `RxSessionManager::buffered_bytes()` stays synchronized across partial completions, duplicate chunks, duplicate payloads, TTL sweeps, and evictions.
- A single sender can consume at most 50% of `max_in_flight_bytes`, leaving headroom for concurrent transmitters.
- Operators deploying on live RF can enforce callsign presence in CI/systemd units via `--require-callsign` and `--token-file`.
