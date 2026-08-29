# Certification

RivetCache is validated independently as a standalone Rust crate.

## Standalone gates

Every release candidate must pass on Linux, Windows, and macOS:

- `cargo fmt --all -- --check`
- `cargo test --all-targets`
- `cargo clippy --all-targets -- -D warnings`

The test suite covers stable and isolated keys, memory LRU eviction, restart persistence, corrupt-disk recovery, TTL expiration, injected components, layered-store replication and conflict rejection, batch operations, explicit invalidation, KV block identity, prefix lookup, tier promotion, targeted movement, background prefetch, write-through, metadata preservation, and KV TTL behavior.

## Claim scope

Public descriptions of RivetCache should state implemented and tested capabilities directly. Compatibility, interoperability, equivalence, or feature-comparison claims involving third-party products require separate documented evidence and are not implied by this certification.

## Production provenance

The cache design was extracted from a production-proven Rust cache subsystem on August 28, 2026. The production integration exercised stable namespaced keys, memory hit/miss behavior, LRU eviction, restart persistence, explicit clearing/invalidation, corruption recovery, TTL expiry, deterministic replay, request isolation, stochastic bypass, and runtime prefix/KV reuse.

The standalone project uses its own `RIVET_CACHE_V1` key domain and `RIVET01` disk format, so its public ABI is independent and intentionally versioned from release 0.1.0 onward.
