# Changelog

## 0.3.0 - 2026-08-28

- Added `VolatileStore` as a volatile reference/backend implementation.
- Added `LayeredStore` for ordered reads and replicated writes across injected stores.
- Added fail-closed checks for conflicting existing replicas.
- Added `get_many`, `put_many`, `invalidate`, and `invalidate_many`.
- Added explicit invalidation telemetry.
- Preserved the RivetCache v1 key and filesystem ABIs.
- Kept the core synchronous and runtime-neutral; no async runtime or external service is required.

## 0.2.0 - 2026-08-28

- Refactored the cache core to dependency-injected Rust traits.
- Added pluggable `PersistentStore`, `KeyStrategy`, `Clock`, `EvictionPolicy`, and `MetricsSink`.
- Added `ContextCacheBuilder` while preserving the original `ContextCache::new` constructor.
- Extracted the default filesystem persistent tier into `FileStore`.
- Added deterministic clock/TTL tests and injected-component tests.
- Retained the RivetCache v1 default key ABI and filesystem format.

## 0.1.0 - 2026-08-28

- Initial standalone MIT release.
- Memory and persistent disk tiers.
- Stable namespaced content-addressed keys.
- LRU eviction, TTL expiry, pinning, atomic writes, checksums, corruption recovery, restart persistence, clearing, and telemetry.
