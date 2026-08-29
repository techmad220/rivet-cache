# Changelog

## 0.6.0 - 2026-08-28

- Added an optional std-only llama-server slot persistence bridge backed by `ContextCache`.
- Added `rivet-cache-llama-slot` for capture, restore, erase, and cache-presence operations across a local/trusted llama-server process boundary.
- Added content-addressed slot identity scoped by model fingerprint and logical state identity under `LLAMA_SERVER_SLOT_V1`.
- Added SHA-256 state receipts and byte-count cross-checks against server save/restore responses.
- Added bounded state and HTTP response sizes, strict basename validation, regular-file checks, canonical root containment, symlink rejection, and atomic restore-file materialization.
- Added deterministic unit coverage for cache round trips, the HTTP slot-action contract, chunked responses, and unsafe-path rejection.
- Kept llama-server integration optional; the RivetCache core gains no mandatory inference-engine, GPU SDK, HTTP framework, or async-runtime dependency.
- Hardware-certified persistent checkpoint reuse on an AMD Radeon RX 6800 XT with Vulkan using exact RivetCache source `45a32bd3e9ab6cabf552b7a3969c49dea9b0cd5f` and exact checkpoint-preserving llama.cpp source `06d9d0ff54b586514a59268e2c780abc08473daa`.
- The certified divergent-prefix test reduced prompt work from `1280` cold tokens to `38` tokens after restoration with `cache_n=1244`; the 555,059,324-byte persisted state and deterministic output both matched their live-control receipts exactly.
- Runtime compatibility remains scoped to separately recorded exact-build certification; RivetCache does not infer reusable semantics from a successful byte restore alone.

## 0.5.0 - 2026-08-28

- Added a standalone std-only TCP KV service and `TcpKvTier` client with bounded payloads, connection limits, timeouts, health checks, clear, get, put, and remove operations.
- Added `DeviceKvTier`, a device-buffer ownership layer, a host reference backend, and an unsafe opt-in FFI callback bridge for accelerator runtimes.
- Added a llama.cpp host-callback runtime adapter with capture, restore, health, and relocated restore for exact reusable segments.
- Added `SegmentIndex` and `restore_reuse` for deterministic block-aligned non-prefix exact segment reuse.
- Added tier health, clear, and pin/unpin control operations to `KvEngine`.
- Kept network/device/runtime integrations optional and dependency-injected; the core remains free of mandatory async, networking framework, GPU SDK, or inference-engine dependencies.

## 0.4.0 - 2026-08-28

- Added runtime-neutral KV block identities scoped by model, token prefix, layer range, block range, and layout version.
- Added `KvEngine` with ordered tiers, configurable read promotion, targeted movement, invalidation, and write-through policy.
- Added background prefetch using the Rust standard library; no async runtime is required.
- Added injected `KvTransport` and `KvAllocator` boundaries for host/device/network-specific implementations.
- Added `ContextCacheTier` adapter so existing RivetCache instances can serve as KV tiers.
- Added metadata-preserving KV envelopes so TTL and pin state survive tier movement.
- Added `PrefixIndex` for deterministic longest-prefix lookup.
- Added `RuntimeKvAdapter` and `KvCaptureRequest` contracts for runtime-specific capture/restore plugins.
- Added KV engine telemetry for hits, misses, writes, promotions, transfers, bytes moved, invalidations, expirations, and prefetches.
- Added deterministic tests for key identity, prefix lookup, promotion, prefetch, movement, write-through, metadata preservation, and TTL.
- Hardened write-through with best-effort rollback, expired-destination prefetch replacement, capture-range overflow checks, runtime adapter identity validation, and cross-model prefix-index validation.

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
