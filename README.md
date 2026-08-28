# RivetCache

**RivetCache** is a small, dependency-light, DI-first Rust cache for high-speed reuse of opaque artifacts.

The default configuration combines a bounded in-memory tier with optional bounded filesystem persistence. The core is not tied to that implementation: persistent storage, key generation, clock/TTL semantics, eviction policy, and telemetry are injected through Rust traits.

## Why RivetCache

- 100% Rust core.
- Stable SHA-256 content-addressed keys by default.
- Bounded in-memory caching.
- Optional bounded persistent storage.
- LRU by default, with injectable eviction policy.
- Injectable persistent backend for remote/shared/custom stores.
- Injectable clock for deterministic TTL behavior.
- Injectable key strategy.
- Injectable metrics sink.
- Per-entry TTL and non-expiring entries.
- Pinned entries protected from quota eviction.
- Atomic filesystem writes.
- SHA-256 payload checksums and corruption recovery.
- Restart persistence and index reconstruction.
- No async runtime requirement.
- No Python dependency.
- No inference-engine or framework lock-in.

The cache stores opaque bytes. Consumers decide whether those bytes are model responses, rendered prompts, embeddings, serialized state, build artifacts, API responses, or other data.

## Zero-config path

The original constructor remains available:

```rust
use rivet_cache::ContextCache;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let cache = ContextCache::new(
        Some("./cache".into()),
        64 * 1024 * 1024,
        512 * 1024 * 1024,
        Duration::from_secs(600),
    )?;

    let key = ContextCache::key("completion/v1", "model-fingerprint", "canonical-request");
    cache.put(&key, b"cached result", None, false)?;
    assert_eq!(cache.get(&key)?, Some(b"cached result".to_vec()));
    Ok(())
}
```

## Dependency injection

Use the builder when replacing components:

```rust
use rivet_cache::{
    Clock, ContextCache, EvictionPolicy, KeyStrategy, MetricsSink, PersistentStore,
};
use std::sync::Arc;
use std::time::Duration;

fn build_cache(
    store: Arc<dyn PersistentStore>,
    clock: Arc<dyn Clock>,
    keys: Arc<dyn KeyStrategy>,
    eviction: Arc<dyn EvictionPolicy>,
    metrics: Arc<dyn MetricsSink>,
) -> std::io::Result<ContextCache> {
    ContextCache::builder()
        .memory_capacity(64 * 1024 * 1024)
        .persistent_capacity(2 * 1024 * 1024 * 1024)
        .default_ttl(Duration::from_secs(600))
        .persistent_store_arc(store)
        .clock_arc(clock)
        .key_strategy_arc(keys)
        .eviction_policy_arc(eviction)
        .metrics_arc(metrics)
        .build()
}
```

The public injection contracts are:

- `PersistentStore` — filesystem, shared memory, Redis-like service, object store, custom daemon, etc.
- `KeyStrategy` — canonical SHA-256 keys by default or application-specific keying.
- `Clock` — system time by default or deterministic/host clocks.
- `EvictionPolicy` — LRU by default or LFU/ARC/size-aware/custom policies.
- `MetricsSink` — no-op by default or application telemetry.

Pinned entries are filtered before a custom eviction policy is invoked, so policies cannot accidentally evict pinned records.

## Persistent-store contract

A store exposes index reconstruction, exact lookup, put-if-absent, remove, and clear operations. This keeps the core provider-neutral while allowing each backend to report its own stored-byte accounting for quota enforcement.

The bundled `FileStore` retains the RivetCache v1 on-disk format:

- magic: `RIVET01`
- extension: `.rivetcache`
- payload checksum: SHA-256

## Key ABI

The default `Sha256KeyStrategy` retains the RivetCache v1 key domain:

`RIVET_CACHE_V1`

Applications that need a different key ABI can inject their own `KeyStrategy` and should version that strategy explicitly.

## Development

```text
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

CI runs these gates on Linux, Windows, and macOS.

## License

MIT.
