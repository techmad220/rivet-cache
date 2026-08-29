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
- Composable ordered storage layers with replicated writes.
- Volatile `VolatileStore` reference backend.
- Batch get/write helpers and explicit single/batch invalidation.
- Runtime-neutral KV block/chunk identity.
- Ordered KV tiers with promotion and explicit movement.
- Background KV prefetch without an async-runtime dependency.
- Injectable KV transport and allocator boundaries.
- Longest-prefix indexing for reusable KV block sets.
- Runtime adapter contract for capture/restore integrations.
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


## Composable storage layers

`LayeredStore` combines independently injected `PersistentStore` backends without coupling the core to a network protocol or service. Reads search backends in configured priority order and writes are replicated to every backend.

The layered implementation deliberately avoids automatic read promotion. This keeps byte accounting deterministic and makes promotion a caller-owned policy instead of hidden behavior. Existing replicas are checked before writes; conflicting payloads or metadata fail closed with `InvalidData` rather than silently choosing a copy.

```rust
use rivet_cache::{ContextCache, FileStore, LayeredStore, VolatileStore, PersistentStore};
use std::sync::Arc;

let hot: Arc<dyn PersistentStore> = Arc::new(VolatileStore::new());
let durable: Arc<dyn PersistentStore> = Arc::new(FileStore::new("./cache")?);
let stores = LayeredStore::new(vec![hot, durable])?;
let cache = ContextCache::builder()
    .persistent_capacity(512 * 1024 * 1024)
    .persistent_store(stores)
    .build()?;
# Ok::<(), std::io::Error>(())
```

## Batch operations and invalidation

`get_many` and `put_many` are convenience APIs built on the same validated single-entry paths, so TTL, eviction, persistence and telemetry semantics remain consistent. They are intentionally not transactions: an I/O error may occur after an earlier item has committed.

`invalidate` and `invalidate_many` explicitly remove keys from both the memory tier and the configured persistent store. Missing keys are treated as already invalidated.


## KV orchestration

RivetCache includes an optional runtime-neutral KV layer on top of the generic cache core. It does not assume a GPU API, model runtime, network transport, or memory allocator.

`KvBlockKey` identities are derived from a versioned RivetCache KV domain plus the model fingerprint, token-prefix hash, block position, token range, layer range, and layout version. This prevents blocks from different model/layout contexts from sharing an identity accidentally.

`KvEngine` composes ordered `KvTier` implementations. A lower-priority hit can be promoted into faster tiers, blocks can be moved explicitly between tiers, and `prefetch_to` can populate a target tier on a background standard-library thread. `KvTransport` and `KvAllocator` are injected, so a host can provide pinned-host memory, device memory, IPC, RDMA, or network transfer implementations without adding those dependencies to the core crate.

`ContextCacheTier` adapts an existing `ContextCache` into a KV tier. KV metadata is carried in a versioned envelope, preserving absolute expiration and pin state during movement between tiers.

```rust
use rivet_cache::{
    ContextCache, ContextCacheTier, KvBlock, KvBlockKey, KvBlockRange, KvEngine,
    KvWritePolicy,
};
use std::sync::Arc;

let fast_cache = Arc::new(ContextCache::builder().memory_capacity(64 << 20).build()?);
let slow_cache = Arc::new(ContextCache::builder().memory_capacity(512 << 20).build()?);

let engine = KvEngine::builder()
    .tier(ContextCacheTier::new("host-fast", fast_cache)?)
    .tier(ContextCacheTier::new("host-capacity", slow_cache)?)
    .write_policy(KvWritePolicy::All)
    .build()?;

let key = KvBlockKey::from_prefix(
    "model-fingerprint",
    &[10, 20, 30],
    KvBlockRange {
        block_index: 0,
        token_start: 0,
        token_count: 3,
        layer_start: 0,
        layer_count: 32,
        layout_version: 1,
    },
);
engine.put(KvBlock::new(key.clone(), vec![1, 2, 3, 4])?, None, false)?;
assert!(engine.get(&key)?.is_some());
# Ok::<(), std::io::Error>(())
```

`RuntimeKvAdapter` is deliberately a contract rather than a built-in dependency on a specific inference engine. Runtime integrations can implement capture/restore while keeping their FFI and engine lifecycle outside RivetCache core.

### Prefix indexing

`KvCaptureRequest::block_keys` builds prefix-scoped block identities for a token sequence. `PrefixIndex` can register completed sequences and return the longest registered prefix for a later request with the same model fingerprint. The index is in-process and deterministic; distributed index implementations can be supplied separately without changing block identity.

## Development

```text
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

CI runs these gates on Linux, Windows, and macOS.

## License

MIT.
