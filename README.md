# aion-cache-rs

`aion-cache-rs` is a dependency-light Rust cache for inference and context artifacts. It was extracted from the production-proven cache core used by AION and released independently under the MIT License.

## Features

- Stable SHA-256 content-addressed keys with explicit namespace and model-fingerprint isolation.
- Bounded in-memory storage with least-recently-used eviction.
- Optional bounded persistent disk storage.
- Per-entry TTL, including non-expiring entries.
- Pinned entries protected from quota eviction.
- Atomic disk writes.
- SHA-256 payload checksums and automatic corrupt-entry removal.
- Restart persistence and disk-index reconstruction.
- Memory/disk hit, miss, write, eviction, expiry, corruption, entry-count, and byte-count telemetry.
- Thread-safe access through an internal mutex.

The cache stores opaque bytes. Consumers decide whether those bytes represent exact completions, rendered prompts, embeddings, serialized state, or other artifacts.

## Compatibility

Version 0.1.0 intentionally preserves AION cache-v1 key and disk-format compatibility (`AION_CONTEXT_CACHE_V1`, `AIONC01`, and `.aioncache`). Existing AION key semantics therefore remain compatible while the implementation becomes an independent dependency.

## Usage

```rust
use aion_cache::ContextCache;
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

`Some(Duration::ZERO)` creates a non-expiring entry. A memory or disk quota of `0` disables that tier. Persistent disk caching also requires a root path.

## Semantics

Keys are content-addressed and should identify immutable values. A non-expired disk entry for an existing key is not overwritten. Pinned entries are not selected for quota eviction, so a cache containing only pinned entries can exceed its configured quota.

## Origin certification

The initial source was extracted from AION commit `0467bfba22ae19939ec6ac1e7f8ab6120488b433`, whose cache subsystem passed AION's canonical live end-to-end production certification on August 28, 2026. That AION integration measured **118.67x deterministic exact-replay acceleration** and **25.29x best llama.cpp prefix/KV-reuse acceleration**.

Those measurements describe the certified AION integration, not a universal standalone-crate benchmark. Prefix/KV reuse is performed by the inference runtime; this crate provides the bounded persistent content cache used alongside that runtime.

## Development

```text
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

CI runs these gates on Linux, Windows, and macOS.

## License

MIT.
