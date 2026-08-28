# RivetCache

**RivetCache** is a small, dependency-light Rust cache built for fast local reuse of opaque artifacts.

It combines a bounded in-memory LRU tier with optional bounded persistent storage while keeping the API intentionally simple: generate a stable namespaced key, store bytes, retrieve bytes, inspect telemetry.

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
- No async runtime requirement and no framework lock-in.

The cache stores opaque bytes. Consumers decide whether those bytes are model responses, rendered prompts, embeddings, serialized state, build artifacts, API responses, or anything else.

## Usage

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

`Some(Duration::ZERO)` creates a non-expiring entry. A memory or disk quota of `0` disables that tier. Persistent storage also requires a root path.

## Storage ABI

RivetCache 0.1 uses:

- key domain: `RIVET_CACHE_V1`
- disk magic: `RIVET01`
- disk extension: `.rivetcache`

The namespace and model fingerprint are length-prefixed before hashing, preventing ambiguous key concatenation.

## Semantics

Keys are content-addressed and should identify immutable values. A non-expired disk entry for an existing key is not overwritten. Pinned entries are not selected for quota eviction, so a cache containing only pinned entries can exceed its configured quota.

## Validation

Every public head is checked on Linux, Windows, and macOS with:

```text
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

The cache design was exercised in a production integration before its standalone release, including memory and disk hits, LRU eviction, restart persistence, explicit invalidation, corruption recovery, TTL expiration, request isolation, deterministic replay, and runtime prefix reuse. Integration-specific speedups are workload-dependent and are not advertised as universal crate performance.

## License

MIT.
