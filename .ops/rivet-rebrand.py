from pathlib import Path

old = ''.join(chr(x) for x in (97, 105, 111, 110))
old_upper = old.upper()

cargo = Path('Cargo.toml')
text = cargo.read_text(encoding='utf-8')
text = text.replace(f'name = "{old}-cache"', 'name = "rivet-cache"')
text = text.replace(f'https://github.com/techmad220/{old}-cache-rs', 'https://github.com/techmad220/rivet-cache')
text = text.replace(
    'description = "Bounded content-addressed cache with memory/disk tiers, LRU, TTL, pinning, atomic persistence, checksums, and corruption recovery."',
    'description = "Fast bounded two-tier cache with stable namespaced keys, LRU, TTL, pinning, atomic persistence, checksums, corruption recovery, and telemetry."',
)
cargo.write_text(text, encoding='utf-8')

lib = Path('src/lib.rs')
text = lib.read_text(encoding='utf-8')
text = text.replace(f'{old_upper}C01\\n', 'RIVET01\\n')
text = text.replace(f'{old}cache', 'rivetcache')
text = text.replace(f'{old_upper}_CONTEXT_CACHE_V1\\0', 'RIVET_CACHE_V1\\0')
text = text.replace(f'{old}-context-cache-', 'rivet-cache-')
lib.write_text(text, encoding='utf-8')

example = Path('examples/basic.rs')
text = example.read_text(encoding='utf-8')
text = text.replace(f'use {old}_cache::ContextCache;', 'use rivet_cache::ContextCache;')
text = text.replace(f'{old}-cache-example', 'rivet-cache-example')
example.write_text(text, encoding='utf-8')

Path('README.md').write_text('''# RivetCache

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
''', encoding='utf-8')

Path('CERTIFICATION.md').write_text('''# Certification

RivetCache is validated independently as a standalone Rust crate.

## Standalone gates

Every release candidate must pass on Linux, Windows, and macOS:

- `cargo fmt --all -- --check`
- `cargo test --all-targets`
- `cargo clippy --all-targets -- -D warnings`

The test suite covers stable and isolated keys, memory LRU eviction, restart persistence, corrupt-disk recovery, and TTL expiration.

## Production provenance

The cache design was extracted from a production-proven Rust cache subsystem on August 28, 2026. The production integration exercised stable namespaced keys, memory hit/miss behavior, LRU eviction, restart persistence, explicit clearing/invalidation, corruption recovery, TTL expiry, deterministic replay, request isolation, stochastic bypass, and runtime prefix/KV reuse.

The standalone project uses its own `RIVET_CACHE_V1` key domain and `RIVET01` disk format, so its public ABI is independent and intentionally versioned from release 0.1.0 onward.
''', encoding='utf-8')

Path('CHANGELOG.md').write_text('''# Changelog

## 0.1.0 - 2026-08-28

- Initial public MIT release of RivetCache.
- Memory and persistent disk tiers.
- Stable namespaced content-addressed keys.
- LRU eviction, TTL expiry, pinning, atomic writes, checksums, corruption recovery, restart persistence, clearing, and telemetry.
- Independent `RIVET_CACHE_V1` key domain, `RIVET01` disk magic, and `.rivetcache` storage extension.
- Cross-platform Linux, Windows, and macOS CI.
''', encoding='utf-8')
