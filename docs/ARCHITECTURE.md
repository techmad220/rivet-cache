# Architecture

RivetCache is a DI-first cache core. The central object coordinates policy and tier behavior; concrete infrastructure lives behind object-safe Rust traits.

```text
                    ContextCache
                         |
        +----------------+----------------+
        |                |                |
   KeyStrategy          Clock       EvictionPolicy
        |                |                |
  SHA-256 default   SystemClock       LRU default
                         |
                    MetricsSink
                         |
                      no-op
                         |
                 PersistentStore
                         |
               FileStore default
```

## Injection boundaries

### `PersistentStore`

Owns persistent-tier I/O and index reconstruction. The core only depends on the trait, so future backends can provide shared-memory, network, distributed, object-storage, or runtime-native implementations without changing cache orchestration.

### `KeyStrategy`

Owns canonical key construction and validation. `Sha256KeyStrategy` is the stable RivetCache v1 default.

### `Clock`

Owns TTL time. Separating time from the cache eliminates sleep-based tests and permits deterministic simulations.

### `EvictionPolicy`

Receives unpinned candidates and selects the victim. The core enforces the pin invariant before invoking policy code.

### `MetricsSink`

Receives cache events without coupling the core to a metrics framework.

## Compatibility

`ContextCache::new(root, memory_bytes, persistent_bytes, ttl)` remains the convenience API and composes the default implementations. Existing callers do not need to adopt the builder until they need custom dependencies.

The default key and filesystem ABIs remain unchanged in 0.2.0.
