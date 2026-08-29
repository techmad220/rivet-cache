# RivetCache

[![CI](https://github.com/techmad220/rivet-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/techmad220/rivet-cache/actions/workflows/ci.yml)
[![NIXL Integration](https://github.com/techmad220/rivet-cache/actions/workflows/nixl-integration.yml/badge.svg)](https://github.com/techmad220/rivet-cache/actions/workflows/nixl-integration.yml)
[![Production Integration](https://github.com/techmad220/rivet-cache/actions/workflows/production-integration.yml/badge.svg)](https://github.com/techmad220/rivet-cache/actions/workflows/production-integration.yml)
[![Security Audit](https://github.com/techmad220/rivet-cache/actions/workflows/security-audit.yml/badge.svg)](https://github.com/techmad220/rivet-cache/actions/workflows/security-audit.yml)
[![Release](https://img.shields.io/github/v/release/techmad220/rivet-cache)](https://github.com/techmad220/rivet-cache/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)

**RivetCache** is a dependency-light, DI-first Rust cache and runtime KV substrate for high-speed reuse of opaque artifacts across memory, disk, remote stores, process boundaries, device memory, and optional native NIXL/UCX transports.

It is deliberately **runtime-neutral**: RivetCache does not require Python, an async runtime, a specific inference engine, a specific GPU API, or a specific storage provider. The core exposes narrow Rust traits so applications can replace storage, clocks, keying, eviction, metrics, runtime capture/restore, device memory, and transfer implementations without rewriting the cache.

> **Latest release: `v0.8.0`** — native NIXL interoperability, exact NIXL/UCX integration certification, and RX 6800 XT AMD interoperability certification are now part of the released line.

## v0.8.0 release status

`v0.8.0` was released from the exact certified source tree.

| Gate | Certified result |
| --- | --- |
| Release | `v0.8.0` |
| Certified PR head | `b310cecb74dfdaea4985d06c6e1393b22f5b1929` |
| Merge / tag target | `227c4bdd7c68dc87c2da4e4387190c4904c4e580` |
| CI | PASS on Linux, Windows, and macOS |
| Security audit | PASS |
| Production integration | PASS |
| Native NIXL integration | PASS |
| NIXL source | `c0a1102b94d173049a5478c23e765ba37681e2ca` — NIXL 1.4.0 |
| UCX source | `b6a9d47fccce849c28111f05a7fa8f1c930ff17d` — UCX 1.21.0 with multithreading enabled |
| NIXL transfer proof | 8 MiB registered-memory `prefill -> decode` UCX transfer with byte verification |
| AMD hardware proof | RX 6800 XT native HIP device-to-device transfer, 64 MiB |
| Vulkan/HIP interoperability | Exact RiftGPU `a154537b50aea48fb32125e7460b693c4fe9569f` hardware E2E PASS |

The release tag resolves to the verified merge commit; the merge contains the certified PR head without source-tree drift.

## Why RivetCache

- **100% Rust core.**
- **DI-first architecture.** Storage, policy, telemetry, clocks, runtime adapters, memory, and transports are replaceable.
- **Stable content addressing.** SHA-256 keys are the default and the v1 key domain remains versioned.
- **Bounded memory and persistence.** Memory and filesystem capacities are explicit.
- **Composable tiers.** Memory, file, volatile, Redis, S3-compatible, TCP, device, and custom tiers can be assembled without changing the core cache.
- **Runtime-neutral KV orchestration.** Block identity, movement, promotion, prefetch, capture/restore contracts, and prefix reuse are independent of a model runtime.
- **Native NIXL path.** Optional Linux-only NIXL 1.4.0 integration supports registered DRAM, metadata exchange, descriptors, asynchronous transfer completion, and bounded polling.
- **GPU/device boundaries without SDK lock-in.** Device memory and transfer capabilities are injected through traits/FFI surfaces.
- **No async-runtime requirement.** Background prefetch uses standard-library threads.
- **No Python dependency.**
- **No inference-engine lock-in.**
- **No hidden hardware claims.** Hardware/runtime interoperability is only claimed where an executable certification path exists.

The generic cache stores opaque bytes. Consumers decide whether those bytes are model responses, KV blocks, rendered prompts, embeddings, serialized state, build artifacts, API responses, or something else entirely.

## Capability map

| Capability | Built in | Notes |
| --- | :---: | --- |
| Bounded in-memory cache | Yes | Default fast tier |
| Bounded filesystem persistence | Yes | Atomic writes, checksums, restart reconstruction |
| Layered persistent stores | Yes | Ordered reads, replicated writes |
| Volatile persistent-store implementation | Yes | Useful for composition/testing |
| Batch get/write + invalidation | Yes | Uses validated single-entry paths |
| Per-entry TTL + non-expiring entries | Yes | Injectable clock |
| Pinned entries | Yes | Protected from quota eviction |
| KV block identity + tier engine | Yes | Runtime-neutral |
| Prefix reuse index | Yes | Longest registered prefix |
| Segment reuse index | Yes | Exact contiguous non-prefix matches |
| Background KV prefetch | Yes | No async runtime required |
| Redis KV tier | Yes | RESP over an injectable dialer |
| S3-compatible KV tier | Yes | SigV4 signing; injectable HTTP client |
| Standalone TCP KV service/client | Yes | Trusted networks only unless externally protected |
| Device KV tier | Yes | Injected device memory / FFI |
| llama.cpp host adapter | Yes | Callback contract, not a claim of private upstream ABI compatibility |
| Same-host GPU-direct transfer adapter | Yes | Injected `GpuDirectIo` provider |
| Native HIP certification path | Yes | RX 6800 XT hardware-certified release gate |
| Native NIXL/UCX transfer | Optional | Linux-only `nixl` feature |

## Quick start

The original zero-config constructor remains available:

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

    let key = ContextCache::key(
        "completion/v1",
        "model-fingerprint",
        "canonical-request",
    );

    cache.put(&key, b"cached result", None, false)?;
    assert_eq!(cache.get(&key)?, Some(b"cached result".to_vec()));

    Ok(())
}
```

To pin the released Git revision directly:

```toml
[dependencies]
rivet-cache = { git = "https://github.com/techmad220/rivet-cache", tag = "v0.8.0" }
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

Primary injection contracts include:

- `PersistentStore` — filesystem, shared memory, remote service, custom daemon, or application-specific persistence.
- `KeyStrategy` — canonical SHA-256 keys by default or application-specific keying.
- `Clock` — system time by default or deterministic/host clocks.
- `EvictionPolicy` — LRU by default or LFU/ARC/size-aware/custom policy implementations.
- `MetricsSink` — no-op by default or application telemetry.
- `KvTier` — storage tier for runtime KV blocks.
- `KvTransport` / transfer-provider contracts — movement without binding the engine to one network or accelerator stack.
- `KvAllocator` / device-memory contracts — caller-owned allocation strategy.
- `RuntimeKvAdapter` — capture/restore integration boundary for inference runtimes.

Pinned entries are filtered before a custom eviction policy is invoked, so a policy cannot accidentally evict pinned records.

## Persistent-store contract

A `PersistentStore` exposes index reconstruction, exact lookup, put-if-absent, remove, and clear operations. The cache core stays provider-neutral while each backend reports its own byte accounting for quota enforcement.

The bundled `FileStore` retains the RivetCache v1 on-disk format:

- magic: `RIVET01`
- extension: `.rivetcache`
- payload checksum: SHA-256
- atomic file replacement
- restart index reconstruction
- corruption detection/recovery

## Key ABI

The default `Sha256KeyStrategy` retains the RivetCache v1 key domain:

```text
RIVET_CACHE_V1
```

Applications that inject a different key strategy should version that key ABI explicitly.

## Composable storage layers

`LayeredStore` combines independent `PersistentStore` implementations without coupling the cache core to a network protocol or service. Reads search backends in configured priority order and writes are replicated to every backend.

Automatic read promotion is intentionally not hidden inside `LayeredStore`. Promotion remains caller-owned policy, keeping byte accounting deterministic. Existing replicas are checked before writes; conflicting payloads or metadata fail closed with `InvalidData` rather than silently selecting one copy.

```rust
use rivet_cache::{ContextCache, FileStore, LayeredStore, PersistentStore, VolatileStore};
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

`get_many` and `put_many` are convenience APIs built on the same validated single-entry paths, so TTL, eviction, persistence, and telemetry semantics remain consistent.

They are intentionally **not transactions**: an I/O failure can occur after an earlier item has committed.

`invalidate` and `invalidate_many` remove keys from both the memory tier and the configured persistent store. Missing keys are treated as already invalidated.

## Runtime KV orchestration

RivetCache includes an optional runtime-neutral KV layer above the generic cache core. It does not assume a GPU API, model runtime, network transport, or allocator.

`KvBlockKey` identities are derived from a versioned RivetCache KV domain plus:

- model fingerprint
- token-prefix hash
- block position
- token range
- layer range
- layout version

That prevents blocks belonging to different model/layout contexts from accidentally sharing an identity.

`KvEngine` composes ordered `KvTier` implementations. A lower-priority hit can be promoted into faster tiers, blocks can move explicitly between tiers, and `prefetch_to` can populate a target tier on a background standard-library thread.

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

### Prefix and segment reuse

`KvCaptureRequest::block_keys` builds prefix-scoped block identities for a token sequence. `PrefixIndex` registers completed sequences and returns the longest registered prefix for a later request with the same model fingerprint.

`SegmentIndex` handles exact contiguous matches outside the prompt prefix, aligns matches to complete cached blocks, and can restore blocks at a relocated token position through `RelocatableRuntimeKvAdapter`.

The indexes are deterministic in-process implementations; distributed indexes can be supplied independently without changing block identity.

## Remote KV connectors

### Redis

`RedisKvTier` implements a direct RESP-based KV tier with:

- injectable `RedisDialer`
- optional username/password authentication
- bounded connect and I/O timeouts
- bounded value sizes
- `GET`, `SET`, `DEL`, bounded `SCAN`-based clear, and `PING` health paths

The default dialer uses TCP. Deployments that require a different connection/security model can inject their own dialer.

### S3-compatible object storage

`S3KvTier` provides SigV4-signed object operations with injectable credentials, clock, and HTTP client.

The bundled `TcpHttpClient` intentionally supports **plain HTTP only**. For HTTPS/S3 production endpoints, inject a TLS-capable `HttpClient`; RivetCache does not silently add a TLS/runtime dependency to the core crate.

`KvTier::clear` intentionally does not bulk-delete an S3 bucket or prefix.

## Standalone TCP KV service

`TcpKvServer` and `TcpKvTier` provide a binary TCP service/client path for cross-process or cross-host cache sharing.

Start the bundled server with:

```text
cargo run --bin rivet-cache-server -- 127.0.0.1:65432 ./rivet-cache-data 512 8192
```

The bundled protocol intentionally does **not** embed authentication or TLS. Use it only on trusted application networks or behind an authenticated/tunneled boundary.

## Device and GPU-direct integration

`DeviceKvTier` stores KV payloads in buffers owned by an injected `DeviceMemory` implementation. `FfiDeviceMemory` exposes an unsafe callback ABI for accelerator integrations without forcing a GPU SDK into the RivetCache core.

`GpuDirectTransferProvider` adapts an injected `GpuDirectIo` implementation into the prefill/decode worker-transfer contract for same-host device movement.

The abstraction exposes capabilities rather than pretending every implementation supports the same operations. Remote RDMA/NIXL implementations remain pluggable through the transfer-provider boundary.

### AMD hardware certification

The `v0.8.0` AMD gate runs on a physical **AMD Radeon RX 6800 XT** and verifies the exact RivetCache revision before executing the hardware path.

The release gate includes:

- native HIP device-to-device transfer
- 64 MiB RivetCache hardware-cert payload
- exact RiftGPU checkout at `a154537b50aea48fb32125e7460b693c4fe9569f`
- Vulkan-to-HIP external-memory hardware E2E validation
- sealed exact-head workflow receipt

This is a certification of the tested hardware/software path, not a universal performance claim for every AMD GPU, driver, or operating-system combination.

## Native NIXL / UCX interoperability

`v0.8.0` adds an optional native NIXL path for Linux hosts.

Enable it explicitly:

```toml
[dependencies]
rivet-cache = {
    git = "https://github.com/techmad220/rivet-cache",
    tag = "v0.8.0",
    features = ["nixl"]
}
```

The `nixl` feature enables the optional `nixl-sys` dependency. It is intentionally absent from the default build so the existing dependency and MSRV surface remains clean.

The native adapter provides:

- `NixlEndpoint`
- registered `NixlHostBuffer` ownership
- local metadata export
- caller-controlled remote metadata loading
- registered DRAM region descriptors
- NIXL `Write` transfers
- bounded completion polling
- timeout/error propagation
- `NixlTransferReceipt`

Metadata exchange is deliberately caller-controlled so deployments can carry opaque NIXL metadata over their existing authenticated control plane instead of RivetCache inventing one.

### Compile-only NIXL surface

For environments that need to compile or document the NIXL API without a native runtime:

```text
cargo check --features nixl-stub --bin rivet-cache-nixl-cert
```

The upstream stub API is **compile-only** and must not be used as runtime proof.

### Reproducing the native certification binary

On a compatible Linux host with real NIXL/UCX libraries available:

```text
cargo run --release --features nixl --bin rivet-cache-nixl-cert
```

The release workflow pins and verifies:

```text
NIXL 1.4.0  c0a1102b94d173049a5478c23e765ba37681e2ca
UCX  1.21.0 b6a9d47fccce849c28111f05a7fa8f1c930ff17d
```

The certification path performs a real **8 MiB registered-memory prefill-to-decode transfer** and requires byte-level verification before emitting PASS receipts.

## llama.cpp integration boundary

`LlamaCppAdapter` maps RivetCache block ranges to capture/restore callbacks supplied by an embedding llama.cpp host.

That callback contract is a **RivetCache integration surface**. It does not claim compatibility with a private or undocumented upstream llama.cpp ABI.

## Development

Minimum declared Rust version: **1.75**.

Core checks:

```text
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

Optional NIXL compile surface:

```text
cargo check --features nixl-stub --bin rivet-cache-nixl-cert
```

The repository also carries dedicated workflows for:

- cross-platform CI
- security audit / secret-history checks
- production integration
- native NIXL/UCX integration
- physical AMD interoperability

## Design and claim boundaries

RivetCache aims to make low-level capabilities **explicit and injectable** rather than implicit.

Accordingly:

- a transport is not called zero-copy unless its implementation reports/proves that capability;
- a runtime adapter is not advertised as an upstream ABI unless that ABI is actually public and supported;
- the TCP service is not advertised as secure transport because it contains no built-in TLS/authentication;
- the built-in HTTP client is not advertised as HTTPS-capable;
- NIXL stubs are not treated as runtime certification;
- hardware throughput and interoperability claims require hardware receipts tied to an exact source revision.

That boundary is intentional: RivetCache should be easy to extend without making claims the code has not earned.

## License

MIT.
