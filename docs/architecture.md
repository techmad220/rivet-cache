# RivetCache architecture

RivetCache is a synchronous, runtime-neutral Rust cache core. It stores opaque bytes and does not assume a model runtime, transport, remote service, serializer, GPU API, or async executor.

## Stable core boundaries

- `PersistentStore`: injected storage backend contract.
- `KeyStrategy`: injected key generation and validation.
- `Clock`: injected TTL time source.
- `EvictionPolicy`: injected victim selection.
- `MetricsSink`: injected telemetry hook.

The bundled `FileStore`, `VolatileStore`, and `LayeredStore` are implementations of the storage contract, not privileged code paths in the cache core.

## Layering semantics

`LayeredStore` performs ordered reads and replicated writes. It does not implicitly promote values after a lower-priority hit. This prevents hidden capacity growth and keeps quota accounting explicit. Conflicting pre-existing replicas fail closed on write, and conflicting metadata fails index reconstruction.

## Error and consistency model

Single-entry operations return `std::io::Result`. Batch helpers validate all keys before mutation but are not transactional across entries. Layered writes attempt rollback of replicas created by the current call if a later backend write fails; externally modified backends remain the backend owner's consistency responsibility.


## KV orchestration layer

The KV layer is built above `ContextCache`; it does not replace the generic cache core. `KvBlockKey` provides deterministic model/token/layer/layout-scoped block identity. `KvEngine` coordinates ordered `KvTier` implementations and delegates movement bytes through injected `KvTransport` and `KvAllocator` contracts.

Tier entries use a small versioned envelope carrying expiration and pin metadata with the opaque KV bytes. This allows a block to move between heterogeneous tiers without losing cache-control semantics. Background prefetch uses a standard-library worker thread and returns a waitable `KvPrefetch`; the core therefore remains independent of Tokio or another async runtime.

`RuntimeKvAdapter` defines capture and restore boundaries without embedding runtime-specific FFI in the core. Device-memory allocators, pinned-host allocators, IPC/RDMA transports, remote services, and engine adapters remain independent implementations of public Rust traits.

`PrefixIndex` is the bundled deterministic in-process prefix locator. It is intentionally separate from storage and transport so a host can replace indexing/discovery independently.

## ABI

The default key domain remains `RIVET_CACHE_V1`. The bundled filesystem format remains `RIVET01` with `.rivetcache` files. Custom `KeyStrategy` and `PersistentStore` implementations may define their own independently versioned external formats.

## Scope

Public claims describe implemented and tested RivetCache capabilities directly. Third-party compatibility, interoperability, equivalence, or comparative-performance claims require separate documented evidence.

## v0.5 execution adapters

The core cache remains runtime-neutral. Optional sibling modules supply execution-specific boundaries:

- Remote service mode uses a versioned RivetCache TCP protocol with bounded messages and explicit health/clear operations. It is an application protocol, not an authenticated network perimeter.
- Device storage owns opaque buffer handles through `DeviceMemory`. The FFI implementation delegates allocation/upload/download/free to caller-supplied callbacks and therefore can be bound to Vulkan, ROCm/HIP, CUDA, shared-memory, or other device systems without adding those SDKs to the core crate.
- The llama.cpp adapter delegates KV extraction/injection to host callbacks defined by RivetCache. This avoids depending on unstable engine internals in the core library.
- Non-prefix reuse is exact and block-aligned: `SegmentIndex` finds contiguous token subsequences and only returns complete registered KV blocks. Relocation is delegated to a runtime adapter. No approximate recomputation or quality-recovery algorithm is implied.
