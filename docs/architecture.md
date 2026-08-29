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

## ABI

The default key domain remains `RIVET_CACHE_V1`. The bundled filesystem format remains `RIVET01` with `.rivetcache` files. Custom `KeyStrategy` and `PersistentStore` implementations may define their own independently versioned external formats.

## Scope

Public claims should describe implemented and tested RivetCache capabilities directly. The project does not require or assert compatibility, equivalence, or feature parity with any third-party cache product.
