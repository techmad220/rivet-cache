# KV runtime contract

RivetCache's KV layer coordinates opaque runtime KV blocks without assuming their tensor shape, element type, GPU API, or inference engine.

## Identity

A `KvBlockKey` includes the model fingerprint, a SHA-256 hash of the token prefix represented by that block, block index, token range, layer range, and layout version. The public cache key is derived under the `RIVET_KV_V1` domain. Hosts should bump their layout version when the byte representation changes incompatibly.

## Tiers

`KvTier` is the storage boundary. Ordered tiers may represent device-local memory, pinned host memory, normal host memory, local persistent storage, a shared-memory service, or a remote cache. RivetCache core does not assign hardware meaning to a tier name.

## Movement

`KvTransport` owns source-to-destination transfer semantics. `KvAllocator` owns destination-buffer allocation/copy semantics. The bundled implementations perform normal Rust `Vec<u8>` copies and are suitable as deterministic defaults and test references. Specialized hosts can replace either independently.

`KvEngine::move_block` performs explicit movement. Read promotion is configurable. `prefetch_to` executes tier population on a background standard-library thread and exposes a waitable result.

## Runtime adapters

`RuntimeKvAdapter` is the engine boundary. An adapter captures blocks for a `KvCaptureRequest` and restores a set of blocks into its runtime. `KvEngine::capture_from` verifies that returned block identities exactly match the requested block identities before caching them, and `restore_into` restores only when every requested block is available. RivetCache does not own runtime lifecycle, device contexts, streams, or FFI handles. This keeps unsafe/runtime-specific code outside the core crate.

## Prefix discovery

`PrefixIndex` records token sequences and their block sets, then returns the longest registered prefix for a request with the same model fingerprint. It is a local reference implementation; storage/discovery can be replaced independently.

## Consistency

KV tier envelopes preserve expiration and pin state across movement. Expired entries are removed when observed by `KvEngine`. A transfer is considered complete only after the destination tier accepts the transferred entry; explicit source removal happens afterward. `KvWritePolicy::All` rolls back replicas written by the current call if a later tier rejects the write. Rollback removal is best-effort so the original backend error is preserved.

## Claim scope

Public RivetCache claims describe implemented and tested RivetCache behavior. Hardware-specific throughput, runtime compatibility, remote-service interoperability, and comparative performance require separate implementation-specific evidence.
