from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}: {old!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# Fix stats update to use one lock acquisition.
replace_once(
    "src/kv.rs",
    "        self.stats_mut()?.writes = self.stats_mut()?.writes.saturating_add(1);\n",
    "        let mut stats = self.stats_mut()?;\n        stats.writes = stats.writes.saturating_add(1);\n",
)

# Wire the KV layer into the public crate surface.
replace_once(
    "src/lib.rs",
    "mod policy;\nmod store;\n",
    "mod kv;\nmod policy;\nmod store;\n",
)
replace_once(
    "src/lib.rs",
    "pub use policy::{\n",
    "pub use kv::{\n    ContextCacheTier, CopyTransport, KvAllocator, KvBlock, KvBlockKey, KvCaptureRequest,\n    KvEngine, KvEngineBuilder, KvEngineStats, KvPrefetch, KvTier, KvTierEntry, KvTransport,\n    KvWritePolicy, PrefetchReport, PrefixIndex, PrefixMatch, RuntimeKvAdapter, VecAllocator,\n};\npub use policy::{\n",
)

# Version and factual package description.
replace_once("Cargo.toml", 'version = "0.3.0"', 'version = "0.4.0"')
replace_once(
    "Cargo.toml",
    'description = "DI-first Rust cache with composable storage tiers, batch operations, invalidation, and telemetry."',
    'description = "DI-first Rust cache with composable storage tiers and runtime-neutral KV orchestration."',
)

# Changelog.
p = Path("CHANGELOG.md")
text = p.read_text(encoding="utf-8")
entry = """## 0.4.0 - 2026-08-28\n\n- Added runtime-neutral KV block identities scoped by model, token prefix, layer range, block range, and layout version.\n- Added `KvEngine` with ordered tiers, configurable read promotion, targeted movement, invalidation, and write-through policy.\n- Added background prefetch using the Rust standard library; no async runtime is required.\n- Added injected `KvTransport` and `KvAllocator` boundaries for host/device/network-specific implementations.\n- Added `ContextCacheTier` adapter so existing RivetCache instances can serve as KV tiers.\n- Added metadata-preserving KV envelopes so TTL and pin state survive tier movement.\n- Added `PrefixIndex` for deterministic longest-prefix lookup.\n- Added `RuntimeKvAdapter` and `KvCaptureRequest` contracts for runtime-specific capture/restore plugins.\n- Added KV engine telemetry for hits, misses, writes, promotions, transfers, bytes moved, invalidations, expirations, and prefetches.\n- Added deterministic tests for key identity, prefix lookup, promotion, prefetch, movement, write-through, metadata preservation, and TTL.\n\n"""
if "## 0.4.0" not in text:
    text = text.replace("# Changelog\n\n", "# Changelog\n\n" + entry, 1)
p.write_text(text, encoding="utf-8")

# README capability list and KV section.
p = Path("README.md")
text = p.read_text(encoding="utf-8")
anchor = "- Batch get/write helpers and explicit single/batch invalidation.\n"
addition = """- Runtime-neutral KV block/chunk identity.\n- Ordered KV tiers with promotion and explicit movement.\n- Background KV prefetch without an async-runtime dependency.\n- Injectable KV transport and allocator boundaries.\n- Longest-prefix indexing for reusable KV block sets.\n- Runtime adapter contract for capture/restore integrations.\n"""
if addition not in text:
    text = text.replace(anchor, anchor + addition, 1)

section = r'''
## KV orchestration

RivetCache includes an optional runtime-neutral KV layer on top of the generic cache core. It does not assume a GPU API, model runtime, network transport, or memory allocator.

`KvBlockKey` identities are derived from a versioned RivetCache KV domain plus the model fingerprint, token-prefix hash, block position, token range, layer range, and layout version. This prevents blocks from different model/layout contexts from sharing an identity accidentally.

`KvEngine` composes ordered `KvTier` implementations. A lower-priority hit can be promoted into faster tiers, blocks can be moved explicitly between tiers, and `prefetch_to` can populate a target tier on a background standard-library thread. `KvTransport` and `KvAllocator` are injected, so a host can provide pinned-host memory, device memory, IPC, RDMA, or network transfer implementations without adding those dependencies to the core crate.

`ContextCacheTier` adapts an existing `ContextCache` into a KV tier. KV metadata is carried in a versioned envelope, preserving absolute expiration and pin state during movement between tiers.

```rust
use rivet_cache::{
    ContextCache, ContextCacheTier, KvBlock, KvBlockKey, KvEngine, KvWritePolicy,
};
use std::sync::Arc;

let fast_cache = Arc::new(ContextCache::builder().memory_capacity(64 << 20).build()?);
let slow_cache = Arc::new(ContextCache::builder().memory_capacity(512 << 20).build()?);

let engine = KvEngine::builder()
    .tier(ContextCacheTier::new("host-fast", fast_cache)?)
    .tier(ContextCacheTier::new("host-capacity", slow_cache)?)
    .write_policy(KvWritePolicy::All)
    .build()?;

let key = KvBlockKey::from_prefix("model-fingerprint", &[10, 20, 30], 0, 0, 3, 0, 32, 1);
engine.put(KvBlock::new(key.clone(), vec![1, 2, 3, 4])?, None, false)?;
assert!(engine.get(&key)?.is_some());
# Ok::<(), std::io::Error>(())
```

`RuntimeKvAdapter` is deliberately a contract rather than a built-in dependency on a specific inference engine. Runtime integrations can implement capture/restore while keeping their FFI and engine lifecycle outside RivetCache core.

### Prefix indexing

`KvCaptureRequest::block_keys` builds prefix-scoped block identities for a token sequence. `PrefixIndex` can register completed sequences and return the longest registered prefix for a later request with the same model fingerprint. The index is in-process and deterministic; distributed index implementations can be supplied separately without changing block identity.

'''
if "## KV orchestration" not in text:
    text = text.replace("## Development\n", section + "## Development\n", 1)
p.write_text(text, encoding="utf-8")

# Architecture documentation.
p = Path("docs/architecture.md")
text = p.read_text(encoding="utf-8")
kv_arch = """
## KV orchestration layer

The KV layer is built above `ContextCache`; it does not replace the generic cache core. `KvBlockKey` provides deterministic model/token/layer/layout-scoped block identity. `KvEngine` coordinates ordered `KvTier` implementations and delegates movement bytes through injected `KvTransport` and `KvAllocator` contracts.

Tier entries use a small versioned envelope carrying expiration and pin metadata with the opaque KV bytes. This allows a block to move between heterogeneous tiers without losing cache-control semantics. Background prefetch uses a standard-library worker thread and returns a waitable `KvPrefetch`; the core therefore remains independent of Tokio or another async runtime.

`RuntimeKvAdapter` defines capture and restore boundaries without embedding runtime-specific FFI in the core. Device-memory allocators, pinned-host allocators, IPC/RDMA transports, remote services, and engine adapters remain independent implementations of public Rust traits.

`PrefixIndex` is the bundled deterministic in-process prefix locator. It is intentionally separate from storage and transport so a host can replace indexing/discovery independently.

"""
if "## KV orchestration layer" not in text:
    text = text.replace("## ABI\n", kv_arch + "## ABI\n", 1)
p.write_text(text, encoding="utf-8")

# Dedicated design contract.
Path("docs/KV_RUNTIME.md").write_text(
    """# KV runtime contract\n\nRivetCache's KV layer coordinates opaque runtime KV blocks without assuming their tensor shape, element type, GPU API, or inference engine.\n\n## Identity\n\nA `KvBlockKey` includes the model fingerprint, a SHA-256 hash of the token prefix represented by that block, block index, token range, layer range, and layout version. The public cache key is derived under the `RIVET_KV_V1` domain. Hosts should bump their layout version when the byte representation changes incompatibly.\n\n## Tiers\n\n`KvTier` is the storage boundary. Ordered tiers may represent device-local memory, pinned host memory, normal host memory, local persistent storage, a shared-memory service, or a remote cache. RivetCache core does not assign hardware meaning to a tier name.\n\n## Movement\n\n`KvTransport` owns source-to-destination transfer semantics. `KvAllocator` owns destination-buffer allocation/copy semantics. The bundled implementations perform normal Rust `Vec<u8>` copies and are suitable as deterministic defaults and test references. Specialized hosts can replace either independently.\n\n`KvEngine::move_block` performs explicit movement. Read promotion is configurable. `prefetch_to` executes tier population on a background standard-library thread and exposes a waitable result.\n\n## Runtime adapters\n\n`RuntimeKvAdapter` is the engine boundary. An adapter captures blocks for a `KvCaptureRequest` and restores a set of blocks into its runtime. RivetCache does not own runtime lifecycle, device contexts, streams, or FFI handles. This keeps unsafe/runtime-specific code outside the core crate.\n\n## Prefix discovery\n\n`PrefixIndex` records token sequences and their block sets, then returns the longest registered prefix for a request with the same model fingerprint. It is a local reference implementation; storage/discovery can be replaced independently.\n\n## Consistency\n\nKV tier envelopes preserve expiration and pin state across movement. Expired entries are removed when observed by `KvEngine`. A transfer is considered complete only after the destination tier accepts the transferred entry; explicit source removal happens afterward.\n\n## Claim scope\n\nPublic RivetCache claims describe implemented and tested RivetCache behavior. Hardware-specific throughput, runtime compatibility, remote-service interoperability, and comparative performance require separate implementation-specific evidence.\n""",
    encoding="utf-8",
)

# Certification scope.
p = Path("CERTIFICATION.md")
text = p.read_text(encoding="utf-8")
old = "The test suite covers stable and isolated keys, memory LRU eviction, restart persistence, corrupt-disk recovery, TTL expiration, injected components, layered-store replication and conflict rejection, batch operations, and explicit invalidation."
new = "The test suite covers stable and isolated keys, memory LRU eviction, restart persistence, corrupt-disk recovery, TTL expiration, injected components, layered-store replication and conflict rejection, batch operations, explicit invalidation, KV block identity, prefix lookup, tier promotion, targeted movement, background prefetch, write-through, metadata preservation, and KV TTL behavior."
if old in text:
    text = text.replace(old, new, 1)
p.write_text(text, encoding="utf-8")

print("RIVET_KV_LAYER_PATCH=READY")
