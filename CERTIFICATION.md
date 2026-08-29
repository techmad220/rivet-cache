# Certification

RivetCache is validated independently as a standalone Rust crate.

## Standalone gates

Every release candidate must pass on Linux, Windows, and macOS:

- `cargo fmt --all -- --check`
- `cargo test --all-targets`
- `cargo clippy --all-targets -- -D warnings`

The test suite covers stable and isolated keys, memory LRU eviction, restart persistence, corrupt-disk recovery, TTL expiration, injected components, layered-store replication and conflict rejection, batch operations, explicit invalidation, KV block identity, prefix lookup, tier promotion, targeted movement, background prefetch, write-through, metadata preservation, KV TTL behavior, runtime adapter validation, and llama-server slot persistence safety checks.

## llama-server slot persistence certification

RivetCache 0.6.0 includes an optional local/trusted-process bridge for llama-server slot save, restore, erase, and cache-presence operations. RivetCache preserves the slot-state file as opaque bytes; reusable runtime semantics still depend on the external runtime serializing every state component it requires.

The integration was hardware-certified on August 28, 2026 against these exact inputs:

- RivetCache source: `45a32bd3e9ab6cabf552b7a3969c49dea9b0cd5f`
- llama.cpp checkpoint-preserving source: `06d9d0ff54b586514a59268e2c780abc08473daa`
- accelerator: AMD Radeon RX 6800 XT
- backend: Vulkan
- model digest: `sha256:95580dbdaad579582ee898257116abc18d7f3625a00c16a15735d41444a09f5e`
- hardware workflow run: `33226944517`
- hardware job: `99032409542`

The certification disabled llama.cpp's separate RAM prompt cache and exercised a divergent-prefix control before and after persistent restoration. The observed receipt was:

- cold request: `prompt_n=1280`, `cache_n=0`
- live checkpoint reuse: `prompt_n=38`, `cache_n=1244`
- persisted state: `555059324` bytes
- persisted-state SHA-256: `c0360ada216e04f6ff14e62c4a1f5beb9bcc0b135c78b5a67d06bc0cf93256ff`
- restored checkpoint reuse: `prompt_n=38`, `cache_n=1244`
- live/restored deterministic output SHA-256: `706156e10c3e1c7e7f063dc1a496be57ce2667ff015b4a21d27773003cbe2558`

The restored request therefore reproduced the live checkpoint rollback exactly for prompt work, cached-token count, and deterministic output on the certified configuration. The exact tested llama.cpp source is recorded because this certification does not imply that every llama.cpp build persists the checkpoint state required by hybrid/recurrent models.

## Claim scope

Public descriptions of RivetCache should state implemented and tested capabilities directly. Compatibility, interoperability, equivalence, or feature-comparison claims involving third-party products require separate documented evidence and are not implied by this certification.

The llama-server result above is an implementation-specific certification for the exact external-runtime source, model, backend, and hardware listed above. It is not a blanket compatibility or performance claim for other builds, models, accelerators, or inference engines.

## Production provenance

The cache design was extracted from a production-proven Rust cache subsystem on August 28, 2026. The production integration exercised stable namespaced keys, memory hit/miss behavior, LRU eviction, restart persistence, explicit clearing/invalidation, corruption recovery, TTL expiry, deterministic replay, request isolation, stochastic bypass, and runtime prefix/KV reuse.

The standalone project uses its own `RIVET_CACHE_V1` key domain and `RIVET01` disk format, so its public ABI is independent and intentionally versioned from release 0.1.0 onward.
