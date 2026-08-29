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

The final reconciled implementation was hardware-certified on August 28, 2026 against these exact inputs:

- RivetCache source: `f9a66a45e43ed4191a630f423a78b8eee6a9a3c1`
- certified Git tree: `aa84b44c0d35c7188fef9ec9dd1f155ceec889de`
- llama.cpp checkpoint-preserving source: `06d9d0ff54b586514a59268e2c780abc08473daa`
- accelerator: AMD Radeon RX 6800 XT
- backend: Vulkan
- model digest: `sha256:95580dbdaad579582ee898257116abc18d7f3625a00c16a15735d41444a09f5e`
- hardware workflow run: `33227532477`
- hardware job: `99034037650`

The certification disabled llama.cpp's separate RAM prompt cache and exercised a divergent-prefix control before and after persistent restoration. The observed receipt was:

- cold request: `prompt_n=1440`
- live checkpoint reuse: `prompt_n=38`, `cache_n=1404`
- persisted state: `565548924` bytes
- persisted-state SHA-256: `10320ef777b04151e8302d56451b7669a93d23ee5553ba6da69f68e1e2f52a71`
- restored checkpoint reuse: `prompt_n=38`, `cache_n=1404`
- live/restored deterministic output SHA-256: `fc0d806d0382dfc42c75fea63a1e7e27097c691826f6ab56100198757a6e64eb`
- production llama-server restoration after the certification: PASS

The restored request therefore reproduced the live checkpoint rollback exactly for prompt work, cached-token count, and deterministic output on the certified configuration. The exact tested llama.cpp source is recorded because this certification does not imply that every llama.cpp build persists the checkpoint state required by hybrid/recurrent models.

The protected merge commit `791f306fb5bb6fd9db35a0c9d1ee8bc50b01d1da` preserved certified tree `aa84b44c0d35c7188fef9ec9dd1f155ceec889de` exactly. Later documentation-only receipt corrections do not alter the v0.6 runtime implementation; release packaging and exact release-head gates are recorded separately by the release workflow.

## Claim scope

Public descriptions of RivetCache should state implemented and tested capabilities directly. Compatibility, interoperability, equivalence, or feature-comparison claims involving third-party products require separate documented evidence and are not implied by this certification.

The llama-server result above is an implementation-specific certification for the exact external-runtime source, model, backend, and hardware listed above. It is not a blanket compatibility or performance claim for other builds, models, accelerators, or inference engines.

## Production provenance

The cache design was extracted from a production-proven Rust cache subsystem on August 28, 2026. The production integration exercised stable namespaced keys, memory hit/miss behavior, LRU eviction, restart persistence, explicit clearing/invalidation, corruption recovery, TTL expiry, deterministic replay, request isolation, stochastic bypass, and runtime prefix/KV reuse.

The standalone project uses its own `RIVET_CACHE_V1` key domain and `RIVET01` disk format, so its public ABI is independent and intentionally versioned from release 0.1.0 onward.
