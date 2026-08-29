# LMCache-class parity work

This document tracks RivetCache's implementation-level comparison surface for the v0.9 parity branch. It is not a blanket performance-equivalence claim.

## Already present before this branch

RivetCache already provided runtime-neutral KV tiers and movement, prefix and non-prefix reuse, quality-aware boundary recomputation, pinned/NUMA host allocation, native HIP, hipFile storage, Redis/Valkey, S3-compatible storage, a standalone TCP cache service, controller quotas and fleet health, Prometheus metrics, plugin factories, pluggable payload codecs, llama.cpp adapters, disaggregated prefill/decode routing, and native NIXL/UCX transfer.

## Added on this branch

- bounded nonblocking KV worker pool with wait/check/finish semantics
- batched asynchronous store/retrieve/move/prefetch/invalidate/pin/clear/health operations
- engine-independent multiprocess request facade with sessions, lookup, store, retrieve, lookup-to-retrieve, pin/unpin, delete, clear and health flows
- event bus with subscriber isolation, Prometheus mapping, and injected OpenTelemetry exporter boundary
- runtime cache-management controller for lookup, move, codec-tier compress/decompress movement, pin/unpin, delete, clear, health and check-finish
- first-class vLLM host callback adapter for capture, restore, relocated restore, health and quality-recovery recomputation
- native runtime-loaded Mooncake Store tier using upstream `store_c.h` / `libmooncake_store.so`

## Claim boundary

The vLLM callback ABI is defined by RivetCache for embedding hosts; it is not presented as an undocumented upstream vLLM internal ABI. Mooncake interoperability depends on a compatible upstream shared library and requires its own executable integration certification before a release may claim a tested Mooncake deployment. InfiniStore and other SDK-specific backends can still be supplied through RivetCache's plugin/transport boundaries until a native binding is implemented and certified.

Feature parity does not imply throughput parity. Comparative performance requires repeatable benchmarks on matched hardware, models, cache layouts and serving workloads.
