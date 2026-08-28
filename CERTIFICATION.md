# Certification provenance

The initial standalone implementation was extracted from the Rust-native AION cache at source commit:

`0467bfba22ae19939ec6ac1e7f8ab6120488b433`

AION's canonical live cache certification for that lineage completed successfully on August 28, 2026 with the final receipt `AION_CACHE_PROVEN=PASS`.

The integration certification covered stable/namespaced keys, memory hit/miss behavior, LRU eviction, restart persistence, explicit clearing/invalidation, corruption recovery, TTL expiry, live routing, deterministic exact replay, request isolation, stochastic bypass, and llama.cpp prefix/KV reuse.

Observed integration performance:

- Deterministic exact replay: **118.67x** speedup.
- Best llama.cpp prefix/KV reuse: **25.29x** speedup.

The standalone repository's CI independently validates the extracted Rust crate. AION integration measurements are not guaranteed performance for every consumer or model runtime.
