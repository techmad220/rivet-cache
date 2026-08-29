# llama-server slot persistence

RivetCache 0.6.0 provides an optional std-only bridge for persisting llama-server slot state through `ContextCache` without making an inference runtime, GPU SDK, HTTP framework, or async executor a core dependency.

The bridge operates across llama-server's local slot-control boundary. It requests slot save/restore/erase operations, validates the resulting state file, stores the complete file as opaque bytes under a content-addressed RivetCache key, and materializes restored bytes atomically back into the configured slot directory.

## Command-line bridge

The `rivet-cache-llama-slot` binary supports four operations:

- `capture` — ask llama-server to save a slot and store the complete state file in RivetCache.
- `contains` — check whether the scoped state key is present.
- `erase` — ask llama-server to erase the selected live slot state.
- `restore` — materialize the cached bytes and ask llama-server to restore them into the selected slot.

The key scope includes the model fingerprint and caller-supplied logical state identity. The bridge also records byte count and SHA-256 receipts so a hardware/runtime certification can verify that the persisted file is restored without byte drift.

## Trust and safety boundary

This bridge is intended for a local or otherwise trusted llama-server control endpoint. It does not add authentication or TLS to llama-server itself.

Slot filenames are restricted to safe basenames. The implementation rejects traversal, hidden paths, non-regular files, symlinks, and paths whose canonical location escapes the configured slot root. State and HTTP response sizes are bounded. Restore materialization uses a temporary file plus rename rather than exposing a partially written state file.

## Runtime semantics

RivetCache preserves the complete state file supplied by the external runtime. A successful byte-for-byte restore is necessary but does not by itself prove that a runtime build serialized every state component required for later reuse.

The release certification therefore requires a live control and a persisted control. Both execute a divergent continuation from the same prefix. Persisted restoration passes only when it reproduces the live runtime's reduced prompt work, cached-token count, and deterministic output.

The exact certified runtime source and hardware receipt are recorded in `CERTIFICATION.md`. Other llama.cpp revisions, models, backends, or accelerators require their own runtime-specific evidence before making a compatibility or performance claim.
