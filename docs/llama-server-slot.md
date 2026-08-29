# llama-server slot persistence

RivetCache v0.6.0 adds an optional process-level bridge for saving and restoring llama-server slot state through `ContextCache`.

The bridge intentionally uses the server's exposed slot persistence endpoint rather than depending on private KV tensor layouts or an inference-runtime internal ABI. The core RivetCache library remains runtime-neutral and does not acquire an inference-engine, GPU SDK, HTTP framework, or async-runtime dependency.

## CLI

```text
rivet-cache-llama-slot <capture|restore|erase|contains> <server-ip:port> <slot-root> <cache-root> <model-fingerprint> <logical-identity> <slot-id> [max-state-bytes] [cache-capacity-bytes]
```

The llama-server process must be configured with its slot endpoint enabled and a slot-save directory equal to `<slot-root>`.

`capture` asks the selected server slot to materialize its state, verifies that the resulting path is a regular non-symlink file directly inside the configured slot directory, bounds the payload size, cross-checks any server-reported byte count, stores the bytes in RivetCache, emits a SHA-256 receipt, and removes the transient slot file.

`restore` reads the cached state, materializes it atomically into the slot directory, asks llama-server to restore it into the selected slot, cross-checks any server-reported byte count, emits a SHA-256 receipt, and removes the transient file.

`erase` clears the selected live server slot. `contains` verifies that the content-addressed slot state is present in RivetCache.

## Cache identity

Slot states are addressed through the injected RivetCache key strategy in the `LLAMA_SERVER_SLOT_V1` namespace. The key input includes both a caller-supplied model fingerprint and logical state identity. Callers should use an immutable model fingerprint and a deterministic identity for the prompt/state they intend to restore.

## Safety boundary

The bundled controller only connects to a caller-supplied socket address and is intended for a local or otherwise trusted server boundary. It does not add authentication or TLS.

The bridge does not trust caller filenames and does not accept arbitrary paths. Generated state filenames are simple basenames. Reads and removals reject symlinks and non-regular files, canonicalized saved files must remain direct children of the configured slot root, restored state is written with a create-new temporary file plus `sync_all` and rename, and state/HTTP response sizes are bounded.

This hardening is part of RivetCache's bridge; it is not a statement that every external server build has equivalent path handling.

## Claim scope

A RivetCache build passing unit and cross-platform CI demonstrates the bridge contract and its local safety checks. Compatibility with a particular llama-server build, model, GPU backend, or hardware combination is claimed only when that exact integration has a separate recorded runtime certification.
