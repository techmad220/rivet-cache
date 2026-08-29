# Multiprocess runtime

The v0.9 parity branch adds an engine-independent multiprocess cache facade without making the core depend on Python, ZeroMQ, CUDA, or a particular serving runtime.

`AsyncKvPipeline` is the canonical nonblocking execution path. Submission methods return an operation id immediately; `snapshot`, `wait`, and `finish` are the explicit observation/blocking points. The bounded queue returns `WouldBlock` rather than growing without limit.

`MpCacheService` adds session-scoped LOOKUP, STORE, RETRIEVE, LOOKUP-to-RETRIEVE, PIN/UNPIN, DELETE, CLEAR, and HEALTH flows. Stored blocks are checked against the configured token/chunk identity before being accepted, and prefix records are only published after the asynchronous store completes.

`RuntimeCacheController` exposes management operations for lookup, tier movement, codec-tier compression/decompression movement, pin/unpin, delete, clear, health, and check-finish. Compression remains codec-injected: the controller moves entries into or out of a `CodecKvTier` rather than hard-coding one algorithm.

`KvEventBus` decouples the data path from metrics/tracing subscribers. `PrometheusEventSubscriber` writes to the built-in registry; `OtelEventSubscriber` delegates to an injected exporter so deployments can select their OpenTelemetry SDK/runtime.

The `rivet-cache-mp` binary provides a process-isolated owner for an MP cache + worker pool. RivetCache's existing `TcpKvServer` remains the built-in binary wire data plane; operators can embed `MpCacheService` behind an HTTP/gRPC/IPC control transport without changing cache semantics.
