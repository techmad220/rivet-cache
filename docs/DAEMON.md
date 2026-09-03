# RivetCache daemon operations

This document covers the dedicated RivetCache daemon and its built-in diagnostic command.

## Start the daemon

The daemon binds both interfaces to loopback by default:

```text
cargo run --release --bin rivet-cache-daemon
```

Default endpoints:

- KV data plane: `127.0.0.1:65432`
- control and Prometheus plane: `127.0.0.1:65433`
- persistent cache root: `rivet-cache-data`
- memory capacity: 512 MiB
- persistent capacity: 8192 MiB

Explicit configuration:

```text
cargo run --release --bin rivet-cache-daemon -- \
  --data-bind 127.0.0.1:65432 \
  --control-bind 127.0.0.1:65433 \
  --root /var/lib/rivet-cache \
  --memory-mib 4096 \
  --disk-mib 65536
```

On Windows, use a Windows path for `--root`. The bind address format is the same.

The ready banner reports the actual data and control addresses. `--version` and `--help` are supported by both daemon and doctor binaries.

## Readiness and health

`rivet-cache-doctor` is the daemon readiness contract. A successful exit requires all of the following:

1. the KV data plane accepts a protocol `PING`;
2. the control plane returns HTTP 200 from `/health`;
3. the control plane returns HTTP 200 from `/metrics`;
4. `rivet_daemon_ready` is exactly `1`;
5. daemon capacity and build-info metrics are present and parseable.

Human-readable check:

```text
cargo run --release --bin rivet-cache-doctor
```

Machine-readable check:

```text
cargo run --release --bin rivet-cache-doctor -- --json
```

Remote or non-default endpoints:

```text
cargo run --release --bin rivet-cache-doctor -- \
  --data 127.0.0.1:7001 \
  --control 127.0.0.1:7002 \
  --json
```

Exit code `0` means the complete readiness contract passed. Any failed data-plane, health, metrics, readiness, or required-metadata check returns a non-zero exit code.

## Prometheus metrics

The control listener exposes `/metrics`. The daemon registers:

- `rivet_daemon_ready` — `1` only after both listeners and shared telemetry are initialized; set to `0` before managed shutdown;
- `rivet_daemon_build_info{tier="daemon-cache",version="..."}` — daemon build/version identity;
- `rivet_daemon_capacity_bytes{tier="memory"}` — configured in-memory capacity;
- `rivet_daemon_capacity_bytes{tier="persistent"}` — configured persistent capacity;
- `rivet_kv_tier_requests_total` — instrumented KV operations and status;
- `rivet_kv_tier_operation_micros` — KV operation latency histogram;
- `rivet_kv_tier_bytes_total` — KV read/write byte counters;
- controller tenant/node metrics when those controller facilities are used.

The doctor consumes the readiness, build-info, and capacity metrics rather than treating a live TCP socket as sufficient proof of service readiness.

## Shutdown behavior

Embedding applications should own `RivetDaemon` and call `RivetDaemon::stop()` for managed shutdown. That path marks readiness false, stops the control listener, stops the KV listener, joins their threads, and returns shutdown failures to the caller.

The standalone `rivet-cache-daemon` binary is intended to run under an external process/service supervisor. Cross-platform signal-to-managed-shutdown handling is not yet implemented in the standalone binary; do not claim graceful process-signal shutdown until that work is certified.

## Security boundary

The daemon defaults to loopback intentionally. The current TCP KV and controller protocols are not a public-internet trust boundary.

For current deployments:

- keep both listeners on loopback or a trusted private boundary;
- do not expose either listener directly to the public internet;
- put authentication, authorization, and TLS/mTLS in front of any non-loopback deployment;
- restrict filesystem permissions on the persistent cache root;
- avoid placing secrets or raw prompt payloads in operational logs.

Native protocol authentication/TLS remains a separate production-parity item. The loopback defaults are a safe default, not a substitute for that work.

## What this does not certify

Daemon health does not prove engine or hardware parity. Real llama.cpp/vLLM interoperability, AMD/NIXL execution, long-running soak behavior, fault injection, and matched LMCache performance still require executable evidence on the relevant runtimes and hardware.
