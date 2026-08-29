use rivet_cache::{
    AsyncKvPipeline, ContextCache, ContextCacheTier, KvEngine, KvEventBus, MpCacheConfig,
    MpCacheService, MpEngineKind, MpTransferMode, PrefixIndex, PrometheusEventSubscriber,
    PrometheusRegistry,
};
use std::env;
use std::io;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let cache_dir = args.next().unwrap_or_else(|| "./rivet-mp-cache".to_owned());
    let memory_mib = parse_u64(args.next().as_deref(), 1024, "memory MiB")?;
    let persistent_mib = parse_u64(args.next().as_deref(), 8192, "persistent MiB")?;
    let block_tokens = parse_usize(args.next().as_deref(), 256, "block tokens")?;
    let workers = parse_usize(args.next().as_deref(), 4, "worker count")?;
    let queue = parse_usize(args.next().as_deref(), 1024, "queue capacity")?;

    let cache = Arc::new(
        ContextCache::builder()
            .memory_capacity(mib(memory_mib)?)
            .persistent_capacity(mib(persistent_mib)?)
            .persistent_store(rivet_cache::FileStore::new(cache_dir)?)
            .build()?,
    );
    let engine = KvEngine::builder()
        .tier(ContextCacheTier::new("mp-l1", cache)?)
        .build()?;
    let metrics = Arc::new(PrometheusRegistry::new());
    let events = Arc::new(KvEventBus::new());
    events.subscribe(Arc::new(PrometheusEventSubscriber::new(metrics.clone())))?;
    let pipeline = AsyncKvPipeline::new(engine.clone(), workers, queue, Some(events))?;
    let service = MpCacheService::new(
        engine,
        pipeline,
        Arc::new(PrefixIndex::new()),
        MpCacheConfig {
            block_tokens,
            layer_start: 0,
            layer_count: 1,
            layout_version: 1,
            l1_tier: 0,
            transfer_mode: MpTransferMode::Auto,
            engine_kind: MpEngineKind::Default,
        },
    )?;

    println!("RIVET_MP_DAEMON=READY");
    println!("chunk_tokens={}", service.chunk_size());
    println!("tiers={}", service.engine().tier_names().join(","));
    println!("mode=embedded-control-facade");
    println!("note=use MpCacheService or RuntimeCacheController from an embedding control plane");

    // The binary owns the process-isolated cache and worker pool. Network/control protocols stay
    // injectable; the existing TcpKvServer remains the built-in wire data plane. Keeping this
    // process alive lets embedding supervisors attach their preferred control transport.
    loop {
        thread::sleep(Duration::from_secs(60));
        if !service.finish(Duration::ZERO)? {
            continue;
        }
        let health = service.engine().health();
        if health.iter().any(|tier| !tier.healthy) {
            eprintln!("RIVET_MP_HEALTH=DEGRADED");
        }
    }
}

fn mib(value: u64) -> io::Result<u64> {
    value
        .checked_mul(1024 * 1024)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "MiB capacity overflows u64"))
}

fn parse_u64(value: Option<&str>, default: u64, name: &str) -> io::Result<u64> {
    match value {
        Some(value) => value
            .parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {name}"))),
        None => Ok(default),
    }
}

fn parse_usize(value: Option<&str>, default: usize, name: &str) -> io::Result<usize> {
    match value {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {name}"))),
        None => Ok(default),
    }
}
