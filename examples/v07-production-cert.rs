use rivet_cache::{
    CacheController, ControllerServer, KvBlock, KvBlockKey, KvBlockRange, KvTier, KvTierEntry,
    PinnedMemoryPool, PrometheusRegistry, RedisKvTier, S3Config, S3Credentials, S3KvTier,
    TcpHttpClient, WorkerRole,
};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

fn main() -> io::Result<()> {
    let command = std::env::args().nth(1).unwrap_or_else(|| "all".to_owned());
    match command.as_str() {
        "native-memory" => certify_native_memory(),
        "controller" => certify_controller(),
        "redis" => certify_redis(),
        "s3" => certify_s3(),
        "all" => {
            certify_native_memory()?;
            certify_controller()?;
            certify_redis()?;
            certify_s3()?;
            println!("RIVET_V07_PRODUCTION_CERT=PASS");
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: v07-production-cert [native-memory|controller|redis|s3|all]",
        )),
    }
}

fn certify_native_memory() -> io::Result<()> {
    let pool = PinnedMemoryPool::native(64 * 1024);
    if !pool.page_locked() {
        return Err(io::Error::other(
            "native memory allocator does not advertise page locking",
        ));
    }
    let mut first = pool.acquire(4096, None)?;
    for (index, byte) in first.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    for (index, byte) in first.iter().copied().enumerate() {
        if byte != (index % 251) as u8 {
            return Err(io::Error::other(
                "native page-locked memory verification failed",
            ));
        }
    }
    drop(first);
    let second = pool.acquire(1024, None)?;
    if second.capacity() < 4096 {
        return Err(io::Error::other(
            "pinned memory pool did not reuse cached region",
        ));
    }
    let stats = pool.stats()?;
    if stats.allocations != 1 || stats.reuses < 1 {
        return Err(io::Error::other(format!(
            "unexpected pinned pool stats allocations={} reuses={}",
            stats.allocations, stats.reuses
        )));
    }
    println!(
        "RIVET_V07_NATIVE_MEMORY=PASS allocator={} numa_supported={} allocations={} reuses={}",
        pool.allocator_name(),
        pool.numa_supported(),
        stats.allocations,
        stats.reuses
    );
    Ok(())
}

fn certify_controller() -> io::Result<()> {
    let metrics = Arc::new(PrometheusRegistry::new());
    let controller = CacheController::new(Arc::clone(&metrics));
    controller.quotas.set_quota(
        "cert-tenant",
        rivet_cache::TenantQuota {
            max_bytes: 1_048_576,
            max_entries: 1024,
            max_inflight: 4,
        },
    )?;
    controller.fleet.heartbeat(
        "prefill-1",
        "127.0.0.1:65432",
        WorkerRole::Prefill,
        1_073_741_824,
        4096,
    )?;
    let server = ControllerServer::spawn("127.0.0.1:0", controller)?;
    let addr = server.local_addr().to_owned();

    let health = http_request(&addr, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    if !health.starts_with("HTTP/1.1 200") || !health.contains("{\"ok\":true}") {
        return Err(io::Error::other(format!(
            "controller health response failed: {health}"
        )));
    }
    let metrics_response = http_request(&addr, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    if !metrics_response.starts_with("HTTP/1.1 200")
        || !metrics_response.contains("rivet_controller_nodes 1")
        || !metrics_response.contains("rivet_controller_tenants 1")
    {
        return Err(io::Error::other(format!(
            "controller metrics response failed: {metrics_response}"
        )));
    }
    server.shutdown()?;
    println!("RIVET_V07_CONTROLLER_HTTP=PASS addr={addr}");
    Ok(())
}

fn certify_redis() -> io::Result<()> {
    let address = std::env::var("RIVET_REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    let namespace = format!(
        "rivet-v07-cert-{}",
        std::env::var("GITHUB_RUN_ID").unwrap_or_else(|_| "local".to_owned())
    );
    let tier = RedisKvTier::new("redis-cert", &address, namespace, 8 * 1024 * 1024)?;
    tier.health()?;
    let entry = cert_entry(0x51)?;
    tier.put(&entry)?;
    let restored = tier
        .get(&entry.block.key)?
        .ok_or_else(|| io::Error::other("Redis connector missed freshly written entry"))?;
    if restored != entry {
        return Err(io::Error::other(
            "Redis connector round-trip changed KV entry",
        ));
    }
    tier.clear()?;
    if tier.get(&entry.block.key)?.is_some() {
        return Err(io::Error::other(
            "Redis namespace clear left KV entry behind",
        ));
    }
    println!("RIVET_V07_REDIS=PASS addr={address}");
    Ok(())
}

fn certify_s3() -> io::Result<()> {
    let endpoint =
        std::env::var("RIVET_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_owned());
    let region = std::env::var("RIVET_S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let bucket = std::env::var("RIVET_S3_BUCKET").unwrap_or_else(|_| "rivet-cache-cert".to_owned());
    let access_key = std::env::var("RIVET_S3_ACCESS_KEY")
        .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "RIVET_S3_ACCESS_KEY is required"))?;
    let secret_key = std::env::var("RIVET_S3_SECRET_KEY")
        .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "RIVET_S3_SECRET_KEY is required"))?;
    let tier = S3KvTier::new(
        "s3-cert",
        S3Config {
            endpoint: endpoint.clone(),
            region,
            bucket,
            prefix: format!(
                "rivet-v07-cert-{}",
                std::env::var("GITHUB_RUN_ID").unwrap_or_else(|_| "local".to_owned())
            ),
            max_value_bytes: 8 * 1024 * 1024,
        },
        S3Credentials {
            access_key,
            secret_key,
            session_token: None,
        },
        Arc::new(TcpHttpClient::default()),
    )?;
    tier.health()?;
    let entry = cert_entry(0xA7)?;
    tier.put(&entry)?;
    let restored = tier
        .get(&entry.block.key)?
        .ok_or_else(|| io::Error::other("S3 connector missed freshly written entry"))?;
    if restored != entry {
        return Err(io::Error::other("S3 connector round-trip changed KV entry"));
    }
    tier.remove(&entry.block.key)?;
    if tier.get(&entry.block.key)?.is_some() {
        return Err(io::Error::other("S3 DELETE left KV entry behind"));
    }
    println!("RIVET_V07_S3_SIGV4=PASS endpoint={endpoint}");
    Ok(())
}

fn cert_entry(pattern: u8) -> io::Result<KvTierEntry> {
    let tokens = [10_u32, 20, 30, 40, 50, 60, 70, 80];
    let key = KvBlockKey::from_prefix(
        "rivet-v07-production-cert",
        &tokens,
        KvBlockRange {
            block_index: 0,
            token_start: 0,
            token_count: tokens.len() as u32,
            layer_start: 0,
            layer_count: 8,
            layout_version: 1,
        },
    );
    Ok(KvTierEntry {
        block: KvBlock::new(key, vec![pattern; 64 * 1024])?,
        expires_at: 0,
        pinned: true,
    })
}

fn http_request(address: &str, request: &str) -> io::Result<String> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}
