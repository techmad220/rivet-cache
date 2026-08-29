use rivet_cache::{ContextCache, ContextCacheTier, KvTier, RemoteLimits, TcpKvServer};
use std::env;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let bind = args.next().unwrap_or_else(|| "127.0.0.1:65432".to_string());
    let root = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("rivet-cache-data"));
    let memory_mib = parse_mib(args.next(), 512, "memory_mib")?;
    let disk_mib = parse_mib(args.next(), 8192, "disk_mib")?;
    if args.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: rivet-cache-server [bind] [root] [memory_mib] [disk_mib]",
        ));
    }

    let mib = 1024_u64 * 1024;
    let cache = Arc::new(ContextCache::new(
        Some(root),
        memory_mib.saturating_mul(mib),
        disk_mib.saturating_mul(mib),
        Duration::ZERO,
    )?);
    let tier: Arc<dyn KvTier> = Arc::new(ContextCacheTier::new("server-cache", cache)?);
    let server = TcpKvServer::spawn(&bind, tier, RemoteLimits::default())?;
    eprintln!("RivetCache TCP KV server listening on {}", server.local_addr());
    eprintln!("Protocol is intended for trusted application networks; terminate or tunnel it at an authenticated boundary when crossing trust zones.");

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn parse_mib(value: Option<String>, default: u64, name: &str) -> io::Result<u64> {
    match value {
        Some(value) => value.parse::<u64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be an unsigned integer"),
            )
        }),
        None => Ok(default),
    }
}
