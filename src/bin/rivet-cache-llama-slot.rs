use rivet_cache::ContextCache;
use std::env;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[path = "../llama_server.rs"]
mod llama_server;

use llama_server::{
    HttpLlamaServerSlotControl, LlamaServerSlotBridge, LlamaServerSlotControl,
    LlamaServerSlotReceipt,
};

const DEFAULT_MAX_STATE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_CACHE_CAPACITY_BYTES: u64 = 4 * 1024 * 1024 * 1024;

fn main() {
    if let Err(error) = run() {
        eprintln!("RIVET_LLAMA_SLOT=FAIL error={error}");
        std::process::exit(1);
    }
}

fn run() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 8 || args.len() > 10 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            usage(),
        ));
    }

    let command = args[1].as_str();
    if !matches!(command, "capture" | "restore" | "erase" | "contains") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            usage(),
        ));
    }

    let server: SocketAddr = args[2].parse().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "invalid llama-server socket address")
    })?;
    let slot_root = PathBuf::from(&args[3]);
    let cache_root = PathBuf::from(&args[4]);
    let model_fingerprint = &args[5];
    let logical_identity = &args[6];
    let slot_id: u32 = args[7].parse().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "invalid llama-server slot id")
    })?;
    let max_state_bytes = parse_optional_u64(&args, 8, DEFAULT_MAX_STATE_BYTES, "max state bytes")?;
    let cache_capacity_bytes = parse_optional_u64(
        &args,
        9,
        DEFAULT_CACHE_CAPACITY_BYTES,
        "cache capacity bytes",
    )?;
    if cache_capacity_bytes < max_state_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache capacity must be at least the maximum slot state size",
        ));
    }

    let cache = Arc::new(ContextCache::new(
        Some(cache_root),
        0,
        cache_capacity_bytes,
        Duration::ZERO,
    )?);
    let control: Arc<dyn LlamaServerSlotControl> = Arc::new(
        HttpLlamaServerSlotControl::new(server)
            .with_timeout(Duration::from_secs(180))?,
    );
    let bridge = LlamaServerSlotBridge::new(cache, control, slot_root, max_state_bytes)?;

    match command {
        "capture" => {
            let receipt = bridge.capture(slot_id, model_fingerprint, logical_identity, true)?;
            print_receipt("CAPTURE", &receipt);
        }
        "restore" => {
            let receipt = bridge.restore(slot_id, model_fingerprint, logical_identity)?;
            print_receipt("RESTORE", &receipt);
        }
        "erase" => {
            let action = bridge.erase(slot_id)?;
            println!(
                "RIVET_LLAMA_SLOT_ERASE=PASS slot={} tokens={}",
                action.slot_id,
                action
                    .token_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            );
        }
        "contains" => {
            let contains = bridge.contains(model_fingerprint, logical_identity)?;
            println!("RIVET_LLAMA_SLOT_CONTAINS={} ", if contains { "PASS" } else { "MISS" });
            if !contains {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "requested slot state is not cached",
                ));
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn print_receipt(operation: &str, receipt: &LlamaServerSlotReceipt) {
    println!(
        "RIVET_LLAMA_SLOT_{operation}=PASS key={} bytes={} sha256={} slot={} tokens={}",
        receipt.cache_key,
        receipt.state_bytes,
        receipt.state_sha256,
        receipt.action.slot_id,
        receipt
            .action
            .token_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
}

fn parse_optional_u64(
    args: &[String],
    index: usize,
    default: u64,
    label: &str,
) -> io::Result<u64> {
    match args.get(index) {
        None => Ok(default),
        Some(value) => {
            let parsed = value.parse::<u64>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {label}"))
            })?;
            if parsed == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{label} must be greater than zero"),
                ));
            }
            Ok(parsed)
        }
    }
}

fn usage() -> &'static str {
    "usage: rivet-cache-llama-slot <capture|restore|erase|contains> <server-ip:port> <slot-root> <cache-root> <model-fingerprint> <logical-identity> <slot-id> [max-state-bytes] [cache-capacity-bytes]"
}
