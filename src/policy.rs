use sha2::{Digest, Sha256};
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

/// Clock abstraction used for TTL semantics.
///
/// Inject a deterministic implementation in tests or a host-specific clock in
/// embedded environments.
pub trait Clock: Send + Sync {
    fn now_seconds(&self) -> u64;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

/// Generates and validates cache keys.
pub trait KeyStrategy: Send + Sync {
    fn make_key(&self, namespace: &str, model_fingerprint: &str, payload: &str) -> String;
    fn validate(&self, key: &str) -> io::Result<()>;
}

/// Default RivetCache v1 SHA-256 key strategy.
#[derive(Debug, Clone, Copy, Default)]
pub struct Sha256KeyStrategy;

impl KeyStrategy for Sha256KeyStrategy {
    fn make_key(&self, namespace: &str, model_fingerprint: &str, payload: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"RIVET_CACHE_V1\0");
        update_length_prefixed(&mut digest, namespace.as_bytes());
        update_length_prefixed(&mut digest, model_fingerprint.as_bytes());
        update_length_prefixed(&mut digest, payload.as_bytes());
        hex::encode(digest.finalize())
    }

    fn validate(&self, key: &str) -> io::Result<()> {
        let valid = key.len() == 64
            && key
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte));
        if valid {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache key must be 64 lowercase hexadecimal characters",
            ))
        }
    }
}

fn update_length_prefixed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionCandidate {
    pub key: String,
    pub last_access: u64,
    pub size_bytes: u64,
}

/// Selects which unpinned entry should be evicted.
///
/// RivetCache filters pinned entries before invoking the policy, so custom
/// policies cannot accidentally evict pinned records.
pub trait EvictionPolicy: Send + Sync {
    fn choose_victim(&self, candidates: &[EvictionCandidate]) -> Option<usize>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LruEviction;

impl EvictionPolicy for LruEviction {
    fn choose_victim(&self, candidates: &[EvictionCandidate]) -> Option<usize> {
        candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, candidate)| candidate.last_access)
            .map(|(index, _)| index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheEvent {
    MemoryHit,
    PersistentHit,
    Miss,
    Write,
    MemoryEviction(u64),
    PersistentEviction(u64),
    Expiration(u64),
    Corruption(u64),
    Invalidation(u64),
    Clear,
}

/// Optional telemetry hook.
///
/// Implementations should return quickly and must not call back into the same
/// cache instance.
pub trait MetricsSink: Send + Sync {
    fn record(&self, event: CacheEvent);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetrics;

impl MetricsSink for NoopMetrics {
    fn record(&self, _event: CacheEvent) {}
}
