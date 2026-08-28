mod policy;
mod store;

pub use policy::{
    CacheEvent, Clock, EvictionCandidate, EvictionPolicy, KeyStrategy, LruEviction, MetricsSink,
    NoopMetrics, Sha256KeyStrategy, SystemClock,
};
pub use store::{
    FileStore, PersistentStore, PutOutcome, StoreRecord, StoreSnapshot, StoredEntry,
};

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub memory_hits: u64,
    pub disk_hits: u64,
    pub misses: u64,
    pub writes: u64,
    pub memory_evictions: u64,
    pub disk_evictions: u64,
    pub expirations: u64,
    pub corruptions: u64,
    pub memory_entries: u64,
    pub disk_entries: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Debug, Clone)]
struct MemoryRecord {
    value: Arc<Vec<u8>>,
    expires_at: u64,
    pinned: bool,
    last_access: u64,
}

#[derive(Debug, Default)]
struct Inner {
    memory: HashMap<String, MemoryRecord>,
    disk: HashMap<String, StoreRecord>,
    memory_bytes: u64,
    disk_bytes: u64,
    tick: u64,
    stats: CacheStats,
}

/// Builder for a dependency-injected RivetCache instance.
///
/// The default components are `SystemClock`, `Sha256KeyStrategy`,
/// `LruEviction`, and `NoopMetrics`. Persistent storage is opt-in.
pub struct ContextCacheBuilder {
    max_memory_bytes: u64,
    max_disk_bytes: u64,
    default_ttl: Duration,
    store: Option<Arc<dyn PersistentStore>>,
    clock: Arc<dyn Clock>,
    key_strategy: Arc<dyn KeyStrategy>,
    eviction_policy: Arc<dyn EvictionPolicy>,
    metrics: Arc<dyn MetricsSink>,
}

impl Default for ContextCacheBuilder {
    fn default() -> Self {
        Self {
            max_memory_bytes: 0,
            max_disk_bytes: 0,
            default_ttl: Duration::ZERO,
            store: None,
            clock: Arc::new(SystemClock),
            key_strategy: Arc::new(Sha256KeyStrategy),
            eviction_policy: Arc::new(LruEviction),
            metrics: Arc::new(NoopMetrics),
        }
    }
}

impl ContextCacheBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn memory_capacity(mut self, bytes: u64) -> Self {
        self.max_memory_bytes = bytes;
        self
    }

    pub fn persistent_capacity(mut self, bytes: u64) -> Self {
        self.max_disk_bytes = bytes;
        self
    }

    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    pub fn persistent_store<T>(mut self, store: T) -> Self
    where
        T: PersistentStore + 'static,
    {
        self.store = Some(Arc::new(store));
        self
    }

    pub fn persistent_store_arc(mut self, store: Arc<dyn PersistentStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn clock<T>(mut self, clock: T) -> Self
    where
        T: Clock + 'static,
    {
        self.clock = Arc::new(clock);
        self
    }

    pub fn clock_arc(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn key_strategy<T>(mut self, strategy: T) -> Self
    where
        T: KeyStrategy + 'static,
    {
        self.key_strategy = Arc::new(strategy);
        self
    }

    pub fn key_strategy_arc(mut self, strategy: Arc<dyn KeyStrategy>) -> Self {
        self.key_strategy = strategy;
        self
    }

    pub fn eviction_policy<T>(mut self, policy: T) -> Self
    where
        T: EvictionPolicy + 'static,
    {
        self.eviction_policy = Arc::new(policy);
        self
    }

    pub fn eviction_policy_arc(mut self, policy: Arc<dyn EvictionPolicy>) -> Self {
        self.eviction_policy = policy;
        self
    }

    pub fn metrics<T>(mut self, sink: T) -> Self
    where
        T: MetricsSink + 'static,
    {
        self.metrics = Arc::new(sink);
        self
    }

    pub fn metrics_arc(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.metrics = sink;
        self
    }

    pub fn build(self) -> io::Result<ContextCache> {
        ContextCache::from_builder(self)
    }
}

/// Bounded cache with dependency-injected policy and persistence components.
///
/// `ContextCache::new` preserves the simple zero-config filesystem API. Use
/// `ContextCache::builder` when replacing storage, keying, clock, eviction, or
/// metrics behavior.
pub struct ContextCache {
    max_memory_bytes: u64,
    max_disk_bytes: u64,
    default_ttl: Duration,
    store: Option<Arc<dyn PersistentStore>>,
    clock: Arc<dyn Clock>,
    key_strategy: Arc<dyn KeyStrategy>,
    eviction_policy: Arc<dyn EvictionPolicy>,
    metrics: Arc<dyn MetricsSink>,
    inner: Mutex<Inner>,
}

impl ContextCache {
    /// Compatibility constructor using the default injected components.
    pub fn new(
        root: Option<PathBuf>,
        max_memory_bytes: u64,
        max_disk_bytes: u64,
        default_ttl: Duration,
    ) -> io::Result<Self> {
        let mut builder = Self::builder()
            .memory_capacity(max_memory_bytes)
            .persistent_capacity(max_disk_bytes)
            .default_ttl(default_ttl);

        if max_disk_bytes > 0 {
            if let Some(root) = root {
                builder = builder.persistent_store(FileStore::new(root)?);
            }
        }

        builder.build()
    }

    pub fn builder() -> ContextCacheBuilder {
        ContextCacheBuilder::new()
    }

    fn from_builder(builder: ContextCacheBuilder) -> io::Result<Self> {
        let cache = Self {
            max_memory_bytes: builder.max_memory_bytes,
            max_disk_bytes: builder.max_disk_bytes,
            default_ttl: builder.default_ttl,
            store: builder.store,
            clock: builder.clock,
            key_strategy: builder.key_strategy,
            eviction_policy: builder.eviction_policy,
            metrics: builder.metrics,
            inner: Mutex::new(Inner::default()),
        };

        let mut corruption_count = 0;
        let mut expiration_count = 0;
        let mut eviction_count = 0;

        if cache.max_disk_bytes > 0 {
            if let Some(store) = cache.store.as_ref() {
                let snapshot = store.load_index()?;
                corruption_count = snapshot.corruptions;
                let now = cache.clock.now_seconds();
                let mut inner = cache.lock_inner()?;
                inner.stats.corruptions = inner.stats.corruptions.saturating_add(corruption_count);

                for record in snapshot.entries {
                    if is_expired(record.expires_at, now) {
                        store.remove(&record.key)?;
                        expiration_count = expiration_count.saturating_add(1);
                        inner.stats.expirations = inner.stats.expirations.saturating_add(1);
                        continue;
                    }

                    inner.tick = inner.tick.max(record.last_access);
                    inner.disk_bytes = inner.disk_bytes.saturating_add(record.stored_bytes);
                    inner.disk.insert(record.key.clone(), record);
                }

                eviction_count = cache.evict_disk_locked(&mut inner)?;
            }
        }

        if corruption_count > 0 {
            cache.metrics.record(CacheEvent::Corruption(corruption_count));
        }
        if expiration_count > 0 {
            cache.metrics.record(CacheEvent::Expiration(expiration_count));
        }
        if eviction_count > 0 {
            cache
                .metrics
                .record(CacheEvent::PersistentEviction(eviction_count));
        }

        Ok(cache)
    }

    /// Build a key with the default RivetCache v1 strategy.
    pub fn key(namespace: &str, model_fingerprint: &str, payload: &str) -> String {
        Sha256KeyStrategy.make_key(namespace, model_fingerprint, payload)
    }

    /// Build a key with this cache instance's injected key strategy.
    pub fn make_key(&self, namespace: &str, model_fingerprint: &str, payload: &str) -> String {
        self.key_strategy
            .make_key(namespace, model_fingerprint, payload)
    }

    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        self.key_strategy.validate(key)?;
        let now = self.clock.now_seconds();
        let mut expiration_count = 0;
        let mut memory_evictions = 0;

        let mut inner = self.lock_inner()?;
        if let Some(record) = inner.memory.get(key) {
            if is_expired(record.expires_at, now) {
                if let Some(expired) = inner.memory.remove(key) {
                    inner.memory_bytes = inner
                        .memory_bytes
                        .saturating_sub(expired.value.len() as u64);
                    inner.stats.expirations = inner.stats.expirations.saturating_add(1);
                    expiration_count = 1;
                }
            } else {
                let tick = next_tick(&mut inner);
                let value = {
                    let record = inner.memory.get_mut(key).expect("entry checked above");
                    record.last_access = tick;
                    Arc::clone(&record.value)
                };
                inner.stats.memory_hits = inner.stats.memory_hits.saturating_add(1);
                drop(inner);
                self.metrics.record(CacheEvent::MemoryHit);
                return Ok(Some((*value).clone()));
            }
        }

        let Some(disk_record) = inner.disk.get(key).cloned() else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            drop(inner);
            if expiration_count > 0 {
                self.metrics
                    .record(CacheEvent::Expiration(expiration_count));
            }
            self.metrics.record(CacheEvent::Miss);
            return Ok(None);
        };

        if is_expired(disk_record.expires_at, now) {
            self.remove_disk_record_locked(&mut inner, key, true)?;
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            expiration_count = expiration_count.saturating_add(1);
            drop(inner);
            self.metrics
                .record(CacheEvent::Expiration(expiration_count));
            self.metrics.record(CacheEvent::Miss);
            return Ok(None);
        }

        let Some(store) = self.store.as_ref() else {
            inner.disk.remove(key);
            inner.disk_bytes = inner.disk_bytes.saturating_sub(disk_record.stored_bytes);
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            drop(inner);
            self.metrics.record(CacheEvent::Miss);
            return Ok(None);
        };

        let entry = match store.get(key) {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                inner.disk.remove(key);
                inner.disk_bytes = inner.disk_bytes.saturating_sub(disk_record.stored_bytes);
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                drop(inner);
                self.metrics.record(CacheEvent::Miss);
                return Ok(None);
            }
            Err(error) if is_corruption_error(&error) => {
                store.remove(key)?;
                inner.disk.remove(key);
                inner.disk_bytes = inner.disk_bytes.saturating_sub(disk_record.stored_bytes);
                inner.stats.corruptions = inner.stats.corruptions.saturating_add(1);
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                drop(inner);
                self.metrics.record(CacheEvent::Corruption(1));
                self.metrics.record(CacheEvent::Miss);
                return Ok(None);
            }
            Err(error) => return Err(error),
        };

        if entry.expires_at != disk_record.expires_at || entry.pinned != disk_record.pinned {
            store.remove(key)?;
            inner.disk.remove(key);
            inner.disk_bytes = inner.disk_bytes.saturating_sub(disk_record.stored_bytes);
            inner.stats.corruptions = inner.stats.corruptions.saturating_add(1);
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            drop(inner);
            self.metrics.record(CacheEvent::Corruption(1));
            self.metrics.record(CacheEvent::Miss);
            return Ok(None);
        }

        let tick = next_tick(&mut inner);
        if let Some(record) = inner.disk.get_mut(key) {
            record.last_access = tick;
        }
        inner.stats.disk_hits = inner.stats.disk_hits.saturating_add(1);

        if self.max_memory_bytes > 0 && (entry.value.len() as u64) <= self.max_memory_bytes {
            self.insert_memory_locked(
                &mut inner,
                key.to_string(),
                entry.value.clone(),
                entry.expires_at,
                entry.pinned,
            );
            memory_evictions = self.evict_memory_locked(&mut inner);
        }

        drop(inner);
        if expiration_count > 0 {
            self.metrics
                .record(CacheEvent::Expiration(expiration_count));
        }
        if memory_evictions > 0 {
            self.metrics
                .record(CacheEvent::MemoryEviction(memory_evictions));
        }
        self.metrics.record(CacheEvent::PersistentHit);
        Ok(Some(entry.value))
    }

    pub fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
        pinned: bool,
    ) -> io::Result<()> {
        self.key_strategy.validate(key)?;
        if value.is_empty() {
            return Ok(());
        }

        let ttl = ttl.unwrap_or(self.default_ttl);
        let now = self.clock.now_seconds();
        let expires_at = if ttl.is_zero() {
            0
        } else {
            now.saturating_add(ttl.as_secs().max(1))
        };

        let mut expiration_count = 0;
        let mut memory_evictions = 0;
        let mut disk_evictions = 0;
        let mut inner = self.lock_inner()?;

        if self.max_disk_bytes > 0 && (value.len() as u64) <= self.max_disk_bytes {
            if let Some(store) = self.store.as_ref() {
                if let Some(existing) = inner.disk.get(key).cloned() {
                    if is_expired(existing.expires_at, now) {
                        self.remove_disk_record_locked(&mut inner, key, true)?;
                        expiration_count = expiration_count.saturating_add(1);
                    }
                }

                if !inner.disk.contains_key(key) {
                    let entry = StoredEntry {
                        value: value.to_vec(),
                        expires_at,
                        pinned,
                    };
                    let outcome = store.put_if_absent(key, &entry)?;
                    if outcome.inserted {
                        let tick = next_tick(&mut inner);
                        inner.disk_bytes = inner.disk_bytes.saturating_add(outcome.stored_bytes);
                        inner.disk.insert(
                            key.to_string(),
                            StoreRecord {
                                key: key.to_string(),
                                stored_bytes: outcome.stored_bytes,
                                expires_at,
                                pinned,
                                last_access: tick,
                            },
                        );
                        disk_evictions = self.evict_disk_locked(&mut inner)?;
                    }
                }
            }
        }

        if self.max_memory_bytes > 0 && (value.len() as u64) <= self.max_memory_bytes {
            self.insert_memory_locked(
                &mut inner,
                key.to_string(),
                value.to_vec(),
                expires_at,
                pinned,
            );
            memory_evictions = self.evict_memory_locked(&mut inner);
        }

        inner.stats.writes = inner.stats.writes.saturating_add(1);
        drop(inner);

        if expiration_count > 0 {
            self.metrics
                .record(CacheEvent::Expiration(expiration_count));
        }
        if memory_evictions > 0 {
            self.metrics
                .record(CacheEvent::MemoryEviction(memory_evictions));
        }
        if disk_evictions > 0 {
            self.metrics
                .record(CacheEvent::PersistentEviction(disk_evictions));
        }
        self.metrics.record(CacheEvent::Write);
        Ok(())
    }

    pub fn clear(&self) -> io::Result<()> {
        if let Some(store) = self.store.as_ref() {
            store.clear()?;
        }

        let mut inner = self.lock_inner()?;
        inner.memory.clear();
        inner.disk.clear();
        inner.memory_bytes = 0;
        inner.disk_bytes = 0;
        drop(inner);

        self.metrics.record(CacheEvent::Clear);
        Ok(())
    }

    pub fn stats(&self) -> io::Result<CacheStats> {
        let inner = self.lock_inner()?;
        let mut stats = inner.stats;
        stats.memory_entries = inner.memory.len() as u64;
        stats.disk_entries = inner.disk.len() as u64;
        stats.memory_bytes = inner.memory_bytes;
        stats.disk_bytes = inner.disk_bytes;
        Ok(stats)
    }

    fn lock_inner(&self) -> io::Result<MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("context cache lock poisoned"))
    }

    fn insert_memory_locked(
        &self,
        inner: &mut Inner,
        key: String,
        value: Vec<u8>,
        expires_at: u64,
        pinned: bool,
    ) {
        if let Some(previous) = inner.memory.remove(&key) {
            inner.memory_bytes = inner
                .memory_bytes
                .saturating_sub(previous.value.len() as u64);
        }

        let tick = next_tick(inner);
        inner.memory_bytes = inner.memory_bytes.saturating_add(value.len() as u64);
        inner.memory.insert(
            key,
            MemoryRecord {
                value: Arc::new(value),
                expires_at,
                pinned,
                last_access: tick,
            },
        );
    }

    fn evict_memory_locked(&self, inner: &mut Inner) -> u64 {
        let mut evicted = 0_u64;

        while inner.memory_bytes > self.max_memory_bytes {
            let candidates: Vec<EvictionCandidate> = inner
                .memory
                .iter()
                .filter(|(_, record)| !record.pinned)
                .map(|(key, record)| EvictionCandidate {
                    key: key.clone(),
                    last_access: record.last_access,
                    size_bytes: record.value.len() as u64,
                })
                .collect();

            let Some(index) = self.eviction_policy.choose_victim(&candidates) else {
                break;
            };
            let Some(candidate) = candidates.get(index) else {
                break;
            };

            if let Some(record) = inner.memory.remove(&candidate.key) {
                inner.memory_bytes = inner.memory_bytes.saturating_sub(record.value.len() as u64);
                inner.stats.memory_evictions = inner.stats.memory_evictions.saturating_add(1);
                evicted = evicted.saturating_add(1);
            }
        }

        evicted
    }

    fn evict_disk_locked(&self, inner: &mut Inner) -> io::Result<u64> {
        let mut evicted = 0_u64;

        while inner.disk_bytes > self.max_disk_bytes {
            let candidates: Vec<EvictionCandidate> = inner
                .disk
                .iter()
                .filter(|(_, record)| !record.pinned)
                .map(|(key, record)| EvictionCandidate {
                    key: key.clone(),
                    last_access: record.last_access,
                    size_bytes: record.stored_bytes,
                })
                .collect();

            let Some(index) = self.eviction_policy.choose_victim(&candidates) else {
                break;
            };
            let Some(candidate) = candidates.get(index) else {
                break;
            };

            self.remove_disk_record_locked(inner, &candidate.key, false)?;
            inner.stats.disk_evictions = inner.stats.disk_evictions.saturating_add(1);
            evicted = evicted.saturating_add(1);
        }

        Ok(evicted)
    }

    fn remove_disk_record_locked(
        &self,
        inner: &mut Inner,
        key: &str,
        expired: bool,
    ) -> io::Result<()> {
        let Some(record) = inner.disk.remove(key) else {
            return Ok(());
        };

        if let Some(store) = self.store.as_ref() {
            store.remove(key)?;
        }
        inner.disk_bytes = inner.disk_bytes.saturating_sub(record.stored_bytes);
        if expired {
            inner.stats.expirations = inner.stats.expirations.saturating_add(1);
        }
        Ok(())
    }
}

fn next_tick(inner: &mut Inner) -> u64 {
    inner.tick = inner.tick.saturating_add(1);
    inner.tick
}

fn is_expired(expires_at: u64, now: u64) -> bool {
    expires_at != 0 && expires_at <= now
}

fn is_corruption_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_root(label: &str) -> PathBuf {
        static NONCE: AtomicU64 = AtomicU64::new(1);
        std::env::temp_dir().join(format!(
            "rivet-cache-{label}-{}-{}",
            std::process::id(),
            NONCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn keys_are_stable_and_namespaced() {
        let first = ContextCache::key("completion/v1", "model-a", "{\"x\":1}");
        let second = ContextCache::key("completion/v1", "model-a", "{\"x\":1}");
        let other = ContextCache::key("kv/v1", "model-a", "{\"x\":1}");
        assert_eq!(first, second);
        assert_ne!(first, other);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn memory_cache_evicts_least_recently_used_entry() {
        let cache = ContextCache::new(None, 6, 0, Duration::from_secs(60)).expect("cache");
        let a = ContextCache::key("test", "m", "a");
        let b = ContextCache::key("test", "m", "b");
        let c = ContextCache::key("test", "m", "c");
        cache.put(&a, b"aaa", None, false).expect("put a");
        cache.put(&b, b"bbb", None, false).expect("put b");
        assert_eq!(cache.get(&a).expect("get a"), Some(b"aaa".to_vec()));
        cache.put(&c, b"ccc", None, false).expect("put c");
        assert_eq!(cache.get(&a).expect("get a again"), Some(b"aaa".to_vec()));
        assert_eq!(cache.get(&b).expect("get b"), None);
        assert_eq!(cache.get(&c).expect("get c"), Some(b"ccc".to_vec()));
    }

    #[test]
    fn disk_cache_survives_reopen() {
        let root = temp_root("roundtrip");
        let key = ContextCache::key("completion/v1", "model", "request");
        {
            let cache =
                ContextCache::new(Some(root.clone()), 0, 1024 * 1024, Duration::from_secs(60))
                    .expect("cache");
            cache.put(&key, b"persisted", None, false).expect("put");
        }
        {
            let cache =
                ContextCache::new(Some(root.clone()), 0, 1024 * 1024, Duration::from_secs(60))
                    .expect("cache");
            assert_eq!(cache.get(&key).expect("get"), Some(b"persisted".to_vec()));
            assert_eq!(cache.stats().expect("stats").disk_hits, 1);
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_disk_entry_is_removed_and_becomes_a_miss() {
        let root = temp_root("corrupt");
        std::fs::create_dir_all(&root).expect("root");
        let key = ContextCache::key("test", "model", "corrupt");
        std::fs::write(root.join(format!("{key}.rivetcache")), b"not-a-cache")
            .expect("corrupt file");

        let cache = ContextCache::new(
            Some(root.clone()),
            0,
            1024 * 1024,
            Duration::from_secs(60),
        )
        .expect("cache");
        assert_eq!(cache.get(&key).expect("get"), None);
        assert_eq!(cache.stats().expect("stats").corruptions, 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[derive(Clone)]
    struct ManualClock(Arc<AtomicU64>);

    impl ManualClock {
        fn new(now: u64) -> Self {
            Self(Arc::new(AtomicU64::new(now)))
        }

        fn advance(&self, seconds: u64) {
            self.0.fetch_add(seconds, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_seconds(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn injected_clock_makes_ttl_deterministic() {
        let clock = ManualClock::new(100);
        let cache = ContextCache::builder()
            .memory_capacity(1024)
            .default_ttl(Duration::from_secs(5))
            .clock(clock.clone())
            .build()
            .expect("cache");

        let key = cache.make_key("test", "model", "ttl");
        cache.put(&key, b"short", None, false).expect("put");
        assert_eq!(cache.get(&key).expect("get"), Some(b"short".to_vec()));

        clock.advance(6);
        assert_eq!(cache.get(&key).expect("expired"), None);
        assert_eq!(cache.stats().expect("stats").expirations, 1);
    }

    struct LiteralKeyStrategy;

    impl KeyStrategy for LiteralKeyStrategy {
        fn make_key(&self, namespace: &str, model_fingerprint: &str, payload: &str) -> String {
            format!("{namespace}:{model_fingerprint}:{payload}")
        }

        fn validate(&self, key: &str) -> io::Result<()> {
            if key.contains(':') {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid test key"))
            }
        }
    }

    #[test]
    fn key_strategy_is_injected() {
        let cache = ContextCache::builder()
            .memory_capacity(1024)
            .key_strategy(LiteralKeyStrategy)
            .build()
            .expect("cache");

        let key = cache.make_key("completion", "model-x", "hello");
        assert_eq!(key, "completion:model-x:hello");
        cache.put(&key, b"world", None, false).expect("put");
        assert_eq!(cache.get(&key).expect("get"), Some(b"world".to_vec()));
    }

    struct LargestFirst;

    impl EvictionPolicy for LargestFirst {
        fn choose_victim(&self, candidates: &[EvictionCandidate]) -> Option<usize> {
            candidates
                .iter()
                .enumerate()
                .max_by_key(|(_, candidate)| candidate.size_bytes)
                .map(|(index, _)| index)
        }
    }

    #[test]
    fn eviction_policy_is_injected() {
        let cache = ContextCache::builder()
            .memory_capacity(6)
            .eviction_policy(LargestFirst)
            .build()
            .expect("cache");

        let small = cache.make_key("test", "m", "small");
        let large = cache.make_key("test", "m", "large");
        let incoming = cache.make_key("test", "m", "incoming");
        cache.put(&small, b"aa", None, false).expect("small");
        cache.put(&large, b"bbbb", None, false).expect("large");
        cache.put(&incoming, b"ccc", None, false).expect("incoming");

        assert_eq!(cache.get(&large).expect("large"), None);
        assert_eq!(cache.get(&small).expect("small"), Some(b"aa".to_vec()));
        assert_eq!(
            cache.get(&incoming).expect("incoming"),
            Some(b"ccc".to_vec())
        );
    }

    #[derive(Clone, Default)]
    struct EventLog(Arc<Mutex<Vec<CacheEvent>>>);

    impl MetricsSink for EventLog {
        fn record(&self, event: CacheEvent) {
            self.0.lock().expect("events").push(event);
        }
    }

    #[test]
    fn metrics_sink_is_injected() {
        let events = EventLog::default();
        let cache = ContextCache::builder()
            .memory_capacity(1024)
            .metrics(events.clone())
            .build()
            .expect("cache");

        let key = cache.make_key("test", "m", "metrics");
        cache.put(&key, b"value", None, false).expect("put");
        assert_eq!(cache.get(&key).expect("get"), Some(b"value".to_vec()));

        let events = events.0.lock().expect("events");
        assert!(events.contains(&CacheEvent::Write));
        assert!(events.contains(&CacheEvent::MemoryHit));
    }

    #[derive(Default)]
    struct MemoryStore {
        entries: Mutex<HashMap<String, StoredEntry>>,
    }

    impl PersistentStore for MemoryStore {
        fn load_index(&self) -> io::Result<StoreSnapshot> {
            let entries = self.entries.lock().expect("store");
            Ok(StoreSnapshot {
                entries: entries
                    .iter()
                    .map(|(key, value)| StoreRecord {
                        key: key.clone(),
                        stored_bytes: value.value.len() as u64,
                        expires_at: value.expires_at,
                        pinned: value.pinned,
                        last_access: 0,
                    })
                    .collect(),
                corruptions: 0,
            })
        }

        fn get(&self, key: &str) -> io::Result<Option<StoredEntry>> {
            Ok(self.entries.lock().expect("store").get(key).cloned())
        }

        fn put_if_absent(&self, key: &str, entry: &StoredEntry) -> io::Result<PutOutcome> {
            let mut entries = self.entries.lock().expect("store");
            if let Some(existing) = entries.get(key) {
                return Ok(PutOutcome {
                    inserted: false,
                    stored_bytes: existing.value.len() as u64,
                });
            }
            entries.insert(key.to_string(), entry.clone());
            Ok(PutOutcome {
                inserted: true,
                stored_bytes: entry.value.len() as u64,
            })
        }

        fn remove(&self, key: &str) -> io::Result<()> {
            self.entries.lock().expect("store").remove(key);
            Ok(())
        }

        fn clear(&self) -> io::Result<()> {
            self.entries.lock().expect("store").clear();
            Ok(())
        }
    }

    #[test]
    fn persistent_store_is_injected() {
        let cache = ContextCache::builder()
            .persistent_capacity(1024)
            .persistent_store(MemoryStore::default())
            .build()
            .expect("cache");

        let key = cache.make_key("test", "m", "store");
        cache.put(&key, b"persisted", None, false).expect("put");
        assert_eq!(
            cache.get(&key).expect("get"),
            Some(b"persisted".to_vec())
        );
        assert_eq!(cache.stats().expect("stats").disk_hits, 1);
    }
}
