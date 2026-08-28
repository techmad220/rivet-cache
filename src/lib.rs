use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 8] = b"RIVET01\n";
const HEADER_LEN: u64 = 8 + 8 + 1 + 8 + 32;
const CACHE_EXTENSION: &str = "rivetcache";

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

#[derive(Debug, Clone)]
struct DiskRecord {
    path: PathBuf,
    payload_len: u64,
    file_len: u64,
    expires_at: u64,
    pinned: bool,
    last_access: u64,
}

#[derive(Debug, Default)]
struct Inner {
    memory: HashMap<String, MemoryRecord>,
    disk: HashMap<String, DiskRecord>,
    memory_bytes: u64,
    disk_bytes: u64,
    tick: u64,
    stats: CacheStats,
}

/// A bounded, content-addressed cache for inference and context artifacts.
///
/// The cache deliberately stores opaque bytes. Provider adapters decide whether
/// those bytes are an exact completion, a rendered prompt, or a future exported
/// engine-state/KV artifact. Cache keys include an explicit namespace and model
/// fingerprint so incompatible engines, quantizations, templates, and cache ABI
/// versions cannot accidentally share state.
#[derive(Debug)]
pub struct ContextCache {
    root: Option<PathBuf>,
    max_memory_bytes: u64,
    max_disk_bytes: u64,
    default_ttl: Duration,
    inner: Mutex<Inner>,
}

impl ContextCache {
    pub fn new(
        root: Option<PathBuf>,
        max_memory_bytes: u64,
        max_disk_bytes: u64,
        default_ttl: Duration,
    ) -> io::Result<Self> {
        let mut inner = Inner::default();
        let root = if max_disk_bytes == 0 {
            None
        } else if let Some(root) = root {
            fs::create_dir_all(&root)?;
            load_disk_index(&root, &mut inner)?;
            Some(root)
        } else {
            None
        };

        let cache = Self {
            root,
            max_memory_bytes,
            max_disk_bytes,
            default_ttl,
            inner: Mutex::new(inner),
        };
        {
            let mut inner = cache.lock_inner()?;
            cache.evict_disk_locked(&mut inner)?;
        }
        Ok(cache)
    }

    /// Build an unambiguous, versionable content-addressed key.
    pub fn key(namespace: &str, model_fingerprint: &str, payload: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"RIVET_CACHE_V1\0");
        update_length_prefixed(&mut digest, namespace.as_bytes());
        update_length_prefixed(&mut digest, model_fingerprint.as_bytes());
        update_length_prefixed(&mut digest, payload.as_bytes());
        hex::encode(digest.finalize())
    }

    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        validate_key(key)?;
        let now = now_seconds();
        let mut inner = self.lock_inner()?;

        let memory_expired = inner
            .memory
            .get(key)
            .map(|record| is_expired(record.expires_at, now))
            .unwrap_or(false);
        if memory_expired {
            if let Some(record) = inner.memory.remove(key) {
                inner.memory_bytes = inner.memory_bytes.saturating_sub(record.value.len() as u64);
                inner.stats.expirations = inner.stats.expirations.saturating_add(1);
            }
        } else if inner.memory.contains_key(key) {
            let tick = next_tick(&mut inner);
            let value = {
                let record = inner.memory.get_mut(key).expect("entry checked above");
                record.last_access = tick;
                Arc::clone(&record.value)
            };
            inner.stats.memory_hits = inner.stats.memory_hits.saturating_add(1);
            return Ok(Some((*value).clone()));
        }

        let Some(disk_record) = inner.disk.get(key).cloned() else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        if is_expired(disk_record.expires_at, now) {
            self.remove_disk_record_locked(&mut inner, key, true)?;
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        }

        let payload = match read_payload(&disk_record.path, &disk_record) {
            Ok(payload) => payload,
            Err(_) => {
                self.remove_disk_record_locked(&mut inner, key, false)?;
                inner.stats.corruptions = inner.stats.corruptions.saturating_add(1);
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                return Ok(None);
            }
        };

        let tick = next_tick(&mut inner);
        if let Some(record) = inner.disk.get_mut(key) {
            record.last_access = tick;
        }
        inner.stats.disk_hits = inner.stats.disk_hits.saturating_add(1);
        self.insert_memory_locked(
            &mut inner,
            key.to_string(),
            payload.clone(),
            disk_record.expires_at,
            disk_record.pinned,
        );
        self.evict_memory_locked(&mut inner);
        Ok(Some(payload))
    }

    pub fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
        pinned: bool,
    ) -> io::Result<()> {
        validate_key(key)?;
        if value.is_empty() {
            return Ok(());
        }
        let ttl = ttl.unwrap_or(self.default_ttl);
        let expires_at = if ttl.is_zero() {
            0
        } else {
            now_seconds().saturating_add(ttl.as_secs().max(1))
        };
        let mut inner = self.lock_inner()?;

        if self.max_disk_bytes > 0 {
            if let Some(root) = self.root.as_ref() {
                let file_len = HEADER_LEN.saturating_add(value.len() as u64);
                if file_len <= self.max_disk_bytes {
                    let path = root.join(format!("{key}.{CACHE_EXTENSION}"));
                    if let Some(existing) = inner.disk.get(key).cloned() {
                        if is_expired(existing.expires_at, now_seconds()) {
                            self.remove_disk_record_locked(&mut inner, key, true)?;
                        }
                    }
                    if !inner.disk.contains_key(key) {
                        write_payload_atomic(root, &path, expires_at, pinned, value)?;
                        let tick = next_tick(&mut inner);
                        inner.disk.insert(
                            key.to_string(),
                            DiskRecord {
                                path,
                                payload_len: value.len() as u64,
                                file_len,
                                expires_at,
                                pinned,
                                last_access: tick,
                            },
                        );
                        inner.disk_bytes = inner.disk_bytes.saturating_add(file_len);
                        self.evict_disk_locked(&mut inner)?;
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
            self.evict_memory_locked(&mut inner);
        }
        inner.stats.writes = inner.stats.writes.saturating_add(1);
        Ok(())
    }

    pub fn clear(&self) -> io::Result<()> {
        let mut inner = self.lock_inner()?;
        let paths: Vec<PathBuf> = inner
            .disk
            .values()
            .map(|record| record.path.clone())
            .collect();
        for path in paths {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        inner.memory.clear();
        inner.disk.clear();
        inner.memory_bytes = 0;
        inner.disk_bytes = 0;
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

    fn evict_memory_locked(&self, inner: &mut Inner) {
        while inner.memory_bytes > self.max_memory_bytes {
            let candidate = inner
                .memory
                .iter()
                .filter(|(_, record)| !record.pinned)
                .min_by_key(|(_, record)| record.last_access)
                .map(|(key, _)| key.clone());
            let Some(key) = candidate else {
                break;
            };
            if let Some(record) = inner.memory.remove(&key) {
                inner.memory_bytes = inner.memory_bytes.saturating_sub(record.value.len() as u64);
                inner.stats.memory_evictions = inner.stats.memory_evictions.saturating_add(1);
            }
        }
    }

    fn evict_disk_locked(&self, inner: &mut Inner) -> io::Result<()> {
        while inner.disk_bytes > self.max_disk_bytes {
            let candidate = inner
                .disk
                .iter()
                .filter(|(_, record)| !record.pinned)
                .min_by_key(|(_, record)| record.last_access)
                .map(|(key, _)| key.clone());
            let Some(key) = candidate else {
                break;
            };
            self.remove_disk_record_locked(inner, &key, false)?;
            inner.stats.disk_evictions = inner.stats.disk_evictions.saturating_add(1);
        }
        Ok(())
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
        inner.disk_bytes = inner.disk_bytes.saturating_sub(record.file_len);
        match fs::remove_file(&record.path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if expired {
            inner.stats.expirations = inner.stats.expirations.saturating_add(1);
        }
        Ok(())
    }
}

fn update_length_prefixed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

fn next_tick(inner: &mut Inner) -> u64 {
    inner.tick = inner.tick.saturating_add(1);
    inner.tick
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_expired(expires_at: u64, now: u64) -> bool {
    expires_at != 0 && expires_at <= now
}

fn validate_key(key: &str) -> io::Result<()> {
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

fn load_disk_index(root: &Path, inner: &mut Inner) -> io::Result<()> {
    let now = now_seconds();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some(CACHE_EXTENSION) {
            continue;
        }
        let Some(key) = path.file_stem().and_then(|value| value.to_str()) else {
            let _ = fs::remove_file(&path);
            continue;
        };
        if validate_key(key).is_err() {
            let _ = fs::remove_file(&path);
            continue;
        }
        let metadata = match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => metadata,
            _ => {
                let _ = fs::remove_file(&path);
                continue;
            }
        };
        let header = match read_header(&path) {
            Ok(header) => header,
            Err(_) => {
                let _ = fs::remove_file(&path);
                inner.stats.corruptions = inner.stats.corruptions.saturating_add(1);
                continue;
            }
        };
        if metadata.len() != HEADER_LEN.saturating_add(header.payload_len) {
            let _ = fs::remove_file(&path);
            inner.stats.corruptions = inner.stats.corruptions.saturating_add(1);
            continue;
        }
        if is_expired(header.expires_at, now) {
            let _ = fs::remove_file(&path);
            inner.stats.expirations = inner.stats.expirations.saturating_add(1);
            continue;
        }
        let last_access = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| value.as_secs())
            .unwrap_or(0);
        inner.tick = inner.tick.max(last_access);
        inner.disk_bytes = inner.disk_bytes.saturating_add(metadata.len());
        inner.disk.insert(
            key.to_string(),
            DiskRecord {
                path,
                payload_len: header.payload_len,
                file_len: metadata.len(),
                expires_at: header.expires_at,
                pinned: header.pinned,
                last_access,
            },
        );
    }
    Ok(())
}

#[derive(Debug)]
struct Header {
    expires_at: u64,
    pinned: bool,
    payload_len: u64,
    checksum: [u8; 32],
}

fn read_header(path: &Path) -> io::Result<Header> {
    let mut file = File::open(path)?;
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid cache magic",
        ));
    }
    let expires_at = read_u64(&mut file)?;
    let mut pinned = [0_u8; 1];
    file.read_exact(&mut pinned)?;
    if pinned[0] > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid cache pinned flag",
        ));
    }
    let payload_len = read_u64(&mut file)?;
    let mut checksum = [0_u8; 32];
    file.read_exact(&mut checksum)?;
    Ok(Header {
        expires_at,
        pinned: pinned[0] == 1,
        payload_len,
        checksum,
    })
}

fn read_u64(file: &mut File) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_payload(path: &Path, record: &DiskRecord) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let header = read_header_from(&mut file)?;
    if header.payload_len != record.payload_len
        || header.expires_at != record.expires_at
        || header.pinned != record.pinned
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache header changed after indexing",
        ));
    }
    let payload_len: usize = header
        .payload_len
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cache payload is too large"))?;
    let mut payload = vec![0_u8; payload_len];
    file.read_exact(&mut payload)?;
    let actual = Sha256::digest(&payload);
    if actual.as_ref() != header.checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache payload checksum mismatch",
        ));
    }
    Ok(payload)
}

fn read_header_from(file: &mut File) -> io::Result<Header> {
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid cache magic",
        ));
    }
    let expires_at = read_u64(file)?;
    let mut pinned = [0_u8; 1];
    file.read_exact(&mut pinned)?;
    if pinned[0] > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid cache pinned flag",
        ));
    }
    let payload_len = read_u64(file)?;
    let mut checksum = [0_u8; 32];
    file.read_exact(&mut checksum)?;
    Ok(Header {
        expires_at,
        pinned: pinned[0] == 1,
        payload_len,
        checksum,
    })
}

fn write_payload_atomic(
    root: &Path,
    final_path: &Path,
    expires_at: u64,
    pinned: bool,
    payload: &[u8],
) -> io::Result<()> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_path = root.join(format!(".{}.{}.tmp", std::process::id(), nonce));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    let checksum = Sha256::digest(payload);
    file.write_all(MAGIC)?;
    file.write_all(&expires_at.to_le_bytes())?;
    file.write_all(&[u8::from(pinned)])?;
    file.write_all(&(payload.len() as u64).to_le_bytes())?;
    file.write_all(checksum.as_ref())?;
    file.write_all(payload)?;
    file.sync_all()?;
    drop(file);

    match fs::rename(&temp_path, final_path) {
        Ok(()) => Ok(()),
        Err(_) if final_path.exists() => {
            let _ = fs::remove_file(&temp_path);
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rivet-cache-{label}-{}-{nonce}",
            std::process::id()
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
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_disk_entry_is_removed_and_becomes_a_miss() {
        let root = temp_root("corrupt");
        fs::create_dir_all(&root).expect("root");
        let key = ContextCache::key("test", "model", "corrupt");
        fs::write(
            root.join(format!("{key}.{CACHE_EXTENSION}")),
            b"not-a-cache",
        )
        .expect("corrupt file");
        let cache = ContextCache::new(Some(root.clone()), 0, 1024 * 1024, Duration::from_secs(60))
            .expect("cache");
        assert_eq!(cache.get(&key).expect("get"), None);
        assert_eq!(cache.stats().expect("stats").corruptions, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ttl_entries_expire() {
        let cache = ContextCache::new(None, 1024, 0, Duration::from_secs(60)).expect("cache");
        let key = ContextCache::key("test", "model", "ttl");
        cache
            .put(&key, b"short", Some(Duration::from_secs(1)), false)
            .expect("put");
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(cache.get(&key).expect("get"), None);
        assert!(cache.stats().expect("stats").expirations >= 1);
    }
}
