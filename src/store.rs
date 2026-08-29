use sha2::{Digest, Sha256};
use std::collections::{hash_map::Entry, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

const MAGIC: &[u8; 8] = b"RIVET01\n";
const HEADER_LEN: u64 = 8 + 8 + 1 + 8 + 32;
const CACHE_EXTENSION: &str = "rivetcache";
static TEMP_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntry {
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRecord {
    pub key: String,
    pub stored_bytes: u64,
    pub expires_at: u64,
    pub pinned: bool,
    pub last_access: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreSnapshot {
    pub entries: Vec<StoreRecord>,
    pub corruptions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutOutcome {
    pub inserted: bool,
    pub stored_bytes: u64,
}

/// Pluggable persistent tier.
///
/// Implementations may be local files, a remote service, shared memory, object
/// storage, or any other synchronous backend. The core cache never assumes a
/// filesystem once this trait is injected.
pub trait PersistentStore: Send + Sync {
    fn load_index(&self) -> io::Result<StoreSnapshot>;
    fn get(&self, key: &str) -> io::Result<Option<StoredEntry>>;
    fn put_if_absent(&self, key: &str, entry: &StoredEntry) -> io::Result<PutOutcome>;
    fn remove(&self, key: &str) -> io::Result<()>;
    fn clear(&self) -> io::Result<()>;
}

/// Volatile in-process implementation of [`PersistentStore`].
///
/// This backend is useful for tests, ephemeral tiers, and as a reference
/// implementation for custom stores. It intentionally does not survive process
/// restart.
#[derive(Default)]
pub struct VolatileStore {
    entries: Mutex<HashMap<String, (StoredEntry, u64)>>,
    tick: AtomicU64,
}

impl VolatileStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> io::Result<usize> {
        Ok(self.lock_entries()?.len())
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.lock_entries()?.is_empty())
    }

    fn lock_entries(&self) -> io::Result<MutexGuard<'_, HashMap<String, (StoredEntry, u64)>>> {
        self.entries
            .lock()
            .map_err(|_| io::Error::other("memory store lock poisoned"))
    }

    fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed).saturating_add(1)
    }
}

impl PersistentStore for VolatileStore {
    fn load_index(&self) -> io::Result<StoreSnapshot> {
        let entries = self.lock_entries()?;
        Ok(StoreSnapshot {
            entries: entries
                .iter()
                .map(|(key, (entry, last_access))| StoreRecord {
                    key: key.clone(),
                    stored_bytes: entry.value.len() as u64,
                    expires_at: entry.expires_at,
                    pinned: entry.pinned,
                    last_access: *last_access,
                })
                .collect(),
            corruptions: 0,
        })
    }

    fn get(&self, key: &str) -> io::Result<Option<StoredEntry>> {
        let tick = self.next_tick();
        let mut entries = self.lock_entries()?;
        let Some((entry, last_access)) = entries.get_mut(key) else {
            return Ok(None);
        };
        *last_access = tick;
        Ok(Some(entry.clone()))
    }

    fn put_if_absent(&self, key: &str, entry: &StoredEntry) -> io::Result<PutOutcome> {
        let tick = self.next_tick();
        let mut entries = self.lock_entries()?;
        match entries.entry(key.to_string()) {
            Entry::Occupied(existing) => Ok(PutOutcome {
                inserted: false,
                stored_bytes: existing.get().0.value.len() as u64,
            }),
            Entry::Vacant(slot) => {
                let stored_bytes = entry.value.len() as u64;
                slot.insert((entry.clone(), tick));
                Ok(PutOutcome {
                    inserted: true,
                    stored_bytes,
                })
            }
        }
    }

    fn remove(&self, key: &str) -> io::Result<()> {
        self.lock_entries()?.remove(key);
        Ok(())
    }

    fn clear(&self) -> io::Result<()> {
        self.lock_entries()?.clear();
        Ok(())
    }
}

/// Ordered composition of multiple persistent-store implementations.
///
/// Reads search stores in priority order. Writes are replicated to every
/// configured store. Existing copies are checked for metadata and payload
/// agreement before a new replica is created. The implementation does not
/// perform implicit read promotion, which keeps storage accounting deterministic
/// and leaves promotion policy to the caller.
#[derive(Clone)]
pub struct LayeredStore {
    stores: Vec<Arc<dyn PersistentStore>>,
}

impl LayeredStore {
    pub fn new(stores: Vec<Arc<dyn PersistentStore>>) -> io::Result<Self> {
        if stores.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "layered store requires at least one backend",
            ));
        }
        Ok(Self { stores })
    }

    pub fn len(&self) -> usize {
        self.stores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }
}

impl PersistentStore for LayeredStore {
    fn load_index(&self) -> io::Result<StoreSnapshot> {
        let mut merged: HashMap<String, StoreRecord> = HashMap::new();
        let mut corruptions = 0_u64;

        for store in &self.stores {
            let snapshot = store.load_index()?;
            corruptions = corruptions.saturating_add(snapshot.corruptions);
            for record in snapshot.entries {
                match merged.entry(record.key.clone()) {
                    Entry::Vacant(slot) => {
                        slot.insert(record);
                    }
                    Entry::Occupied(mut slot) => {
                        let current = slot.get_mut();
                        if current.expires_at != record.expires_at
                            || current.pinned != record.pinned
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("layered store metadata mismatch for key {}", record.key),
                            ));
                        }
                        current.stored_bytes =
                            current.stored_bytes.saturating_add(record.stored_bytes);
                        current.last_access = current.last_access.max(record.last_access);
                    }
                }
            }
        }

        Ok(StoreSnapshot {
            entries: merged.into_values().collect(),
            corruptions,
        })
    }

    fn get(&self, key: &str) -> io::Result<Option<StoredEntry>> {
        for store in &self.stores {
            if let Some(entry) = store.get(key)? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    fn put_if_absent(&self, key: &str, entry: &StoredEntry) -> io::Result<PutOutcome> {
        for store in &self.stores {
            if let Some(existing) = store.get(key)? {
                if existing != *entry {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("layered store payload mismatch for key {key}"),
                    ));
                }
            }
        }

        let mut inserted_backends = Vec::new();
        let mut stored_bytes = 0_u64;

        for (index, store) in self.stores.iter().enumerate() {
            match store.put_if_absent(key, entry) {
                Ok(outcome) => {
                    stored_bytes = stored_bytes.saturating_add(outcome.stored_bytes);
                    if outcome.inserted {
                        inserted_backends.push(index);
                    }
                }
                Err(error) => {
                    for inserted_index in inserted_backends {
                        let _ = self.stores[inserted_index].remove(key);
                    }
                    return Err(error);
                }
            }
        }

        Ok(PutOutcome {
            inserted: !inserted_backends.is_empty(),
            stored_bytes,
        })
    }

    fn remove(&self, key: &str) -> io::Result<()> {
        let mut first_error = None;
        for store in &self.stores {
            if let Err(error) = store.remove(key) {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn clear(&self) -> io::Result<()> {
        let mut first_error = None;
        for store in &self.stores {
            if let Err(error) = store.clear() {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Default filesystem-backed persistent tier.
#[derive(Debug, Clone)]
pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path_for(&self, key: &str) -> io::Result<PathBuf> {
        validate_file_key(key)?;
        Ok(self.root.join(format!("{key}.{CACHE_EXTENSION}")))
    }
}

impl PersistentStore for FileStore {
    fn load_index(&self) -> io::Result<StoreSnapshot> {
        let mut snapshot = StoreSnapshot::default();

        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some(CACHE_EXTENSION) {
                continue;
            }

            let Some(key) = path.file_stem().and_then(|value| value.to_str()) else {
                let _ = fs::remove_file(&path);
                snapshot.corruptions = snapshot.corruptions.saturating_add(1);
                continue;
            };
            if validate_file_key(key).is_err() {
                let _ = fs::remove_file(&path);
                snapshot.corruptions = snapshot.corruptions.saturating_add(1);
                continue;
            }

            let metadata = match fs::metadata(&path) {
                Ok(metadata) if metadata.is_file() => metadata,
                _ => {
                    let _ = fs::remove_file(&path);
                    snapshot.corruptions = snapshot.corruptions.saturating_add(1);
                    continue;
                }
            };

            let header = match read_header(&path) {
                Ok(header) => header,
                Err(_) => {
                    let _ = fs::remove_file(&path);
                    snapshot.corruptions = snapshot.corruptions.saturating_add(1);
                    continue;
                }
            };

            if metadata.len() != HEADER_LEN.saturating_add(header.payload_len) {
                let _ = fs::remove_file(&path);
                snapshot.corruptions = snapshot.corruptions.saturating_add(1);
                continue;
            }

            let last_access = metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_secs())
                .unwrap_or(0);

            snapshot.entries.push(StoreRecord {
                key: key.to_string(),
                stored_bytes: metadata.len(),
                expires_at: header.expires_at,
                pinned: header.pinned,
                last_access,
            });
        }

        Ok(snapshot)
    }

    fn get(&self, key: &str) -> io::Result<Option<StoredEntry>> {
        let path = self.path_for(key)?;
        if !path.exists() {
            return Ok(None);
        }

        let mut file = File::open(&path)?;
        let header = read_header_from(&mut file)?;
        let payload_len: usize = header.payload_len.try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "cache payload is too large")
        })?;

        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;
        let actual = Sha256::digest(&payload);
        if actual.as_ref() != header.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache payload checksum mismatch",
            ));
        }

        Ok(Some(StoredEntry {
            value: payload,
            expires_at: header.expires_at,
            pinned: header.pinned,
        }))
    }

    fn put_if_absent(&self, key: &str, entry: &StoredEntry) -> io::Result<PutOutcome> {
        let final_path = self.path_for(key)?;
        if let Ok(metadata) = fs::metadata(&final_path) {
            return Ok(PutOutcome {
                inserted: false,
                stored_bytes: metadata.len(),
            });
        }

        let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let temp_path = self
            .root
            .join(format!(".{}.{}.tmp", std::process::id(), nonce));

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        let checksum = Sha256::digest(&entry.value);
        file.write_all(MAGIC)?;
        file.write_all(&entry.expires_at.to_le_bytes())?;
        file.write_all(&[u8::from(entry.pinned)])?;
        file.write_all(&(entry.value.len() as u64).to_le_bytes())?;
        file.write_all(checksum.as_ref())?;
        file.write_all(&entry.value)?;
        file.sync_all()?;
        let stored_bytes = HEADER_LEN.saturating_add(entry.value.len() as u64);
        drop(file);

        match fs::rename(&temp_path, &final_path) {
            Ok(()) => Ok(PutOutcome {
                inserted: true,
                stored_bytes,
            }),
            Err(_) if final_path.exists() => {
                let _ = fs::remove_file(&temp_path);
                let stored_bytes = fs::metadata(&final_path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(stored_bytes);
                Ok(PutOutcome {
                    inserted: false,
                    stored_bytes,
                })
            }
            Err(error) => {
                let _ = fs::remove_file(&temp_path);
                Err(error)
            }
        }
    }

    fn remove(&self, key: &str) -> io::Result<()> {
        let path = self.path_for(key)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn clear(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some(CACHE_EXTENSION) {
                continue;
            }
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
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
    read_header_from(&mut file)
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

fn read_u64(file: &mut File) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn validate_file_key(key: &str) -> io::Result<()> {
    let valid = !key.is_empty()
        && key != "."
        && key != ".."
        && !key.contains('/')
        && !key.contains('\\')
        && !key.contains('\0');

    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache key is not safe for the filesystem store",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rivet-store-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn file_store_round_trips_and_indexes() {
        let root = temp_root("roundtrip");
        let store = FileStore::new(root.clone()).expect("store");
        let entry = StoredEntry {
            value: b"payload".to_vec(),
            expires_at: 123,
            pinned: true,
        };

        let outcome = store.put_if_absent("abc", &entry).expect("put");
        assert!(outcome.inserted);
        assert_eq!(store.get("abc").expect("get"), Some(entry.clone()));

        let snapshot = store.load_index().expect("index");
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].key, "abc");
        assert!(snapshot.entries[0].pinned);

        store.clear().expect("clear");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn memory_store_round_trips() {
        let store = VolatileStore::new();
        let entry = StoredEntry {
            value: b"memory".to_vec(),
            expires_at: 0,
            pinned: false,
        };
        assert!(store.put_if_absent("key", &entry).expect("put").inserted);
        assert_eq!(store.get("key").expect("get"), Some(entry));
        assert_eq!(store.len().expect("len"), 1);
    }

    #[test]
    fn layered_store_replicates_and_reads_in_order() {
        let fast = Arc::new(VolatileStore::new());
        let durable = Arc::new(VolatileStore::new());
        let store = LayeredStore::new(vec![fast.clone(), durable.clone()]).expect("layered");
        let entry = StoredEntry {
            value: b"replicated".to_vec(),
            expires_at: 0,
            pinned: true,
        };

        let outcome = store.put_if_absent("key", &entry).expect("put");
        assert!(outcome.inserted);
        assert_eq!(outcome.stored_bytes, (entry.value.len() * 2) as u64);
        assert_eq!(fast.get("key").expect("fast"), Some(entry.clone()));
        assert_eq!(durable.get("key").expect("durable"), Some(entry.clone()));
        assert_eq!(store.get("key").expect("layered get"), Some(entry));

        let snapshot = store.load_index().expect("index");
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].stored_bytes, 20);
    }

    #[test]
    fn layered_store_rejects_conflicting_existing_payloads() {
        let first = Arc::new(VolatileStore::new());
        let second = Arc::new(VolatileStore::new());
        let store = LayeredStore::new(vec![first.clone(), second]).expect("layered");
        let existing = StoredEntry {
            value: b"old".to_vec(),
            expires_at: 0,
            pinned: false,
        };
        first.put_if_absent("key", &existing).expect("seed");

        let conflicting = StoredEntry {
            value: b"new".to_vec(),
            expires_at: 0,
            pinned: false,
        };
        let error = store
            .put_if_absent("key", &conflicting)
            .expect_err("must reject mismatch");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
