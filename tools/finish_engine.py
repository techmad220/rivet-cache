from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(f"expected exactly one anchor in {path}: {old!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


def insert_before(path: str, anchor: str, addition: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if text.count(anchor) != 1:
        raise SystemExit(f"expected exactly one anchor in {path}: {anchor!r}")
    p.write_text(text.replace(anchor, addition + anchor, 1), encoding="utf-8")


# ---------- store.rs: public volatile memory backend + layered backend ----------
replace_once(
    "src/store.rs",
    "use sha2::{Digest, Sha256};\nuse std::fs::{self, File, OpenOptions};",
    "use sha2::{Digest, Sha256};\nuse std::collections::{hash_map::Entry, HashMap};\nuse std::fs::{self, File, OpenOptions};",
)
replace_once(
    "src/store.rs",
    "use std::sync::atomic::{AtomicU64, Ordering};",
    "use std::sync::atomic::{AtomicU64, Ordering};\nuse std::sync::{Arc, Mutex, MutexGuard};",
)

store_addition = r'''
/// Volatile in-process implementation of [`PersistentStore`].
///
/// This backend is useful for tests, ephemeral tiers, and as a reference
/// implementation for custom stores. It intentionally does not survive process
/// restart.
#[derive(Default)]
pub struct MemoryStore {
    entries: Mutex<HashMap<String, (StoredEntry, u64)>>,
    tick: AtomicU64,
}

impl MemoryStore {
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
        self.tick
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }
}

impl PersistentStore for MemoryStore {
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
                                format!(
                                    "layered store metadata mismatch for key {}",
                                    record.key
                                ),
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

'''
insert_before(
    "src/store.rs",
    "/// Default filesystem-backed persistent tier.\n",
    store_addition,
)

# Store-level tests for ordered reads, replication, rollback-safe agreement, and indexing.
p = Path("src/store.rs")
text = p.read_text(encoding="utf-8")
store_tests = r'''

    #[test]
    fn memory_store_round_trips() {
        let store = MemoryStore::new();
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
        let fast = Arc::new(MemoryStore::new());
        let durable = Arc::new(MemoryStore::new());
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
        let first = Arc::new(MemoryStore::new());
        let second = Arc::new(MemoryStore::new());
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
'''
idx = text.rfind("\n}")
if idx < 0:
    raise SystemExit("could not find store test-module closing brace")
p.write_text(text[:idx] + store_tests + text[idx:], encoding="utf-8")

# ---------- policy.rs: explicit invalidation telemetry ----------
replace_once(
    "src/policy.rs",
    "    Corruption(u64),\n    Clear,",
    "    Corruption(u64),\n    Invalidation(u64),\n    Clear,",
)

# ---------- lib.rs: export backends + batch/invalidation APIs ----------
replace_once(
    "src/lib.rs",
    "pub use store::{FileStore, PersistentStore, PutOutcome, StoreRecord, StoreSnapshot, StoredEntry};",
    "pub use store::{\n    FileStore, LayeredStore, MemoryStore, PersistentStore, PutOutcome, StoreRecord, StoreSnapshot,\n    StoredEntry,\n};",
)

cache_methods = r'''
    /// Fetch multiple keys in input order.
    ///
    /// Each lookup preserves normal hit/miss, TTL, promotion-to-memory, and
    /// telemetry semantics. The operation is not transactional.
    pub fn get_many<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a str>,
    ) -> io::Result<Vec<Option<Vec<u8>>>> {
        keys.into_iter().map(|key| self.get(key)).collect()
    }

    /// Write multiple entries using the normal cache write path.
    ///
    /// All keys are validated before the first write. Backend I/O can still
    /// fail after earlier entries have been committed, so this is intentionally
    /// a batch convenience API rather than a transaction boundary.
    pub fn put_many<'a>(
        &self,
        entries: impl IntoIterator<Item = (&'a str, &'a [u8], Option<Duration>, bool)>,
    ) -> io::Result<()> {
        let entries: Vec<_> = entries.into_iter().collect();
        for (key, _, _, _) in &entries {
            self.key_strategy.validate(key)?;
        }
        for (key, value, ttl, pinned) in entries {
            self.put(key, value, ttl, pinned)?;
        }
        Ok(())
    }

    /// Explicitly invalidate one key from memory and the injected persistent
    /// store. Missing keys are treated as already invalidated.
    pub fn invalidate(&self, key: &str) -> io::Result<()> {
        self.key_strategy.validate(key)?;

        if let Some(store) = self.store.as_ref() {
            store.remove(key)?;
        }

        let mut inner = self.lock_inner()?;
        if let Some(record) = inner.memory.remove(key) {
            inner.memory_bytes = inner
                .memory_bytes
                .saturating_sub(record.value.len() as u64);
        }
        if let Some(record) = inner.disk.remove(key) {
            inner.disk_bytes = inner.disk_bytes.saturating_sub(record.stored_bytes);
        }
        drop(inner);

        self.metrics.record(CacheEvent::Invalidation(1));
        Ok(())
    }

    /// Explicitly invalidate several keys.
    ///
    /// All keys are validated before the first invalidation. Backend I/O can
    /// still fail partway through the operation.
    pub fn invalidate_many<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a str>,
    ) -> io::Result<()> {
        let keys: Vec<_> = keys.into_iter().collect();
        for key in &keys {
            self.key_strategy.validate(key)?;
        }
        for key in keys {
            self.invalidate(key)?;
        }
        Ok(())
    }

'''
insert_before("src/lib.rs", "    pub fn clear(&self) -> io::Result<()> {\n", cache_methods)

p = Path("src/lib.rs")
text = p.read_text(encoding="utf-8")
lib_tests = r'''

    #[test]
    fn batch_reads_writes_and_invalidation_use_normal_semantics() {
        let cache = ContextCache::builder()
            .memory_capacity(1024)
            .persistent_capacity(1024)
            .persistent_store(MemoryStore::new())
            .build()
            .expect("cache");
        let first = cache.make_key("batch", "m", "one");
        let second = cache.make_key("batch", "m", "two");

        cache
            .put_many([
                (first.as_str(), b"one".as_slice(), None, false),
                (second.as_str(), b"two".as_slice(), None, false),
            ])
            .expect("put many");
        assert_eq!(
            cache
                .get_many([first.as_str(), second.as_str()])
                .expect("get many"),
            vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
        );

        cache
            .invalidate_many([first.as_str(), second.as_str()])
            .expect("invalidate many");
        assert_eq!(cache.get(&first).expect("first"), None);
        assert_eq!(cache.get(&second).expect("second"), None);
    }

    #[test]
    fn layered_store_can_back_a_context_cache() {
        let fast = Arc::new(MemoryStore::new());
        let durable = Arc::new(MemoryStore::new());
        let layered = LayeredStore::new(vec![fast.clone(), durable.clone()]).expect("layered");
        let cache = ContextCache::builder()
            .persistent_capacity(4096)
            .persistent_store(layered)
            .build()
            .expect("cache");
        let key = cache.make_key("tier", "m", "artifact");

        cache.put(&key, b"value", None, false).expect("put");
        assert_eq!(fast.get(&key).expect("fast"), Some(StoredEntry {
            value: b"value".to_vec(),
            expires_at: 0,
            pinned: false,
        }));
        assert!(durable.get(&key).expect("durable").is_some());
        assert_eq!(cache.get(&key).expect("get"), Some(b"value".to_vec()));
    }
'''
idx = text.rfind("\n}")
if idx < 0:
    raise SystemExit("could not find lib test-module closing brace")
p.write_text(text[:idx] + lib_tests + text[idx:], encoding="utf-8")

# ---------- crate metadata and factual public documentation ----------
replace_once("Cargo.toml", 'version = "0.2.0"', 'version = "0.3.0"')
replace_once(
    "Cargo.toml",
    'description = "DI-first Rust cache with pluggable persistence, keying, clock, eviction, and telemetry."',
    'description = "DI-first Rust cache with composable storage tiers, batch operations, invalidation, and telemetry."',
)

p = Path("CHANGELOG.md")
text = p.read_text(encoding="utf-8")
entry = """## 0.3.0 - 2026-08-28\n\n- Added `MemoryStore` as a volatile reference/backend implementation.\n- Added `LayeredStore` for ordered reads and replicated writes across injected stores.\n- Added fail-closed checks for conflicting existing replicas.\n- Added `get_many`, `put_many`, `invalidate`, and `invalidate_many`.\n- Added explicit invalidation telemetry.\n- Preserved the RivetCache v1 key and filesystem ABIs.\n- Kept the core synchronous and runtime-neutral; no async runtime or external service is required.\n\n"""
if "## 0.3.0" not in text:
    text = text.replace("# Changelog\n\n", "# Changelog\n\n" + entry, 1)
p.write_text(text, encoding="utf-8")

p = Path("README.md")
text = p.read_text(encoding="utf-8")
text = text.replace(
    "- Injectable persistent backend for remote/shared/custom stores.\n",
    "- Injectable persistent backend for remote/shared/custom stores.\n- Composable ordered storage layers with replicated writes.\n- Volatile `MemoryStore` reference backend.\n- Batch get/write helpers and explicit single/batch invalidation.\n",
    1,
)
advanced = r'''
## Composable storage layers

`LayeredStore` combines independently injected `PersistentStore` backends without coupling the core to a network protocol or service. Reads search backends in configured priority order and writes are replicated to every backend.

The layered implementation deliberately avoids automatic read promotion. This keeps byte accounting deterministic and makes promotion a caller-owned policy instead of hidden behavior. Existing replicas are checked before writes; conflicting payloads or metadata fail closed with `InvalidData` rather than silently choosing a copy.

```rust
use rivet_cache::{ContextCache, FileStore, LayeredStore, MemoryStore, PersistentStore};
use std::sync::Arc;

let hot: Arc<dyn PersistentStore> = Arc::new(MemoryStore::new());
let durable: Arc<dyn PersistentStore> = Arc::new(FileStore::new("./cache")?);
let stores = LayeredStore::new(vec![hot, durable])?;
let cache = ContextCache::builder()
    .persistent_capacity(512 * 1024 * 1024)
    .persistent_store(stores)
    .build()?;
# Ok::<(), std::io::Error>(())
```

## Batch operations and invalidation

`get_many` and `put_many` are convenience APIs built on the same validated single-entry paths, so TTL, eviction, persistence and telemetry semantics remain consistent. They are intentionally not transactions: an I/O error may occur after an earlier item has committed.

`invalidate` and `invalidate_many` explicitly remove keys from both the memory tier and the configured persistent store. Missing keys are treated as already invalidated.

'''
if "## Composable storage layers" not in text:
    text = text.replace("## Development\n", advanced + "## Development\n", 1)
p.write_text(text, encoding="utf-8")

p = Path("CERTIFICATION.md")
text = p.read_text(encoding="utf-8")
text = text.replace(
    "The test suite covers stable and isolated keys, memory LRU eviction, restart persistence, corrupt-disk recovery, and TTL expiration.",
    "The test suite covers stable and isolated keys, memory LRU eviction, restart persistence, corrupt-disk recovery, TTL expiration, injected components, layered-store replication and conflict rejection, batch operations, and explicit invalidation.",
    1,
)
p.write_text(text, encoding="utf-8")

Path("docs/architecture.md").write_text(
    """# RivetCache architecture\n\nRivetCache is a synchronous, runtime-neutral Rust cache core. It stores opaque bytes and does not assume a model runtime, transport, remote service, serializer, GPU API, or async executor.\n\n## Stable core boundaries\n\n- `PersistentStore`: injected storage backend contract.\n- `KeyStrategy`: injected key generation and validation.\n- `Clock`: injected TTL time source.\n- `EvictionPolicy`: injected victim selection.\n- `MetricsSink`: injected telemetry hook.\n\nThe bundled `FileStore`, `MemoryStore`, and `LayeredStore` are implementations of the storage contract, not privileged code paths in the cache core.\n\n## Layering semantics\n\n`LayeredStore` performs ordered reads and replicated writes. It does not implicitly promote values after a lower-priority hit. This prevents hidden capacity growth and keeps quota accounting explicit. Conflicting pre-existing replicas fail closed on write, and conflicting metadata fails index reconstruction.\n\n## Error and consistency model\n\nSingle-entry operations return `std::io::Result`. Batch helpers validate all keys before mutation but are not transactional across entries. Layered writes attempt rollback of replicas created by the current call if a later backend write fails; externally modified backends remain the backend owner's consistency responsibility.\n\n## ABI\n\nThe default key domain remains `RIVET_CACHE_V1`. The bundled filesystem format remains `RIVET01` with `.rivetcache` files. Custom `KeyStrategy` and `PersistentStore` implementations may define their own independently versioned external formats.\n\n## Scope\n\nPublic claims should describe implemented and tested RivetCache capabilities directly. The project does not require or assert compatibility, equivalence, or feature parity with any third-party cache product.\n""",
    encoding="utf-8",
)

print("RIVETCACHE_FINISH_PATCH=READY")
