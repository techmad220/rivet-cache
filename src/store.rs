use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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
}
