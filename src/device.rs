use crate::kv::{KvBlock, KvBlockKey, KvTier, KvTierEntry};
use std::collections::HashMap;
use std::ffi::c_void;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeviceBuffer {
    pub handle: u64,
    pub len: usize,
}

pub trait DeviceMemory: Send + Sync {
    fn name(&self) -> &str;
    fn allocate(&self, len: usize) -> io::Result<DeviceBuffer>;
    fn upload(&self, buffer: DeviceBuffer, bytes: &[u8]) -> io::Result<()>;
    fn download(&self, buffer: DeviceBuffer) -> io::Result<Vec<u8>>;
    fn free(&self, buffer: DeviceBuffer) -> io::Result<()>;
    fn health(&self) -> io::Result<()> {
        Ok(())
    }
}

pub struct HostDeviceMemory {
    name: String,
    next: AtomicU64,
    buffers: Mutex<HashMap<u64, Vec<u8>>>,
}

impl HostDeviceMemory {
    pub fn new(name: impl Into<String>) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device backend name must not be empty",
            ));
        }
        Ok(Self {
            name,
            next: AtomicU64::new(1),
            buffers: Mutex::new(HashMap::new()),
        })
    }
}

impl DeviceMemory for HostDeviceMemory {
    fn name(&self) -> &str {
        &self.name
    }

    fn allocate(&self, len: usize) -> io::Result<DeviceBuffer> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device allocation length must be greater than zero",
            ));
        }
        let handle = self.next.fetch_add(1, Ordering::Relaxed);
        self.buffers
            .lock()
            .map_err(|_| io::Error::other("host device memory lock poisoned"))?
            .insert(handle, vec![0_u8; len]);
        Ok(DeviceBuffer { handle, len })
    }

    fn upload(&self, buffer: DeviceBuffer, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() != buffer.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device upload length does not match allocation",
            ));
        }
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|_| io::Error::other("host device memory lock poisoned"))?;
        let slot = buffers
            .get_mut(&buffer.handle)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "device buffer not found"))?;
        slot.copy_from_slice(bytes);
        Ok(())
    }

    fn download(&self, buffer: DeviceBuffer) -> io::Result<Vec<u8>> {
        let buffers = self
            .buffers
            .lock()
            .map_err(|_| io::Error::other("host device memory lock poisoned"))?;
        let bytes = buffers
            .get(&buffer.handle)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "device buffer not found"))?;
        if bytes.len() != buffer.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "device allocation metadata length mismatch",
            ));
        }
        Ok(bytes.clone())
    }

    fn free(&self, buffer: DeviceBuffer) -> io::Result<()> {
        self.buffers
            .lock()
            .map_err(|_| io::Error::other("host device memory lock poisoned"))?
            .remove(&buffer.handle);
        Ok(())
    }
}

pub type DeviceAllocFn =
    unsafe extern "C" fn(context: *mut c_void, len: usize, out_handle: *mut u64) -> i32;
pub type DeviceUploadFn =
    unsafe extern "C" fn(context: *mut c_void, handle: u64, bytes: *const u8, len: usize) -> i32;
pub type DeviceDownloadFn =
    unsafe extern "C" fn(context: *mut c_void, handle: u64, bytes: *mut u8, len: usize) -> i32;
pub type DeviceFreeFn = unsafe extern "C" fn(context: *mut c_void, handle: u64) -> i32;
pub type DeviceHealthFn = unsafe extern "C" fn(context: *mut c_void) -> i32;

#[derive(Clone, Copy)]
pub struct FfiDeviceOps {
    pub context: *mut c_void,
    pub allocate: DeviceAllocFn,
    pub upload: DeviceUploadFn,
    pub download: DeviceDownloadFn,
    pub free: DeviceFreeFn,
    pub health: Option<DeviceHealthFn>,
}

pub struct FfiDeviceMemory {
    name: String,
    ops: FfiDeviceOps,
}

impl FfiDeviceMemory {
    /// # Safety
    ///
    /// The caller must guarantee that `ops.context` and every callback remain
    /// valid for this object's lifetime, that callbacks are thread-safe, and
    /// that returned handles remain valid until `free` succeeds.
    pub unsafe fn new(name: impl Into<String>, ops: FfiDeviceOps) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device backend name must not be empty",
            ));
        }
        Ok(Self { name, ops })
    }

    fn check(&self, operation: &str, code: i32) -> io::Result<()> {
        if code == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "device backend {} operation {operation} failed with status {code}",
                self.name
            )))
        }
    }
}

// SAFETY: construction requires the caller to guarantee thread-safe callbacks
// and context lifetime. The wrapper itself performs no direct pointer access.
unsafe impl Send for FfiDeviceMemory {}
unsafe impl Sync for FfiDeviceMemory {}

impl DeviceMemory for FfiDeviceMemory {
    fn name(&self) -> &str {
        &self.name
    }

    fn allocate(&self, len: usize) -> io::Result<DeviceBuffer> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device allocation length must be greater than zero",
            ));
        }
        let mut handle = 0_u64;
        let code = unsafe { (self.ops.allocate)(self.ops.context, len, &mut handle) };
        self.check("allocate", code)?;
        if handle == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "device backend returned an invalid zero handle",
            ));
        }
        Ok(DeviceBuffer { handle, len })
    }

    fn upload(&self, buffer: DeviceBuffer, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() != buffer.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device upload length does not match allocation",
            ));
        }
        let code = unsafe {
            (self.ops.upload)(self.ops.context, buffer.handle, bytes.as_ptr(), bytes.len())
        };
        self.check("upload", code)
    }

    fn download(&self, buffer: DeviceBuffer) -> io::Result<Vec<u8>> {
        let mut bytes = vec![0_u8; buffer.len];
        let code = unsafe {
            (self.ops.download)(
                self.ops.context,
                buffer.handle,
                bytes.as_mut_ptr(),
                bytes.len(),
            )
        };
        self.check("download", code)?;
        Ok(bytes)
    }

    fn free(&self, buffer: DeviceBuffer) -> io::Result<()> {
        let code = unsafe { (self.ops.free)(self.ops.context, buffer.handle) };
        self.check("free", code)
    }

    fn health(&self) -> io::Result<()> {
        match self.ops.health {
            Some(health) => {
                let code = unsafe { health(self.ops.context) };
                self.check("health", code)
            }
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DeviceEntry {
    buffer: DeviceBuffer,
    expires_at: u64,
    pinned: bool,
}

pub struct DeviceKvTier {
    name: String,
    device: Arc<dyn DeviceMemory>,
    entries: Mutex<HashMap<String, DeviceEntry>>,
}

impl DeviceKvTier {
    pub fn new(name: impl Into<String>, device: Arc<dyn DeviceMemory>) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "device tier name must not be empty",
            ));
        }
        Ok(Self {
            name,
            device,
            entries: Mutex::new(HashMap::new()),
        })
    }

    pub fn device_name(&self) -> &str {
        self.device.name()
    }

    pub fn len(&self) -> io::Result<usize> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| io::Error::other("device tier lock poisoned"))?
            .len())
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
}

impl KvTier for DeviceKvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let entry = self
            .entries
            .lock()
            .map_err(|_| io::Error::other("device tier lock poisoned"))?
            .get(&key.cache_key())
            .copied();
        let Some(entry) = entry else {
            return Ok(None);
        };
        let bytes = self.device.download(entry.buffer)?;
        Ok(Some(KvTierEntry {
            block: KvBlock {
                key: key.clone(),
                bytes,
            },
            expires_at: entry.expires_at,
            pinned: entry.pinned,
        }))
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        let buffer = self.device.allocate(entry.block.bytes.len())?;
        if let Err(error) = self.device.upload(buffer, &entry.block.bytes) {
            let _ = self.device.free(buffer);
            return Err(error);
        }

        let replaced = self
            .entries
            .lock()
            .map_err(|_| io::Error::other("device tier lock poisoned"))?
            .insert(
                entry.block.key.cache_key(),
                DeviceEntry {
                    buffer,
                    expires_at: entry.expires_at,
                    pinned: entry.pinned,
                },
            );
        if let Some(old) = replaced {
            self.device.free(old.buffer)?;
        }
        Ok(())
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        let removed = self
            .entries
            .lock()
            .map_err(|_| io::Error::other("device tier lock poisoned"))?
            .remove(&key.cache_key());
        if let Some(entry) = removed {
            self.device.free(entry.buffer)?;
        }
        Ok(())
    }

    fn clear(&self) -> io::Result<()> {
        let buffers: Vec<DeviceBuffer> = self
            .entries
            .lock()
            .map_err(|_| io::Error::other("device tier lock poisoned"))?
            .drain()
            .map(|(_, entry)| entry.buffer)
            .collect();
        let mut first_error = None;
        for buffer in buffers {
            if let Err(error) = self.device.free(buffer) {
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

    fn health(&self) -> io::Result<()> {
        self.device.health()
    }
}

impl Drop for DeviceKvTier {
    fn drop(&mut self) {
        if let Ok(entries) = self.entries.get_mut() {
            for (_, entry) in entries.drain() {
                let _ = self.device.free(entry.buffer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvBlockRange;

    fn key() -> KvBlockKey {
        KvBlockKey::from_prefix(
            "device-test",
            &[7, 8],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 2,
                layer_start: 0,
                layer_count: 8,
                layout_version: 1,
            },
        )
    }

    #[test]
    fn device_tier_round_trips_and_releases() {
        let device: Arc<dyn DeviceMemory> =
            Arc::new(HostDeviceMemory::new("host-reference-device").expect("device"));
        let tier = DeviceKvTier::new("device", device).expect("tier");
        let entry = KvTierEntry {
            block: KvBlock {
                key: key(),
                bytes: vec![1, 2, 3, 4],
            },
            expires_at: 99,
            pinned: true,
        };
        tier.put(&entry).expect("put");
        assert_eq!(tier.len().expect("len"), 1);
        assert!(!tier.is_empty().expect("is_empty"));
        assert_eq!(
            tier.get(&entry.block.key).expect("get"),
            Some(entry.clone())
        );
        tier.remove(&entry.block.key).expect("remove");
        assert!(tier.is_empty().expect("is_empty after remove"));
        assert!(tier
            .get(&entry.block.key)
            .expect("get after remove")
            .is_none());
    }
}
