use crate::{KvBlock, KvBlockKey, KvTier, KvTierEntry};
use std::ffi::c_void;
use std::io;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRole {
    Prefill,
    Decode,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    Rdma,
    Nixl,
    RiftGpu,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportCapabilities {
    pub kind: TransportKind,
    pub device_to_device: bool,
    pub host_to_device: bool,
    pub device_to_host: bool,
    pub zero_copy: bool,
    pub remote: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectTransferRequest {
    pub source_handle: u64,
    pub destination_handle: u64,
    pub bytes: usize,
    pub source_offset: usize,
    pub destination_offset: usize,
}

pub trait DeviceTransferProvider: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> TransportCapabilities;
    fn transfer(&self, request: DirectTransferRequest) -> io::Result<()>;
    fn health(&self) -> io::Result<()> {
        Ok(())
    }
}

pub type DeviceTransferFn = unsafe extern "C" fn(
    context: *mut c_void,
    source_handle: u64,
    destination_handle: u64,
    bytes: usize,
    source_offset: usize,
    destination_offset: usize,
) -> i32;
pub type DeviceTransferHealthFn = unsafe extern "C" fn(context: *mut c_void) -> i32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FfiDeviceTransferOps {
    pub context: *mut c_void,
    pub transfer: DeviceTransferFn,
    pub health: Option<DeviceTransferHealthFn>,
}

pub struct FfiDeviceTransferProvider {
    name: String,
    capabilities: TransportCapabilities,
    ops: FfiDeviceTransferOps,
}

// SAFETY: construction is unsafe and requires the caller to guarantee that the opaque
// context and callbacks remain valid and thread-safe for the provider lifetime.
unsafe impl Send for FfiDeviceTransferProvider {}
// SAFETY: see Send justification above.
unsafe impl Sync for FfiDeviceTransferProvider {}

impl FfiDeviceTransferProvider {
    /// # Safety
    ///
    /// `ops.context` and all callbacks must remain valid for the provider lifetime.
    /// Callbacks must be safe to invoke concurrently when the provider is shared.
    pub unsafe fn new(
        name: impl Into<String>,
        capabilities: TransportCapabilities,
        ops: FfiDeviceTransferOps,
    ) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct-transfer provider name must not be empty",
            ));
        }
        if !capabilities.device_to_device {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct-transfer provider must advertise device-to-device support",
            ));
        }
        Ok(Self {
            name,
            capabilities,
            ops,
        })
    }
}

impl DeviceTransferProvider for FfiDeviceTransferProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> TransportCapabilities {
        self.capabilities
    }

    fn transfer(&self, request: DirectTransferRequest) -> io::Result<()> {
        if request.bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct transfer size must be greater than zero",
            ));
        }
        // SAFETY: callback validity/thread-safety is guaranteed by the constructor contract.
        let status = unsafe {
            (self.ops.transfer)(
                self.ops.context,
                request.source_handle,
                request.destination_handle,
                request.bytes,
                request.source_offset,
                request.destination_offset,
            )
        };
        status_to_result(status, "device transfer")
    }

    fn health(&self) -> io::Result<()> {
        let Some(health) = self.ops.health else {
            return Ok(());
        };
        // SAFETY: callback validity/thread-safety is guaranteed by the constructor contract.
        status_to_result(
            unsafe { health(self.ops.context) },
            "device transfer health",
        )
    }
}

fn status_to_result(status: i32, operation: &str) -> io::Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{operation} provider returned status {status}"
        )))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KvHandoffReport {
    pub requested: u64,
    pub transferred: u64,
    pub missed: u64,
    pub bytes: u64,
}

pub struct DisaggregatedKvRouter {
    prefill_sink: Arc<dyn KvTier>,
    decode_source: Arc<dyn KvTier>,
}

impl DisaggregatedKvRouter {
    pub fn new(prefill_sink: Arc<dyn KvTier>, decode_source: Arc<dyn KvTier>) -> Self {
        Self {
            prefill_sink,
            decode_source,
        }
    }

    pub fn publish_prefill(
        &self,
        blocks: &[KvBlock],
        expires_at: u64,
        pinned: bool,
    ) -> io::Result<KvHandoffReport> {
        let mut report = KvHandoffReport {
            requested: blocks.len() as u64,
            ..KvHandoffReport::default()
        };
        let mut written: Vec<KvBlockKey> = Vec::with_capacity(blocks.len());
        for block in blocks {
            let entry = KvTierEntry {
                block: block.clone(),
                expires_at,
                pinned,
            };
            if let Err(error) = self.prefill_sink.put(&entry) {
                for key in written.iter().rev() {
                    let _ = self.prefill_sink.remove(key);
                }
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "prefill handoff failed after {} blocks; rolled back published blocks: {error}",
                        written.len()
                    ),
                ));
            }
            written.push(block.key.clone());
            report.transferred = report.transferred.saturating_add(1);
            report.bytes = report.bytes.saturating_add(block.bytes.len() as u64);
        }
        Ok(report)
    }

    pub fn fetch_decode(&self, keys: &[KvBlockKey]) -> io::Result<(Vec<KvBlock>, KvHandoffReport)> {
        let mut report = KvHandoffReport {
            requested: keys.len() as u64,
            ..KvHandoffReport::default()
        };
        let mut blocks = Vec::with_capacity(keys.len());
        for key in keys {
            match self.decode_source.get(key)? {
                Some(entry) => {
                    report.transferred = report.transferred.saturating_add(1);
                    report.bytes = report.bytes.saturating_add(entry.block.bytes.len() as u64);
                    blocks.push(entry.block);
                }
                None => report.missed = report.missed.saturating_add(1),
            }
        }
        Ok((blocks, report))
    }

    pub fn health(&self) -> Vec<(&str, io::Result<()>)> {
        vec![
            (self.prefill_sink.name(), self.prefill_sink.health()),
            (self.decode_source.name(), self.decode_source.health()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvBlockRange;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemoryTier {
        values: Mutex<HashMap<String, KvTierEntry>>,
    }

    impl KvTier for MemoryTier {
        fn name(&self) -> &str {
            "handoff"
        }

        fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
            Ok(self.values.lock().unwrap().get(&key.cache_key()).cloned())
        }

        fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
            self.values
                .lock()
                .unwrap()
                .insert(entry.block.key.cache_key(), entry.clone());
            Ok(())
        }

        fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
            self.values.lock().unwrap().remove(&key.cache_key());
            Ok(())
        }

        fn clear(&self) -> io::Result<()> {
            self.values.lock().unwrap().clear();
            Ok(())
        }
    }

    fn blocks() -> Vec<KvBlock> {
        (0..2)
            .map(|index| {
                let tokens = [1, 2, 3, 4];
                let key = KvBlockKey::from_prefix(
                    "m",
                    &tokens,
                    KvBlockRange {
                        block_index: index,
                        token_start: index * 2,
                        token_count: 2,
                        layer_start: 0,
                        layer_count: 8,
                        layout_version: 1,
                    },
                );
                KvBlock::new(key, vec![index as u8 + 1; 32]).unwrap()
            })
            .collect()
    }

    #[test]
    fn prefill_to_decode_round_trip() {
        let shared: Arc<dyn KvTier> = Arc::new(MemoryTier::default());
        let router = DisaggregatedKvRouter::new(Arc::clone(&shared), shared);
        let blocks = blocks();
        let sent = router.publish_prefill(&blocks, 0, false).unwrap();
        assert_eq!(sent.transferred, 2);
        let keys: Vec<_> = blocks.iter().map(|block| block.key.clone()).collect();
        let (restored, received) = router.fetch_decode(&keys).unwrap();
        assert_eq!(received.transferred, 2);
        assert_eq!(restored, blocks);
    }

    #[test]
    fn ffi_provider_propagates_status() {
        unsafe extern "C" fn transfer(
            _context: *mut c_void,
            _source: u64,
            _destination: u64,
            _bytes: usize,
            _source_offset: usize,
            _destination_offset: usize,
        ) -> i32 {
            0
        }
        let provider = unsafe {
            FfiDeviceTransferProvider::new(
                "nixl-test",
                TransportCapabilities {
                    kind: TransportKind::Nixl,
                    device_to_device: true,
                    host_to_device: true,
                    device_to_host: true,
                    zero_copy: true,
                    remote: true,
                },
                FfiDeviceTransferOps {
                    context: std::ptr::null_mut(),
                    transfer,
                    health: None,
                },
            )
        }
        .unwrap();
        provider
            .transfer(DirectTransferRequest {
                source_handle: 1,
                destination_handle: 2,
                bytes: 4096,
                source_offset: 0,
                destination_offset: 0,
            })
            .unwrap();
        assert_eq!(provider.capabilities().kind, TransportKind::Nixl);
    }
}
