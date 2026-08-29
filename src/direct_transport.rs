use crate::{
    DeviceBuffer, DeviceTransferProvider, DirectTransferRequest, GpuDirectIo,
    TransportCapabilities, TransportKind,
};
use std::io;
use std::sync::Arc;

/// Adapts a local GPU-direct provider into the worker transfer interface used by
/// prefill/decode orchestration. Remote RDMA/NIXL implementations remain pluggable
/// through `DeviceTransferProvider`; this adapter is for same-host device transports.
pub struct GpuDirectTransferProvider {
    name: String,
    kind: TransportKind,
    io: Arc<dyn GpuDirectIo>,
}

impl GpuDirectTransferProvider {
    pub fn new(
        name: impl Into<String>,
        kind: TransportKind,
        io: Arc<dyn GpuDirectIo>,
    ) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GPU worker transport name must not be empty",
            ));
        }
        if !io.capabilities().device_copy {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GPU worker transport requires device-copy capability",
            ));
        }
        Ok(Self { name, kind, io })
    }

    pub fn rift_gpu(io: Arc<dyn GpuDirectIo>) -> io::Result<Self> {
        Self::new("riftgpu-worker-transfer", TransportKind::RiftGpu, io)
    }

    pub fn hip(io: Arc<dyn GpuDirectIo>) -> io::Result<Self> {
        Self::new("hip-worker-transfer", TransportKind::Custom, io)
    }
}

impl DeviceTransferProvider for GpuDirectTransferProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> TransportCapabilities {
        let capabilities = self.io.capabilities();
        TransportCapabilities {
            kind: self.kind,
            device_to_device: capabilities.device_copy,
            host_to_device: false,
            device_to_host: false,
            zero_copy: capabilities.zero_copy,
            remote: false,
        }
    }

    fn transfer(&self, request: DirectTransferRequest) -> io::Result<()> {
        if request.bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GPU worker transfer size must be greater than zero",
            ));
        }
        let source_len = request
            .source_offset
            .checked_add(request.bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "source range overflow"))?;
        let destination_len = request
            .destination_offset
            .checked_add(request.bytes)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "destination range overflow")
            })?;
        self.io.copy_device_range(
            DeviceBuffer {
                handle: request.source_handle,
                len: source_len,
            },
            request.source_offset,
            DeviceBuffer {
                handle: request.destination_handle,
                len: destination_len,
            },
            request.destination_offset,
            request.bytes,
        )
    }

    fn health(&self) -> io::Result<()> {
        self.io.health()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GpuDirectCapabilities;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Io {
        copied: AtomicU64,
    }

    impl GpuDirectIo for Io {
        fn name(&self) -> &str {
            "test"
        }

        fn capabilities(&self) -> GpuDirectCapabilities {
            GpuDirectCapabilities {
                device_copy: true,
                storage_read: false,
                storage_write: false,
                external_memory: false,
                zero_copy: true,
            }
        }

        fn copy_device(
            &self,
            _source: DeviceBuffer,
            _destination: DeviceBuffer,
            bytes: usize,
        ) -> io::Result<()> {
            self.copied.fetch_add(bytes as u64, Ordering::Relaxed);
            Ok(())
        }

        fn read_file(
            &self,
            _path: &Path,
            _destination: DeviceBuffer,
            _destination_offset: usize,
            _bytes: usize,
        ) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::Unsupported, "test"))
        }

        fn write_file(
            &self,
            _source: DeviceBuffer,
            _source_offset: usize,
            _path: &Path,
            _bytes: usize,
        ) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::Unsupported, "test"))
        }
    }

    #[test]
    fn same_host_worker_provider_moves_full_buffer() {
        let io = Arc::new(Io {
            copied: AtomicU64::new(0),
        });
        let provider = GpuDirectTransferProvider::hip(io.clone()).unwrap();
        provider
            .transfer(DirectTransferRequest {
                source_handle: 1,
                destination_handle: 2,
                bytes: 4096,
                source_offset: 0,
                destination_offset: 0,
            })
            .unwrap();
        assert_eq!(io.copied.load(Ordering::Relaxed), 4096);
        assert!(provider.capabilities().zero_copy);
        assert!(!provider.capabilities().remote);
    }
}
