use crate::DeviceBuffer;
use std::ffi::{c_char, c_void, CString};
use std::io;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GpuDirectCapabilities {
    pub device_copy: bool,
    pub storage_read: bool,
    pub storage_write: bool,
    pub external_memory: bool,
    pub zero_copy: bool,
}

pub trait GpuDirectIo: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> GpuDirectCapabilities;
    fn copy_device(
        &self,
        source: DeviceBuffer,
        destination: DeviceBuffer,
        bytes: usize,
    ) -> io::Result<()>;
    fn read_file(
        &self,
        path: &Path,
        destination: DeviceBuffer,
        destination_offset: usize,
        bytes: usize,
    ) -> io::Result<()>;
    fn write_file(
        &self,
        source: DeviceBuffer,
        source_offset: usize,
        path: &Path,
        bytes: usize,
    ) -> io::Result<()>;
    fn health(&self) -> io::Result<()> {
        Ok(())
    }
}

pub type GpuCopyFn = unsafe extern "C" fn(
    context: *mut c_void,
    source_handle: u64,
    destination_handle: u64,
    bytes: usize,
) -> i32;
pub type GpuFileReadFn = unsafe extern "C" fn(
    context: *mut c_void,
    path: *const c_char,
    destination_handle: u64,
    destination_offset: usize,
    bytes: usize,
) -> i32;
pub type GpuFileWriteFn = unsafe extern "C" fn(
    context: *mut c_void,
    source_handle: u64,
    source_offset: usize,
    path: *const c_char,
    bytes: usize,
) -> i32;
pub type GpuDirectHealthFn = unsafe extern "C" fn(context: *mut c_void) -> i32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FfiGpuDirectOps {
    pub context: *mut c_void,
    pub copy_device: Option<GpuCopyFn>,
    pub read_file: Option<GpuFileReadFn>,
    pub write_file: Option<GpuFileWriteFn>,
    pub health: Option<GpuDirectHealthFn>,
}

pub struct FfiGpuDirectIo {
    name: String,
    capabilities: GpuDirectCapabilities,
    ops: FfiGpuDirectOps,
}

// SAFETY: constructor contract requires context/callback lifetime and thread safety.
unsafe impl Send for FfiGpuDirectIo {}
// SAFETY: constructor contract requires context/callback lifetime and thread safety.
unsafe impl Sync for FfiGpuDirectIo {}

impl FfiGpuDirectIo {
    /// # Safety
    ///
    /// The opaque context and callbacks must remain valid for this object's lifetime and
    /// must be safe for concurrent invocation. Device handles must be valid for the
    /// backend represented by the callbacks.
    pub unsafe fn new(
        name: impl Into<String>,
        capabilities: GpuDirectCapabilities,
        ops: FfiGpuDirectOps,
    ) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GPU-direct provider name must not be empty",
            ));
        }
        if capabilities.device_copy && ops.copy_device.is_none()
            || capabilities.storage_read && ops.read_file.is_none()
            || capabilities.storage_write && ops.write_file.is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "advertised GPU-direct capabilities require matching callbacks",
            ));
        }
        Ok(Self {
            name,
            capabilities,
            ops,
        })
    }

    /// Convenience constructor for a RiftGPU/Vulkan-HIP host binding. RivetCache does not
    /// link a GPU SDK; the host supplies callbacks backed by the certified RiftGPU path.
    ///
    /// # Safety
    /// Same lifetime/thread-safety contract as `new`.
    pub unsafe fn rift_gpu(ops: FfiGpuDirectOps) -> io::Result<Self> {
        Self::new(
            "riftgpu-vulkan-hip",
            GpuDirectCapabilities {
                device_copy: ops.copy_device.is_some(),
                storage_read: ops.read_file.is_some(),
                storage_write: ops.write_file.is_some(),
                external_memory: true,
                zero_copy: true,
            },
            ops,
        )
    }
}

impl GpuDirectIo for FfiGpuDirectIo {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> GpuDirectCapabilities {
        self.capabilities
    }

    fn copy_device(
        &self,
        source: DeviceBuffer,
        destination: DeviceBuffer,
        bytes: usize,
    ) -> io::Result<()> {
        if bytes == 0 || bytes > source.len || bytes > destination.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "GPU-direct copy range exceeds a device buffer",
            ));
        }
        let callback = self.ops.copy_device.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "device copy is not supported")
        })?;
        // SAFETY: callback validity and handle ownership are guaranteed by the binding contract.
        status(
            unsafe { callback(self.ops.context, source.handle, destination.handle, bytes) },
            "device copy",
        )
    }

    fn read_file(
        &self,
        path: &Path,
        destination: DeviceBuffer,
        destination_offset: usize,
        bytes: usize,
    ) -> io::Result<()> {
        validate_range(destination, destination_offset, bytes)?;
        let callback = self.ops.read_file.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "GPU-direct file read is not supported",
            )
        })?;
        let path = path_to_cstring(path)?;
        // SAFETY: callback validity and destination handle are guaranteed by the binding contract.
        status(
            unsafe {
                callback(
                    self.ops.context,
                    path.as_ptr(),
                    destination.handle,
                    destination_offset,
                    bytes,
                )
            },
            "GPU-direct file read",
        )
    }

    fn write_file(
        &self,
        source: DeviceBuffer,
        source_offset: usize,
        path: &Path,
        bytes: usize,
    ) -> io::Result<()> {
        validate_range(source, source_offset, bytes)?;
        let callback = self.ops.write_file.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "GPU-direct file write is not supported",
            )
        })?;
        let path = path_to_cstring(path)?;
        // SAFETY: callback validity and source handle are guaranteed by the binding contract.
        status(
            unsafe {
                callback(
                    self.ops.context,
                    source.handle,
                    source_offset,
                    path.as_ptr(),
                    bytes,
                )
            },
            "GPU-direct file write",
        )
    }

    fn health(&self) -> io::Result<()> {
        let Some(callback) = self.ops.health else {
            return Ok(());
        };
        // SAFETY: callback validity is guaranteed by the binding contract.
        status(unsafe { callback(self.ops.context) }, "GPU-direct health")
    }
}

fn validate_range(buffer: DeviceBuffer, offset: usize, bytes: usize) -> io::Result<()> {
    if bytes == 0 || offset.checked_add(bytes).is_none() || offset + bytes > buffer.len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "GPU-direct range exceeds the device buffer",
        ));
    }
    Ok(())
}

fn path_to_cstring(path: &Path) -> io::Result<CString> {
    let path = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "GPU-direct path is not valid UTF-8",
        )
    })?;
    CString::new(path)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "GPU-direct path contains NUL"))
}

fn status(code: i32, operation: &str) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{operation} provider returned status {code}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Context {
        copies: AtomicU64,
        reads: AtomicU64,
        writes: AtomicU64,
    }

    unsafe extern "C" fn copy(
        context: *mut c_void,
        _source: u64,
        _destination: u64,
        bytes: usize,
    ) -> i32 {
        // SAFETY: test passes a valid Context pointer for the provider lifetime.
        let context = unsafe { &*(context.cast::<Context>()) };
        context.copies.fetch_add(bytes as u64, Ordering::Relaxed);
        0
    }

    unsafe extern "C" fn read(
        context: *mut c_void,
        _path: *const c_char,
        _destination: u64,
        _offset: usize,
        bytes: usize,
    ) -> i32 {
        // SAFETY: test passes a valid Context pointer for the provider lifetime.
        let context = unsafe { &*(context.cast::<Context>()) };
        context.reads.fetch_add(bytes as u64, Ordering::Relaxed);
        0
    }

    unsafe extern "C" fn write(
        context: *mut c_void,
        _source: u64,
        _offset: usize,
        _path: *const c_char,
        bytes: usize,
    ) -> i32 {
        // SAFETY: test passes a valid Context pointer for the provider lifetime.
        let context = unsafe { &*(context.cast::<Context>()) };
        context.writes.fetch_add(bytes as u64, Ordering::Relaxed);
        0
    }

    #[test]
    fn rift_gpu_binding_exercises_all_paths() {
        let mut context = Box::new(Context {
            copies: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        });
        let ops = FfiGpuDirectOps {
            context: (&mut *context as *mut Context).cast::<c_void>(),
            copy_device: Some(copy),
            read_file: Some(read),
            write_file: Some(write),
            health: None,
        };
        let io = unsafe { FfiGpuDirectIo::rift_gpu(ops) }.unwrap();
        let source = DeviceBuffer {
            handle: 1,
            len: 4096,
        };
        let destination = DeviceBuffer {
            handle: 2,
            len: 4096,
        };
        io.copy_device(source, destination, 2048).unwrap();
        io.read_file(Path::new("state.bin"), destination, 0, 1024)
            .unwrap();
        io.write_file(source, 512, Path::new("state.bin"), 1024)
            .unwrap();
        assert_eq!(context.copies.load(Ordering::Relaxed), 2048);
        assert_eq!(context.reads.load(Ordering::Relaxed), 1024);
        assert_eq!(context.writes.load(Ordering::Relaxed), 1024);
        assert!(io.capabilities().zero_copy);
    }
}
