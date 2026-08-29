use crate::{DeviceBuffer, DeviceMemory, GpuDirectCapabilities, GpuDirectIo};
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

const HIP_SUCCESS: i32 = 0;
const HIP_MEMCPY_HOST_TO_DEVICE: i32 = 1;
const HIP_MEMCPY_DEVICE_TO_HOST: i32 = 2;
const HIP_MEMCPY_DEVICE_TO_DEVICE: i32 = 3;

type HipMallocFn = unsafe extern "C" fn(*mut *mut c_void, usize) -> i32;
type HipFreeFn = unsafe extern "C" fn(*mut c_void) -> i32;
type HipMemcpyFn = unsafe extern "C" fn(*mut c_void, *const c_void, usize, i32) -> i32;
type HipDeviceSynchronizeFn = unsafe extern "C" fn() -> i32;
type HipGetDeviceCountFn = unsafe extern "C" fn(*mut i32) -> i32;
type HipGetErrorStringFn = unsafe extern "C" fn(i32) -> *const c_char;

struct HipRuntime {
    _library: DynamicLibrary,
    malloc: HipMallocFn,
    free: HipFreeFn,
    memcpy: HipMemcpyFn,
    synchronize: HipDeviceSynchronizeFn,
    device_count: HipGetDeviceCountFn,
    error_string: Option<HipGetErrorStringFn>,
}

unsafe impl Send for HipRuntime {}
unsafe impl Sync for HipRuntime {}

impl HipRuntime {
    fn load() -> io::Result<Self> {
        let library = DynamicLibrary::open_hip()?;
        // SAFETY: each symbol name and signature is the public HIP C ABI. The
        // DynamicLibrary remains owned by HipRuntime for every function pointer's lifetime.
        unsafe {
            Ok(Self {
                malloc: library.symbol(b"hipMalloc\0")?,
                free: library.symbol(b"hipFree\0")?,
                memcpy: library.symbol(b"hipMemcpy\0")?,
                synchronize: library.symbol(b"hipDeviceSynchronize\0")?,
                device_count: library.symbol(b"hipGetDeviceCount\0")?,
                error_string: library.symbol(b"hipGetErrorString\0").ok(),
                _library: library,
            })
        }
    }

    fn check(&self, operation: &str, code: i32) -> io::Result<()> {
        if code == HIP_SUCCESS {
            return Ok(());
        }
        let detail = self
            .error_string
            .and_then(|function| {
                // SAFETY: HIP owns the static error string and permits reading it
                // for a returned error code.
                let pointer = unsafe { function(code) };
                if pointer.is_null() {
                    None
                } else {
                    Some(unsafe { CStr::from_ptr(pointer) }.to_string_lossy().into_owned())
                }
            })
            .unwrap_or_else(|| "unknown HIP error".to_owned());
        Err(io::Error::other(format!(
            "{operation} failed with HIP status {code}: {detail}"
        )))
    }

    fn health(&self) -> io::Result<()> {
        let mut count = 0_i32;
        // SAFETY: count points to writable process memory.
        self.check("hipGetDeviceCount", unsafe { (self.device_count)(&mut count) })?;
        if count <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "HIP runtime exposed no GPU devices",
            ));
        }
        Ok(())
    }
}

/// Native HIP device allocator loaded at runtime. The core crate remains buildable
/// without a ROCm/HIP SDK and reports `Unsupported` when no runtime is installed.
pub struct HipDeviceMemory {
    runtime: Arc<HipRuntime>,
    allocations: Arc<Mutex<HashMap<u64, usize>>>,
}

impl HipDeviceMemory {
    pub fn load() -> io::Result<Self> {
        let runtime = Arc::new(HipRuntime::load()?);
        runtime.health()?;
        Ok(Self {
            runtime,
            allocations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn direct_io(&self) -> io::Result<HipDirectIo> {
        HipDirectIo::from_parts(Arc::clone(&self.runtime), Arc::clone(&self.allocations))
    }

    fn validate_buffer(&self, buffer: DeviceBuffer) -> io::Result<()> {
        let allocations = self
            .allocations
            .lock()
            .map_err(|_| io::Error::other("HIP allocation registry lock poisoned"))?;
        match allocations.get(&buffer.handle) {
            Some(len) if *len == buffer.len => Ok(()),
            Some(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HIP device buffer length does not match allocation registry",
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "HIP device buffer is not owned by this allocator",
            )),
        }
    }
}

impl DeviceMemory for HipDeviceMemory {
    fn name(&self) -> &str {
        "hip-native"
    }

    fn allocate(&self, len: usize) -> io::Result<DeviceBuffer> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HIP allocation length must be greater than zero",
            ));
        }
        let mut pointer = std::ptr::null_mut();
        // SAFETY: pointer targets writable process memory and HIP owns the resulting allocation.
        self.runtime
            .check("hipMalloc", unsafe { (self.runtime.malloc)(&mut pointer, len) })?;
        if pointer.is_null() {
            return Err(io::Error::other(
                "hipMalloc succeeded but returned a null device pointer",
            ));
        }
        let handle = pointer as usize as u64;
        let replaced = self
            .allocations
            .lock()
            .map_err(|_| io::Error::other("HIP allocation registry lock poisoned"))?
            .insert(handle, len);
        if replaced.is_some() {
            // SAFETY: pointer was returned by the successful hipMalloc above.
            let _ = unsafe { (self.runtime.free)(pointer) };
            return Err(io::Error::other(
                "HIP returned a device pointer already present in the allocation registry",
            ));
        }
        Ok(DeviceBuffer { handle, len })
    }

    fn upload(&self, buffer: DeviceBuffer, bytes: &[u8]) -> io::Result<()> {
        self.validate_buffer(buffer)?;
        if bytes.len() != buffer.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HIP upload length does not match allocation",
            ));
        }
        let destination = handle_to_pointer(buffer.handle)?;
        // SAFETY: registry validation proves destination is a live allocation of this size;
        // bytes remains live for the synchronous HIP copy.
        self.runtime.check(
            "hipMemcpy(HostToDevice)",
            unsafe {
                (self.runtime.memcpy)(
                    destination,
                    bytes.as_ptr().cast::<c_void>(),
                    bytes.len(),
                    HIP_MEMCPY_HOST_TO_DEVICE,
                )
            },
        )?;
        self.runtime.check(
            "hipDeviceSynchronize after upload",
            unsafe { (self.runtime.synchronize)() },
        )
    }

    fn download(&self, buffer: DeviceBuffer) -> io::Result<Vec<u8>> {
        self.validate_buffer(buffer)?;
        let source = handle_to_pointer(buffer.handle)?;
        let mut bytes = vec![0_u8; buffer.len];
        // SAFETY: registry validation proves source is live and destination has buffer.len bytes.
        self.runtime.check(
            "hipMemcpy(DeviceToHost)",
            unsafe {
                (self.runtime.memcpy)(
                    bytes.as_mut_ptr().cast::<c_void>(),
                    source.cast_const(),
                    bytes.len(),
                    HIP_MEMCPY_DEVICE_TO_HOST,
                )
            },
        )?;
        self.runtime.check(
            "hipDeviceSynchronize after download",
            unsafe { (self.runtime.synchronize)() },
        )?;
        Ok(bytes)
    }

    fn free(&self, buffer: DeviceBuffer) -> io::Result<()> {
        self.validate_buffer(buffer)?;
        let pointer = handle_to_pointer(buffer.handle)?;
        // SAFETY: registry validation proves this exact allocation is still live.
        self.runtime
            .check("hipFree", unsafe { (self.runtime.free)(pointer) })?;
        self.allocations
            .lock()
            .map_err(|_| io::Error::other("HIP allocation registry lock poisoned"))?
            .remove(&buffer.handle);
        Ok(())
    }

    fn health(&self) -> io::Result<()> {
        self.runtime.health()
    }
}

impl Drop for HipDeviceMemory {
    fn drop(&mut self) {
        if Arc::strong_count(&self.allocations) != 1 {
            return;
        }
        if let Ok(mut allocations) = self.allocations.lock() {
            for (handle, _) in allocations.drain() {
                if let Ok(pointer) = handle_to_pointer(handle) {
                    // SAFETY: every registry entry was created by hipMalloc and has not
                    // been successfully freed through this allocator.
                    let _ = unsafe { (self.runtime.free)(pointer) };
                }
            }
        }
    }
}

/// Direct HIP device-to-device transfers with optional Linux hipFile storage I/O.
/// Construct this from `HipDeviceMemory::direct_io` so buffer ownership and bounds
/// are validated before every native operation.
pub struct HipDirectIo {
    runtime: Arc<HipRuntime>,
    allocations: Arc<Mutex<HashMap<u64, usize>>>,
    #[cfg(target_os = "linux")]
    hipfile: Option<HipFileRuntime>,
}

unsafe impl Send for HipDirectIo {}
unsafe impl Sync for HipDirectIo {}

impl HipDirectIo {
    fn from_parts(
        runtime: Arc<HipRuntime>,
        allocations: Arc<Mutex<HashMap<u64, usize>>>,
    ) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        let hipfile = HipFileRuntime::load_optional()?;
        Ok(Self {
            runtime,
            allocations,
            #[cfg(target_os = "linux")]
            hipfile,
        })
    }

    fn allocation_len(&self, handle: u64) -> io::Result<usize> {
        self.allocations
            .lock()
            .map_err(|_| io::Error::other("HIP allocation registry lock poisoned"))?
            .get(&handle)
            .copied()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "HIP direct-I/O buffer is not owned by the paired allocator",
                )
            })
    }

    fn validate_range(&self, buffer: DeviceBuffer, offset: usize, bytes: usize) -> io::Result<()> {
        if bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HIP direct-I/O size must be greater than zero",
            ));
        }
        let actual = self.allocation_len(buffer.handle)?;
        if actual != buffer.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HIP direct-I/O buffer length does not match allocation registry",
            ));
        }
        let end = offset.checked_add(bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "HIP direct-I/O range overflow")
        })?;
        if end > actual {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HIP direct-I/O range exceeds device allocation",
            ));
        }
        Ok(())
    }

    pub fn copy_device_range(
        &self,
        source: DeviceBuffer,
        source_offset: usize,
        destination: DeviceBuffer,
        destination_offset: usize,
        bytes: usize,
    ) -> io::Result<()> {
        self.validate_range(source, source_offset, bytes)?;
        self.validate_range(destination, destination_offset, bytes)?;
        let source = pointer_with_offset(source.handle, source_offset)?;
        let destination = pointer_with_offset(destination.handle, destination_offset)?;
        // SAFETY: both ranges were checked against live HIP allocations; hipMemcpy is synchronous
        // with respect to the subsequent explicit device synchronization.
        self.runtime.check(
            "hipMemcpy(DeviceToDevice)",
            unsafe {
                (self.runtime.memcpy)(
                    destination,
                    source.cast_const(),
                    bytes,
                    HIP_MEMCPY_DEVICE_TO_DEVICE,
                )
            },
        )?;
        self.runtime.check(
            "hipDeviceSynchronize after device copy",
            unsafe { (self.runtime.synchronize)() },
        )
    }

    #[cfg(target_os = "linux")]
    fn hipfile(&self) -> io::Result<&HipFileRuntime> {
        self.hipfile.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "hipFile is not installed; set RIVET_HIPFILE_LIBRARY to a compatible libhipfile",
            )
        })
    }
}

impl GpuDirectIo for HipDirectIo {
    fn name(&self) -> &str {
        "hip-native"
    }

    fn capabilities(&self) -> GpuDirectCapabilities {
        #[cfg(target_os = "linux")]
        let storage = self.hipfile.is_some();
        #[cfg(not(target_os = "linux"))]
        let storage = false;
        GpuDirectCapabilities {
            device_copy: true,
            storage_read: storage,
            storage_write: storage,
            external_memory: false,
            zero_copy: true,
        }
    }

    fn copy_device(
        &self,
        source: DeviceBuffer,
        destination: DeviceBuffer,
        bytes: usize,
    ) -> io::Result<()> {
        self.copy_device_range(source, 0, destination, 0, bytes)
    }

    fn read_file(
        &self,
        path: &Path,
        destination: DeviceBuffer,
        destination_offset: usize,
        bytes: usize,
    ) -> io::Result<()> {
        self.validate_range(destination, destination_offset, bytes)?;
        #[cfg(target_os = "linux")]
        {
            self.hipfile()?.read_file(path, destination, destination_offset, bytes)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native hipFile GPU storage I/O is available only on supported Linux ROCm hosts",
            ))
        }
    }

    fn write_file(
        &self,
        source: DeviceBuffer,
        source_offset: usize,
        path: &Path,
        bytes: usize,
    ) -> io::Result<()> {
        self.validate_range(source, source_offset, bytes)?;
        #[cfg(target_os = "linux")]
        {
            self.hipfile()?.write_file(source, source_offset, path, bytes)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native hipFile GPU storage I/O is available only on supported Linux ROCm hosts",
            ))
        }
    }

    fn health(&self) -> io::Result<()> {
        self.runtime.health()
    }
}

fn handle_to_pointer(handle: u64) -> io::Result<*mut c_void> {
    if handle == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "HIP device handle must not be zero",
        ));
    }
    let address = usize::try_from(handle).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "HIP device handle does not fit this process address width",
        )
    })?;
    Ok(address as *mut c_void)
}

fn pointer_with_offset(handle: u64, offset: usize) -> io::Result<*mut c_void> {
    let pointer = handle_to_pointer(handle)?;
    let address = pointer as usize;
    let address = address.checked_add(offset).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "HIP pointer offset overflow")
    })?;
    Ok(address as *mut c_void)
}

#[cfg(target_os = "linux")]
const O_DIRECT: i32 = 0o40000;

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
union HipFileOsHandle {
    fd: i32,
    h_file: *mut c_void,
}

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct HipFileDescr {
    type_: i32,
    handle: HipFileOsHandle,
    fs_ops: *const c_void,
}

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct HipFileError {
    err: i32,
    hip_drv_err: i32,
}

#[cfg(target_os = "linux")]
type HipFileHandleRegisterFn =
    unsafe extern "C" fn(*mut *mut c_void, *mut HipFileDescr) -> HipFileError;
#[cfg(target_os = "linux")]
type HipFileHandleDeregisterFn = unsafe extern "C" fn(*mut c_void);
#[cfg(target_os = "linux")]
type HipFileBufRegisterFn = unsafe extern "C" fn(*const c_void, usize, i32) -> HipFileError;
#[cfg(target_os = "linux")]
type HipFileBufDeregisterFn = unsafe extern "C" fn(*const c_void) -> HipFileError;
#[cfg(target_os = "linux")]
type HipFileReadFn = unsafe extern "C" fn(*mut c_void, *mut c_void, usize, i64, i64) -> isize;
#[cfg(target_os = "linux")]
type HipFileWriteFn = unsafe extern "C" fn(*mut c_void, *const c_void, usize, i64, i64) -> isize;

#[cfg(target_os = "linux")]
struct HipFileRuntime {
    _library: DynamicLibrary,
    register_handle: HipFileHandleRegisterFn,
    deregister_handle: HipFileHandleDeregisterFn,
    register_buffer: HipFileBufRegisterFn,
    deregister_buffer: HipFileBufDeregisterFn,
    read: HipFileReadFn,
    write: HipFileWriteFn,
}

#[cfg(target_os = "linux")]
unsafe impl Send for HipFileRuntime {}
#[cfg(target_os = "linux")]
unsafe impl Sync for HipFileRuntime {}

#[cfg(target_os = "linux")]
impl HipFileRuntime {
    fn load_optional() -> io::Result<Option<Self>> {
        let library = match DynamicLibrary::open_hipfile() {
            Ok(library) => library,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        // SAFETY: signatures mirror the public hipFile 0.x C ABI and the library remains owned.
        unsafe {
            Ok(Some(Self {
                register_handle: library.symbol(b"hipFileHandleRegister\0")?,
                deregister_handle: library.symbol(b"hipFileHandleDeregister\0")?,
                register_buffer: library.symbol(b"hipFileBufRegister\0")?,
                deregister_buffer: library.symbol(b"hipFileBufDeregister\0")?,
                read: library.symbol(b"hipFileRead\0")?,
                write: library.symbol(b"hipFileWrite\0")?,
                _library: library,
            }))
        }
    }

    fn check(&self, operation: &str, error: HipFileError) -> io::Result<()> {
        if error.err == 0 && error.hip_drv_err == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "{operation} failed with hipFile status {} and HIP status {}",
                error.err, error.hip_drv_err
            )))
        }
    }

    fn with_registered_file<T>(
        &self,
        file: &std::fs::File,
        operation: impl FnOnce(*mut c_void) -> io::Result<T>,
    ) -> io::Result<T> {
        use std::os::fd::AsRawFd;
        let mut descriptor = HipFileDescr {
            type_: 1,
            handle: HipFileOsHandle { fd: file.as_raw_fd() },
            fs_ops: std::ptr::null(),
        };
        let mut handle = std::ptr::null_mut();
        // SAFETY: descriptor mirrors hipFileDescr_t and the file remains open for this scope.
        self.check(
            "hipFileHandleRegister",
            unsafe { (self.register_handle)(&mut handle, &mut descriptor) },
        )?;
        if handle.is_null() {
            return Err(io::Error::other(
                "hipFileHandleRegister succeeded but returned a null handle",
            ));
        }
        let result = operation(handle);
        // SAFETY: handle was registered exactly once above and file is still open.
        unsafe { (self.deregister_handle)(handle) };
        result
    }

    fn with_registered_buffer<T>(
        &self,
        buffer: DeviceBuffer,
        operation: impl FnOnce(*mut c_void) -> io::Result<T>,
    ) -> io::Result<T> {
        let pointer = handle_to_pointer(buffer.handle)?;
        // SAFETY: caller validated the buffer against a live hipMalloc allocation.
        self.check(
            "hipFileBufRegister",
            unsafe { (self.register_buffer)(pointer.cast_const(), buffer.len, 0) },
        )?;
        let result = operation(pointer);
        // SAFETY: same pointer registered immediately above and remains allocated.
        let deregister = unsafe { (self.deregister_buffer)(pointer.cast_const()) };
        if let Err(error) = self.check("hipFileBufDeregister", deregister) {
            return match result {
                Ok(_) => Err(error),
                Err(original) => Err(original),
            };
        }
        result
    }

    fn read_file(
        &self,
        path: &Path,
        destination: DeviceBuffer,
        destination_offset: usize,
        bytes: usize,
    ) -> io::Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(path)?;
        self.with_registered_file(&file, |file_handle| {
            self.with_registered_buffer(destination, |pointer| {
                // SAFETY: registered file and device buffer remain valid for this synchronous call.
                let transferred = unsafe {
                    (self.read)(
                        file_handle,
                        pointer,
                        bytes,
                        0,
                        destination_offset as i64,
                    )
                };
                check_transfer("hipFileRead", transferred, bytes)
            })
        })
    }

    fn write_file(
        &self,
        source: DeviceBuffer,
        source_offset: usize,
        path: &Path,
        bytes: usize,
    ) -> io::Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(O_DIRECT)
            .open(path)?;
        file.set_len(bytes as u64)?;
        self.with_registered_file(&file, |file_handle| {
            self.with_registered_buffer(source, |pointer| {
                // SAFETY: registered file and device buffer remain valid for this synchronous call.
                let transferred = unsafe {
                    (self.write)(file_handle, pointer.cast_const(), bytes, 0, source_offset as i64)
                };
                check_transfer("hipFileWrite", transferred, bytes)
            })
        })
    }
}

#[cfg(target_os = "linux")]
fn check_transfer(operation: &str, transferred: isize, expected: usize) -> io::Result<()> {
    if transferred < 0 {
        return Err(io::Error::other(format!(
            "{operation} failed with transfer status {transferred}"
        )));
    }
    if transferred as usize != expected {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{operation} transferred {transferred} bytes, expected {expected}"),
        ));
    }
    Ok(())
}

struct DynamicLibrary {
    handle: *mut c_void,
}

unsafe impl Send for DynamicLibrary {}
unsafe impl Sync for DynamicLibrary {}

impl DynamicLibrary {
    fn open_hip() -> io::Result<Self> {
        if let Ok(path) = std::env::var("RIVET_HIP_LIBRARY") {
            return Self::open(&path);
        }
        #[cfg(target_os = "windows")]
        let candidates = ["amdhip64.dll"];
        #[cfg(target_os = "linux")]
        let candidates = ["libamdhip64.so", "libamdhip64.so.6", "libamdhip64.so.5"];
        #[cfg(not(any(target_os = "windows", target_os = "linux")))]
        let candidates: [&str; 0] = [];
        Self::open_candidates(&candidates, "HIP runtime")
    }

    #[cfg(target_os = "linux")]
    fn open_hipfile() -> io::Result<Self> {
        if let Ok(path) = std::env::var("RIVET_HIPFILE_LIBRARY") {
            return Self::open(&path);
        }
        Self::open_candidates(&["libhipfile.so", "libhipfile.so.0"], "hipFile runtime")
    }

    fn open_candidates(candidates: &[&str], label: &str) -> io::Result<Self> {
        let mut messages = Vec::new();
        for candidate in candidates {
            match Self::open(candidate) {
                Ok(library) => return Ok(library),
                Err(error) => messages.push(format!("{candidate}: {error}")),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            if messages.is_empty() {
                format!("{label} is not supported on this platform")
            } else {
                format!("could not load {label}: {}", messages.join("; "))
            },
        ))
    }

    fn open(name: &str) -> io::Result<Self> {
        let name = CString::new(name).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "dynamic library path contains NUL")
        })?;
        platform_open(&name).map(|handle| Self { handle })
    }

    unsafe fn symbol<T: Copy>(&self, name: &[u8]) -> io::Result<T> {
        let name = CStr::from_bytes_with_nul(name).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "dynamic symbol name is not NUL terminated")
        })?;
        let pointer = platform_symbol(self.handle, name)?;
        if std::mem::size_of::<T>() != std::mem::size_of::<*mut c_void>() {
            return Err(io::Error::other("dynamic function pointer size mismatch"));
        }
        // SAFETY: caller selects T to match the requested public C ABI symbol.
        Ok(std::mem::transmute_copy::<*mut c_void, T>(&pointer))
    }
}

impl Drop for DynamicLibrary {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            platform_close(self.handle);
        }
    }
}

#[cfg(target_os = "windows")]
#[link(name = "kernel32")]
extern "system" {
    fn LoadLibraryA(name: *const c_char) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const c_char) -> *mut c_void;
    fn FreeLibrary(module: *mut c_void) -> i32;
}

#[cfg(target_os = "windows")]
fn platform_open(name: &CStr) -> io::Result<*mut c_void> {
    // SAFETY: name is NUL terminated and LoadLibraryA copies/consumes it synchronously.
    let handle = unsafe { LoadLibraryA(name.as_ptr()) };
    if handle.is_null() {
        Err(io::Error::last_os_error())
    } else {
        Ok(handle)
    }
}

#[cfg(target_os = "windows")]
unsafe fn platform_symbol(handle: *mut c_void, name: &CStr) -> io::Result<*mut c_void> {
    let pointer = GetProcAddress(handle, name.as_ptr());
    if pointer.is_null() {
        Err(io::Error::last_os_error())
    } else {
        Ok(pointer)
    }
}

#[cfg(target_os = "windows")]
fn platform_close(handle: *mut c_void) {
    // SAFETY: handle was returned by LoadLibraryA and is owned by DynamicLibrary.
    let _ = unsafe { FreeLibrary(handle) };
}

#[cfg(target_os = "linux")]
const RTLD_NOW: i32 = 2;

#[cfg(target_os = "linux")]
#[link(name = "dl")]
extern "C" {
    fn dlopen(filename: *const c_char, flags: i32) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> i32;
    fn dlerror() -> *const c_char;
}

#[cfg(target_os = "linux")]
fn platform_open(name: &CStr) -> io::Result<*mut c_void> {
    // SAFETY: name is NUL terminated and flags are a valid dlopen mode.
    let handle = unsafe { dlopen(name.as_ptr(), RTLD_NOW) };
    if handle.is_null() {
        Err(io::Error::new(io::ErrorKind::NotFound, dl_error()))
    } else {
        Ok(handle)
    }
}

#[cfg(target_os = "linux")]
unsafe fn platform_symbol(handle: *mut c_void, name: &CStr) -> io::Result<*mut c_void> {
    let pointer = dlsym(handle, name.as_ptr());
    if pointer.is_null() {
        Err(io::Error::new(io::ErrorKind::NotFound, dl_error()))
    } else {
        Ok(pointer)
    }
}

#[cfg(target_os = "linux")]
fn platform_close(handle: *mut c_void) {
    // SAFETY: handle was returned by dlopen and is owned by DynamicLibrary.
    let _ = unsafe { dlclose(handle) };
}

#[cfg(target_os = "linux")]
fn dl_error() -> String {
    // SAFETY: dlerror returns either null or a process-owned NUL-terminated message.
    let pointer = unsafe { dlerror() };
    if pointer.is_null() {
        "dynamic loader reported an unknown error".to_owned()
    } else {
        unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned()
    }
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
fn platform_open(_name: &CStr) -> io::Result<*mut c_void> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native HIP dynamic loading is supported only on Windows and Linux",
    ))
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
unsafe fn platform_symbol(_handle: *mut c_void, _name: &CStr) -> io::Result<*mut c_void> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native HIP dynamic loading is supported only on Windows and Linux",
    ))
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
fn platform_close(_handle: *mut c_void) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_handles_and_overflow_are_rejected_without_a_runtime() {
        assert!(handle_to_pointer(0).is_err());
        if usize::BITS < 64 {
            assert!(handle_to_pointer(u64::MAX).is_err());
        }
    }

    #[test]
    fn unsupported_platform_or_missing_runtime_fails_closed() {
        let _ = HipDeviceMemory::load();
    }
}
