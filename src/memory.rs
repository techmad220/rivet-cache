use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ffi::c_void;
use std::io;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub trait PageAllocator: Send + Sync {
    fn name(&self) -> &str;
    fn allocate(&self, bytes: usize, numa_node: Option<u32>) -> io::Result<NonNull<u8>>;
    fn deallocate(&self, pointer: NonNull<u8>, bytes: usize, numa_node: Option<u32>);
    fn page_locked(&self) -> bool;
    fn numa_supported(&self) -> bool;
}

#[derive(Debug, Default)]
pub struct HeapPageAllocator;

impl PageAllocator for HeapPageAllocator {
    fn name(&self) -> &str {
        "heap-reference"
    }

    fn allocate(&self, bytes: usize, numa_node: Option<u32>) -> io::Result<NonNull<u8>> {
        if numa_node.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "heap reference allocator does not implement NUMA placement",
            ));
        }
        let layout = layout(bytes)?;
        // SAFETY: layout is valid and non-zero. The allocation is owned until deallocate.
        NonNull::new(unsafe { alloc_zeroed(layout) })
            .ok_or_else(|| io::Error::other("heap allocation failed"))
    }

    fn deallocate(&self, pointer: NonNull<u8>, bytes: usize, _numa_node: Option<u32>) {
        if let Ok(layout) = layout(bytes) {
            // SAFETY: pointer was allocated by this allocator with the same layout.
            unsafe { dealloc(pointer.as_ptr(), layout) };
        }
    }

    fn page_locked(&self) -> bool {
        false
    }

    fn numa_supported(&self) -> bool {
        false
    }
}

#[derive(Debug, Default)]
pub struct NativePinnedAllocator;

impl PageAllocator for NativePinnedAllocator {
    fn name(&self) -> &str {
        "native-page-locked"
    }

    fn allocate(&self, bytes: usize, numa_node: Option<u32>) -> io::Result<NonNull<u8>> {
        native::allocate(bytes, numa_node)
    }

    fn deallocate(&self, pointer: NonNull<u8>, bytes: usize, numa_node: Option<u32>) {
        native::deallocate(pointer, bytes, numa_node)
    }

    fn page_locked(&self) -> bool {
        true
    }

    fn numa_supported(&self) -> bool {
        native::numa_supported()
    }
}

struct Region {
    pointer: NonNull<u8>,
    capacity: usize,
    numa_node: Option<u32>,
    allocator: Arc<dyn PageAllocator>,
}

// SAFETY: Region uniquely owns its allocation and only exposes mutation through &mut self.
unsafe impl Send for Region {}

impl Drop for Region {
    fn drop(&mut self) {
        self.allocator
            .deallocate(self.pointer, self.capacity, self.numa_node);
    }
}

struct PoolInner {
    allocator: Arc<dyn PageAllocator>,
    max_cached_bytes: usize,
    cached: Mutex<Vec<Region>>,
    cached_bytes: AtomicU64,
    allocations: AtomicU64,
    reuses: AtomicU64,
}

#[derive(Clone)]
pub struct PinnedMemoryPool {
    inner: Arc<PoolInner>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PinnedPoolStats {
    pub cached_bytes: u64,
    pub cached_regions: u64,
    pub allocations: u64,
    pub reuses: u64,
}

impl PinnedMemoryPool {
    pub fn native(max_cached_bytes: usize) -> Self {
        Self::with_allocator(max_cached_bytes, Arc::new(NativePinnedAllocator))
    }

    pub fn with_allocator(max_cached_bytes: usize, allocator: Arc<dyn PageAllocator>) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                allocator,
                max_cached_bytes,
                cached: Mutex::new(Vec::new()),
                cached_bytes: AtomicU64::new(0),
                allocations: AtomicU64::new(0),
                reuses: AtomicU64::new(0),
            }),
        }
    }

    pub fn allocator_name(&self) -> &str {
        self.inner.allocator.name()
    }

    pub fn page_locked(&self) -> bool {
        self.inner.allocator.page_locked()
    }

    pub fn numa_supported(&self) -> bool {
        self.inner.allocator.numa_supported()
    }

    pub fn acquire(&self, bytes: usize, numa_node: Option<u32>) -> io::Result<PinnedLease> {
        if bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pinned allocation size must be greater than zero",
            ));
        }
        if numa_node.is_some() && !self.inner.allocator.numa_supported() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "allocator {} does not support NUMA placement",
                    self.inner.allocator.name()
                ),
            ));
        }

        let mut cached = self
            .inner
            .cached
            .lock()
            .map_err(|_| io::Error::other("pinned pool lock poisoned"))?;
        let selected = cached
            .iter()
            .enumerate()
            .filter(|(_, region)| region.numa_node == numa_node && region.capacity >= bytes)
            .min_by_key(|(_, region)| region.capacity)
            .map(|(index, _)| index);

        let region = if let Some(index) = selected {
            let region = cached.swap_remove(index);
            self.inner
                .cached_bytes
                .fetch_sub(region.capacity as u64, Ordering::Relaxed);
            self.inner.reuses.fetch_add(1, Ordering::Relaxed);
            region
        } else {
            drop(cached);
            let pointer = self.inner.allocator.allocate(bytes, numa_node)?;
            self.inner.allocations.fetch_add(1, Ordering::Relaxed);
            Region {
                pointer,
                capacity: bytes,
                numa_node,
                allocator: Arc::clone(&self.inner.allocator),
            }
        };

        Ok(PinnedLease {
            pool: Arc::clone(&self.inner),
            region: Some(region),
            len: bytes,
        })
    }

    pub fn stats(&self) -> io::Result<PinnedPoolStats> {
        let cached = self
            .inner
            .cached
            .lock()
            .map_err(|_| io::Error::other("pinned pool lock poisoned"))?;
        Ok(PinnedPoolStats {
            cached_bytes: self.inner.cached_bytes.load(Ordering::Relaxed),
            cached_regions: cached.len() as u64,
            allocations: self.inner.allocations.load(Ordering::Relaxed),
            reuses: self.inner.reuses.load(Ordering::Relaxed),
        })
    }
}

pub struct PinnedLease {
    pool: Arc<PoolInner>,
    region: Option<Region>,
    len: usize,
}

impl PinnedLease {
    pub fn capacity(&self) -> usize {
        self.region
            .as_ref()
            .map(|region| region.capacity)
            .unwrap_or(0)
    }

    pub fn numa_node(&self) -> Option<u32> {
        self.region.as_ref().and_then(|region| region.numa_node)
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.region
            .as_ref()
            .map(|region| region.pointer.as_ptr().cast_const())
            .unwrap_or(std::ptr::null())
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.region
            .as_mut()
            .map(|region| region.pointer.as_ptr())
            .unwrap_or(std::ptr::null_mut())
    }
}

impl Deref for PinnedLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let region = self.region.as_ref().expect("pinned lease region missing");
        // SAFETY: the lease owns the region for its lifetime and len <= capacity.
        unsafe { slice::from_raw_parts(region.pointer.as_ptr(), self.len) }
    }
}

impl DerefMut for PinnedLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let region = self.region.as_mut().expect("pinned lease region missing");
        // SAFETY: the lease has exclusive access to the region and len <= capacity.
        unsafe { slice::from_raw_parts_mut(region.pointer.as_ptr(), self.len) }
    }
}

impl Drop for PinnedLease {
    fn drop(&mut self) {
        let Some(region) = self.region.take() else {
            return;
        };
        if region.capacity > self.pool.max_cached_bytes {
            drop(region);
            return;
        }
        let Ok(mut cached) = self.pool.cached.lock() else {
            drop(region);
            return;
        };
        let current = self.pool.cached_bytes.load(Ordering::Relaxed) as usize;
        if current.saturating_add(region.capacity) > self.pool.max_cached_bytes {
            drop(region);
            return;
        }
        self.pool
            .cached_bytes
            .fetch_add(region.capacity as u64, Ordering::Relaxed);
        cached.push(region);
    }
}

fn layout(bytes: usize) -> io::Result<Layout> {
    Layout::from_size_align(bytes, 64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "allocation size cannot be represented by the host allocator",
        )
    })
}

#[cfg(target_os = "windows")]
mod native {
    use super::*;

    const MEM_COMMIT: u32 = 0x1000;
    const MEM_RESERVE: u32 = 0x2000;
    const MEM_RELEASE: u32 = 0x8000;
    const PAGE_READWRITE: u32 = 0x04;

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn VirtualAlloc(
            address: *mut c_void,
            size: usize,
            allocation_type: u32,
            protect: u32,
        ) -> *mut c_void;
        fn VirtualAllocExNuma(
            process: *mut c_void,
            address: *mut c_void,
            size: usize,
            allocation_type: u32,
            protect: u32,
            preferred_node: u32,
        ) -> *mut c_void;
        fn VirtualFree(address: *mut c_void, size: usize, free_type: u32) -> i32;
        fn VirtualLock(address: *mut c_void, size: usize) -> i32;
        fn VirtualUnlock(address: *mut c_void, size: usize) -> i32;
    }

    pub fn allocate(bytes: usize, numa_node: Option<u32>) -> io::Result<NonNull<u8>> {
        if bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero allocation",
            ));
        }
        // SAFETY: calls documented Win32 allocation APIs with null desired address.
        let pointer = unsafe {
            match numa_node {
                Some(node) => VirtualAllocExNuma(
                    GetCurrentProcess(),
                    std::ptr::null_mut(),
                    bytes,
                    MEM_RESERVE | MEM_COMMIT,
                    PAGE_READWRITE,
                    node,
                ),
                None => VirtualAlloc(
                    std::ptr::null_mut(),
                    bytes,
                    MEM_RESERVE | MEM_COMMIT,
                    PAGE_READWRITE,
                ),
            }
        };
        let pointer = NonNull::new(pointer.cast::<u8>()).ok_or_else(io::Error::last_os_error)?;
        // SAFETY: pointer denotes a committed region of at least bytes bytes.
        if unsafe { VirtualLock(pointer.as_ptr().cast::<c_void>(), bytes) } == 0 {
            let error = io::Error::last_os_error();
            // SAFETY: releases the allocation just created above.
            unsafe {
                VirtualFree(pointer.as_ptr().cast::<c_void>(), 0, MEM_RELEASE);
            }
            return Err(error);
        }
        Ok(pointer)
    }

    pub fn deallocate(pointer: NonNull<u8>, bytes: usize, _numa_node: Option<u32>) {
        // SAFETY: pointer/size originated from allocate; unlock is best effort before release.
        unsafe {
            VirtualUnlock(pointer.as_ptr().cast::<c_void>(), bytes);
            VirtualFree(pointer.as_ptr().cast::<c_void>(), 0, MEM_RELEASE);
        }
    }

    pub fn numa_supported() -> bool {
        true
    }
}

#[cfg(unix)]
mod native {
    use super::*;

    const PROT_READ: i32 = 0x1;
    const PROT_WRITE: i32 = 0x2;
    const MAP_PRIVATE: i32 = 0x2;
    #[cfg(target_os = "macos")]
    const MAP_ANON: i32 = 0x1000;
    #[cfg(not(target_os = "macos"))]
    const MAP_ANON: i32 = 0x20;
    #[cfg(target_os = "linux")]
    const MPOL_BIND: i32 = 2;

    extern "C" {
        fn mmap(
            address: *mut c_void,
            len: usize,
            prot: i32,
            flags: i32,
            fd: i32,
            offset: isize,
        ) -> *mut c_void;
        fn munmap(address: *mut c_void, len: usize) -> i32;
        fn mlock(address: *const c_void, len: usize) -> i32;
        fn munlock(address: *const c_void, len: usize) -> i32;
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        fn syscall(number: i64, ...) -> i64;
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        fn syscall(number: i64, ...) -> i64;
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    const SYS_MBIND: i64 = 237;
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    const SYS_MBIND: i64 = 235;

    pub fn allocate(bytes: usize, numa_node: Option<u32>) -> io::Result<NonNull<u8>> {
        if bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero allocation",
            ));
        }
        #[cfg(not(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )))]
        if numa_node.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native NUMA placement is implemented on Linux x86_64/aarch64 and Windows",
            ));
        }

        // SAFETY: requests a new anonymous private mapping owned by this allocator.
        let raw = unsafe {
            mmap(
                std::ptr::null_mut(),
                bytes,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANON,
                -1,
                0,
            )
        };
        if raw as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        let pointer = NonNull::new(raw.cast::<u8>()).ok_or_else(io::Error::last_os_error)?;

        #[cfg(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ))]
        if let Some(node) = numa_node {
            if node >= 64 {
                // SAFETY: releases the mapping created above.
                unsafe { munmap(raw, bytes) };
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "built-in NUMA binder currently supports node ids below 64",
                ));
            }
            let mask = 1_u64 << node;
            // SAFETY: mbind receives the valid mapping and a one-word node mask.
            let result = unsafe {
                syscall(
                    SYS_MBIND,
                    raw,
                    bytes,
                    MPOL_BIND,
                    &mask as *const u64,
                    64_usize,
                    0_u32,
                )
            };
            if result != 0 {
                let error = io::Error::last_os_error();
                // SAFETY: releases the mapping created above.
                unsafe { munmap(raw, bytes) };
                return Err(error);
            }
        }

        // First-touch the mapping after NUMA policy is installed, then page-lock it.
        // SAFETY: mapping is writable for bytes bytes.
        unsafe { std::ptr::write_bytes(pointer.as_ptr(), 0, bytes) };
        // SAFETY: mapping is valid for bytes bytes.
        if unsafe { mlock(raw.cast_const(), bytes) } != 0 {
            let error = io::Error::last_os_error();
            // SAFETY: releases the mapping created above.
            unsafe { munmap(raw, bytes) };
            return Err(error);
        }
        Ok(pointer)
    }

    pub fn deallocate(pointer: NonNull<u8>, bytes: usize, _numa_node: Option<u32>) {
        let raw = pointer.as_ptr().cast::<c_void>();
        // SAFETY: pointer/size originated from allocate; unlock is best effort before unmap.
        unsafe {
            munlock(raw.cast_const(), bytes);
            munmap(raw, bytes);
        }
    }

    pub fn numa_supported() -> bool {
        cfg!(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ))
    }
}

#[cfg(not(any(unix, target_os = "windows")))]
mod native {
    use super::*;

    pub fn allocate(_bytes: usize, _numa_node: Option<u32>) -> io::Result<NonNull<u8>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native page locking is not implemented on this platform",
        ))
    }

    pub fn deallocate(_pointer: NonNull<u8>, _bytes: usize, _numa_node: Option<u32>) {}

    pub fn numa_supported() -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_reuses_best_fit_region() {
        let pool = PinnedMemoryPool::with_allocator(32 * 1024, Arc::new(HeapPageAllocator));
        {
            let mut lease = pool.acquire(4096, None).expect("acquire");
            lease[0] = 7;
            assert_eq!(lease.capacity(), 4096);
        }
        let lease = pool.acquire(1024, None).expect("reuse");
        assert_eq!(lease.capacity(), 4096);
        let stats = pool.stats().expect("stats");
        assert_eq!(stats.allocations, 1);
        assert_eq!(stats.reuses, 1);
    }

    #[test]
    fn unsupported_numa_fails_closed() {
        let pool = PinnedMemoryPool::with_allocator(4096, Arc::new(HeapPageAllocator));
        assert_eq!(
            pool.acquire(1024, Some(0))
                .err()
                .expect("unsupported NUMA error")
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
}
