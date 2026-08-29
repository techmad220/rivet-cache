#[cfg(target_os = "linux")]
use crate::NativeKvSdk;
use crate::{KvBlockKey, KvTier, KvTierEntry, NativeSdkKvTier};
use std::io;
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MooncakeInit {
    Setup {
        local_hostname: String,
        metadata_server: String,
        global_segment_size: u64,
        local_buffer_size: u64,
        protocol: String,
        device_name: String,
        master_server_addr: String,
    },
    InitAll {
        protocol: String,
        device_name: String,
        mount_segment_size: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MooncakeConfig {
    pub name: String,
    pub namespace: String,
    pub library_path: Option<String>,
    pub init: MooncakeInit,
    pub force_remove: bool,
    pub max_value_bytes: usize,
}

impl MooncakeConfig {
    pub fn validate(&self) -> io::Result<()> {
        validate_id(&self.name, "tier name")?;
        validate_id(&self.namespace, "namespace")?;
        if self.max_value_bytes == 0 {
            return invalid("Mooncake max value bytes must be greater than zero");
        }
        match &self.init {
            MooncakeInit::Setup {
                local_hostname,
                metadata_server,
                protocol,
                master_server_addr,
                ..
            } => {
                for (name, value) in [
                    ("local hostname", local_hostname),
                    ("metadata server", metadata_server),
                    ("protocol", protocol),
                    ("master server", master_server_addr),
                ] {
                    validate_c_string(value, name)?;
                }
            }
            MooncakeInit::InitAll { protocol, .. } => validate_c_string(protocol, "protocol")?,
        }
        Ok(())
    }
}

pub struct MooncakeKvTier {
    inner: NativeSdkKvTier,
}

impl MooncakeKvTier {
    pub fn connect(config: MooncakeConfig) -> io::Result<Self> {
        config.validate()?;
        #[cfg(target_os = "linux")]
        {
            let sdk = Arc::new(native::MooncakeSdk::connect(&config)?);
            let inner =
                NativeSdkKvTier::new(config.name, config.namespace, config.max_value_bytes, sdk)?;
            Ok(Self { inner })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = config;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native Mooncake Store is currently supported on Linux hosts",
            ))
        }
    }

    pub fn sdk_name(&self) -> &str {
        self.inner.sdk_name()
    }
}

impl KvTier for MooncakeKvTier {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        self.inner.get(key)
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        self.inner.put(entry)
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        self.inner.remove(key)
    }

    fn clear(&self) -> io::Result<()> {
        self.inner.clear()
    }

    fn health(&self) -> io::Result<()> {
        self.inner.health()
    }
}

fn validate_id(value: &str, name: &str) -> io::Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid Mooncake {name}"),
        ));
    }
    Ok(())
}

fn validate_c_string(value: &str, name: &str) -> io::Result<()> {
    if value.trim().is_empty() || value.as_bytes().contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid Mooncake {name}"),
        ));
    }
    Ok(())
}

fn invalid<T>(message: &str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidInput, message))
}

#[cfg(target_os = "linux")]
mod native {
    use super::*;
    use std::ffi::{c_char, c_void, CStr, CString};
    use std::sync::{Mutex, MutexGuard};

    const RTLD_NOW: i32 = 2;
    type Store = *mut c_void;
    type CreateFn = unsafe extern "C" fn() -> Store;
    type DestroyFn = unsafe extern "C" fn(Store);
    type SetupFn = unsafe extern "C" fn(
        Store,
        *const c_char,
        *const c_char,
        u64,
        u64,
        *const c_char,
        *const c_char,
        *const c_char,
    ) -> i32;
    type InitAllFn = unsafe extern "C" fn(Store, *const c_char, *const c_char, u64) -> i32;
    type HealthFn = unsafe extern "C" fn(Store) -> i32;
    type PutFn =
        unsafe extern "C" fn(Store, *const c_char, *const c_void, usize, *const c_void) -> i32;
    type GetIntoFn = unsafe extern "C" fn(Store, *const c_char, *mut c_void, usize) -> i64;
    type ExistFn = unsafe extern "C" fn(Store, *const c_char) -> i32;
    type GetSizeFn = unsafe extern "C" fn(Store, *const c_char) -> i64;
    type RemoveFn = unsafe extern "C" fn(Store, *const c_char, i32) -> i32;
    type RemoveRegexFn = unsafe extern "C" fn(Store, *const c_char, i32) -> i64;

    #[link(name = "dl")]
    extern "C" {
        fn dlopen(filename: *const c_char, flags: i32) -> *mut c_void;
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
        fn dlclose(handle: *mut c_void) -> i32;
        fn dlerror() -> *const c_char;
    }

    #[derive(Clone, Copy)]
    struct Api {
        create: CreateFn,
        destroy: DestroyFn,
        setup: SetupFn,
        init_all: InitAllFn,
        health: HealthFn,
        put: PutFn,
        get_into: GetIntoFn,
        exist: ExistFn,
        get_size: GetSizeFn,
        remove: RemoveFn,
        remove_regex: RemoveRegexFn,
    }

    struct StoreState {
        handle: usize,
    }

    pub struct MooncakeSdk {
        library: usize,
        api: Api,
        store: Mutex<StoreState>,
        force_remove: bool,
    }

    impl MooncakeSdk {
        pub fn connect(config: &MooncakeConfig) -> io::Result<Self> {
            let path = config
                .library_path
                .clone()
                .or_else(|| std::env::var("MOONCAKE_STORE_LIBRARY").ok())
                .unwrap_or_else(|| "libmooncake_store.so".to_owned());
            let path = cstring(&path, "library path")?;
            let library = unsafe { dlopen(path.as_ptr(), RTLD_NOW) };
            if library.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("failed to load Mooncake Store library: {}", dl_error()),
                ));
            }
            let api = match unsafe { Api::load(library) } {
                Ok(api) => api,
                Err(error) => {
                    unsafe {
                        dlclose(library);
                    }
                    return Err(error);
                }
            };
            let store = unsafe { (api.create)() };
            if store.is_null() {
                unsafe {
                    dlclose(library);
                }
                return Err(io::Error::other("Mooncake Store create returned null"));
            }
            if let Err(error) = initialize(store, api, &config.init) {
                unsafe {
                    (api.destroy)(store);
                    dlclose(library);
                }
                return Err(error);
            }
            let sdk = Self {
                library: library as usize,
                api,
                store: Mutex::new(StoreState {
                    handle: store as usize,
                }),
                force_remove: config.force_remove,
            };
            sdk.health()?;
            Ok(sdk)
        }

        fn lock(&self) -> io::Result<MutexGuard<'_, StoreState>> {
            self.store
                .lock()
                .map_err(|_| io::Error::other("Mooncake Store lock poisoned"))
        }

        fn exists(&self, store: Store, key: &CString) -> io::Result<bool> {
            let status = unsafe { (self.api.exist)(store, key.as_ptr()) };
            match status {
                0 => Ok(false),
                1 => Ok(true),
                other => Err(io::Error::other(format!(
                    "Mooncake is_exist returned status {other}"
                ))),
            }
        }
    }

    impl NativeKvSdk for MooncakeSdk {
        fn name(&self) -> &str {
            "mooncake-store"
        }

        fn get(&self, key: &str, max_bytes: usize) -> io::Result<Option<Vec<u8>>> {
            let key = cstring(key, "key")?;
            let guard = self.lock()?;
            let store = guard.handle as Store;
            if !self.exists(store, &key)? {
                return Ok(None);
            }
            let size = unsafe { (self.api.get_size)(store, key.as_ptr()) };
            if size <= 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Mooncake get_size returned {size}"),
                ));
            }
            let size = usize::try_from(size).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Mooncake object size overflow")
            })?;
            if size > max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Mooncake object exceeds configured value limit",
                ));
            }
            let mut bytes = vec![0_u8; size];
            let read = unsafe {
                (self.api.get_into)(
                    store,
                    key.as_ptr(),
                    bytes.as_mut_ptr().cast::<c_void>(),
                    bytes.len(),
                )
            };
            if read <= 0 {
                return Err(io::Error::other(format!(
                    "Mooncake get_into returned {read}"
                )));
            }
            let read = usize::try_from(read).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Mooncake read size overflow")
            })?;
            if read > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Mooncake get_into exceeded the destination buffer",
                ));
            }
            bytes.truncate(read);
            Ok(Some(bytes))
        }

        fn put(&self, key: &str, value: &[u8]) -> io::Result<()> {
            if value.is_empty() {
                return invalid("Mooncake put payload must not be empty");
            }
            let key = cstring(key, "key")?;
            let guard = self.lock()?;
            let store = guard.handle as Store;
            if self.exists(store, &key)? {
                check_zero(
                    unsafe { (self.api.remove)(store, key.as_ptr(), i32::from(self.force_remove)) },
                    "remove-before-put",
                )?;
            }
            check_zero(
                unsafe {
                    (self.api.put)(
                        store,
                        key.as_ptr(),
                        value.as_ptr().cast::<c_void>(),
                        value.len(),
                        std::ptr::null(),
                    )
                },
                "put",
            )
        }

        fn remove(&self, key: &str) -> io::Result<()> {
            let key = cstring(key, "key")?;
            let guard = self.lock()?;
            let store = guard.handle as Store;
            if !self.exists(store, &key)? {
                return Ok(());
            }
            check_zero(
                unsafe { (self.api.remove)(store, key.as_ptr(), i32::from(self.force_remove)) },
                "remove",
            )
        }

        fn clear_prefix(&self, prefix: &str) -> io::Result<()> {
            let pattern = cstring(&format!("^{}", regex_escape(prefix)), "prefix regex")?;
            let guard = self.lock()?;
            let removed = unsafe {
                (self.api.remove_regex)(
                    guard.handle as Store,
                    pattern.as_ptr(),
                    i32::from(self.force_remove),
                )
            };
            if removed < 0 {
                Err(io::Error::other(format!(
                    "Mooncake remove_by_regex returned {removed}"
                )))
            } else {
                Ok(())
            }
        }

        fn health(&self) -> io::Result<()> {
            let guard = self.lock()?;
            check_zero(
                unsafe { (self.api.health)(guard.handle as Store) },
                "health_check",
            )
        }
    }

    impl Drop for MooncakeSdk {
        fn drop(&mut self) {
            let handle = match self.store.get_mut() {
                Ok(store) => store.handle,
                Err(poisoned) => poisoned.into_inner().handle,
            };
            unsafe {
                (self.api.destroy)(handle as Store);
                dlclose(self.library as *mut c_void);
            }
        }
    }

    impl Api {
        unsafe fn load(library: *mut c_void) -> io::Result<Self> {
            Ok(Self {
                create: symbol(library, b"mooncake_store_create\0")?,
                destroy: symbol(library, b"mooncake_store_destroy\0")?,
                setup: symbol(library, b"mooncake_store_setup\0")?,
                init_all: symbol(library, b"mooncake_store_init_all\0")?,
                health: symbol(library, b"mooncake_store_health_check\0")?,
                put: symbol(library, b"mooncake_store_put\0")?,
                get_into: symbol(library, b"mooncake_store_get_into\0")?,
                exist: symbol(library, b"mooncake_store_is_exist\0")?,
                get_size: symbol(library, b"mooncake_store_get_size\0")?,
                remove: symbol(library, b"mooncake_store_remove\0")?,
                remove_regex: symbol(library, b"mooncake_store_remove_by_regex\0")?,
            })
        }
    }

    unsafe fn symbol<T: Copy>(library: *mut c_void, name: &'static [u8]) -> io::Result<T> {
        let pointer = dlsym(library, name.as_ptr().cast::<c_char>());
        if pointer.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Mooncake symbol {} not found: {}",
                    String::from_utf8_lossy(&name[..name.len().saturating_sub(1)]),
                    dl_error()
                ),
            ));
        }
        if std::mem::size_of::<T>() != std::mem::size_of::<*mut c_void>() {
            return Err(io::Error::other(
                "unexpected Mooncake function pointer size",
            ));
        }
        Ok(std::mem::transmute_copy::<*mut c_void, T>(&pointer))
    }

    fn initialize(store: Store, api: Api, init: &MooncakeInit) -> io::Result<()> {
        match init {
            MooncakeInit::Setup {
                local_hostname,
                metadata_server,
                global_segment_size,
                local_buffer_size,
                protocol,
                device_name,
                master_server_addr,
            } => {
                let hostname = cstring(local_hostname, "local hostname")?;
                let metadata = cstring(metadata_server, "metadata server")?;
                let protocol = cstring(protocol, "protocol")?;
                let device = cstring(device_name, "device name")?;
                let master = cstring(master_server_addr, "master server")?;
                check_zero(
                    unsafe {
                        (api.setup)(
                            store,
                            hostname.as_ptr(),
                            metadata.as_ptr(),
                            *global_segment_size,
                            *local_buffer_size,
                            protocol.as_ptr(),
                            device.as_ptr(),
                            master.as_ptr(),
                        )
                    },
                    "setup",
                )
            }
            MooncakeInit::InitAll {
                protocol,
                device_name,
                mount_segment_size,
            } => {
                let protocol = cstring(protocol, "protocol")?;
                let device = cstring(device_name, "device name")?;
                check_zero(
                    unsafe {
                        (api.init_all)(
                            store,
                            protocol.as_ptr(),
                            device.as_ptr(),
                            *mount_segment_size,
                        )
                    },
                    "init_all",
                )
            }
        }
    }

    fn cstring(value: &str, name: &str) -> io::Result<CString> {
        CString::new(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Mooncake {name} contains a NUL byte"),
            )
        })
    }

    fn check_zero(status: i32, operation: &str) -> io::Result<()> {
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "Mooncake {operation} returned status {status}"
            )))
        }
    }

    fn dl_error() -> String {
        let error = unsafe { dlerror() };
        if error.is_null() {
            "unknown dynamic loader error".to_owned()
        } else {
            unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned()
        }
    }

    fn regex_escape(value: &str) -> String {
        let mut out = String::with_capacity(value.len());
        for ch in value.chars() {
            if matches!(
                ch,
                '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\'
            ) {
                out.push('\\');
            }
            out.push(ch);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_invalid_namespace() {
        let config = MooncakeConfig {
            name: "mooncake".to_owned(),
            namespace: "bad namespace".to_owned(),
            library_path: None,
            init: MooncakeInit::InitAll {
                protocol: "tcp".to_owned(),
                device_name: String::new(),
                mount_segment_size: 0,
            },
            force_remove: false,
            max_value_bytes: 1024,
        };
        assert!(config.validate().is_err());
    }
}
