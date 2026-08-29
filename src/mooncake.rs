use crate::{KvBlock, KvBlockKey, KvTier, KvTierEntry};
use sha2::{Digest, Sha256};
use std::ffi::CString;
use std::io;
use std::sync::Arc;

const FRAME_MAGIC: &[u8; 8] = b"RIVEMC1\n";

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
        validate_identifier(&self.name, "tier name")?;
        validate_identifier(&self.namespace, "namespace")?;
        if self.max_value_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Mooncake max value bytes must be greater than zero",
            ));
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
                    if value.trim().is_empty() || value.as_bytes().contains(&0) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Mooncake {name} is invalid"),
                        ));
                    }
                }
            }
            MooncakeInit::InitAll { protocol, .. } => {
                if protocol.trim().is_empty() || protocol.as_bytes().contains(&0) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Mooncake protocol is invalid",
                    ));
                }
            }
        }
        Ok(())
    }
}

pub struct MooncakeKvTier {
    name: String,
    namespace: String,
    force_remove: bool,
    max_value_bytes: usize,
    #[cfg(target_os = "linux")]
    client: Arc<native::MooncakeClient>,
}

impl MooncakeKvTier {
    pub fn connect(config: MooncakeConfig) -> io::Result<Self> {
        config.validate()?;
        #[cfg(target_os = "linux")]
        {
            let client = native::MooncakeClient::connect(&config)?;
            return Ok(Self {
                name: config.name,
                namespace: config.namespace,
                force_remove: config.force_remove,
                max_value_bytes: config.max_value_bytes,
                client: Arc::new(client),
            });
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = config;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Mooncake Store native tier is currently supported on Linux hosts",
            ))
        }
    }

    fn object_key(&self, key: &KvBlockKey) -> String {
        format!("rivet:{}:{}", self.namespace, key.cache_key())
    }

    fn namespace_regex(&self) -> String {
        format!("^rivet:{}:", self.namespace)
    }
}

impl KvTier for MooncakeKvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        #[cfg(target_os = "linux")]
        {
            let object_key = self.object_key(key);
            let Some(raw) = self.client.get(&object_key, self.max_value_bytes)? else {
                return Ok(None);
            };
            decode_entry(key.clone(), &raw, self.max_value_bytes).map(Some)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = key;
            unsupported()
        }
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let encoded = encode_entry(entry)?;
            if encoded.len() > self.max_value_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Mooncake KV entry exceeds configured limit",
                ));
            }
            let object_key = self.object_key(&entry.block.key);
            self.client.upsert_copy(&object_key, &encoded, self.force_remove)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = entry;
            unsupported()
        }
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.client
                .remove(&self.object_key(key), self.force_remove)
                .map(|_| ())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = key;
            unsupported()
        }
    }

    fn clear(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.client
                .remove_regex(&self.namespace_regex(), self.force_remove)
                .map(|_| ())
        }
        #[cfg(not(target_os = "linux"))]
        {
            unsupported()
        }
    }

    fn health(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.client.health()
        }
        #[cfg(not(target_os = "linux"))]
        {
            unsupported()
        }
    }
}

fn encode_entry(entry: &KvTierEntry) -> io::Result<Vec<u8>> {
    if entry.block.bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Mooncake KV payload must not be empty",
        ));
    }
    let digest: [u8; 32] = Sha256::digest(&entry.block.bytes).into();
    let mut encoded = Vec::with_capacity(8 + 8 + 1 + 8 + 32 + entry.block.bytes.len());
    encoded.extend_from_slice(FRAME_MAGIC);
    encoded.extend_from_slice(&entry.expires_at.to_le_bytes());
    encoded.push(u8::from(entry.pinned));
    encoded.extend_from_slice(&(entry.block.bytes.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&digest);
    encoded.extend_from_slice(&entry.block.bytes);
    Ok(encoded)
}

fn decode_entry(key: KvBlockKey, bytes: &[u8], max_value_bytes: usize) -> io::Result<KvTierEntry> {
    let header = FRAME_MAGIC.len() + 8 + 1 + 8 + 32;
    if bytes.len() < header || bytes.len() > max_value_bytes || &bytes[..8] != FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Mooncake RivetCache frame",
        ));
    }
    let mut cursor = FRAME_MAGIC.len();
    let expires_at = read_u64(bytes, &mut cursor)?;
    let pinned = match bytes.get(cursor).copied() {
        Some(0) => false,
        Some(1) => true,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Mooncake pin flag",
            ))
        }
    };
    cursor += 1;
    let payload_len = usize::try_from(read_u64(bytes, &mut cursor)?).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "Mooncake payload length overflow")
    })?;
    if bytes.len().saturating_sub(cursor) < 32 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Mooncake checksum is truncated",
        ));
    }
    let expected = &bytes[cursor..cursor + 32];
    cursor += 32;
    if bytes.len().saturating_sub(cursor) != payload_len || payload_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Mooncake payload length mismatch",
        ));
    }
    let payload = bytes[cursor..].to_vec();
    let actual: [u8; 32] = Sha256::digest(&payload).into();
    if actual.as_slice() != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Mooncake payload checksum mismatch",
        ));
    }
    Ok(KvTierEntry {
        block: KvBlock::new(key, payload)?,
        expires_at,
        pinned,
    })
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor.saturating_add(8);
    let raw = bytes.get(*cursor..end).ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "Mooncake frame is truncated")
    })?;
    *cursor = end;
    let mut value = [0_u8; 8];
    value.copy_from_slice(raw);
    Ok(u64::from_le_bytes(value))
}

fn validate_identifier(value: &str, name: &str) -> io::Result<()> {
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

#[cfg(not(target_os = "linux"))]
fn unsupported<T>() -> io::Result<T> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Mooncake Store native tier is currently supported on Linux hosts",
    ))
}

#[cfg(target_os = "linux")]
mod native {
    use super::*;
    use std::ffi::{c_char, c_void};
    use std::sync::Mutex;

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
    type PutFn = unsafe extern "C" fn(Store, *const c_char, *const c_void, usize, *const c_void) -> i32;
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

    struct Inner {
        store: usize,
    }

    pub struct MooncakeClient {
        library: usize,
        api: Api,
        inner: Mutex<Inner>,
    }

    impl MooncakeClient {
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

            let init_result = init_store(store, api, &config.init);
            if let Err(error) = init_result {
                unsafe {
                    (api.destroy)(store);
                    dlclose(library);
                }
                return Err(error);
            }

            let client = Self {
                library: library as usize,
                api,
                inner: Mutex::new(Inner {
                    store: store as usize,
                }),
            };
            client.health()?;
            Ok(client)
        }

        pub fn health(&self) -> io::Result<()> {
            let inner = self.lock()?;
            check_zero(
                unsafe { (self.api.health)(inner.store as Store) },
                "health_check",
            )
        }

        pub fn get(&self, key: &str, max: usize) -> io::Result<Option<Vec<u8>>> {
            let key = cstring(key, "key")?;
            let inner = self.lock()?;
            let exists = unsafe { (self.api.exist)(inner.store as Store, key.as_ptr()) };
            if exists == 0 {
                return Ok(None);
            }
            if exists < 0 {
                return Err(io::Error::other(format!(
                    "Mooncake is_exist failed with status {exists}"
                )));
            }
            let size = unsafe { (self.api.get_size)(inner.store as Store, key.as_ptr()) };
            if size <= 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Mooncake get_size returned {size}"),
                ));
            }
            let size = usize::try_from(size).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Mooncake object size overflow")
            })?;
            if size > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Mooncake object exceeds configured value limit",
                ));
            }
            let mut bytes = vec![0_u8; size];
            let read = unsafe {
                (self.api.get_into)(
                    inner.store as Store,
                    key.as_ptr(),
                    bytes.as_mut_ptr().cast::<c_void>(),
                    bytes.len(),
                )
            };
            if read < 0 {
                return Err(io::Error::other(format!(
                    "Mooncake get_into failed with status {read}"
                )));
            }
            let read = usize::try_from(read).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Mooncake read size overflow")
            })?;
            if read == 0 || read > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Mooncake get_into returned an invalid byte count",
                ));
            }
            bytes.truncate(read);
            Ok(Some(bytes))
        }

        pub fn upsert_copy(&self, key: &str, value: &[u8], force_remove: bool) -> io::Result<()> {
            let key = cstring(key, "key")?;
            let inner = self.lock()?;
            let exists = unsafe { (self.api.exist)(inner.store as Store, key.as_ptr()) };
            if exists < 0 {
                return Err(io::Error::other(format!(
                    "Mooncake is_exist failed with status {exists}"
                )));
            }
            if exists == 1 {
                check_zero(
                    unsafe {
                        (self.api.remove)(
                            inner.store as Store,
                            key.as_ptr(),
                            i32::from(force_remove),
                        )
                    },
                    "remove-before-put",
                )?;
            }
            check_zero(
                unsafe {
                    (self.api.put)(
                        inner.store as Store,
                        key.as_ptr(),
                        value.as_ptr().cast::<c_void>(),
                        value.len(),
                        std::ptr::null(),
                    )
                },
                "put",
            )
        }

        pub fn remove(&self, key: &str, force: bool) -> io::Result<i32> {
            let key = cstring(key, "key")?;
            let inner = self.lock()?;
            let status = unsafe {
                (self.api.remove)(inner.store as Store, key.as_ptr(), i32::from(force))
            };
            if status == 0 {
                Ok(status)
            } else {
                let exists = unsafe { (self.api.exist)(inner.store as Store, key.as_ptr()) };
                if exists == 0 {
                    Ok(0)
                } else {
                    Err(io::Error::other(format!(
                        "Mooncake remove failed with status {status}"
                    )))
                }
            }
        }

        pub fn remove_regex(&self, regex: &str, force: bool) -> io::Result<i64> {
            let regex = cstring(regex, "regex")?;
            let inner = self.lock()?;
            let removed = unsafe {
                (self.api.remove_regex)(inner.store as Store, regex.as_ptr(), i32::from(force))
            };
            if removed < 0 {
                Err(io::Error::other(format!(
                    "Mooncake remove_by_regex failed with status {removed}"
                )))
            } else {
                Ok(removed)
            }
        }

        fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, Inner>> {
            self.inner
                .lock()
                .map_err(|_| io::Error::other("Mooncake client lock poisoned"))
        }
    }

    impl Drop for MooncakeClient {
        fn drop(&mut self) {
            let store = match self.inner.get_mut() {
                Ok(inner) => inner.store,
                Err(poisoned) => poisoned.into_inner().store,
            };
            unsafe {
                (self.api.destroy)(store as Store);
                dlclose(self.library as *mut c_void);
            }
        }
    }

    impl Api {
        unsafe fn load(library: *mut c_void) -> io::Result<Self> {
            macro_rules! symbol {
                ($name:literal, $ty:ty) => {{
                    let ptr = dlsym(library, concat!($name, "\0").as_ptr().cast::<c_char>());
                    if ptr.is_null() {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("Mooncake symbol {} not found: {}", $name, dl_error()),
                        ));
                    }
                    std::mem::transmute::<*mut c_void, $ty>(ptr)
                }};
            }
            Ok(Self {
                create: symbol!("mooncake_store_create", CreateFn),
                destroy: symbol!("mooncake_store_destroy", DestroyFn),
                setup: symbol!("mooncake_store_setup", SetupFn),
                init_all: symbol!("mooncake_store_init_all", InitAllFn),
                health: symbol!("mooncake_store_health_check", HealthFn),
                put: symbol!("mooncake_store_put", PutFn),
                get_into: symbol!("mooncake_store_get_into", GetIntoFn),
                exist: symbol!("mooncake_store_is_exist", ExistFn),
                get_size: symbol!("mooncake_store_get_size", GetSizeFn),
                remove: symbol!("mooncake_store_remove", RemoveFn),
                remove_regex: symbol!("mooncake_store_remove_by_regex", RemoveRegexFn),
            })
        }
    }

    fn init_store(store: Store, api: Api, init: &MooncakeInit) -> io::Result<()> {
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
                let local_hostname = cstring(local_hostname, "local hostname")?;
                let metadata_server = cstring(metadata_server, "metadata server")?;
                let protocol = cstring(protocol, "protocol")?;
                let device_name = cstring(device_name, "device name")?;
                let master_server_addr = cstring(master_server_addr, "master server")?;
                check_zero(
                    unsafe {
                        (api.setup)(
                            store,
                            local_hostname.as_ptr(),
                            metadata_server.as_ptr(),
                            *global_segment_size,
                            *local_buffer_size,
                            protocol.as_ptr(),
                            device_name.as_ptr(),
                            master_server_addr.as_ptr(),
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
                let device_name = cstring(device_name, "device name")?;
                check_zero(
                    unsafe {
                        (api.init_all)(store, protocol.as_ptr(), device_name.as_ptr(), *mount_segment_size)
                    },
                    "init_all",
                )
            }
        }
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

    fn cstring(value: &str, name: &str) -> io::Result<CString> {
        CString::new(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Mooncake {name} contains a NUL byte"),
            )
        })
    }

    fn dl_error() -> String {
        let error = unsafe { dlerror() };
        if error.is_null() {
            "unknown dynamic loader error".to_owned()
        } else {
            unsafe { std::ffi::CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvBlockRange;

    fn key() -> KvBlockKey {
        KvBlockKey::from_prefix(
            "model",
            &[1, 2, 3],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 3,
                layer_start: 0,
                layer_count: 8,
                layout_version: 1,
            },
        )
    }

    #[test]
    fn frame_round_trip_preserves_cache_metadata() {
        let entry = KvTierEntry {
            block: KvBlock::new(key(), vec![7; 128]).unwrap(),
            expires_at: 123,
            pinned: true,
        };
        let encoded = encode_entry(&entry).unwrap();
        let decoded = decode_entry(entry.block.key.clone(), &encoded, 1024).unwrap();
        assert_eq!(decoded, entry);
    }

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