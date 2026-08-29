use crate::kv::{KvBlock, KvBlockKey, KvCaptureRequest, RuntimeKvAdapter};
use crate::reuse::RelocatableRuntimeKvAdapter;
use std::collections::HashMap;
use std::ffi::c_void;
use std::io;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LlamaKvSlice {
    pub token_start: u32,
    pub token_count: u32,
    pub layer_start: u32,
    pub layer_count: u32,
    pub layout_version: u32,
}

impl LlamaKvSlice {
    fn from_key(key: &KvBlockKey) -> Self {
        Self {
            token_start: key.token_start,
            token_count: key.token_count,
            layer_start: key.layer_start,
            layer_count: key.layer_count,
            layout_version: key.layout_version,
        }
    }
}

pub trait LlamaCppKvApi: Send + Sync {
    fn read_slice(&self, slice: LlamaKvSlice) -> io::Result<Vec<u8>>;
    fn write_slice(&self, slice: LlamaKvSlice, bytes: &[u8]) -> io::Result<()>;
    fn health(&self) -> io::Result<()> {
        Ok(())
    }
}

pub type LlamaReadKvFn = unsafe extern "C" fn(
    context: *mut c_void,
    token_start: u32,
    token_count: u32,
    layer_start: u32,
    layer_count: u32,
    layout_version: u32,
    destination: *mut u8,
    destination_capacity: usize,
    out_len: *mut usize,
) -> i32;

pub type LlamaWriteKvFn = unsafe extern "C" fn(
    context: *mut c_void,
    token_start: u32,
    token_count: u32,
    layer_start: u32,
    layer_count: u32,
    layout_version: u32,
    source: *const u8,
    source_len: usize,
) -> i32;

pub type LlamaHealthFn = unsafe extern "C" fn(context: *mut c_void) -> i32;

#[derive(Clone, Copy)]
pub struct LlamaCppFfiOps {
    pub context: *mut c_void,
    pub read: LlamaReadKvFn,
    pub write: LlamaWriteKvFn,
    pub health: Option<LlamaHealthFn>,
}

pub struct FfiLlamaCppKvApi {
    ops: LlamaCppFfiOps,
    max_block_bytes: usize,
}

impl FfiLlamaCppKvApi {
    /// # Safety
    ///
    /// The supplied context and callbacks must remain valid for this object's
    /// lifetime, be safe to call from multiple threads, and implement the
    /// documented two-phase read contract: a null destination with capacity
    /// zero reports the required byte count through `out_len`.
    pub unsafe fn new(ops: LlamaCppFfiOps, max_block_bytes: usize) -> io::Result<Self> {
        if max_block_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "llama.cpp maximum block size must be greater than zero",
            ));
        }
        Ok(Self {
            ops,
            max_block_bytes,
        })
    }

    fn check(operation: &str, code: i32) -> io::Result<()> {
        if code == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "llama.cpp callback {operation} failed with status {code}"
            )))
        }
    }
}

// SAFETY: the constructor requires the host to guarantee callback/context
// thread safety and lifetime. The wrapper does not dereference the context.
unsafe impl Send for FfiLlamaCppKvApi {}
unsafe impl Sync for FfiLlamaCppKvApi {}

impl LlamaCppKvApi for FfiLlamaCppKvApi {
    fn read_slice(&self, slice: LlamaKvSlice) -> io::Result<Vec<u8>> {
        let mut required = 0_usize;
        let first = unsafe {
            (self.ops.read)(
                self.ops.context,
                slice.token_start,
                slice.token_count,
                slice.layer_start,
                slice.layer_count,
                slice.layout_version,
                std::ptr::null_mut(),
                0,
                &mut required,
            )
        };
        Self::check("read-size", first)?;
        if required == 0 || required > self.max_block_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "llama.cpp callback reported an invalid KV block length",
            ));
        }

        let mut bytes = vec![0_u8; required];
        let mut written = required;
        let second = unsafe {
            (self.ops.read)(
                self.ops.context,
                slice.token_start,
                slice.token_count,
                slice.layer_start,
                slice.layer_count,
                slice.layout_version,
                bytes.as_mut_ptr(),
                bytes.len(),
                &mut written,
            )
        };
        Self::check("read", second)?;
        if written == 0 || written > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "llama.cpp callback wrote an invalid KV block length",
            ));
        }
        bytes.truncate(written);
        Ok(bytes)
    }

    fn write_slice(&self, slice: LlamaKvSlice, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > self.max_block_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "llama.cpp KV restore payload exceeds configured limits",
            ));
        }
        let code = unsafe {
            (self.ops.write)(
                self.ops.context,
                slice.token_start,
                slice.token_count,
                slice.layer_start,
                slice.layer_count,
                slice.layout_version,
                bytes.as_ptr(),
                bytes.len(),
            )
        };
        Self::check("write", code)
    }

    fn health(&self) -> io::Result<()> {
        match self.ops.health {
            Some(health) => {
                let code = unsafe { health(self.ops.context) };
                Self::check("health", code)
            }
            None => Ok(()),
        }
    }
}

pub struct LlamaCppAdapter {
    name: String,
    api: Arc<dyn LlamaCppKvApi>,
}

impl LlamaCppAdapter {
    pub fn new(api: Arc<dyn LlamaCppKvApi>) -> Self {
        Self {
            name: "llama.cpp-callback".to_string(),
            api,
        }
    }

    pub fn named(name: impl Into<String>, api: Arc<dyn LlamaCppKvApi>) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime adapter name must not be empty",
            ));
        }
        Ok(Self { name, api })
    }

    pub fn health(&self) -> io::Result<()> {
        self.api.health()
    }
}

impl RuntimeKvAdapter for LlamaCppAdapter {
    fn runtime_name(&self) -> &str {
        &self.name
    }

    fn capture(&self, request: &KvCaptureRequest) -> io::Result<Vec<KvBlock>> {
        request
            .block_keys()?
            .into_iter()
            .map(|key| {
                let bytes = self.api.read_slice(LlamaKvSlice::from_key(&key))?;
                KvBlock::new(key, bytes)
            })
            .collect()
    }

    fn restore(&self, blocks: &[KvBlock]) -> io::Result<()> {
        validate_restore_blocks(blocks)?;
        for block in blocks {
            self.api
                .write_slice(LlamaKvSlice::from_key(&block.key), &block.bytes)?;
        }
        Ok(())
    }
}

impl RelocatableRuntimeKvAdapter for LlamaCppAdapter {
    fn restore_relocated(&self, blocks: &[KvBlock], target_token_start: u32) -> io::Result<()> {
        validate_restore_blocks(blocks)?;
        let source_start = blocks
            .first()
            .map(|block| block.key.token_start)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "no KV blocks to restore")
            })?;
        for block in blocks {
            let relative = block
                .key
                .token_start
                .checked_sub(source_start)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "KV blocks are not token ordered",
                    )
                })?;
            let token_start = target_token_start.checked_add(relative).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "relocated token range overflow",
                )
            })?;
            let slice = LlamaKvSlice {
                token_start,
                token_count: block.key.token_count,
                layer_start: block.key.layer_start,
                layer_count: block.key.layer_count,
                layout_version: block.key.layout_version,
            };
            self.api.write_slice(slice, &block.bytes)?;
        }
        Ok(())
    }
}

fn validate_restore_blocks(blocks: &[KvBlock]) -> io::Result<()> {
    let first = blocks
        .first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no KV blocks to restore"))?;
    let mut expected_start = first.key.token_start;
    for block in blocks {
        if block.bytes.is_empty()
            || block.key.model_fingerprint != first.key.model_fingerprint
            || block.key.layer_start != first.key.layer_start
            || block.key.layer_count != first.key.layer_count
            || block.key.layout_version != first.key.layout_version
            || block.key.token_start != expected_start
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime restore requires contiguous blocks with shared model/layer/layout identity",
            ));
        }
        expected_start = expected_start
            .checked_add(block.key.token_count)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "token range overflow"))?;
    }
    Ok(())
}

#[derive(Default)]
pub struct MockLlamaCppKvApi {
    reads: Mutex<HashMap<LlamaKvSlice, Vec<u8>>>,
    writes: Mutex<HashMap<LlamaKvSlice, Vec<u8>>>,
}

impl MockLlamaCppKvApi {
    pub fn insert_capture(&self, slice: LlamaKvSlice, bytes: Vec<u8>) -> io::Result<()> {
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mock capture payload must not be empty",
            ));
        }
        self.reads
            .lock()
            .map_err(|_| io::Error::other("mock llama.cpp read map lock poisoned"))?
            .insert(slice, bytes);
        Ok(())
    }

    pub fn restored(&self, slice: LlamaKvSlice) -> io::Result<Option<Vec<u8>>> {
        Ok(self
            .writes
            .lock()
            .map_err(|_| io::Error::other("mock llama.cpp write map lock poisoned"))?
            .get(&slice)
            .cloned())
    }
}

impl LlamaCppKvApi for MockLlamaCppKvApi {
    fn read_slice(&self, slice: LlamaKvSlice) -> io::Result<Vec<u8>> {
        self.reads
            .lock()
            .map_err(|_| io::Error::other("mock llama.cpp read map lock poisoned"))?
            .get(&slice)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "mock KV slice not found"))
    }

    fn write_slice(&self, slice: LlamaKvSlice, bytes: &[u8]) -> io::Result<()> {
        self.writes
            .lock()
            .map_err(|_| io::Error::other("mock llama.cpp write map lock poisoned"))?
            .insert(slice, bytes.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reuse::RelocatableRuntimeKvAdapter;

    #[test]
    fn callback_adapter_captures_and_restores_contiguous_blocks() {
        let api = Arc::new(MockLlamaCppKvApi::default());
        let request = KvCaptureRequest {
            model_fingerprint: "m".to_string(),
            tokens: vec![1, 2, 3, 4],
            block_tokens: 2,
            layer_start: 0,
            layer_count: 8,
            layout_version: 1,
        };
        for (index, key) in request.block_keys().expect("keys").iter().enumerate() {
            api.insert_capture(LlamaKvSlice::from_key(key), vec![index as u8 + 1, 9])
                .expect("capture payload");
        }
        let adapter = LlamaCppAdapter::new(api.clone());
        let blocks = adapter.capture(&request).expect("capture");
        assert_eq!(blocks.len(), 2);
        adapter.restore(&blocks).expect("restore");
        assert_eq!(
            api.restored(LlamaKvSlice::from_key(&blocks[0].key))
                .expect("read restored"),
            Some(vec![1, 9])
        );

        adapter
            .restore_relocated(&blocks, 10)
            .expect("relocated restore");
        let relocated = LlamaKvSlice {
            token_start: 12,
            token_count: 2,
            layer_start: 0,
            layer_count: 8,
            layout_version: 1,
        };
        assert_eq!(
            api.restored(relocated).expect("relocated"),
            Some(vec![2, 9])
        );
    }
}
