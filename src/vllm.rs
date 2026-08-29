use crate::{
    KvBlock, KvBlockKey, KvCaptureRequest, QualityAwareRuntimeKvAdapter,
    RelocatableRuntimeKvAdapter, RuntimeKvAdapter, TokenRange,
};
use std::ffi::c_void;
use std::io;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VllmKvSlice {
    pub token_start: u32,
    pub token_count: u32,
    pub layer_start: u32,
    pub layer_count: u32,
    pub layout_version: u32,
}

impl VllmKvSlice {
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

pub trait VllmKvApi: Send + Sync {
    fn read_slice(&self, slice: VllmKvSlice) -> io::Result<Vec<u8>>;
    fn write_slice(&self, slice: VllmKvSlice, bytes: &[u8]) -> io::Result<()>;
    fn recompute_ranges(&self, query_tokens: &[u32], ranges: &[TokenRange]) -> io::Result<()>;
    fn health(&self) -> io::Result<()> {
        Ok(())
    }
}

pub type VllmReadKvFn = unsafe extern "C" fn(
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

pub type VllmWriteKvFn = unsafe extern "C" fn(
    context: *mut c_void,
    token_start: u32,
    token_count: u32,
    layer_start: u32,
    layer_count: u32,
    layout_version: u32,
    source: *const u8,
    source_len: usize,
) -> i32;

pub type VllmRecomputeFn = unsafe extern "C" fn(
    context: *mut c_void,
    query_tokens: *const u32,
    query_len: usize,
    range_starts: *const usize,
    range_lens: *const usize,
    range_count: usize,
) -> i32;

pub type VllmHealthFn = unsafe extern "C" fn(context: *mut c_void) -> i32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct VllmFfiOps {
    pub context: *mut c_void,
    pub read: VllmReadKvFn,
    pub write: VllmWriteKvFn,
    pub recompute: VllmRecomputeFn,
    pub health: Option<VllmHealthFn>,
}

pub struct FfiVllmKvApi {
    ops: VllmFfiOps,
    max_block_bytes: usize,
    max_recompute_ranges: usize,
}

// SAFETY: construction requires the embedding host to provide callbacks/context that remain
// valid and are safe for concurrent invocation for the lifetime of this wrapper.
unsafe impl Send for FfiVllmKvApi {}
unsafe impl Sync for FfiVllmKvApi {}

impl FfiVllmKvApi {
    /// Create a vLLM host callback bridge.
    ///
    /// # Safety
    ///
    /// This is a RivetCache integration ABI, not an upstream/private vLLM ABI. The caller must
    /// keep `ops.context` and all callbacks alive, make them thread-safe, and implement the
    /// two-phase read contract: null destination + zero capacity reports the required byte count.
    pub unsafe fn new(
        ops: VllmFfiOps,
        max_block_bytes: usize,
        max_recompute_ranges: usize,
    ) -> io::Result<Self> {
        if max_block_bytes == 0 || max_recompute_ranges == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "vLLM callback limits must be greater than zero",
            ));
        }
        Ok(Self {
            ops,
            max_block_bytes,
            max_recompute_ranges,
        })
    }

    fn check(operation: &str, status: i32) -> io::Result<()> {
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "vLLM host callback {operation} returned status {status}"
            )))
        }
    }
}

impl VllmKvApi for FfiVllmKvApi {
    fn read_slice(&self, slice: VllmKvSlice) -> io::Result<Vec<u8>> {
        let mut required = 0_usize;
        let status = unsafe {
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
        Self::check("read-size", status)?;
        if required == 0 || required > self.max_block_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "vLLM host reported an invalid KV block length",
            ));
        }

        let mut bytes = vec![0_u8; required];
        let mut written = required;
        let status = unsafe {
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
        Self::check("read", status)?;
        if written == 0 || written > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "vLLM host wrote an invalid KV block length",
            ));
        }
        bytes.truncate(written);
        Ok(bytes)
    }

    fn write_slice(&self, slice: VllmKvSlice, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > self.max_block_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "vLLM restore payload exceeds configured limits",
            ));
        }
        Self::check("write", unsafe {
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
        })
    }

    fn recompute_ranges(&self, query_tokens: &[u32], ranges: &[TokenRange]) -> io::Result<()> {
        if query_tokens.is_empty() || ranges.is_empty() || ranges.len() > self.max_recompute_ranges
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "vLLM recomputation requires bounded non-empty tokens and ranges",
            ));
        }
        for range in ranges {
            if range.len == 0 || range.end() > query_tokens.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "vLLM recomputation range is outside the query",
                ));
            }
        }
        let starts = ranges.iter().map(|range| range.start).collect::<Vec<_>>();
        let lens = ranges.iter().map(|range| range.len).collect::<Vec<_>>();
        Self::check("recompute", unsafe {
            (self.ops.recompute)(
                self.ops.context,
                query_tokens.as_ptr(),
                query_tokens.len(),
                starts.as_ptr(),
                lens.as_ptr(),
                ranges.len(),
            )
        })
    }

    fn health(&self) -> io::Result<()> {
        match self.ops.health {
            Some(health) => Self::check("health", unsafe { health(self.ops.context) }),
            None => Ok(()),
        }
    }
}

pub struct VllmAdapter {
    name: String,
    api: Arc<dyn VllmKvApi>,
}

impl VllmAdapter {
    pub fn new(api: Arc<dyn VllmKvApi>) -> Self {
        Self {
            name: "vllm-host-callback".to_owned(),
            api,
        }
    }

    pub fn named(name: impl Into<String>, api: Arc<dyn VllmKvApi>) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() || name.len() > 128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "vLLM adapter name must be 1-128 characters",
            ));
        }
        Ok(Self { name, api })
    }

    pub fn health(&self) -> io::Result<()> {
        self.api.health()
    }
}

impl RuntimeKvAdapter for VllmAdapter {
    fn runtime_name(&self) -> &str {
        &self.name
    }

    fn capture(&self, request: &KvCaptureRequest) -> io::Result<Vec<KvBlock>> {
        request
            .block_keys()?
            .into_iter()
            .map(|key| {
                let bytes = self.api.read_slice(VllmKvSlice::from_key(&key))?;
                KvBlock::new(key, bytes)
            })
            .collect()
    }

    fn restore(&self, blocks: &[KvBlock]) -> io::Result<()> {
        validate_restore_blocks(blocks)?;
        for block in blocks {
            self.api
                .write_slice(VllmKvSlice::from_key(&block.key), &block.bytes)?;
        }
        Ok(())
    }
}

impl RelocatableRuntimeKvAdapter for VllmAdapter {
    fn restore_relocated(&self, blocks: &[KvBlock], target_token_start: u32) -> io::Result<()> {
        validate_restore_blocks(blocks)?;
        let source_start = blocks[0].key.token_start;
        for block in blocks {
            let relative = block
                .key
                .token_start
                .checked_sub(source_start)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "unordered KV blocks")
                })?;
            let token_start = target_token_start.checked_add(relative).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "relocated token range overflow",
                )
            })?;
            self.api.write_slice(
                VllmKvSlice {
                    token_start,
                    token_count: block.key.token_count,
                    layer_start: block.key.layer_start,
                    layer_count: block.key.layer_count,
                    layout_version: block.key.layout_version,
                },
                &block.bytes,
            )?;
        }
        Ok(())
    }
}

impl QualityAwareRuntimeKvAdapter for VllmAdapter {
    fn recompute_ranges(&self, query_tokens: &[u32], ranges: &[TokenRange]) -> io::Result<()> {
        self.api.recompute_ranges(query_tokens, ranges)
    }
}

fn validate_restore_blocks(blocks: &[KvBlock]) -> io::Result<()> {
    let first = blocks
        .first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no vLLM KV blocks"))?;
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
                "vLLM restore requires contiguous blocks with shared model/layer/layout identity",
            ));
        }
        expected_start = expected_start
            .checked_add(block.key.token_count)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "vLLM token range overflow")
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvBlockRange;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockApi {
        reads: Mutex<HashMap<VllmKvSlice, Vec<u8>>>,
        writes: Mutex<HashMap<VllmKvSlice, Vec<u8>>>,
        recomputes: Mutex<Vec<TokenRange>>,
    }

    impl VllmKvApi for MockApi {
        fn read_slice(&self, slice: VllmKvSlice) -> io::Result<Vec<u8>> {
            self.reads
                .lock()
                .unwrap()
                .get(&slice)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing mock slice"))
        }
        fn write_slice(&self, slice: VllmKvSlice, bytes: &[u8]) -> io::Result<()> {
            self.writes.lock().unwrap().insert(slice, bytes.to_vec());
            Ok(())
        }
        fn recompute_ranges(&self, _query_tokens: &[u32], ranges: &[TokenRange]) -> io::Result<()> {
            self.recomputes.lock().unwrap().extend_from_slice(ranges);
            Ok(())
        }
    }

    fn key(start: u32) -> KvBlockKey {
        KvBlockKey::from_prefix(
            "model",
            &[1, 2],
            KvBlockRange {
                block_index: start / 2,
                token_start: start,
                token_count: 2,
                layer_start: 0,
                layer_count: 8,
                layout_version: 1,
            },
        )
    }

    #[test]
    fn adapter_restores_relocated_blocks_and_recomputes() {
        let api = Arc::new(MockApi::default());
        let adapter = VllmAdapter::new(api.clone());
        let blocks = vec![
            KvBlock::new(key(0), vec![1; 16]).unwrap(),
            KvBlock::new(key(2), vec![2; 16]).unwrap(),
        ];
        adapter.restore_relocated(&blocks, 10).unwrap();
        assert!(api
            .writes
            .lock()
            .unwrap()
            .keys()
            .any(|slice| slice.token_start == 10));
        adapter
            .recompute_ranges(&[1, 2, 3, 4], &[TokenRange { start: 1, len: 2 }])
            .unwrap();
        assert_eq!(api.recomputes.lock().unwrap().len(), 1);
    }
}
