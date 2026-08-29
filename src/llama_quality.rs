use crate::{
    KvBlock, KvCaptureRequest, LlamaCppAdapter, QualityAwareRuntimeKvAdapter,
    RelocatableRuntimeKvAdapter, RuntimeKvAdapter, TokenRange,
};
use std::ffi::c_void;
use std::io;
use std::sync::Arc;

/// Host callback used to recompute token ranges after arbitrary-position KV reuse.
/// The callback receives the complete query token array plus one validated range.
pub type LlamaRecomputeFn = unsafe extern "C" fn(
    context: *mut c_void,
    query_tokens: *const u32,
    query_token_count: usize,
    range_start: usize,
    range_len: usize,
) -> i32;

pub trait LlamaCppRecomputeApi: Send + Sync {
    fn recompute_range(&self, query_tokens: &[u32], range: TokenRange) -> io::Result<()>;
}

pub struct FfiLlamaCppRecomputeApi {
    context: *mut c_void,
    recompute: LlamaRecomputeFn,
}

// SAFETY: construction requires the host context and callback to remain valid and
// thread-safe for the wrapper lifetime. The wrapper never dereferences context.
unsafe impl Send for FfiLlamaCppRecomputeApi {}
unsafe impl Sync for FfiLlamaCppRecomputeApi {}

impl FfiLlamaCppRecomputeApi {
    /// # Safety
    ///
    /// `context` and `recompute` must remain valid for this object's lifetime and
    /// the callback must be safe to call concurrently. A zero return value means success.
    pub unsafe fn new(context: *mut c_void, recompute: LlamaRecomputeFn) -> Self {
        Self { context, recompute }
    }
}

impl LlamaCppRecomputeApi for FfiLlamaCppRecomputeApi {
    fn recompute_range(&self, query_tokens: &[u32], range: TokenRange) -> io::Result<()> {
        validate_range(query_tokens, range)?;
        // SAFETY: callback/context lifetime and thread safety are guaranteed by the
        // unsafe constructor. query_tokens is live for the complete synchronous call.
        let status = unsafe {
            (self.recompute)(
                self.context,
                query_tokens.as_ptr(),
                query_tokens.len(),
                range.start,
                range.len,
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "llama.cpp recompute callback failed with status {status}"
            )))
        }
    }
}

/// Quality-aware llama.cpp host adapter. Exact KV restoration is delegated to the
/// existing llama.cpp adapter, then boundary ranges are synchronously recomputed by
/// an injected host runtime before generation resumes.
pub struct LlamaCppQualityAdapter {
    base: LlamaCppAdapter,
    recompute: Arc<dyn LlamaCppRecomputeApi>,
}

impl LlamaCppQualityAdapter {
    pub fn new(base: LlamaCppAdapter, recompute: Arc<dyn LlamaCppRecomputeApi>) -> Self {
        Self { base, recompute }
    }

    pub fn health(&self) -> io::Result<()> {
        self.base.health()
    }
}

impl RuntimeKvAdapter for LlamaCppQualityAdapter {
    fn runtime_name(&self) -> &str {
        self.base.runtime_name()
    }

    fn capture(&self, request: &KvCaptureRequest) -> io::Result<Vec<KvBlock>> {
        self.base.capture(request)
    }

    fn restore(&self, blocks: &[KvBlock]) -> io::Result<()> {
        self.base.restore(blocks)
    }
}

impl RelocatableRuntimeKvAdapter for LlamaCppQualityAdapter {
    fn restore_relocated(&self, blocks: &[KvBlock], target_token_start: u32) -> io::Result<()> {
        self.base.restore_relocated(blocks, target_token_start)
    }
}

impl QualityAwareRuntimeKvAdapter for LlamaCppQualityAdapter {
    fn recompute_ranges(&self, query_tokens: &[u32], ranges: &[TokenRange]) -> io::Result<()> {
        for range in ranges.iter().copied() {
            validate_range(query_tokens, range)?;
        }
        for range in ranges.iter().copied() {
            self.recompute.recompute_range(query_tokens, range)?;
        }
        Ok(())
    }
}

fn validate_range(query_tokens: &[u32], range: TokenRange) -> io::Result<()> {
    if range.len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "llama.cpp recompute range must not be empty",
        ));
    }
    let end = range.start.checked_add(range.len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "llama.cpp recompute range overflow",
        )
    })?;
    if end > query_tokens.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "llama.cpp recompute range exceeds query tokens",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LlamaCppKvApi, MockLlamaCppKvApi};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Context {
        calls: AtomicU64,
        tokens: AtomicU64,
    }

    unsafe extern "C" fn recompute(
        context: *mut c_void,
        query_tokens: *const u32,
        query_token_count: usize,
        range_start: usize,
        range_len: usize,
    ) -> i32 {
        if context.is_null()
            || query_tokens.is_null()
            || query_token_count != 6
            || range_start != 2
            || range_len != 2
        {
            return -1;
        }
        // SAFETY: test passes a valid Context and six live query tokens.
        let context = unsafe { &*(context.cast::<Context>()) };
        let tokens = unsafe { std::slice::from_raw_parts(query_tokens, query_token_count) };
        context.calls.fetch_add(1, Ordering::Relaxed);
        context.tokens.fetch_add(
            (tokens[range_start] + tokens[range_start + 1]) as u64,
            Ordering::Relaxed,
        );
        0
    }

    #[test]
    fn quality_adapter_executes_runtime_recompute_callback() {
        let mut context = Box::new(Context {
            calls: AtomicU64::new(0),
            tokens: AtomicU64::new(0),
        });
        let recompute_api: Arc<dyn LlamaCppRecomputeApi> = Arc::new(unsafe {
            FfiLlamaCppRecomputeApi::new(
                (&mut *context as *mut Context).cast::<c_void>(),
                recompute,
            )
        });
        let kv_api: Arc<dyn LlamaCppKvApi> = Arc::new(MockLlamaCppKvApi::default());
        let adapter = LlamaCppQualityAdapter::new(LlamaCppAdapter::new(kv_api), recompute_api);
        adapter
            .recompute_ranges(
                &[10, 11, 12, 13, 14, 15],
                &[TokenRange { start: 2, len: 2 }],
            )
            .unwrap();
        assert_eq!(context.calls.load(Ordering::Relaxed), 1);
        assert_eq!(context.tokens.load(Ordering::Relaxed), 25);
    }

    #[test]
    fn invalid_recompute_range_fails_before_host_callback() {
        let mut context = Box::new(Context {
            calls: AtomicU64::new(0),
            tokens: AtomicU64::new(0),
        });
        let recompute_api: Arc<dyn LlamaCppRecomputeApi> = Arc::new(unsafe {
            FfiLlamaCppRecomputeApi::new(
                (&mut *context as *mut Context).cast::<c_void>(),
                recompute,
            )
        });
        let kv_api: Arc<dyn LlamaCppKvApi> = Arc::new(MockLlamaCppKvApi::default());
        let adapter = LlamaCppQualityAdapter::new(LlamaCppAdapter::new(kv_api), recompute_api);
        assert!(adapter
            .recompute_ranges(&[1, 2], &[TokenRange { start: 1, len: 2 }])
            .is_err());
        assert_eq!(context.calls.load(Ordering::Relaxed), 0);
    }
}
