use crate::{
    AsyncKvJobSnapshot, AsyncKvJobState, AsyncKvPipeline, KvBlockKey, KvCaptureRequest, KvEngine,
    KvTierHealth, MpCacheConfig, MpCacheService, MpRequestStatus,
};
use std::io;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLookupResult {
    pub request_id: u64,
    pub found_chunks: u64,
    pub total_chunks: u64,
    pub missed_chunks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthResult {
    pub healthy: bool,
    pub tiers: Vec<KvTierHealth>,
}

#[derive(Clone)]
pub struct RuntimeCacheController {
    engine: KvEngine,
    pipeline: AsyncKvPipeline,
    mp: MpCacheService,
    config: MpCacheConfig,
}

impl RuntimeCacheController {
    pub fn new(
        engine: KvEngine,
        pipeline: AsyncKvPipeline,
        mp: MpCacheService,
    ) -> io::Result<Self> {
        let config = mp.config();
        if engine.tier_names() != mp.engine().tier_names() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime controller and MP service must reference equivalent tier topology",
            ));
        }
        Ok(Self {
            engine,
            pipeline,
            mp,
            config,
        })
    }

    pub fn lookup(
        &self,
        session_id: impl Into<String>,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        timeout: Duration,
    ) -> io::Result<RuntimeLookupResult> {
        let ticket = self.mp.submit_lookup(session_id, model_fingerprint, tokens)?;
        let status = self.mp.wait(ticket.request_id, timeout)?;
        Ok(RuntimeLookupResult {
            request_id: status.request_id,
            found_chunks: status.found_chunks,
            total_chunks: status.total_chunks,
            missed_chunks: status.missed_chunks,
        })
    }

    pub fn query_mp(&self, request_id: u64) -> io::Result<Option<MpRequestStatus>> {
        self.mp.query(request_id)
    }

    pub fn check_finish(&self, operation_id: u64) -> io::Result<Option<AsyncKvJobSnapshot>> {
        self.pipeline.snapshot(operation_id)
    }

    pub fn wait_operation(
        &self,
        operation_id: u64,
        timeout: Duration,
    ) -> io::Result<AsyncKvJobSnapshot> {
        self.pipeline.wait(operation_id, timeout)
    }

    pub fn submit_move(
        &self,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        source_tier: usize,
        destination_tier: usize,
        remove_source: bool,
    ) -> io::Result<u64> {
        let keys = self.keys_for(model_fingerprint.into(), tokens)?;
        self.pipeline
            .submit_move_many(keys, source_tier, destination_tier, remove_source)
    }

    /// Move cache entries into a caller-configured codec tier.
    ///
    /// RivetCache does not assume a compression algorithm here: when the destination is a
    /// `CodecKvTier`, its injected codec performs the transform. This keeps compression policy
    /// pluggable while retaining a controller operation compatible with remote management flows.
    pub fn submit_compress(
        &self,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        plain_tier: usize,
        codec_tier: usize,
        remove_plain: bool,
    ) -> io::Result<u64> {
        self.submit_move(
            model_fingerprint,
            tokens,
            plain_tier,
            codec_tier,
            remove_plain,
        )
    }

    /// Move cache entries from a caller-configured codec tier back to a plain tier. A
    /// `CodecKvTier` decodes on read before `KvEngine` writes the destination entry.
    pub fn submit_decompress(
        &self,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        codec_tier: usize,
        plain_tier: usize,
        remove_encoded: bool,
    ) -> io::Result<u64> {
        self.submit_move(
            model_fingerprint,
            tokens,
            codec_tier,
            plain_tier,
            remove_encoded,
        )
    }

    pub fn submit_pin(
        &self,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        pinned: bool,
    ) -> io::Result<u64> {
        let keys = self.keys_for(model_fingerprint.into(), tokens)?;
        self.pipeline.submit_set_pinned(keys, pinned)
    }

    pub fn submit_delete(
        &self,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
    ) -> io::Result<u64> {
        let keys = self.keys_for(model_fingerprint.into(), tokens)?;
        self.pipeline.submit_invalidate(keys)
    }

    pub fn submit_clear(&self) -> io::Result<u64> {
        self.pipeline.submit_clear()
    }

    pub fn submit_health(&self) -> io::Result<u64> {
        self.pipeline.submit_health()
    }

    pub fn health(&self) -> RuntimeHealthResult {
        let tiers = self.engine.health();
        RuntimeHealthResult {
            healthy: tiers.iter().all(|tier| tier.healthy),
            tiers,
        }
    }

    pub fn finish(&self, timeout: Duration) -> io::Result<bool> {
        self.pipeline.finish(timeout)
    }

    pub fn is_finished(&self, operation_id: u64) -> io::Result<Option<bool>> {
        Ok(self.pipeline.snapshot(operation_id)?.map(|snapshot| {
            matches!(
                snapshot.state,
                AsyncKvJobState::Completed | AsyncKvJobState::Failed
            )
        }))
    }

    fn keys_for(
        &self,
        model_fingerprint: String,
        tokens: Vec<u32>,
    ) -> io::Result<Vec<KvBlockKey>> {
        if model_fingerprint.trim().is_empty() || tokens.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime cache operation requires a model fingerprint and tokens",
            ));
        }
        KvCaptureRequest {
            model_fingerprint,
            tokens,
            block_tokens: self.config.block_tokens,
            layer_start: self.config.layer_start,
            layer_count: self.config.layer_count,
            layout_version: self.config.layout_version,
        }
        .block_keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvBlock, KvTier, KvTierEntry, MpEngineKind, MpTransferMode, PrefixIndex};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryTier {
        name: String,
        values: Mutex<HashMap<String, KvTierEntry>>,
    }

    impl MemoryTier {
        fn named(name: &str) -> Self {
            Self {
                name: name.to_owned(),
                values: Mutex::new(HashMap::new()),
            }
        }
    }

    impl KvTier for MemoryTier {
        fn name(&self) -> &str {
            &self.name
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

    fn controller() -> RuntimeCacheController {
        let engine = KvEngine::builder()
            .tier(MemoryTier::named("l1"))
            .tier(MemoryTier::named("l2"))
            .build()
            .unwrap();
        let pipeline = AsyncKvPipeline::new(engine.clone(), 2, 16, None).unwrap();
        let mp = MpCacheService::new(
            engine.clone(),
            pipeline.clone(),
            Arc::new(PrefixIndex::new()),
            MpCacheConfig {
                block_tokens: 2,
                layer_start: 0,
                layer_count: 8,
                layout_version: 1,
                l1_tier: 0,
                transfer_mode: MpTransferMode::Auto,
                engine_kind: MpEngineKind::Default,
            },
        )
        .unwrap();
        RuntimeCacheController::new(engine, pipeline, mp).unwrap()
    }

    #[test]
    fn move_and_check_finish_are_controller_operations() {
        let controller = controller();
        let keys = controller
            .keys_for("model".to_owned(), vec![1, 2])
            .unwrap();
        controller
            .engine
            .put_to(
                0,
                KvBlock::new(keys[0].clone(), vec![1; 32]).unwrap(),
                None,
                false,
            )
            .unwrap();
        let id = controller
            .submit_move("model", vec![1, 2], 0, 1, false)
            .unwrap();
        controller
            .wait_operation(id, Duration::from_secs(2))
            .unwrap();
        assert_eq!(controller.is_finished(id).unwrap(), Some(true));
    }
}