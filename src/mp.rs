use crate::{
    AsyncKvJobSnapshot, AsyncKvJobState, AsyncKvOperation, AsyncKvPipeline, KvBlock, KvBlockKey,
    KvCaptureRequest, KvEngine, PrefixIndex,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MpTransferMode {
    ServerDriven,
    EngineDriven,
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MpEngineKind {
    Default,
    Blend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MpCacheConfig {
    pub block_tokens: usize,
    pub layer_start: u32,
    pub layer_count: u32,
    pub layout_version: u32,
    pub l1_tier: usize,
    pub transfer_mode: MpTransferMode,
    pub engine_kind: MpEngineKind,
}

impl MpCacheConfig {
    pub fn validate(self) -> io::Result<Self> {
        if self.block_tokens == 0 || self.layer_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MP cache requires non-zero block_tokens and layer_count",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MpRequestKind {
    Lookup,
    Store,
    Retrieve,
    Pin,
    Unpin,
    Delete,
    Clear,
    Health,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpRequestTicket {
    pub request_id: u64,
    pub kind: MpRequestKind,
    pub total_chunks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpRequestStatus {
    pub request_id: u64,
    pub kind: MpRequestKind,
    pub state: AsyncKvJobState,
    pub found_chunks: u64,
    pub total_chunks: u64,
    pub missed_chunks: u64,
    pub bytes: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
struct RequestRecord {
    session: String,
    kind: MpRequestKind,
    job_id: u64,
    total: u64,
    keys: Vec<KvBlockKey>,
    model: String,
    tokens: Vec<u32>,
    prefix_indexed: bool,
}

#[derive(Clone)]
pub struct MpCacheService {
    engine: KvEngine,
    pipeline: AsyncKvPipeline,
    prefix: Arc<PrefixIndex>,
    config: MpCacheConfig,
    requests: Arc<Mutex<BTreeMap<u64, RequestRecord>>>,
    sessions: Arc<Mutex<BTreeMap<String, BTreeSet<u64>>>>,
}

impl MpCacheService {
    pub fn new(
        engine: KvEngine,
        pipeline: AsyncKvPipeline,
        prefix: Arc<PrefixIndex>,
        config: MpCacheConfig,
    ) -> io::Result<Self> {
        let config = config.validate()?;
        if config.l1_tier >= engine.tier_names().len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MP L1 tier index is outside the configured KV engine",
            ));
        }
        Ok(Self {
            engine,
            pipeline,
            prefix,
            config,
            requests: Arc::new(Mutex::new(BTreeMap::new())),
            sessions: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn config(&self) -> MpCacheConfig {
        self.config
    }

    pub fn chunk_size(&self) -> usize {
        self.config.block_tokens
    }

    pub fn engine(&self) -> &KvEngine {
        &self.engine
    }

    pub fn prefix_index(&self) -> Arc<PrefixIndex> {
        Arc::clone(&self.prefix)
    }

    pub fn submit_lookup(
        &self,
        session: impl Into<String>,
        model: impl Into<String>,
        tokens: Vec<u32>,
    ) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let model = validate_model(model.into())?;
        if tokens.is_empty() {
            return invalid("MP lookup requires at least one token");
        }
        let keys = match self.prefix.longest_prefix(&model, &tokens)? {
            Some(found) if !found.block_keys.is_empty() => found.block_keys,
            _ => self.keys_for(&model, &tokens)?,
        };
        let job_id = self
            .pipeline
            .submit_prefetch(keys.clone(), self.config.l1_tier)?;
        self.register(RequestRecord {
            session,
            kind: MpRequestKind::Lookup,
            job_id,
            total: keys.len() as u64,
            keys,
            model,
            tokens,
            prefix_indexed: true,
        })
    }

    pub fn submit_store(
        &self,
        session: impl Into<String>,
        model: impl Into<String>,
        tokens: Vec<u32>,
        blocks: Vec<KvBlock>,
        ttl: Option<Duration>,
        pinned: bool,
    ) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let model = validate_model(model.into())?;
        if tokens.is_empty() || blocks.is_empty() {
            return invalid("MP store requires tokens and KV blocks");
        }
        let keys = self.keys_for(&model, &tokens)?;
        if blocks.len() != keys.len()
            || blocks
                .iter()
                .zip(&keys)
                .any(|(block, expected)| &block.key != expected)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MP store blocks do not match configured token/chunk identity",
            ));
        }
        let job_id = self.pipeline.submit_store(blocks, ttl, pinned)?;
        self.register(RequestRecord {
            session,
            kind: MpRequestKind::Store,
            job_id,
            total: keys.len() as u64,
            keys,
            model,
            tokens,
            prefix_indexed: false,
        })
    }

    pub fn submit_retrieve(
        &self,
        session: impl Into<String>,
        model: impl Into<String>,
        tokens: Vec<u32>,
    ) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let model = validate_model(model.into())?;
        let keys = self.keys_for(&model, &tokens)?;
        if keys.is_empty() {
            return invalid("MP retrieve requires at least one token");
        }
        let job_id = self.pipeline.submit_retrieve(keys.clone())?;
        self.register(RequestRecord {
            session,
            kind: MpRequestKind::Retrieve,
            job_id,
            total: keys.len() as u64,
            keys,
            model,
            tokens,
            prefix_indexed: true,
        })
    }

    pub fn submit_retrieve_lookup(
        &self,
        session: impl Into<String>,
        lookup_request_id: u64,
    ) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let lookup = self.record(lookup_request_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown MP lookup request {lookup_request_id}"),
            )
        })?;
        if lookup.kind != MpRequestKind::Lookup {
            return invalid("retrieve_lookup requires a lookup request id");
        }
        let job_id = self.pipeline.submit_retrieve(lookup.keys.clone())?;
        self.register(RequestRecord {
            session,
            kind: MpRequestKind::Retrieve,
            job_id,
            total: lookup.total,
            keys: lookup.keys,
            model: lookup.model,
            tokens: lookup.tokens,
            prefix_indexed: true,
        })
    }

    pub fn submit_pin(
        &self,
        session: impl Into<String>,
        model: impl Into<String>,
        tokens: Vec<u32>,
        pinned: bool,
    ) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let model = validate_model(model.into())?;
        let keys = self.keys_for(&model, &tokens)?;
        if keys.is_empty() {
            return invalid("MP pin operation requires at least one token");
        }
        let kind = if pinned {
            MpRequestKind::Pin
        } else {
            MpRequestKind::Unpin
        };
        let job_id = self.pipeline.submit_set_pinned(keys.clone(), pinned)?;
        self.register(RequestRecord {
            session,
            kind,
            job_id,
            total: keys.len() as u64,
            keys,
            model,
            tokens,
            prefix_indexed: true,
        })
    }

    pub fn submit_delete(
        &self,
        session: impl Into<String>,
        model: impl Into<String>,
        tokens: Vec<u32>,
    ) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let model = validate_model(model.into())?;
        let keys = self.keys_for(&model, &tokens)?;
        if keys.is_empty() {
            return invalid("MP delete requires at least one token");
        }
        let job_id = self.pipeline.submit_invalidate(keys.clone())?;
        self.register(RequestRecord {
            session,
            kind: MpRequestKind::Delete,
            job_id,
            total: keys.len() as u64,
            keys,
            model,
            tokens,
            prefix_indexed: true,
        })
    }

    pub fn submit_clear(&self, session: impl Into<String>) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let job_id = self.pipeline.submit_clear()?;
        self.register(RequestRecord {
            session,
            kind: MpRequestKind::Clear,
            job_id,
            total: 1,
            keys: Vec::new(),
            model: String::new(),
            tokens: Vec::new(),
            prefix_indexed: true,
        })
    }

    pub fn submit_health(&self, session: impl Into<String>) -> io::Result<MpRequestTicket> {
        let session = validate_id(session.into(), "session")?;
        let job_id = self.pipeline.submit_health()?;
        self.register(RequestRecord {
            session,
            kind: MpRequestKind::Health,
            job_id,
            total: 1,
            keys: Vec::new(),
            model: String::new(),
            tokens: Vec::new(),
            prefix_indexed: true,
        })
    }

    pub fn query(&self, id: u64) -> io::Result<Option<MpRequestStatus>> {
        let Some(snapshot) = self.pipeline.snapshot(id)? else {
            return Ok(None);
        };
        let Some(record) = self.record(id)? else {
            return Ok(None);
        };
        self.finalize_prefix(&record, &snapshot)?;
        Ok(Some(status(&record, &snapshot)))
    }

    pub fn wait(&self, id: u64, timeout: Duration) -> io::Result<MpRequestStatus> {
        let snapshot = self.pipeline.wait(id, timeout)?;
        let record = self.record(id)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown MP request {id}"))
        })?;
        self.finalize_prefix(&record, &snapshot)?;
        Ok(status(&record, &snapshot))
    }

    pub fn retrieve_result(&self, id: u64) -> io::Result<Option<Vec<KvBlock>>> {
        let Some(snapshot) = self.pipeline.snapshot(id)? else {
            return Ok(None);
        };
        if snapshot.operation != AsyncKvOperation::Retrieve {
            return invalid("request is not a retrieve operation");
        }
        Ok(snapshot.result.map(|result| result.blocks))
    }

    pub fn finish(&self, timeout: Duration) -> io::Result<bool> {
        self.pipeline.finish(timeout)
    }

    pub fn end_session(&self, session: &str) -> io::Result<usize> {
        let ids = self
            .sessions
            .lock()
            .map_err(|_| io::Error::other("MP session registry lock poisoned"))?
            .remove(session)
            .unwrap_or_default();
        let mut requests = self
            .requests
            .lock()
            .map_err(|_| io::Error::other("MP request registry lock poisoned"))?;
        Ok(ids
            .into_iter()
            .filter(|id| requests.remove(id).is_some())
            .count())
    }

    fn keys_for(&self, model: &str, tokens: &[u32]) -> io::Result<Vec<KvBlockKey>> {
        KvCaptureRequest {
            model_fingerprint: model.to_owned(),
            tokens: tokens.to_vec(),
            block_tokens: self.config.block_tokens,
            layer_start: self.config.layer_start,
            layer_count: self.config.layer_count,
            layout_version: self.config.layout_version,
        }
        .block_keys()
    }

    fn register(&self, record: RequestRecord) -> io::Result<MpRequestTicket> {
        let ticket = MpRequestTicket {
            request_id: record.job_id,
            kind: record.kind,
            total_chunks: record.total,
        };
        self.sessions
            .lock()
            .map_err(|_| io::Error::other("MP session registry lock poisoned"))?
            .entry(record.session.clone())
            .or_default()
            .insert(record.job_id);
        self.requests
            .lock()
            .map_err(|_| io::Error::other("MP request registry lock poisoned"))?
            .insert(record.job_id, record);
        Ok(ticket)
    }

    fn record(&self, id: u64) -> io::Result<Option<RequestRecord>> {
        Ok(self
            .requests
            .lock()
            .map_err(|_| io::Error::other("MP request registry lock poisoned"))?
            .get(&id)
            .cloned())
    }

    fn finalize_prefix(
        &self,
        record: &RequestRecord,
        snapshot: &AsyncKvJobSnapshot,
    ) -> io::Result<()> {
        if record.kind != MpRequestKind::Store
            || record.prefix_indexed
            || snapshot.state != AsyncKvJobState::Completed
        {
            return Ok(());
        }
        self.prefix.register(
            record.model.clone(),
            record.tokens.clone(),
            record.keys.clone(),
        )?;
        if let Ok(mut requests) = self.requests.lock() {
            if let Some(current) = requests.get_mut(&record.job_id) {
                current.prefix_indexed = true;
            }
        }
        Ok(())
    }
}

fn status(record: &RequestRecord, snapshot: &AsyncKvJobSnapshot) -> MpRequestStatus {
    let (found, total, missed, bytes) = snapshot
        .result
        .as_ref()
        .map(|result| {
            (
                result.completed,
                result.requested,
                result.missed,
                result.bytes,
            )
        })
        .unwrap_or((0, record.total, 0, 0));
    MpRequestStatus {
        request_id: record.job_id,
        kind: record.kind,
        state: snapshot.state,
        found_chunks: found,
        total_chunks: total,
        missed_chunks: missed,
        bytes,
        error: snapshot.error.as_ref().map(|error| error.message.clone()),
    }
}

fn validate_id(value: String, kind: &str) -> io::Result<String> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid MP {kind} id"),
        ));
    }
    Ok(value)
}

fn validate_model(value: String) -> io::Result<String> {
    if value.is_empty() || value.len() > 4096 || value.as_bytes().contains(&0) {
        return invalid("invalid model fingerprint");
    }
    Ok(value)
}

fn invalid<T>(message: &str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvTier, KvTierEntry};
    use std::collections::HashMap;

    #[derive(Default)]
    struct MemoryTier(Mutex<HashMap<String, KvTierEntry>>);

    impl KvTier for MemoryTier {
        fn name(&self) -> &str {
            "l1"
        }

        fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
            Ok(self.0.lock().unwrap().get(&key.cache_key()).cloned())
        }

        fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
            self.0
                .lock()
                .unwrap()
                .insert(entry.block.key.cache_key(), entry.clone());
            Ok(())
        }

        fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
            self.0.lock().unwrap().remove(&key.cache_key());
            Ok(())
        }

        fn clear(&self) -> io::Result<()> {
            self.0.lock().unwrap().clear();
            Ok(())
        }
    }

    fn service() -> MpCacheService {
        let engine = KvEngine::builder().tier(MemoryTier::default()).build().unwrap();
        let pipeline = AsyncKvPipeline::new(engine.clone(), 2, 16, None).unwrap();
        MpCacheService::new(
            engine,
            pipeline,
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
        .unwrap()
    }

    #[test]
    fn store_lookup_retrieve_flow() {
        let service = service();
        let tokens = vec![1, 2, 3, 4];
        let keys = service.keys_for("model", &tokens).unwrap();
        let blocks = keys
            .iter()
            .enumerate()
            .map(|(index, key)| KvBlock::new(key.clone(), vec![index as u8 + 1; 32]).unwrap())
            .collect::<Vec<_>>();
        let store = service
            .submit_store("s", "model", tokens.clone(), blocks.clone(), None, false)
            .unwrap();
        assert_eq!(
            service
                .wait(store.request_id, Duration::from_secs(2))
                .unwrap()
                .found_chunks,
            2
        );
        let lookup = service
            .submit_lookup("s", "model", vec![1, 2, 3, 4, 5, 6])
            .unwrap();
        service
            .wait(lookup.request_id, Duration::from_secs(2))
            .unwrap();
        let retrieve = service
            .submit_retrieve_lookup("s", lookup.request_id)
            .unwrap();
        service
            .wait(retrieve.request_id, Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            service.retrieve_result(retrieve.request_id).unwrap().unwrap(),
            blocks
        );
    }
}
