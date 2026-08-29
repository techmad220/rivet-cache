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
    session_id: String,
    kind: MpRequestKind,
    job_id: u64,
    keys: Vec<KvBlockKey>,
    model_fingerprint: String,
    tokens: Vec<u32>,
    indexed: bool,
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

    pub fn submit_lookup(
        &self,
        session_id: impl Into<String>,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
    ) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let model_fingerprint = validate_model(model_fingerprint.into())?;
        if tokens.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MP lookup requires at least one token",
            ));
        }

        let keys = match self.prefix.longest_prefix(&model_fingerprint, &tokens)? {
            Some(prefix) if !prefix.block_keys.is_empty() => prefix.block_keys,
            _ => self.keys_for(&model_fingerprint, &tokens)?,
        };
        let job_id = self
            .pipeline
            .submit_prefetch(keys.clone(), self.config.l1_tier)?;
        self.register_request(RequestRecord {
            session_id,
            kind: MpRequestKind::Lookup,
            job_id,
            keys: keys.clone(),
            model_fingerprint,
            tokens,
            indexed: true,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind: MpRequestKind::Lookup,
            total_chunks: keys.len() as u64,
        })
    }

    pub fn submit_store(
        &self,
        session_id: impl Into<String>,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        blocks: Vec<KvBlock>,
        ttl: Option<Duration>,
        pinned: bool,
    ) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let model_fingerprint = validate_model(model_fingerprint.into())?;
        if tokens.is_empty() || blocks.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MP store requires tokens and KV blocks",
            ));
        }
        let keys = self.keys_for(&model_fingerprint, &tokens)?;
        if blocks.len() != keys.len()
            || blocks
                .iter()
                .zip(keys.iter())
                .any(|(block, expected)| &block.key != expected)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MP store blocks do not match the configured token/chunk identity",
            ));
        }
        let job_id = self.pipeline.submit_store(blocks, ttl, pinned)?;
        self.register_request(RequestRecord {
            session_id,
            kind: MpRequestKind::Store,
            job_id,
            keys: keys.clone(),
            model_fingerprint,
            tokens,
            indexed: false,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind: MpRequestKind::Store,
            total_chunks: keys.len() as u64,
        })
    }

    pub fn submit_retrieve(
        &self,
        session_id: impl Into<String>,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
    ) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let model_fingerprint = validate_model(model_fingerprint.into())?;
        let keys = self.keys_for(&model_fingerprint, &tokens)?;
        if keys.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MP retrieve requires at least one token",
            ));
        }
        let job_id = self.pipeline.submit_retrieve(keys.clone())?;
        self.register_request(RequestRecord {
            session_id,
            kind: MpRequestKind::Retrieve,
            job_id,
            keys: keys.clone(),
            model_fingerprint,
            tokens,
            indexed: true,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind: MpRequestKind::Retrieve,
            total_chunks: keys.len() as u64,
        })
    }

    pub fn submit_retrieve_lookup(
        &self,
        session_id: impl Into<String>,
        lookup_request_id: u64,
    ) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let lookup = self.request_record(lookup_request_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown MP lookup request {lookup_request_id}"),
            )
        })?;
        if lookup.kind != MpRequestKind::Lookup {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "retrieve_lookup requires a lookup request id",
            ));
        }
        let job_id = self.pipeline.submit_retrieve(lookup.keys.clone())?;
        self.register_request(RequestRecord {
            session_id,
            kind: MpRequestKind::Retrieve,
            job_id,
            keys: lookup.keys.clone(),
            model_fingerprint: lookup.model_fingerprint,
            tokens: lookup.tokens,
            indexed: true,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind: MpRequestKind::Retrieve,
            total_chunks: lookup.keys.len() as u64,
        })
    }

    pub fn submit_pin(
        &self,
        session_id: impl Into<String>,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        pinned: bool,
    ) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let model_fingerprint = validate_model(model_fingerprint.into())?;
        let keys = self.keys_for(&model_fingerprint, &tokens)?;
        if keys.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MP pin operation requires at least one token",
            ));
        }
        let job_id = self.pipeline.submit_set_pinned(keys.clone(), pinned)?;
        let kind = if pinned {
            MpRequestKind::Pin
        } else {
            MpRequestKind::Unpin
        };
        self.register_request(RequestRecord {
            session_id,
            kind,
            job_id,
            keys: keys.clone(),
            model_fingerprint,
            tokens,
            indexed: true,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind,
            total_chunks: keys.len() as u64,
        })
    }

    pub fn submit_delete(
        &self,
        session_id: impl Into<String>,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
    ) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let model_fingerprint = validate_model(model_fingerprint.into())?;
        let keys = self.keys_for(&model_fingerprint, &tokens)?;
        if keys.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MP delete requires at least one token",
            ));
        }
        let job_id = self.pipeline.submit_invalidate(keys.clone())?;
        self.register_request(RequestRecord {
            session_id,
            kind: MpRequestKind::Delete,
            job_id,
            keys: keys.clone(),
            model_fingerprint,
            tokens,
            indexed: true,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind: MpRequestKind::Delete,
            total_chunks: keys.len() as u64,
        })
    }

    pub fn submit_clear(&self, session_id: impl Into<String>) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let job_id = self.pipeline.submit_clear()?;
        self.register_request(RequestRecord {
            session_id,
            kind: MpRequestKind::Clear,
            job_id,
            keys: Vec::new(),
            model_fingerprint: String::new(),
            tokens: Vec::new(),
            indexed: true,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind: MpRequestKind::Clear,
            total_chunks: 1,
        })
    }

    pub fn submit_health(&self, session_id: impl Into<String>) -> io::Result<MpRequestTicket> {
        let session_id = validate_session(session_id.into())?;
        let job_id = self.pipeline.submit_health()?;
        self.register_request(RequestRecord {
            session_id,
            kind: MpRequestKind::Health,
            job_id,
            keys: Vec::new(),
            model_fingerprint: String::new(),
            tokens: Vec::new(),
            indexed: true,
        })?;
        Ok(MpRequestTicket {
            request_id: job_id,
            kind: MpRequestKind::Health,
            total_chunks: 1,
        })
    }

    pub fn query(&self, request_id: u64) -> io::Result<Option<MpRequestStatus>> {
        let Some(snapshot) = self.pipeline.snapshot(request_id)? else {
            return Ok(None);
        };
        let Some(record) = self.request_record(request_id)? else {
            return Ok(None);
        };
        self.finalize_index_if_needed(&record, &snapshot)?;
        Ok(Some(status_from(&record, &snapshot)))
    }

    pub fn wait(&self, request_id: u64, timeout: Duration) -> io::Result<MpRequestStatus> {
        let snapshot = self.pipeline.wait(request_id, timeout)?;
        let record = self.request_record(request_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown MP request {request_id}"),
            )
        })?;
        self.finalize_index_if_needed(&record, &snapshot)?;
        Ok(status_from(&record, &snapshot))
    }

    pub fn retrieve_result(&self, request_id: u64) -> io::Result<Option<Vec<KvBlock>>> {
        let Some(snapshot) = self.pipeline.snapshot(request_id)? else {
            return Ok(None);
        };
        if snapshot.operation != AsyncKvOperation::Retrieve {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request is not a retrieve operation",
            ));
        }
        Ok(snapshot.result.map(|result| result.blocks))
    }

    pub fn finish(&self, timeout: Duration) -> io::Result<bool> {
        self.pipeline.finish(timeout)
    }

    pub fn end_session(&self, session_id: &str) -> io::Result<usize> {
        let ids = self
            .sessions
            .lock()
            .map_err(|_| io::Error::other("MP session registry lock poisoned"))?
            .remove(session_id)
            .unwrap_or_default();
        let mut requests = self
            .requests
            .lock()
            .map_err(|_| io::Error::other("MP request registry lock poisoned"))?;
        let mut removed = 0;
        for id in ids {
            if requests.remove(&id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub fn engine(&self) -> &KvEngine {
        &self.engine
    }

    pub fn prefix_index(&self) -> Arc<PrefixIndex> {
        Arc::clone(&self.prefix)
    }

    fn keys_for(&self, model_fingerprint: &str, tokens: &[u32]) -> io::Result<Vec<KvBlockKey>> {
        KvCaptureRequest {
            model_fingerprint: model_fingerprint.to_owned(),
            tokens: tokens.to_vec(),
            block_tokens: self.config.block_tokens,
            layer_start: self.config.layer_start,
            layer_count: self.config.layer_count,
            layout_version: self.config.layout_version,
        }
        .block_keys()
    }

    fn register_request(&self, record: RequestRecord) -> io::Result<()> {
        let id = record.job_id;
        let session = record.session_id.clone();
        self.requests
            .lock()
            .map_err(|_| io::Error::other("MP request registry lock poisoned"))?
            .insert(id, record);
        self.sessions
            .lock()
            .map_err(|_| io::Error::other("MP session registry lock poisoned"))?
            .entry(session)
            .or_default()
            .insert(id);
        Ok(())
    }

    fn request_record(&self, request_id: u64) -> io::Result<Option<RequestRecord>> {
        Ok(self
            .requests
            .lock()
            .map_err(|_| io::Error::other("MP request registry lock poisoned"))?
            .get(&request_id)
            .cloned())
    }

    fn finalize_index_if_needed(
        &self,
        record: &RequestRecord,
        snapshot: &AsyncKvJobSnapshot,
    ) -> io::Result<()> {
        if record.kind != MpRequestKind::Store
            || record.indexed
            || snapshot.state != AsyncKvJobState::Completed
        {
            return Ok(());
        }
        self.prefix.register(
            record.model_fingerprint.clone(),
            record.tokens.clone(),
            record.keys.clone(),
        )?;
        if let Ok(mut requests) = self.requests.lock() {
            if let Some(current) = requests.get_mut(&record.job_id) {
                current.indexed = true;
            }
        }
        Ok(())
    }
}

fn status_from(record: &RequestRecord, snapshot: &AsyncKvJobSnapshot) -> MpRequestStatus {
    let (found_chunks, total_chunks, missed_chunks, bytes) = snapshot
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
        .unwrap_or((0, record.keys.len() as u64, 0, 0));
    MpRequestStatus {
        request_id: record.job_id,
        kind: record.kind,
        state: snapshot.state,
        found_chunks,
        total_chunks,
        missed_chunks,
        bytes,
        error: snapshot.error.as_ref().map(|error| error.message.clone()),
    }
}

fn validate_session(session: String) -> io::Result<String> {
    validate_identifier(session, "session")
}

fn validate_model(model: String) -> io::Result<String> {
    if model.is_empty() || model.len() > 4096 || model.bytes().any(|byte| byte == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid model fingerprint",
        ));
    }
    Ok(model)
}

fn validate_identifier(value: String, name: &str) -> io::Result<String> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid MP {name} id"),
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvTier, KvTierEntry};
    use std::collections::HashMap;

    #[derive(Default)]
    struct MemoryTier {
        values: Mutex<HashMap<String, KvTierEntry>>,
    }

    impl KvTier for MemoryTier {
        fn name(&self) -> &str {
            "l1"
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
        let status = service.wait(store.request_id, Duration::from_secs(2)).unwrap();
        assert_eq!(status.found_chunks, 2);

        let lookup = service
            .submit_lookup("s", "model", vec![1, 2, 3, 4, 5, 6])
            .unwrap();
        let status = service.wait(lookup.request_id, Duration::from_secs(2)).unwrap();
        assert_eq!(status.found_chunks, 2);

        let retrieve = service
            .submit_retrieve_lookup("s", lookup.request_id)
            .unwrap();
        service
            .wait(retrieve.request_id, Duration::from_secs(2))
            .unwrap();
        assert_eq!(service.retrieve_result(retrieve.request_id).unwrap().unwrap(), blocks);
        assert!(service.finish(Duration::from_secs(1)).unwrap());
    }

    #[test]
    fn end_session_forgets_request_metadata_not_cache() {
        let service = service();
        let tokens = vec![1, 2];
        let key = service.keys_for("model", &tokens).unwrap().remove(0);
        let block = KvBlock::new(key, vec![1; 16]).unwrap();
        let ticket = service
            .submit_store("session-a", "model", tokens, vec![block], None, false)
            .unwrap();
        service
            .wait(ticket.request_id, Duration::from_secs(2))
            .unwrap();
        assert_eq!(service.end_session("session-a").unwrap(), 1);
        assert!(service.query(ticket.request_id).unwrap().is_none());
    }
}