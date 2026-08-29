use crate::{Clock, ContextCache, SystemClock};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io;
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

const KV_KEY_DOMAIN: &[u8] = b"RIVET_KV_V1\0";
const TIER_ENVELOPE_MAGIC: &[u8; 6] = b"RKV01\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KvBlockRange {
    pub block_index: u32,
    pub token_start: u32,
    pub token_count: u32,
    pub layer_start: u32,
    pub layer_count: u32,
    pub layout_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KvBlockKey {
    pub model_fingerprint: String,
    pub sequence_hash: [u8; 32],
    pub block_index: u32,
    pub token_start: u32,
    pub token_count: u32,
    pub layer_start: u32,
    pub layer_count: u32,
    pub layout_version: u32,
}

impl KvBlockKey {
    pub fn from_prefix(
        model_fingerprint: impl Into<String>,
        prefix_tokens: &[u32],
        range: KvBlockRange,
    ) -> Self {
        let mut sequence_hasher = Sha256::new();
        sequence_hasher.update(KV_KEY_DOMAIN);
        sequence_hasher.update((prefix_tokens.len() as u64).to_le_bytes());
        for token in prefix_tokens {
            sequence_hasher.update(token.to_le_bytes());
        }
        let sequence_hash: [u8; 32] = sequence_hasher.finalize().into();

        Self {
            model_fingerprint: model_fingerprint.into(),
            sequence_hash,
            block_index: range.block_index,
            token_start: range.token_start,
            token_count: range.token_count,
            layer_start: range.layer_start,
            layer_count: range.layer_count,
            layout_version: range.layout_version,
        }
    }

    pub fn cache_key(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(KV_KEY_DOMAIN);
        write_len_prefixed(&mut hasher, self.model_fingerprint.as_bytes());
        hasher.update(self.sequence_hash);
        hasher.update(self.block_index.to_le_bytes());
        hasher.update(self.token_start.to_le_bytes());
        hasher.update(self.token_count.to_le_bytes());
        hasher.update(self.layer_start.to_le_bytes());
        hasher.update(self.layer_count.to_le_bytes());
        hasher.update(self.layout_version.to_le_bytes());
        hex::encode(hasher.finalize())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvBlock {
    pub key: KvBlockKey,
    pub bytes: Vec<u8>,
}

impl KvBlock {
    pub fn new(key: KvBlockKey, bytes: Vec<u8>) -> io::Result<Self> {
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "KV block payload must not be empty",
            ));
        }
        Ok(Self { key, bytes })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvTierEntry {
    pub block: KvBlock,
    pub expires_at: u64,
    pub pinned: bool,
}

pub trait KvTier: Send + Sync {
    fn name(&self) -> &str;
    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>>;
    fn put(&self, entry: &KvTierEntry) -> io::Result<()>;
    fn remove(&self, key: &KvBlockKey) -> io::Result<()>;
}

pub struct ContextCacheTier {
    name: String,
    cache: Arc<ContextCache>,
}

impl ContextCacheTier {
    pub fn new(name: impl Into<String>, cache: Arc<ContextCache>) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tier name must not be empty",
            ));
        }
        Ok(Self { name, cache })
    }
}

impl KvTier for ContextCacheTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let Some(encoded) = self.cache.get(&key.cache_key())? else {
            return Ok(None);
        };
        decode_tier_entry(key.clone(), &encoded).map(Some)
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        let encoded = encode_tier_entry(entry)?;
        self.cache.put(
            &entry.block.key.cache_key(),
            &encoded,
            Some(Duration::ZERO),
            entry.pinned,
        )
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        self.cache.invalidate(&key.cache_key())
    }
}

pub trait KvAllocator: Send + Sync {
    fn copy(&self, bytes: &[u8]) -> io::Result<Vec<u8>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct VecAllocator;

impl KvAllocator for VecAllocator {
    fn copy(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

pub trait KvTransport: Send + Sync {
    fn transfer(
        &self,
        key: &KvBlockKey,
        bytes: &[u8],
        source_tier: &str,
        destination_tier: &str,
    ) -> io::Result<Vec<u8>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CopyTransport;

impl KvTransport for CopyTransport {
    fn transfer(
        &self,
        _key: &KvBlockKey,
        bytes: &[u8],
        _source_tier: &str,
        _destination_tier: &str,
    ) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum KvWritePolicy {
    #[default]
    Primary,
    All,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KvEngineStats {
    pub hits: u64,
    pub misses: u64,
    pub writes: u64,
    pub promotions: u64,
    pub transfers: u64,
    pub bytes_transferred: u64,
    pub invalidations: u64,
    pub expirations: u64,
    pub prefetches: u64,
}

struct KvEngineInner {
    tiers: Vec<Arc<dyn KvTier>>,
    transport: Arc<dyn KvTransport>,
    allocator: Arc<dyn KvAllocator>,
    clock: Arc<dyn Clock>,
    promote_on_read: bool,
    write_policy: KvWritePolicy,
    stats: Mutex<KvEngineStats>,
}

#[derive(Clone)]
pub struct KvEngine {
    inner: Arc<KvEngineInner>,
}

pub struct KvEngineBuilder {
    tiers: Vec<Arc<dyn KvTier>>,
    transport: Arc<dyn KvTransport>,
    allocator: Arc<dyn KvAllocator>,
    clock: Arc<dyn Clock>,
    promote_on_read: bool,
    write_policy: KvWritePolicy,
}

impl Default for KvEngineBuilder {
    fn default() -> Self {
        Self {
            tiers: Vec::new(),
            transport: Arc::new(CopyTransport),
            allocator: Arc::new(VecAllocator),
            clock: Arc::new(SystemClock),
            promote_on_read: true,
            write_policy: KvWritePolicy::Primary,
        }
    }
}

impl KvEngineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tier<T>(mut self, tier: T) -> Self
    where
        T: KvTier + 'static,
    {
        self.tiers.push(Arc::new(tier));
        self
    }

    pub fn tier_arc(mut self, tier: Arc<dyn KvTier>) -> Self {
        self.tiers.push(tier);
        self
    }

    pub fn transport<T>(mut self, transport: T) -> Self
    where
        T: KvTransport + 'static,
    {
        self.transport = Arc::new(transport);
        self
    }

    pub fn transport_arc(mut self, transport: Arc<dyn KvTransport>) -> Self {
        self.transport = transport;
        self
    }

    pub fn allocator<T>(mut self, allocator: T) -> Self
    where
        T: KvAllocator + 'static,
    {
        self.allocator = Arc::new(allocator);
        self
    }

    pub fn allocator_arc(mut self, allocator: Arc<dyn KvAllocator>) -> Self {
        self.allocator = allocator;
        self
    }

    pub fn clock<T>(mut self, clock: T) -> Self
    where
        T: Clock + 'static,
    {
        self.clock = Arc::new(clock);
        self
    }

    pub fn clock_arc(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn promote_on_read(mut self, enabled: bool) -> Self {
        self.promote_on_read = enabled;
        self
    }

    pub fn write_policy(mut self, policy: KvWritePolicy) -> Self {
        self.write_policy = policy;
        self
    }

    pub fn build(self) -> io::Result<KvEngine> {
        if self.tiers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "KV engine requires at least one tier",
            ));
        }

        let mut names = HashSet::new();
        for tier in &self.tiers {
            if !names.insert(tier.name().to_string()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate KV tier name: {}", tier.name()),
                ));
            }
        }

        Ok(KvEngine {
            inner: Arc::new(KvEngineInner {
                tiers: self.tiers,
                transport: self.transport,
                allocator: self.allocator,
                clock: self.clock,
                promote_on_read: self.promote_on_read,
                write_policy: self.write_policy,
                stats: Mutex::new(KvEngineStats::default()),
            }),
        })
    }
}

impl KvEngine {
    pub fn builder() -> KvEngineBuilder {
        KvEngineBuilder::new()
    }

    pub fn tier_names(&self) -> Vec<String> {
        self.inner
            .tiers
            .iter()
            .map(|tier| tier.name().to_string())
            .collect()
    }

    pub fn put(&self, block: KvBlock, ttl: Option<Duration>, pinned: bool) -> io::Result<()> {
        let expires_at = ttl
            .filter(|ttl| !ttl.is_zero())
            .map(|ttl| {
                self.inner
                    .clock
                    .now_seconds()
                    .saturating_add(ttl.as_secs().max(1))
            })
            .unwrap_or(0);
        let entry = KvTierEntry {
            block,
            expires_at,
            pinned,
        };

        match self.inner.write_policy {
            KvWritePolicy::Primary => self.put_entry_to(0, &entry)?,
            KvWritePolicy::All => {
                let mut written: Vec<usize> = Vec::new();
                for index in 0..self.inner.tiers.len() {
                    if let Err(error) = self.put_entry_to(index, &entry) {
                        for written_index in written {
                            let _ = self.inner.tiers[written_index].remove(&entry.block.key);
                        }
                        return Err(error);
                    }
                    written.push(index);
                }
            }
        }
        let mut stats = self.stats_mut()?;
        stats.writes = stats.writes.saturating_add(1);
        Ok(())
    }

    pub fn put_to(
        &self,
        tier_index: usize,
        block: KvBlock,
        ttl: Option<Duration>,
        pinned: bool,
    ) -> io::Result<()> {
        let expires_at = ttl
            .filter(|ttl| !ttl.is_zero())
            .map(|ttl| {
                self.inner
                    .clock
                    .now_seconds()
                    .saturating_add(ttl.as_secs().max(1))
            })
            .unwrap_or(0);
        let entry = KvTierEntry {
            block,
            expires_at,
            pinned,
        };
        self.put_entry_to(tier_index, &entry)?;
        let mut stats = self.stats_mut()?;
        stats.writes = stats.writes.saturating_add(1);
        Ok(())
    }

    pub fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvBlock>> {
        let now = self.inner.clock.now_seconds();
        for index in 0..self.inner.tiers.len() {
            let Some(entry) = self.inner.tiers[index].get(key)? else {
                continue;
            };

            if is_expired(entry.expires_at, now) {
                self.inner.tiers[index].remove(key)?;
                let mut stats = self.stats_mut()?;
                stats.expirations = stats.expirations.saturating_add(1);
                continue;
            }

            {
                let mut stats = self.stats_mut()?;
                stats.hits = stats.hits.saturating_add(1);
            }

            if self.inner.promote_on_read && index > 0 {
                for destination in (0..index).rev() {
                    self.transfer_entry(&entry, index, destination)?;
                    let mut stats = self.stats_mut()?;
                    stats.promotions = stats.promotions.saturating_add(1);
                }
            }

            let bytes = self.inner.allocator.copy(&entry.block.bytes)?;
            return Ok(Some(KvBlock {
                key: entry.block.key,
                bytes,
            }));
        }

        let mut stats = self.stats_mut()?;
        stats.misses = stats.misses.saturating_add(1);
        Ok(None)
    }

    pub fn move_block(
        &self,
        key: &KvBlockKey,
        source_index: usize,
        destination_index: usize,
        remove_source: bool,
    ) -> io::Result<bool> {
        self.require_tier(source_index)?;
        self.require_tier(destination_index)?;
        let Some(entry) = self.inner.tiers[source_index].get(key)? else {
            return Ok(false);
        };
        if is_expired(entry.expires_at, self.inner.clock.now_seconds()) {
            self.inner.tiers[source_index].remove(key)?;
            let mut stats = self.stats_mut()?;
            stats.expirations = stats.expirations.saturating_add(1);
            return Ok(false);
        }

        self.transfer_entry(&entry, source_index, destination_index)?;
        if remove_source && source_index != destination_index {
            self.inner.tiers[source_index].remove(key)?;
        }
        Ok(true)
    }

    pub fn capture_from(
        &self,
        adapter: &dyn RuntimeKvAdapter,
        request: &KvCaptureRequest,
        ttl: Option<Duration>,
        pinned: bool,
    ) -> io::Result<usize> {
        let expected = request.block_keys()?;
        let blocks = adapter.capture(request)?;
        if blocks.len() != expected.len()
            || blocks
                .iter()
                .zip(expected.iter())
                .any(|(block, key)| &block.key != key)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime adapter {} returned blocks that do not match the capture request",
                    adapter.runtime_name()
                ),
            ));
        }
        let count = blocks.len();
        for block in blocks {
            self.put(block, ttl, pinned)?;
        }
        Ok(count)
    }

    pub fn restore_into(
        &self,
        adapter: &dyn RuntimeKvAdapter,
        keys: &[KvBlockKey],
    ) -> io::Result<bool> {
        let mut blocks = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(block) = self.get(key)? else {
                return Ok(false);
            };
            blocks.push(block);
        }
        adapter.restore(&blocks)?;
        Ok(true)
    }

    pub fn invalidate(&self, key: &KvBlockKey) -> io::Result<()> {
        for tier in &self.inner.tiers {
            tier.remove(key)?;
        }
        let mut stats = self.stats_mut()?;
        stats.invalidations = stats.invalidations.saturating_add(1);
        Ok(())
    }

    pub fn prefetch_to(&self, keys: Vec<KvBlockKey>, destination_index: usize) -> KvPrefetch {
        let engine = self.clone();
        let (sender, receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let result = engine.prefetch_worker(keys, destination_index);
            let _ = sender.send(result);
        });
        KvPrefetch {
            receiver,
            handle: Some(handle),
        }
    }

    pub fn stats(&self) -> io::Result<KvEngineStats> {
        Ok(*self.stats_mut()?)
    }

    fn prefetch_worker(
        &self,
        keys: Vec<KvBlockKey>,
        destination_index: usize,
    ) -> io::Result<PrefetchReport> {
        self.require_tier(destination_index)?;
        let requested = keys.len() as u64;
        let mut populated = 0_u64;
        let mut missed = 0_u64;

        for key in keys {
            let now = self.inner.clock.now_seconds();
            if let Some(destination_entry) = self.inner.tiers[destination_index].get(&key)? {
                if is_expired(destination_entry.expires_at, now) {
                    self.inner.tiers[destination_index].remove(&key)?;
                    let mut stats = self.stats_mut()?;
                    stats.expirations = stats.expirations.saturating_add(1);
                } else {
                    populated = populated.saturating_add(1);
                    continue;
                }
            }

            let mut found = None;
            for source_index in 0..self.inner.tiers.len() {
                if source_index == destination_index {
                    continue;
                }
                if let Some(entry) = self.inner.tiers[source_index].get(&key)? {
                    if is_expired(entry.expires_at, now) {
                        self.inner.tiers[source_index].remove(&key)?;
                        let mut stats = self.stats_mut()?;
                        stats.expirations = stats.expirations.saturating_add(1);
                        continue;
                    }
                    found = Some((source_index, entry));
                    break;
                }
            }

            if let Some((source_index, entry)) = found {
                self.transfer_entry(&entry, source_index, destination_index)?;
                populated = populated.saturating_add(1);
            } else {
                missed = missed.saturating_add(1);
            }
        }

        let mut stats = self.stats_mut()?;
        stats.prefetches = stats.prefetches.saturating_add(requested);
        Ok(PrefetchReport {
            requested,
            populated,
            missed,
        })
    }

    fn put_entry_to(&self, tier_index: usize, entry: &KvTierEntry) -> io::Result<()> {
        self.require_tier(tier_index)?;
        self.inner.tiers[tier_index].put(entry)
    }

    fn transfer_entry(
        &self,
        entry: &KvTierEntry,
        source_index: usize,
        destination_index: usize,
    ) -> io::Result<()> {
        self.require_tier(source_index)?;
        self.require_tier(destination_index)?;
        if source_index == destination_index {
            return self.inner.tiers[destination_index].put(entry);
        }

        let source = self.inner.tiers[source_index].name();
        let destination = self.inner.tiers[destination_index].name();
        let transported = self.inner.transport.transfer(
            &entry.block.key,
            &entry.block.bytes,
            source,
            destination,
        )?;
        let allocated = self.inner.allocator.copy(&transported)?;
        let transferred = KvTierEntry {
            block: KvBlock {
                key: entry.block.key.clone(),
                bytes: allocated,
            },
            expires_at: entry.expires_at,
            pinned: entry.pinned,
        };
        self.inner.tiers[destination_index].put(&transferred)?;

        let mut stats = self.stats_mut()?;
        stats.transfers = stats.transfers.saturating_add(1);
        stats.bytes_transferred = stats
            .bytes_transferred
            .saturating_add(transferred.block.bytes.len() as u64);
        Ok(())
    }

    fn require_tier(&self, tier_index: usize) -> io::Result<()> {
        if tier_index >= self.inner.tiers.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("KV tier index {tier_index} is out of range"),
            ));
        }
        Ok(())
    }

    fn stats_mut(&self) -> io::Result<MutexGuard<'_, KvEngineStats>> {
        self.inner
            .stats
            .lock()
            .map_err(|_| io::Error::other("KV engine stats lock poisoned"))
    }
}

pub struct KvPrefetch {
    receiver: mpsc::Receiver<io::Result<PrefetchReport>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl KvPrefetch {
    pub fn wait(mut self) -> io::Result<PrefetchReport> {
        let result = self.receiver.recv().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "KV prefetch worker exited without a result",
            )
        })?;
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| io::Error::other("KV prefetch worker panicked"))?;
        }
        result
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrefetchReport {
    pub requested: u64,
    pub populated: u64,
    pub missed: u64,
}

#[derive(Debug, Clone)]
pub struct KvCaptureRequest {
    pub model_fingerprint: String,
    pub tokens: Vec<u32>,
    pub block_tokens: usize,
    pub layer_start: u32,
    pub layer_count: u32,
    pub layout_version: u32,
}

impl KvCaptureRequest {
    pub fn block_keys(&self) -> io::Result<Vec<KvBlockKey>> {
        if self.block_tokens == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_tokens must be greater than zero",
            ));
        }
        if self.layer_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "layer_count must be greater than zero",
            ));
        }
        if self.tokens.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "token sequence is too large for KV block identity",
            ));
        }
        let block_count = self.tokens.len().div_ceil(self.block_tokens);
        if block_count > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block count is too large for KV block identity",
            ));
        }

        let mut keys = Vec::with_capacity(block_count);
        for (block_index, chunk) in self.tokens.chunks(self.block_tokens).enumerate() {
            let token_start = block_index.saturating_mul(self.block_tokens);
            let prefix_end = token_start.saturating_add(chunk.len());
            keys.push(KvBlockKey::from_prefix(
                self.model_fingerprint.clone(),
                &self.tokens[..prefix_end],
                KvBlockRange {
                    block_index: block_index as u32,
                    token_start: token_start as u32,
                    token_count: chunk.len() as u32,
                    layer_start: self.layer_start,
                    layer_count: self.layer_count,
                    layout_version: self.layout_version,
                },
            ));
        }
        Ok(keys)
    }
}

pub trait RuntimeKvAdapter: Send + Sync {
    fn runtime_name(&self) -> &str;
    fn capture(&self, request: &KvCaptureRequest) -> io::Result<Vec<KvBlock>>;
    fn restore(&self, blocks: &[KvBlock]) -> io::Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixMatch {
    pub matched_tokens: usize,
    pub block_keys: Vec<KvBlockKey>,
}

#[derive(Debug, Clone)]
struct PrefixRecord {
    model_fingerprint: String,
    tokens: Vec<u32>,
    block_keys: Vec<KvBlockKey>,
}

#[derive(Default)]
pub struct PrefixIndex {
    records: Mutex<Vec<PrefixRecord>>,
}

impl PrefixIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        model_fingerprint: impl Into<String>,
        tokens: Vec<u32>,
        block_keys: Vec<KvBlockKey>,
    ) -> io::Result<()> {
        let model_fingerprint = model_fingerprint.into();
        if tokens.is_empty() || block_keys.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "prefix records require tokens and block keys",
            ));
        }
        if block_keys
            .iter()
            .any(|key| key.model_fingerprint != model_fingerprint)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "prefix block keys must match the registered model fingerprint",
            ));
        }
        let mut records = self
            .records
            .lock()
            .map_err(|_| io::Error::other("prefix index lock poisoned"))?;
        records.retain(|record| {
            record.model_fingerprint != model_fingerprint || record.tokens != tokens
        });
        records.push(PrefixRecord {
            model_fingerprint,
            tokens,
            block_keys,
        });
        Ok(())
    }

    pub fn longest_prefix(
        &self,
        model_fingerprint: &str,
        tokens: &[u32],
    ) -> io::Result<Option<PrefixMatch>> {
        let records = self
            .records
            .lock()
            .map_err(|_| io::Error::other("prefix index lock poisoned"))?;
        let best = records
            .iter()
            .filter(|record| {
                record.model_fingerprint == model_fingerprint
                    && tokens.starts_with(record.tokens.as_slice())
            })
            .max_by_key(|record| record.tokens.len());

        Ok(best.map(|record| PrefixMatch {
            matched_tokens: record.tokens.len(),
            block_keys: record.block_keys.clone(),
        }))
    }

    pub fn remove_model(&self, model_fingerprint: &str) -> io::Result<usize> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| io::Error::other("prefix index lock poisoned"))?;
        let before = records.len();
        records.retain(|record| record.model_fingerprint != model_fingerprint);
        Ok(before.saturating_sub(records.len()))
    }
}

fn encode_tier_entry(entry: &KvTierEntry) -> io::Result<Vec<u8>> {
    if entry.block.bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KV tier entry payload must not be empty",
        ));
    }
    let mut encoded = Vec::with_capacity(TIER_ENVELOPE_MAGIC.len() + 17 + entry.block.bytes.len());
    encoded.extend_from_slice(TIER_ENVELOPE_MAGIC);
    encoded.extend_from_slice(&entry.expires_at.to_le_bytes());
    encoded.push(u8::from(entry.pinned));
    encoded.extend_from_slice(&(entry.block.bytes.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&entry.block.bytes);
    Ok(encoded)
}

fn decode_tier_entry(key: KvBlockKey, encoded: &[u8]) -> io::Result<KvTierEntry> {
    let header = TIER_ENVELOPE_MAGIC.len() + 8 + 1 + 8;
    if encoded.len() < header || &encoded[..TIER_ENVELOPE_MAGIC.len()] != TIER_ENVELOPE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid KV tier envelope",
        ));
    }
    let mut cursor = TIER_ENVELOPE_MAGIC.len();
    let expires_at = read_u64(encoded, &mut cursor)?;
    let pinned = match encoded.get(cursor).copied() {
        Some(0) => false,
        Some(1) => true,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid KV tier pin flag",
            ))
        }
    };
    cursor += 1;
    let payload_len = read_u64(encoded, &mut cursor)? as usize;
    if encoded.len().saturating_sub(cursor) != payload_len || payload_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid KV tier payload length",
        ));
    }
    Ok(KvTierEntry {
        block: KvBlock {
            key,
            bytes: encoded[cursor..].to_vec(),
        },
        expires_at,
        pinned,
    })
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor.saturating_add(8);
    let raw = bytes.get(*cursor..end).ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "truncated KV tier envelope")
    })?;
    *cursor = end;
    Ok(u64::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn is_expired(expires_at: u64, now: u64) -> bool {
    expires_at != 0 && expires_at <= now
}

fn write_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct ManualClock {
        now: AtomicU64,
    }

    impl ManualClock {
        fn new(now: u64) -> Self {
            Self {
                now: AtomicU64::new(now),
            }
        }

        fn advance(&self, seconds: u64) {
            self.now.fetch_add(seconds, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_seconds(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
    }

    fn key(tokens: &[u32]) -> KvBlockKey {
        KvBlockKey::from_prefix(
            "model-a",
            tokens,
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: tokens.len() as u32,
                layer_start: 0,
                layer_count: 32,
                layout_version: 1,
            },
        )
    }

    fn tier(name: &str) -> Arc<dyn KvTier> {
        let cache = Arc::new(
            ContextCache::builder()
                .memory_capacity(1024 * 1024)
                .build()
                .expect("cache"),
        );
        Arc::new(ContextCacheTier::new(name, cache).expect("tier"))
    }

    struct FailingTier {
        name: String,
    }

    impl KvTier for FailingTier {
        fn name(&self) -> &str {
            &self.name
        }

        fn get(&self, _key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
            Ok(None)
        }

        fn put(&self, _entry: &KvTierEntry) -> io::Result<()> {
            Err(io::Error::other("injected tier write failure"))
        }

        fn remove(&self, _key: &KvBlockKey) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingAdapter {
        restored: Mutex<Vec<KvBlock>>,
    }

    impl RuntimeKvAdapter for RecordingAdapter {
        fn runtime_name(&self) -> &str {
            "recording-test-runtime"
        }

        fn capture(&self, request: &KvCaptureRequest) -> io::Result<Vec<KvBlock>> {
            request
                .block_keys()?
                .into_iter()
                .enumerate()
                .map(|(index, key)| KvBlock::new(key, vec![(index as u8).saturating_add(1)]))
                .collect()
        }

        fn restore(&self, blocks: &[KvBlock]) -> io::Result<()> {
            *self
                .restored
                .lock()
                .map_err(|_| io::Error::other("recording adapter lock poisoned"))? =
                blocks.to_vec();
            Ok(())
        }
    }

    #[test]
    fn kv_keys_are_stable_and_context_sensitive() {
        let first = key(&[1, 2, 3]);
        let same = key(&[1, 2, 3]);
        let different = key(&[1, 2, 4]);
        assert_eq!(first.cache_key(), same.cache_key());
        assert_ne!(first.cache_key(), different.cache_key());
        assert_eq!(first.cache_key().len(), 64);

        let other_model = KvBlockKey::from_prefix(
            "model-b",
            &[1, 2, 3],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 3,
                layer_start: 0,
                layer_count: 32,
                layout_version: 1,
            },
        );
        assert_ne!(first.cache_key(), other_model.cache_key());
    }

    #[test]
    fn capture_request_builds_prefix_scoped_block_keys() {
        let request = KvCaptureRequest {
            model_fingerprint: "m".to_string(),
            tokens: vec![10, 20, 30, 40, 50],
            block_tokens: 2,
            layer_start: 0,
            layer_count: 24,
            layout_version: 7,
        };
        let keys = request.block_keys().expect("keys");
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0].token_start, 0);
        assert_eq!(keys[1].token_start, 2);
        assert_eq!(keys[2].token_count, 1);
        assert_ne!(keys[0].sequence_hash, keys[1].sequence_hash);
    }

    #[test]
    fn prefix_index_returns_longest_registered_prefix() {
        let index = PrefixIndex::new();
        let short = key(&[1, 2]);
        let long = key(&[1, 2, 3, 4]);
        index
            .register("model-a", vec![1, 2], vec![short.clone()])
            .expect("register short");
        index
            .register("model-a", vec![1, 2, 3, 4], vec![short, long.clone()])
            .expect("register long");

        let matched = index
            .longest_prefix("model-a", &[1, 2, 3, 4, 5])
            .expect("match")
            .expect("prefix");
        assert_eq!(matched.matched_tokens, 4);
        assert_eq!(matched.block_keys.last(), Some(&long));
    }

    #[test]
    fn lower_tier_hit_promotes_to_faster_tier() {
        let fast = tier("fast");
        let slow = tier("slow");
        let engine = KvEngine::builder()
            .tier_arc(fast.clone())
            .tier_arc(slow.clone())
            .build()
            .expect("engine");
        let key = key(&[7, 8, 9]);
        let entry = KvTierEntry {
            block: KvBlock::new(key.clone(), b"kv".to_vec()).expect("block"),
            expires_at: 0,
            pinned: false,
        };
        slow.put(&entry).expect("seed slow");
        assert!(fast.get(&key).expect("fast miss").is_none());

        let block = engine.get(&key).expect("get").expect("hit");
        assert_eq!(block.bytes, b"kv");
        assert!(fast.get(&key).expect("promoted").is_some());
        let stats = engine.stats().expect("stats");
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.promotions, 1);
        assert_eq!(stats.transfers, 1);
    }

    #[test]
    fn write_through_populates_all_tiers() {
        let first = tier("first");
        let second = tier("second");
        let engine = KvEngine::builder()
            .tier_arc(first.clone())
            .tier_arc(second.clone())
            .write_policy(KvWritePolicy::All)
            .build()
            .expect("engine");
        let key = key(&[11]);
        engine
            .put(
                KvBlock::new(key.clone(), b"payload".to_vec()).expect("block"),
                None,
                true,
            )
            .expect("put");
        assert!(first.get(&key).expect("first").is_some());
        assert!(second.get(&key).expect("second").is_some());
    }

    #[test]
    fn prefetch_populates_requested_tier_in_background() {
        let fast = tier("fast");
        let slow = tier("slow");
        let engine = KvEngine::builder()
            .tier_arc(fast.clone())
            .tier_arc(slow.clone())
            .promote_on_read(false)
            .build()
            .expect("engine");
        let key = key(&[21, 22]);
        slow.put(&KvTierEntry {
            block: KvBlock::new(key.clone(), b"prefetched".to_vec()).expect("block"),
            expires_at: 0,
            pinned: false,
        })
        .expect("seed");

        let report = engine
            .prefetch_to(vec![key.clone()], 0)
            .wait()
            .expect("prefetch");
        assert_eq!(report.requested, 1);
        assert_eq!(report.populated, 1);
        assert_eq!(report.missed, 0);
        assert!(fast.get(&key).expect("fast").is_some());
    }

    #[test]
    fn move_block_can_remove_source() {
        let first = tier("first");
        let second = tier("second");
        let engine = KvEngine::builder()
            .tier_arc(first.clone())
            .tier_arc(second.clone())
            .build()
            .expect("engine");
        let key = key(&[31]);
        first
            .put(&KvTierEntry {
                block: KvBlock::new(key.clone(), b"move".to_vec()).expect("block"),
                expires_at: 0,
                pinned: false,
            })
            .expect("seed");

        assert!(engine.move_block(&key, 0, 1, true).expect("move"));
        assert!(first.get(&key).expect("source").is_none());
        assert_eq!(
            second
                .get(&key)
                .expect("destination")
                .expect("entry")
                .block
                .bytes,
            b"move"
        );
    }

    #[test]
    fn engine_expires_entries_using_injected_clock() {
        let clock = Arc::new(ManualClock::new(100));
        let only = tier("only");
        let engine = KvEngine::builder()
            .tier_arc(only.clone())
            .clock_arc(clock.clone())
            .build()
            .expect("engine");
        let key = key(&[41]);
        engine
            .put_to(
                0,
                KvBlock::new(key.clone(), b"ttl".to_vec()).expect("block"),
                Some(Duration::from_secs(5)),
                false,
            )
            .expect("put");
        assert!(engine.get(&key).expect("before").is_some());
        clock.advance(5);
        assert!(engine.get(&key).expect("after").is_none());
        assert!(only.get(&key).expect("removed").is_none());
    }

    #[test]
    fn envelope_round_trip_preserves_metadata() {
        let key = key(&[51, 52]);
        let entry = KvTierEntry {
            block: KvBlock::new(key.clone(), vec![1, 2, 3, 4]).expect("block"),
            expires_at: 777,
            pinned: true,
        };
        let encoded = encode_tier_entry(&entry).expect("encode");
        let decoded = decode_tier_entry(key, &encoded).expect("decode");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn write_through_rolls_back_earlier_tiers_on_failure() {
        let first = tier("first");
        let engine = KvEngine::builder()
            .tier_arc(first.clone())
            .tier(FailingTier {
                name: "failing".to_string(),
            })
            .write_policy(KvWritePolicy::All)
            .build()
            .expect("engine");
        let key = key(&[61]);
        let error = engine
            .put(
                KvBlock::new(key.clone(), b"rollback".to_vec()).expect("block"),
                None,
                false,
            )
            .expect_err("write must fail");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(first.get(&key).expect("first tier").is_none());
    }

    #[test]
    fn prefetch_replaces_expired_destination_entry() {
        let clock = Arc::new(ManualClock::new(100));
        let fast = tier("fast");
        let slow = tier("slow");
        let engine = KvEngine::builder()
            .tier_arc(fast.clone())
            .tier_arc(slow.clone())
            .clock_arc(clock.clone())
            .promote_on_read(false)
            .build()
            .expect("engine");
        let key = key(&[71]);
        fast.put(&KvTierEntry {
            block: KvBlock::new(key.clone(), b"stale".to_vec()).expect("stale block"),
            expires_at: 101,
            pinned: false,
        })
        .expect("seed fast");
        slow.put(&KvTierEntry {
            block: KvBlock::new(key.clone(), b"fresh".to_vec()).expect("fresh block"),
            expires_at: 0,
            pinned: false,
        })
        .expect("seed slow");
        clock.advance(1);

        let report = engine
            .prefetch_to(vec![key.clone()], 0)
            .wait()
            .expect("prefetch");
        assert_eq!(report.populated, 1);
        assert_eq!(
            fast.get(&key)
                .expect("fast")
                .expect("replacement")
                .block
                .bytes,
            b"fresh"
        );
        assert_eq!(engine.stats().expect("stats").expirations, 1);
    }

    #[test]
    fn runtime_adapter_capture_and_restore_are_validated() {
        let cache_tier = tier("runtime-cache");
        let engine = KvEngine::builder()
            .tier_arc(cache_tier)
            .build()
            .expect("engine");
        let request = KvCaptureRequest {
            model_fingerprint: "runtime-model".to_string(),
            tokens: vec![1, 2, 3, 4],
            block_tokens: 2,
            layer_start: 0,
            layer_count: 8,
            layout_version: 1,
        };
        let adapter = RecordingAdapter::default();
        let captured = engine
            .capture_from(&adapter, &request, None, false)
            .expect("capture");
        assert_eq!(captured, 2);
        let keys = request.block_keys().expect("keys");
        assert!(engine.restore_into(&adapter, &keys).expect("restore"));
        assert_eq!(
            adapter.restored.lock().expect("restored lock").len(),
            keys.len()
        );
    }

    #[test]
    fn prefix_index_rejects_cross_model_block_keys() {
        let index = PrefixIndex::new();
        let wrong = KvBlockKey::from_prefix(
            "other-model",
            &[1, 2],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 2,
                layer_start: 0,
                layer_count: 4,
                layout_version: 1,
            },
        );
        let error = index
            .register("model-a", vec![1, 2], vec![wrong])
            .expect_err("model mismatch must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
