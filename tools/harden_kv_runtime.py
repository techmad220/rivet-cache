from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}: {old!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# Write-through is rollback-safe for replicas created by the current call.
replace_once(
    "src/kv.rs",
    """            KvWritePolicy::All => {
                for index in 0..self.inner.tiers.len() {
                    self.put_entry_to(index, &entry)?;
                }
            }
""",
    """            KvWritePolicy::All => {
                let mut written = Vec::new();
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
""",
)

# Destination entries in prefetch are checked for expiry rather than blindly
# treated as populated.
replace_once(
    "src/kv.rs",
    """        for key in keys {
            if self.inner.tiers[destination_index].get(&key)?.is_some() {
                populated = populated.saturating_add(1);
                continue;
            }

            let mut found = None;
""",
    """        for key in keys {
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
""",
)
replace_once(
    "src/kv.rs",
    """                    if is_expired(entry.expires_at, self.inner.clock.now_seconds()) {
""",
    """                    if is_expired(entry.expires_at, now) {
""",
)

# Capture request refuses values that cannot be represented by the public u32
# block identity fields.
replace_once(
    "src/kv.rs",
    """        if self.layer_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "layer_count must be greater than zero",
            ));
        }

        let mut keys = Vec::new();
""",
    """        if self.layer_count == 0 {
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
""",
)

# Prefix registrations are internally consistent with their declared model.
replace_once(
    "src/kv.rs",
    """        if tokens.is_empty() || block_keys.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "prefix records require tokens and block keys",
            ));
        }
        let mut records = self
""",
    """        if tokens.is_empty() || block_keys.is_empty() {
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
""",
)

# Runtime adapters can now be orchestrated directly through the engine.
replace_once(
    "src/kv.rs",
    """    pub fn invalidate(&self, key: &KvBlockKey) -> io::Result<()> {
""",
    """    pub fn capture_from(
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
""",
)

# Add a deterministic test-only tier that can fail writes.
test_anchor = """    fn tier(name: &str) -> Arc<dyn KvTier> {
        let cache = Arc::new(
            ContextCache::builder()
                .memory_capacity(1024 * 1024)
                .build()
                .expect("cache"),
        );
        Arc::new(ContextCacheTier::new(name, cache).expect("tier"))
    }

"""
test_add = """    struct FailingTier {
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

"""
replace_once("src/kv.rs", test_anchor, test_anchor + test_add)

# Add hardening tests before the final test-module brace.
p = Path("src/kv.rs")
text = p.read_text(encoding="utf-8")
addition = r'''

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
'''
idx = text.rfind("\n}")
if idx < 0:
    raise SystemExit("could not find kv test module closing brace")
p.write_text(text[:idx] + addition + text[idx:], encoding="utf-8")

# Documentation: write-through rollback and direct runtime orchestration.
p = Path("docs/KV_RUNTIME.md")
text = p.read_text(encoding="utf-8")
text = text.replace(
    "A transfer is considered complete only after the destination tier accepts the transferred entry; explicit source removal happens afterward.",
    "A transfer is considered complete only after the destination tier accepts the transferred entry; explicit source removal happens afterward. `KvWritePolicy::All` rolls back replicas written by the current call if a later tier rejects the write. Rollback removal is best-effort so the original backend error is preserved.",
)
text = text.replace(
    "`RuntimeKvAdapter` is the engine boundary. An adapter captures blocks for a `KvCaptureRequest` and restores a set of blocks into its runtime.",
    "`RuntimeKvAdapter` is the engine boundary. An adapter captures blocks for a `KvCaptureRequest` and restores a set of blocks into its runtime. `KvEngine::capture_from` verifies that returned block identities exactly match the requested block identities before caching them, and `restore_into` restores only when every requested block is available.",
)
p.write_text(text, encoding="utf-8")

p = Path("CHANGELOG.md")
text = p.read_text(encoding="utf-8")
needle = "- Added deterministic tests for key identity, prefix lookup, promotion, prefetch, movement, write-through, metadata preservation, and TTL.\n"
extra = "- Hardened write-through with best-effort rollback, expired-destination prefetch replacement, capture-range overflow checks, runtime adapter identity validation, and cross-model prefix-index validation.\n"
if extra not in text:
    text = text.replace(needle, needle + extra, 1)
p.write_text(text, encoding="utf-8")

print("RIVET_KV_HARDENING=READY")
