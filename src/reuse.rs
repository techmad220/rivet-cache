use crate::kv::{KvBlock, KvBlockKey, KvEngine, RuntimeKvAdapter};
use std::io;
use std::sync::Mutex;

pub trait RelocatableRuntimeKvAdapter: RuntimeKvAdapter {
    fn restore_relocated(&self, blocks: &[KvBlock], target_token_start: u32) -> io::Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReuseSpan {
    pub query_start: usize,
    pub source_start: usize,
    pub matched_tokens: usize,
    pub source_keys: Vec<KvBlockKey>,
}

#[derive(Debug, Clone)]
struct SegmentRecord {
    model_fingerprint: String,
    tokens: Vec<u32>,
    block_keys: Vec<KvBlockKey>,
}

#[derive(Default)]
pub struct SegmentIndex {
    records: Mutex<Vec<SegmentRecord>>,
}

impl SegmentIndex {
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
        validate_record(&model_fingerprint, &tokens, &block_keys)?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| io::Error::other("segment index lock poisoned"))?;
        records.retain(|record| {
            record.model_fingerprint != model_fingerprint || record.tokens != tokens
        });
        records.push(SegmentRecord {
            model_fingerprint,
            tokens,
            block_keys,
        });
        Ok(())
    }

    pub fn best_segment(
        &self,
        model_fingerprint: &str,
        query_tokens: &[u32],
        min_match_tokens: usize,
    ) -> io::Result<Option<ReuseSpan>> {
        if min_match_tokens == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "min_match_tokens must be greater than zero",
            ));
        }
        if query_tokens.is_empty() {
            return Ok(None);
        }

        let records = self
            .records
            .lock()
            .map_err(|_| io::Error::other("segment index lock poisoned"))?;
        let mut best: Option<ReuseSpan> = None;
        for record in records
            .iter()
            .filter(|record| record.model_fingerprint == model_fingerprint)
        {
            let Some(raw) = longest_common_substring(query_tokens, &record.tokens) else {
                continue;
            };
            if raw.len < min_match_tokens {
                continue;
            }
            let Some(aligned) = align_to_blocks(record, raw) else {
                continue;
            };
            if aligned.matched_tokens < min_match_tokens {
                continue;
            }
            let replace = best
                .as_ref()
                .map(|current| {
                    aligned.matched_tokens > current.matched_tokens
                        || (aligned.matched_tokens == current.matched_tokens
                            && aligned.query_start < current.query_start)
                })
                .unwrap_or(true);
            if replace {
                best = Some(aligned);
            }
        }
        Ok(best)
    }

    pub fn remove_model(&self, model_fingerprint: &str) -> io::Result<usize> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| io::Error::other("segment index lock poisoned"))?;
        let before = records.len();
        records.retain(|record| record.model_fingerprint != model_fingerprint);
        Ok(before.saturating_sub(records.len()))
    }
}

pub fn restore_reuse(
    engine: &KvEngine,
    adapter: &dyn RelocatableRuntimeKvAdapter,
    span: &ReuseSpan,
) -> io::Result<bool> {
    if span.source_keys.is_empty() || span.matched_tokens == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reuse span must contain at least one KV block",
        ));
    }
    if span.query_start > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reuse target position exceeds runtime adapter range",
        ));
    }
    let mut blocks = Vec::with_capacity(span.source_keys.len());
    for key in &span.source_keys {
        let Some(block) = engine.get(key)? else {
            return Ok(false);
        };
        blocks.push(block);
    }
    adapter.restore_relocated(&blocks, span.query_start as u32)?;
    Ok(true)
}

#[derive(Debug, Clone, Copy)]
struct RawMatch {
    query_start: usize,
    source_start: usize,
    len: usize,
}

fn longest_common_substring(query: &[u32], source: &[u32]) -> Option<RawMatch> {
    if query.is_empty() || source.is_empty() {
        return None;
    }
    let mut previous = vec![0_usize; source.len() + 1];
    let mut best = RawMatch {
        query_start: 0,
        source_start: 0,
        len: 0,
    };

    for (query_index, query_token) in query.iter().enumerate() {
        let mut current = vec![0_usize; source.len() + 1];
        for (source_index, source_token) in source.iter().enumerate() {
            if query_token == source_token {
                let len = previous[source_index].saturating_add(1);
                current[source_index + 1] = len;
                if len > best.len {
                    best = RawMatch {
                        query_start: query_index + 1 - len,
                        source_start: source_index + 1 - len,
                        len,
                    };
                }
            }
        }
        previous = current;
    }

    (best.len > 0).then_some(best)
}

fn align_to_blocks(record: &SegmentRecord, raw: RawMatch) -> Option<ReuseSpan> {
    let raw_source_end = raw.source_start.checked_add(raw.len)?;
    let mut keys: Vec<KvBlockKey> = record
        .block_keys
        .iter()
        .filter(|key| {
            let start = key.token_start as usize;
            let end = start.saturating_add(key.token_count as usize);
            start >= raw.source_start && end <= raw_source_end
        })
        .cloned()
        .collect();
    keys.sort_by_key(|key| key.token_start);
    let first = keys.first()?;
    let mut expected = first.token_start as usize;
    let mut contiguous = Vec::new();
    for key in keys {
        if key.token_start as usize != expected {
            break;
        }
        expected = expected.saturating_add(key.token_count as usize);
        contiguous.push(key);
    }
    let first = contiguous.first()?;
    let source_start = first.token_start as usize;
    let source_end = contiguous
        .last()
        .map(|key| key.token_start as usize + key.token_count as usize)?;
    let matched_tokens = source_end.saturating_sub(source_start);
    let query_start = raw
        .query_start
        .saturating_add(source_start.saturating_sub(raw.source_start));
    Some(ReuseSpan {
        query_start,
        source_start,
        matched_tokens,
        source_keys: contiguous,
    })
}

fn validate_record(
    model_fingerprint: &str,
    tokens: &[u32],
    block_keys: &[KvBlockKey],
) -> io::Result<()> {
    if model_fingerprint.trim().is_empty() || tokens.is_empty() || block_keys.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment records require a model fingerprint, tokens, and block keys",
        ));
    }
    let first = &block_keys[0];
    let mut expected_start = first.token_start;
    for key in block_keys {
        if key.model_fingerprint != model_fingerprint
            || key.token_count == 0
            || key.layer_count == 0
            || key.layer_start != first.layer_start
            || key.layer_count != first.layer_count
            || key.layout_version != first.layout_version
            || key.token_start != expected_start
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment block keys must be contiguous and share model/layer/layout identity",
            ));
        }
        let end = (key.token_start as usize).saturating_add(key.token_count as usize);
        if end > tokens.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment block key exceeds registered token sequence",
            ));
        }
        expected_start = key.token_start.saturating_add(key.token_count);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvBlockRange, KvCaptureRequest};

    fn keys(tokens: &[u32], block_tokens: usize) -> Vec<KvBlockKey> {
        KvCaptureRequest {
            model_fingerprint: "m".to_string(),
            tokens: tokens.to_vec(),
            block_tokens,
            layer_start: 0,
            layer_count: 16,
            layout_version: 1,
        }
        .block_keys()
        .expect("keys")
    }

    #[test]
    fn finds_block_aligned_non_prefix_segment() {
        let source = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let index = SegmentIndex::new();
        index
            .register("m", source.clone(), keys(&source, 2))
            .expect("register");
        let query = vec![90, 91, 3, 4, 5, 6, 99];
        let found = index
            .best_segment("m", &query, 4)
            .expect("lookup")
            .expect("match");
        assert_eq!(found.query_start, 2);
        assert_eq!(found.source_start, 2);
        assert_eq!(found.matched_tokens, 4);
        assert_eq!(found.source_keys.len(), 2);
    }

    #[test]
    fn rejects_mismatched_model_keys() {
        let token_vec = vec![1, 2];
        let wrong = KvBlockKey::from_prefix(
            "other",
            &token_vec,
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 2,
                layer_start: 0,
                layer_count: 1,
                layout_version: 1,
            },
        );
        assert!(SegmentIndex::new()
            .register("m", token_vec, vec![wrong])
            .is_err());
    }
}
