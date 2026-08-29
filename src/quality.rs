use crate::{KvBlock, KvEngine, RelocatableRuntimeKvAdapter, ReuseSpan, SegmentIndex};
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRange {
    pub start: usize,
    pub len: usize,
}

impl TokenRange {
    pub fn end(self) -> usize {
        self.start.saturating_add(self.len)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QualityReusePolicy {
    pub min_match_tokens: usize,
    pub boundary_recompute_tokens: usize,
    pub max_segments: usize,
    pub min_coverage_per_mille: u16,
}

impl QualityReusePolicy {
    pub fn validate(self) -> io::Result<Self> {
        if self.min_match_tokens == 0
            || self.max_segments == 0
            || self.max_segments > 1024
            || self.min_coverage_per_mille > 1000
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "quality reuse policy has invalid match/segment/coverage limits",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityReusePlan {
    pub spans: Vec<ReuseSpan>,
    pub recompute_ranges: Vec<TokenRange>,
    pub query_tokens: usize,
    pub reused_tokens: usize,
    pub coverage_per_mille: u16,
}

pub trait QualityAwareRuntimeKvAdapter: RelocatableRuntimeKvAdapter {
    /// Recompute the supplied query-token ranges after exact KV restoration. Implementations
    /// should overwrite restored state for those ranges before generation continues.
    fn recompute_ranges(&self, query_tokens: &[u32], ranges: &[TokenRange]) -> io::Result<()>;
}

pub fn plan_quality_reuse(
    index: &SegmentIndex,
    model_fingerprint: &str,
    query_tokens: &[u32],
    policy: QualityReusePolicy,
) -> io::Result<Option<QualityReusePlan>> {
    let policy = policy.validate()?;
    if query_tokens.is_empty() {
        return Ok(None);
    }

    let mut spans = Vec::new();
    let mut ranges = vec![(0_usize, query_tokens.len())];
    while !ranges.is_empty() && spans.len() < policy.max_segments {
        let mut best_choice: Option<(usize, usize, ReuseSpan)> = None;
        for (range_index, (start, end)) in ranges.iter().copied().enumerate() {
            if end.saturating_sub(start) < policy.min_match_tokens {
                continue;
            }
            let Some(mut span) = index.best_segment(
                model_fingerprint,
                &query_tokens[start..end],
                policy.min_match_tokens,
            )? else {
                continue;
            };
            span.query_start = span.query_start.saturating_add(start);
            let replace = best_choice
                .as_ref()
                .map(|(_, _, current)| {
                    span.matched_tokens > current.matched_tokens
                        || (span.matched_tokens == current.matched_tokens
                            && span.query_start < current.query_start)
                })
                .unwrap_or(true);
            if replace {
                best_choice = Some((range_index, start, span));
            }
        }
        let Some((range_index, range_start, span)) = best_choice else {
            break;
        };
        let (_, range_end) = ranges.swap_remove(range_index);
        let span_end = span.query_start.saturating_add(span.matched_tokens);
        if span.query_start > range_start {
            ranges.push((range_start, span.query_start));
        }
        if span_end < range_end {
            ranges.push((span_end, range_end));
        }
        spans.push(span);
    }

    if spans.is_empty() {
        return Ok(None);
    }
    spans.sort_by_key(|span| span.query_start);
    let reused_tokens = spans
        .iter()
        .fold(0_usize, |total, span| total.saturating_add(span.matched_tokens));
    let coverage = reused_tokens
        .saturating_mul(1000)
        .checked_div(query_tokens.len())
        .unwrap_or(0)
        .min(1000) as u16;
    if coverage < policy.min_coverage_per_mille {
        return Ok(None);
    }

    let recompute_ranges = boundary_ranges(
        &spans,
        query_tokens.len(),
        policy.boundary_recompute_tokens,
    );
    Ok(Some(QualityReusePlan {
        spans,
        recompute_ranges,
        query_tokens: query_tokens.len(),
        reused_tokens,
        coverage_per_mille: coverage,
    }))
}

pub fn apply_quality_reuse(
    engine: &KvEngine,
    adapter: &dyn QualityAwareRuntimeKvAdapter,
    query_tokens: &[u32],
    plan: &QualityReusePlan,
) -> io::Result<bool> {
    if query_tokens.len() != plan.query_tokens || plan.spans.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "quality reuse plan does not match the supplied query",
        ));
    }

    // Load every block before mutating runtime state. A miss therefore leaves the runtime
    // untouched rather than partially restoring a multi-segment plan.
    let mut loaded: Vec<(usize, Vec<KvBlock>)> = Vec::with_capacity(plan.spans.len());
    for span in &plan.spans {
        let mut blocks = Vec::with_capacity(span.source_keys.len());
        for key in &span.source_keys {
            let Some(block) = engine.get(key)? else {
                return Ok(false);
            };
            blocks.push(block);
        }
        loaded.push((span.query_start, blocks));
    }

    for (target, blocks) in loaded {
        if target > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "quality reuse target exceeds runtime position range",
            ));
        }
        adapter.restore_relocated(&blocks, target as u32)?;
    }
    if !plan.recompute_ranges.is_empty() {
        adapter.recompute_ranges(query_tokens, &plan.recompute_ranges)?;
    }
    Ok(true)
}

fn boundary_ranges(spans: &[ReuseSpan], query_len: usize, halo: usize) -> Vec<TokenRange> {
    if halo == 0 {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    for span in spans {
        let end = span
            .query_start
            .saturating_add(span.matched_tokens)
            .min(query_len);
        let left_len = halo.min(span.matched_tokens).min(query_len.saturating_sub(span.query_start));
        if left_len > 0 {
            ranges.push(TokenRange {
                start: span.query_start,
                len: left_len,
            });
        }
        let right_len = halo.min(span.matched_tokens).min(end);
        if right_len > 0 {
            ranges.push(TokenRange {
                start: end.saturating_sub(right_len),
                len: right_len,
            });
        }
    }
    merge_ranges(ranges)
}

fn merge_ranges(mut ranges: Vec<TokenRange>) -> Vec<TokenRange> {
    ranges.retain(|range| range.len > 0);
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<TokenRange> = Vec::new();
    for range in ranges {
        if let Some(last) = merged.last_mut() {
            if range.start <= last.end() {
                let end = last.end().max(range.end());
                last.len = end.saturating_sub(last.start);
                continue;
            }
        }
        merged.push(range);
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvBlockKey, KvBlockRange, KvCaptureRequest};

    fn keys(tokens: &[u32], block_tokens: usize) -> Vec<KvBlockKey> {
        KvCaptureRequest {
            model_fingerprint: "m".to_owned(),
            tokens: tokens.to_vec(),
            block_tokens,
            layer_start: 0,
            layer_count: 8,
            layout_version: 1,
        }
        .block_keys()
        .unwrap()
    }

    #[test]
    fn plans_multiple_arbitrary_position_spans_with_boundary_recompute() {
        let index = SegmentIndex::new();
        let source_a = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let source_b = vec![20, 21, 22, 23, 24, 25, 26, 27];
        index.register("m", source_a.clone(), keys(&source_a, 2)).unwrap();
        index.register("m", source_b.clone(), keys(&source_b, 2)).unwrap();
        let query = vec![90, 91, 3, 4, 5, 6, 99, 20, 21, 22, 23, 77];
        let plan = plan_quality_reuse(
            &index,
            "m",
            &query,
            QualityReusePolicy {
                min_match_tokens: 4,
                boundary_recompute_tokens: 1,
                max_segments: 4,
                min_coverage_per_mille: 500,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(plan.spans.len(), 2);
        assert_eq!(plan.spans[0].query_start, 2);
        assert_eq!(plan.spans[1].query_start, 7);
        assert!(plan.coverage_per_mille >= 600);
        assert!(plan.recompute_ranges.iter().any(|range| range.start == 2));
    }

    #[test]
    fn coverage_guard_rejects_low_value_reuse() {
        let index = SegmentIndex::new();
        let source = vec![1, 2, 3, 4];
        index.register("m", source.clone(), keys(&source, 2)).unwrap();
        let query = vec![0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0];
        assert!(plan_quality_reuse(
            &index,
            "m",
            &query,
            QualityReusePolicy {
                min_match_tokens: 4,
                boundary_recompute_tokens: 1,
                max_segments: 2,
                min_coverage_per_mille: 500,
            },
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn token_range_end_saturates() {
        assert_eq!(
            TokenRange {
                start: usize::MAX,
                len: 4
            }
            .end(),
            usize::MAX
        );
    }

    #[test]
    fn source_key_shape_is_still_block_aligned() {
        let key = KvBlockKey::from_prefix(
            "m",
            &[1, 2],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 2,
                layer_start: 0,
                layer_count: 1,
                layout_version: 1,
            },
        );
        assert_eq!(key.token_count, 2);
    }
}
