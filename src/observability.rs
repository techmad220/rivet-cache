use crate::{KvBlockKey, KvTier, KvTierEntry};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    name: String,
    labels: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct Histogram {
    buckets: [u64; 8],
    count: u64,
    sum_micros: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; 8],
            count: 0,
            sum_micros: 0,
        }
    }
}

#[derive(Debug, Clone)]
enum MetricState {
    Counter(u64),
    Gauge(i64),
    Histogram(Histogram),
}

#[derive(Default)]
pub struct PrometheusRegistry {
    metrics: Mutex<BTreeMap<MetricKey, (MetricKind, MetricState)>>,
}

impl PrometheusRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_counter(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        delta: u64,
    ) -> io::Result<()> {
        let key = metric_key(name, labels)?;
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| io::Error::other("metrics registry lock poisoned"))?;
        let entry = metrics
            .entry(key)
            .or_insert((MetricKind::Counter, MetricState::Counter(0)));
        match &mut entry.1 {
            MetricState::Counter(value) => {
                *value = value.saturating_add(delta);
                Ok(())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metric already exists with a different type",
            )),
        }
    }

    pub fn set_gauge(&self, name: &str, labels: &[(&str, &str)], value: i64) -> io::Result<()> {
        let key = metric_key(name, labels)?;
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| io::Error::other("metrics registry lock poisoned"))?;
        let entry = metrics
            .entry(key)
            .or_insert((MetricKind::Gauge, MetricState::Gauge(value)));
        match &mut entry.1 {
            MetricState::Gauge(current) => {
                *current = value;
                Ok(())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metric already exists with a different type",
            )),
        }
    }

    pub fn observe_micros(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        micros: u64,
    ) -> io::Result<()> {
        const LIMITS: [u64; 7] = [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
        let key = metric_key(name, labels)?;
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| io::Error::other("metrics registry lock poisoned"))?;
        let entry = metrics
            .entry(key)
            .or_insert((MetricKind::Histogram, MetricState::Histogram(Histogram::default())));
        match &mut entry.1 {
            MetricState::Histogram(histogram) => {
                histogram.count = histogram.count.saturating_add(1);
                histogram.sum_micros = histogram.sum_micros.saturating_add(micros);
                for (index, limit) in LIMITS.iter().enumerate() {
                    if micros <= *limit {
                        histogram.buckets[index] = histogram.buckets[index].saturating_add(1);
                    }
                }
                histogram.buckets[7] = histogram.buckets[7].saturating_add(1);
                Ok(())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metric already exists with a different type",
            )),
        }
    }

    pub fn render(&self) -> io::Result<String> {
        const LIMITS: [&str; 7] = ["10", "100", "1000", "10000", "100000", "1000000", "10000000"];
        let metrics = self
            .metrics
            .lock()
            .map_err(|_| io::Error::other("metrics registry lock poisoned"))?;
        let mut output = String::new();
        let mut emitted_type: BTreeMap<&str, MetricKind> = BTreeMap::new();
        for (key, (kind, state)) in metrics.iter() {
            if !emitted_type.contains_key(key.name.as_str()) {
                let kind_name = match kind {
                    MetricKind::Counter => "counter",
                    MetricKind::Gauge => "gauge",
                    MetricKind::Histogram => "histogram",
                };
                output.push_str("# TYPE ");
                output.push_str(&key.name);
                output.push(' ');
                output.push_str(kind_name);
                output.push('\n');
                emitted_type.insert(key.name.as_str(), *kind);
            }
            match state {
                MetricState::Counter(value) => line(&mut output, &key.name, &key.labels, None, *value as i128),
                MetricState::Gauge(value) => line(&mut output, &key.name, &key.labels, None, *value as i128),
                MetricState::Histogram(histogram) => {
                    for (index, count) in histogram.buckets.iter().enumerate() {
                        let le = if index < LIMITS.len() { LIMITS[index] } else { "+Inf" };
                        line(
                            &mut output,
                            &format!("{}_bucket", key.name),
                            &key.labels,
                            Some(("le", le)),
                            *count as i128,
                        );
                    }
                    line(
                        &mut output,
                        &format!("{}_count", key.name),
                        &key.labels,
                        None,
                        histogram.count as i128,
                    );
                    line(
                        &mut output,
                        &format!("{}_sum", key.name),
                        &key.labels,
                        None,
                        histogram.sum_micros as i128,
                    );
                }
            }
        }
        Ok(output)
    }
}

pub struct InstrumentedKvTier {
    name: String,
    inner: Arc<dyn KvTier>,
    metrics: Arc<PrometheusRegistry>,
}

impl InstrumentedKvTier {
    pub fn new(
        name: impl Into<String>,
        inner: Arc<dyn KvTier>,
        metrics: Arc<PrometheusRegistry>,
    ) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "tier name must not be empty"));
        }
        Ok(Self { name, inner, metrics })
    }

    pub fn registry(&self) -> Arc<PrometheusRegistry> {
        Arc::clone(&self.metrics)
    }

    fn finish<T>(&self, operation: &str, started: Instant, result: io::Result<T>) -> io::Result<T> {
        let status = if result.is_ok() { "ok" } else { "error" };
        let elapsed = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        let _ = self.metrics.inc_counter(
            "rivet_kv_tier_requests_total",
            &[("tier", &self.name), ("operation", operation), ("status", status)],
            1,
        );
        let _ = self.metrics.observe_micros(
            "rivet_kv_tier_operation_micros",
            &[("tier", &self.name), ("operation", operation)],
            elapsed,
        );
        result
    }
}

impl KvTier for InstrumentedKvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let started = Instant::now();
        let result = self.inner.get(key);
        if let Ok(Some(entry)) = &result {
            let _ = self.metrics.inc_counter(
                "rivet_kv_tier_bytes_total",
                &[("tier", &self.name), ("direction", "read")],
                entry.block.bytes.len() as u64,
            );
        }
        self.finish("get", started, result)
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        let started = Instant::now();
        let result = self.inner.put(entry);
        if result.is_ok() {
            let _ = self.metrics.inc_counter(
                "rivet_kv_tier_bytes_total",
                &[("tier", &self.name), ("direction", "write")],
                entry.block.bytes.len() as u64,
            );
        }
        self.finish("put", started, result)
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        let started = Instant::now();
        self.finish("remove", started, self.inner.remove(key))
    }

    fn clear(&self) -> io::Result<()> {
        let started = Instant::now();
        self.finish("clear", started, self.inner.clear())
    }

    fn health(&self) -> io::Result<()> {
        let started = Instant::now();
        self.finish("health", started, self.inner.health())
    }
}

fn metric_key(name: &str, labels: &[(&str, &str)]) -> io::Result<MetricKey> {
    validate_metric_name(name)?;
    let mut normalized = Vec::with_capacity(labels.len());
    for (key, value) in labels {
        validate_label_name(key)?;
        normalized.push(((*key).to_owned(), (*value).to_owned()));
    }
    normalized.sort();
    normalized.dedup_by(|left, right| left.0 == right.0);
    Ok(MetricKey {
        name: name.to_owned(),
        labels: normalized,
    })
}

fn validate_metric_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' ))
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid Prometheus metric name"));
    }
    Ok(())
}

fn validate_label_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid Prometheus label name"));
    }
    Ok(())
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn line(
    output: &mut String,
    name: &str,
    labels: &[(String, String)],
    extra: Option<(&str, &str)>,
    value: i128,
) {
    output.push_str(name);
    if !labels.is_empty() || extra.is_some() {
        output.push('{');
        let mut first = true;
        for (key, label_value) in labels {
            if !first {
                output.push(',');
            }
            first = false;
            output.push_str(key);
            output.push_str("=\"");
            output.push_str(&escape_label(label_value));
            output.push('"');
        }
        if let Some((key, label_value)) = extra {
            if !first {
                output.push(',');
            }
            output.push_str(key);
            output.push_str("=\"");
            output.push_str(&escape_label(label_value));
            output.push('"');
        }
        output.push('}');
    }
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvBlock, KvBlockRange};
    use std::collections::HashMap;

    #[derive(Default)]
    struct MemoryTier {
        entries: Mutex<HashMap<String, KvTierEntry>>,
    }

    impl KvTier for MemoryTier {
        fn name(&self) -> &str {
            "inner"
        }

        fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
            Ok(self.entries.lock().unwrap().get(&key.cache_key()).cloned())
        }

        fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
            self.entries
                .lock()
                .unwrap()
                .insert(entry.block.key.cache_key(), entry.clone());
            Ok(())
        }

        fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
            self.entries.lock().unwrap().remove(&key.cache_key());
            Ok(())
        }

        fn clear(&self) -> io::Result<()> {
            self.entries.lock().unwrap().clear();
            Ok(())
        }
    }

    fn entry() -> KvTierEntry {
        let tokens = [1, 2, 3, 4];
        let key = KvBlockKey::from_prefix(
            "model",
            &tokens,
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 4,
                layer_start: 0,
                layer_count: 8,
                layout_version: 1,
            },
        );
        KvTierEntry {
            block: KvBlock::new(key, vec![4; 32]).unwrap(),
            expires_at: 0,
            pinned: false,
        }
    }

    #[test]
    fn renders_prometheus_histograms_and_labels() {
        let registry = PrometheusRegistry::new();
        registry.inc_counter("rivet_requests_total", &[("tier", "hot")], 2).unwrap();
        registry.observe_micros("rivet_latency_micros", &[("tier", "hot")], 125).unwrap();
        let rendered = registry.render().unwrap();
        assert!(rendered.contains("# TYPE rivet_requests_total counter"));
        assert!(rendered.contains("rivet_requests_total{tier=\"hot\"} 2"));
        assert!(rendered.contains("rivet_latency_micros_bucket{tier=\"hot\",le=\"1000\"} 1"));
    }

    #[test]
    fn instrumented_tier_counts_bytes_and_operations() {
        let registry = Arc::new(PrometheusRegistry::new());
        let tier = InstrumentedKvTier::new(
            "hot",
            Arc::new(MemoryTier::default()),
            Arc::clone(&registry),
        )
        .unwrap();
        let entry = entry();
        tier.put(&entry).unwrap();
        assert_eq!(tier.get(&entry.block.key).unwrap().unwrap(), entry);
        let rendered = registry.render().unwrap();
        assert!(rendered.contains("operation=\"put\""));
        assert!(rendered.contains("direction=\"write\""));
        assert!(rendered.contains("direction=\"read\""));
    }
}
