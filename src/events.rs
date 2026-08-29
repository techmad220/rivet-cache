use crate::PrometheusRegistry;
use std::collections::BTreeMap;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvEventKind {
    Lookup,
    Store,
    Retrieve,
    Move,
    Prefetch,
    Invalidate,
    Pin,
    Unpin,
    Clear,
    Health,
    Controller,
}

impl KvEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lookup => "lookup",
            Self::Store => "store",
            Self::Retrieve => "retrieve",
            Self::Move => "move",
            Self::Prefetch => "prefetch",
            Self::Invalidate => "invalidate",
            Self::Pin => "pin",
            Self::Unpin => "unpin",
            Self::Clear => "clear",
            Self::Health => "health",
            Self::Controller => "controller",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvEventStatus {
    Started,
    Completed,
    Miss,
    Error,
}

impl KvEventStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Miss => "miss",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEvent {
    pub sequence: u64,
    pub timestamp_micros: u64,
    pub request_id: Option<u64>,
    pub kind: KvEventKind,
    pub status: KvEventStatus,
    pub model_fingerprint: Option<String>,
    pub tier: Option<String>,
    pub blocks: u64,
    pub bytes: u64,
    pub duration_micros: u64,
    pub detail: Option<String>,
}

impl KvEvent {
    pub fn new(kind: KvEventKind, status: KvEventStatus) -> Self {
        Self {
            sequence: 0,
            timestamp_micros: 0,
            request_id: None,
            kind,
            status,
            model_fingerprint: None,
            tier: None,
            blocks: 0,
            bytes: 0,
            duration_micros: 0,
            detail: None,
        }
    }

    pub fn request_id(mut self, request_id: u64) -> Self {
        self.request_id = Some(request_id);
        self
    }

    pub fn model(mut self, model_fingerprint: impl Into<String>) -> Self {
        self.model_fingerprint = Some(model_fingerprint.into());
        self
    }

    pub fn tier(mut self, tier: impl Into<String>) -> Self {
        self.tier = Some(tier.into());
        self
    }

    pub fn counts(mut self, blocks: u64, bytes: u64) -> Self {
        self.blocks = blocks;
        self.bytes = bytes;
        self
    }

    pub fn duration_micros(mut self, duration_micros: u64) -> Self {
        self.duration_micros = duration_micros;
        self
    }

    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

pub trait KvEventSubscriber: Send + Sync {
    fn on_event(&self, event: &KvEvent);
}

#[derive(Default)]
pub struct KvEventBus {
    subscribers: RwLock<BTreeMap<u64, Arc<dyn KvEventSubscriber>>>,
    next_subscriber: AtomicU64,
    next_sequence: AtomicU64,
    published: AtomicU64,
    subscriber_failures: AtomicU64,
}

impl KvEventBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, subscriber: Arc<dyn KvEventSubscriber>) -> io::Result<u64> {
        let id = self
            .next_subscriber
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.subscribers
            .write()
            .map_err(|_| io::Error::other("event subscriber registry lock poisoned"))?
            .insert(id, subscriber);
        Ok(id)
    }

    pub fn unsubscribe(&self, id: u64) -> io::Result<bool> {
        Ok(self
            .subscribers
            .write()
            .map_err(|_| io::Error::other("event subscriber registry lock poisoned"))?
            .remove(&id)
            .is_some())
    }

    pub fn publish(&self, mut event: KvEvent) -> io::Result<KvEvent> {
        event.sequence = self
            .next_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        event.timestamp_micros = now_micros()?;

        let subscribers = self
            .subscribers
            .read()
            .map_err(|_| io::Error::other("event subscriber registry lock poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();

        for subscriber in subscribers {
            if catch_unwind(AssertUnwindSafe(|| subscriber.on_event(&event))).is_err() {
                self.subscriber_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.published.fetch_add(1, Ordering::Relaxed);
        Ok(event)
    }

    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    pub fn subscriber_failures(&self) -> u64 {
        self.subscriber_failures.load(Ordering::Relaxed)
    }
}

pub trait OtelEventExporter: Send + Sync {
    fn export(&self, event: &KvEvent) -> io::Result<()>;
}

pub struct OtelEventSubscriber {
    exporter: Arc<dyn OtelEventExporter>,
    failures: AtomicU64,
}

impl OtelEventSubscriber {
    pub fn new(exporter: Arc<dyn OtelEventExporter>) -> Self {
        Self {
            exporter,
            failures: AtomicU64::new(0),
        }
    }

    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
}

impl KvEventSubscriber for OtelEventSubscriber {
    fn on_event(&self, event: &KvEvent) {
        if self.exporter.export(event).is_err() {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct PrometheusEventSubscriber {
    registry: Arc<PrometheusRegistry>,
}

impl PrometheusEventSubscriber {
    pub fn new(registry: Arc<PrometheusRegistry>) -> Self {
        Self { registry }
    }
}

impl KvEventSubscriber for PrometheusEventSubscriber {
    fn on_event(&self, event: &KvEvent) {
        let operation = event.kind.as_str();
        let status = event.status.as_str();
        let _ = self.registry.inc_counter(
            "rivet_event_total",
            &[("operation", operation), ("status", status)],
            1,
        );
        if event.bytes > 0 {
            let _ = self.registry.inc_counter(
                "rivet_event_bytes_total",
                &[("operation", operation), ("status", status)],
                event.bytes,
            );
        }
        if event.duration_micros > 0 {
            let _ = self.registry.observe_micros(
                "rivet_event_duration_micros",
                &[("operation", operation), ("status", status)],
                event.duration_micros,
            );
        }
    }
}

fn now_micros() -> io::Result<u64> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| io::Error::other("system clock is before Unix epoch"))?
        .as_micros();
    Ok(micros.min(u128::from(u64::MAX)) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<KvEvent>>);

    impl KvEventSubscriber for Recorder {
        fn on_event(&self, event: &KvEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

    #[test]
    fn bus_sequences_and_unsubscribes() {
        let bus = KvEventBus::new();
        let recorder = Arc::new(Recorder::default());
        let id = bus.subscribe(recorder.clone()).unwrap();
        let first = bus
            .publish(KvEvent::new(KvEventKind::Lookup, KvEventStatus::Started))
            .unwrap();
        let second = bus
            .publish(KvEvent::new(KvEventKind::Lookup, KvEventStatus::Completed))
            .unwrap();
        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert!(second.timestamp_micros >= first.timestamp_micros);
        assert_eq!(recorder.0.lock().unwrap().len(), 2);
        assert!(bus.unsubscribe(id).unwrap());
        bus.publish(KvEvent::new(KvEventKind::Health, KvEventStatus::Completed))
            .unwrap();
        assert_eq!(recorder.0.lock().unwrap().len(), 2);
    }

    #[test]
    fn prometheus_subscriber_records_events() {
        let registry = Arc::new(PrometheusRegistry::new());
        let subscriber = PrometheusEventSubscriber::new(registry.clone());
        subscriber.on_event(
            &KvEvent::new(KvEventKind::Store, KvEventStatus::Completed)
                .counts(2, 4096)
                .duration_micros(123),
        );
        let rendered = registry.render().unwrap();
        assert!(rendered.contains("rivet_event_total"));
        assert!(rendered.contains("rivet_event_bytes_total"));
        assert!(rendered.contains("rivet_event_duration_micros"));
    }
}