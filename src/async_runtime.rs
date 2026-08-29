use crate::{
    KvBlock, KvBlockKey, KvEngine, KvEvent, KvEventBus, KvEventKind, KvEventStatus, KvTierHealth,
};
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncKvOperation {
    Store,
    Retrieve,
    Move,
    Prefetch,
    Invalidate,
    Pin,
    Unpin,
    Clear,
    Health,
}

impl AsyncKvOperation {
    fn event_kind(self) -> KvEventKind {
        match self {
            Self::Store => KvEventKind::Store,
            Self::Retrieve => KvEventKind::Retrieve,
            Self::Move => KvEventKind::Move,
            Self::Prefetch => KvEventKind::Prefetch,
            Self::Invalidate => KvEventKind::Invalidate,
            Self::Pin => KvEventKind::Pin,
            Self::Unpin => KvEventKind::Unpin,
            Self::Clear => KvEventKind::Clear,
            Self::Health => KvEventKind::Health,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncKvJobState {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncKvError {
    pub kind: io::ErrorKind,
    pub message: String,
}

impl AsyncKvError {
    pub fn to_io_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

impl From<io::Error> for AsyncKvError {
    fn from(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncKvResult {
    pub job_id: u64,
    pub operation: AsyncKvOperation,
    pub found: bool,
    pub requested: u64,
    pub completed: u64,
    pub missed: u64,
    pub bytes: u64,
    pub elapsed_micros: u64,
    pub blocks: Vec<KvBlock>,
    pub health: Vec<KvTierHealth>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncKvJobSnapshot {
    pub job_id: u64,
    pub operation: AsyncKvOperation,
    pub state: AsyncKvJobState,
    pub result: Option<AsyncKvResult>,
    pub error: Option<AsyncKvError>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AsyncKvPipelineStats {
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub inflight: u64,
}

#[derive(Debug, Clone)]
struct MoveRequest {
    key: KvBlockKey,
    source_index: usize,
    destination_index: usize,
    remove_source: bool,
}

enum JobPayload {
    Store {
        blocks: Vec<KvBlock>,
        ttl: Option<Duration>,
        pinned: bool,
    },
    Retrieve(Vec<KvBlockKey>),
    Move(Vec<MoveRequest>),
    Prefetch {
        keys: Vec<KvBlockKey>,
        destination_index: usize,
    },
    Invalidate(Vec<KvBlockKey>),
    Pin {
        keys: Vec<KvBlockKey>,
        pinned: bool,
    },
    Clear,
    Health,
}

struct Job {
    id: u64,
    operation: AsyncKvOperation,
    payload: JobPayload,
}

struct PipelineState {
    jobs: Mutex<BTreeMap<u64, AsyncKvJobSnapshot>>,
    wait_lock: Mutex<()>,
    changed: Condvar,
    next_id: AtomicU64,
    submitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    inflight: AtomicU64,
}

impl PipelineState {
    fn new() -> Self {
        Self {
            jobs: Mutex::new(BTreeMap::new()),
            wait_lock: Mutex::new(()),
            changed: Condvar::new(),
            next_id: AtomicU64::new(0),
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
        }
    }

    fn set_running(&self, id: u64) {
        if let Ok(mut jobs) = self.jobs.lock() {
            if let Some(job) = jobs.get_mut(&id) {
                job.state = AsyncKvJobState::Running;
            }
        }
        self.changed.notify_all();
    }

    fn finish(
        &self,
        id: u64,
        operation: AsyncKvOperation,
        result: Result<AsyncKvResult, AsyncKvError>,
    ) {
        if let Ok(mut jobs) = self.jobs.lock() {
            match result {
                Ok(result) => {
                    jobs.insert(
                        id,
                        AsyncKvJobSnapshot {
                            job_id: id,
                            operation,
                            state: AsyncKvJobState::Completed,
                            result: Some(result),
                            error: None,
                        },
                    );
                    self.completed.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    jobs.insert(
                        id,
                        AsyncKvJobSnapshot {
                            job_id: id,
                            operation,
                            state: AsyncKvJobState::Failed,
                            result: None,
                            error: Some(error),
                        },
                    );
                    self.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }

        // submit() reserves the inflight slot before the job can become visible to a worker.
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_all();
    }
}

struct PipelineInner {
    sender: Mutex<Option<mpsc::SyncSender<Job>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    state: Arc<PipelineState>,
}

impl Drop for PipelineInner {
    fn drop(&mut self) {
        match self.sender.get_mut() {
            Ok(sender) => {
                sender.take();
            }
            Err(poisoned) => {
                poisoned.into_inner().take();
            }
        }
        let workers = match self.workers.get_mut() {
            Ok(workers) => workers,
            Err(poisoned) => poisoned.into_inner(),
        };
        for worker in workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[derive(Clone)]
pub struct AsyncKvPipeline {
    inner: Arc<PipelineInner>,
}

impl AsyncKvPipeline {
    pub fn new(
        engine: KvEngine,
        worker_threads: usize,
        queue_capacity: usize,
        events: Option<Arc<KvEventBus>>,
    ) -> io::Result<Self> {
        if worker_threads == 0 || queue_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "async KV pipeline requires non-zero workers and queue capacity",
            ));
        }

        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let state = Arc::new(PipelineState::new());
        let mut workers = Vec::with_capacity(worker_threads);

        for index in 0..worker_threads {
            let engine = engine.clone();
            let receiver = Arc::clone(&receiver);
            let state = Arc::clone(&state);
            let events = events.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("rivet-kv-worker-{index}"))
                    .spawn(move || worker_loop(engine, receiver, state, events))?,
            );
        }

        Ok(Self {
            inner: Arc::new(PipelineInner {
                sender: Mutex::new(Some(sender)),
                workers: Mutex::new(workers),
                state,
            }),
        })
    }

    pub fn submit_store(
        &self,
        blocks: Vec<KvBlock>,
        ttl: Option<Duration>,
        pinned: bool,
    ) -> io::Result<u64> {
        if blocks.is_empty() {
            return invalid("async store requires at least one KV block");
        }
        self.submit(
            AsyncKvOperation::Store,
            JobPayload::Store {
                blocks,
                ttl,
                pinned,
            },
        )
    }

    pub fn submit_retrieve(&self, keys: Vec<KvBlockKey>) -> io::Result<u64> {
        if keys.is_empty() {
            return invalid("async retrieve requires at least one key");
        }
        self.submit(AsyncKvOperation::Retrieve, JobPayload::Retrieve(keys))
    }

    pub fn submit_move(
        &self,
        key: KvBlockKey,
        source_index: usize,
        destination_index: usize,
        remove_source: bool,
    ) -> io::Result<u64> {
        self.submit_move_many(
            vec![key],
            source_index,
            destination_index,
            remove_source,
        )
    }

    pub fn submit_move_many(
        &self,
        keys: Vec<KvBlockKey>,
        source_index: usize,
        destination_index: usize,
        remove_source: bool,
    ) -> io::Result<u64> {
        if keys.is_empty() {
            return invalid("async move requires at least one key");
        }
        let requests = keys
            .into_iter()
            .map(|key| MoveRequest {
                key,
                source_index,
                destination_index,
                remove_source,
            })
            .collect();
        self.submit(AsyncKvOperation::Move, JobPayload::Move(requests))
    }

    pub fn submit_prefetch(
        &self,
        keys: Vec<KvBlockKey>,
        destination_index: usize,
    ) -> io::Result<u64> {
        if keys.is_empty() {
            return invalid("async prefetch requires at least one key");
        }
        self.submit(
            AsyncKvOperation::Prefetch,
            JobPayload::Prefetch {
                keys,
                destination_index,
            },
        )
    }

    pub fn submit_invalidate(&self, keys: Vec<KvBlockKey>) -> io::Result<u64> {
        if keys.is_empty() {
            return invalid("async invalidation requires at least one key");
        }
        self.submit(AsyncKvOperation::Invalidate, JobPayload::Invalidate(keys))
    }

    pub fn submit_set_pinned(&self, keys: Vec<KvBlockKey>, pinned: bool) -> io::Result<u64> {
        if keys.is_empty() {
            return invalid("async pin operation requires at least one key");
        }
        self.submit(
            if pinned {
                AsyncKvOperation::Pin
            } else {
                AsyncKvOperation::Unpin
            },
            JobPayload::Pin { keys, pinned },
        )
    }

    pub fn submit_clear(&self) -> io::Result<u64> {
        self.submit(AsyncKvOperation::Clear, JobPayload::Clear)
    }

    pub fn submit_health(&self) -> io::Result<u64> {
        self.submit(AsyncKvOperation::Health, JobPayload::Health)
    }

    pub fn snapshot(&self, job_id: u64) -> io::Result<Option<AsyncKvJobSnapshot>> {
        Ok(self
            .inner
            .state
            .jobs
            .lock()
            .map_err(|_| io::Error::other("async KV job registry lock poisoned"))?
            .get(&job_id)
            .cloned())
    }

    pub fn take(&self, job_id: u64) -> io::Result<Option<AsyncKvJobSnapshot>> {
        let mut jobs = self
            .inner
            .state
            .jobs
            .lock()
            .map_err(|_| io::Error::other("async KV job registry lock poisoned"))?;
        let terminal = jobs
            .get(&job_id)
            .map(|job| matches!(job.state, AsyncKvJobState::Completed | AsyncKvJobState::Failed))
            .unwrap_or(false);
        if terminal {
            Ok(jobs.remove(&job_id))
        } else {
            Ok(jobs.get(&job_id).cloned())
        }
    }

    pub fn wait(&self, job_id: u64, timeout: Duration) -> io::Result<AsyncKvJobSnapshot> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "async job timeout overflow"))?;
        loop {
            let snapshot = self.snapshot(job_id)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("unknown async job {job_id}"))
            })?;
            if matches!(snapshot.state, AsyncKvJobState::Completed | AsyncKvJobState::Failed) {
                return Ok(snapshot);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("async job {job_id} did not finish before timeout"),
                ));
            }
            let guard = self
                .inner
                .state
                .wait_lock
                .lock()
                .map_err(|_| io::Error::other("async KV wait lock poisoned"))?;
            let remaining = deadline.saturating_duration_since(now);
            let _ = self
                .inner
                .state
                .changed
                .wait_timeout(guard, remaining.min(Duration::from_millis(50)))
                .map_err(|_| io::Error::other("async KV wait poisoned"))?;
        }
    }

    pub fn finish(&self, timeout: Duration) -> io::Result<bool> {
        if self.inner.state.inflight.load(Ordering::Acquire) == 0 {
            return Ok(true);
        }
        let guard = self
            .inner
            .state
            .wait_lock
            .lock()
            .map_err(|_| io::Error::other("async KV wait lock poisoned"))?;
        let (_guard, wait) = self
            .inner
            .state
            .changed
            .wait_timeout_while(guard, timeout, |_| {
                self.inner.state.inflight.load(Ordering::Acquire) != 0
            })
            .map_err(|_| io::Error::other("async KV wait poisoned"))?;
        Ok(!wait.timed_out() || self.inner.state.inflight.load(Ordering::Acquire) == 0)
    }

    pub fn stats(&self) -> AsyncKvPipelineStats {
        AsyncKvPipelineStats {
            submitted: self.inner.state.submitted.load(Ordering::Relaxed),
            completed: self.inner.state.completed.load(Ordering::Relaxed),
            failed: self.inner.state.failed.load(Ordering::Relaxed),
            inflight: self.inner.state.inflight.load(Ordering::Acquire),
        }
    }

    fn submit(&self, operation: AsyncKvOperation, payload: JobPayload) -> io::Result<u64> {
        let id = self
            .inner
            .state
            .next_id
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.inner
            .state
            .jobs
            .lock()
            .map_err(|_| io::Error::other("async KV job registry lock poisoned"))?
            .insert(
                id,
                AsyncKvJobSnapshot {
                    job_id: id,
                    operation,
                    state: AsyncKvJobState::Pending,
                    result: None,
                    error: None,
                },
            );

        // Reserve before enqueue. A worker can complete immediately after try_send succeeds,
        // so reserving later creates an underflow race in the completion path.
        self.inner.state.inflight.fetch_add(1, Ordering::AcqRel);
        let send_result = {
            let sender = self
                .inner
                .sender
                .lock()
                .map_err(|_| io::Error::other("async KV sender lock poisoned"))?;
            match sender.as_ref() {
                Some(sender) => sender.try_send(Job {
                    id,
                    operation,
                    payload,
                }),
                None => {
                    self.inner.state.inflight.fetch_sub(1, Ordering::AcqRel);
                    self.inner
                        .state
                        .jobs
                        .lock()
                        .map_err(|_| io::Error::other("async KV job registry lock poisoned"))?
                        .remove(&id);
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "async KV pipeline is shut down",
                    ));
                }
            }
        };

        match send_result {
            Ok(()) => {
                self.inner.state.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(id)
            }
            Err(mpsc::TrySendError::Full(_)) => {
                self.rollback_submit(id)?;
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "async KV pipeline queue is full",
                ))
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.rollback_submit(id)?;
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "async KV pipeline workers are unavailable",
                ))
            }
        }
    }

    fn rollback_submit(&self, id: u64) -> io::Result<()> {
        self.inner.state.inflight.fetch_sub(1, Ordering::AcqRel);
        self.inner
            .state
            .jobs
            .lock()
            .map_err(|_| io::Error::other("async KV job registry lock poisoned"))?
            .remove(&id);
        self.inner.state.changed.notify_all();
        Ok(())
    }
}

fn worker_loop(
    engine: KvEngine,
    receiver: Arc<Mutex<mpsc::Receiver<Job>>>,
    state: Arc<PipelineState>,
    events: Option<Arc<KvEventBus>>,
) {
    loop {
        let job = {
            let receiver = match receiver.lock() {
                Ok(receiver) => receiver,
                Err(_) => return,
            };
            match receiver.recv() {
                Ok(job) => job,
                Err(_) => return,
            }
        };
        state.set_running(job.id);

        if let Some(events) = &events {
            let _ = events.publish(
                KvEvent::new(job.operation.event_kind(), KvEventStatus::Started).request_id(job.id),
            );
        }
        let started = Instant::now();
        let result = execute_job(&engine, job.id, job.operation, job.payload, started)
            .map_err(AsyncKvError::from);
        if let Some(events) = &events {
            match &result {
                Ok(result) => {
                    let status = if result.found {
                        KvEventStatus::Completed
                    } else {
                        KvEventStatus::Miss
                    };
                    let _ = events.publish(
                        KvEvent::new(job.operation.event_kind(), status)
                            .request_id(job.id)
                            .counts(result.completed, result.bytes)
                            .duration_micros(result.elapsed_micros),
                    );
                }
                Err(error) => {
                    let _ = events.publish(
                        KvEvent::new(job.operation.event_kind(), KvEventStatus::Error)
                            .request_id(job.id)
                            .duration_micros(elapsed_micros(started))
                            .detail(error.message.clone()),
                    );
                }
            }
        }
        state.finish(job.id, job.operation, result);
    }
}

fn execute_job(
    engine: &KvEngine,
    job_id: u64,
    operation: AsyncKvOperation,
    payload: JobPayload,
    started: Instant,
) -> io::Result<AsyncKvResult> {
    let mut result = AsyncKvResult {
        job_id,
        operation,
        found: true,
        requested: 0,
        completed: 0,
        missed: 0,
        bytes: 0,
        elapsed_micros: 0,
        blocks: Vec::new(),
        health: Vec::new(),
    };

    match payload {
        JobPayload::Store {
            blocks,
            ttl,
            pinned,
        } => {
            result.requested = blocks.len() as u64;
            for block in blocks {
                result.bytes = result.bytes.saturating_add(block.bytes.len() as u64);
                engine.put(block, ttl, pinned)?;
                result.completed = result.completed.saturating_add(1);
            }
        }
        JobPayload::Retrieve(keys) => {
            result.requested = keys.len() as u64;
            for key in keys {
                match engine.get(&key)? {
                    Some(block) => {
                        result.bytes = result.bytes.saturating_add(block.bytes.len() as u64);
                        result.completed = result.completed.saturating_add(1);
                        result.blocks.push(block);
                    }
                    None => result.missed = result.missed.saturating_add(1),
                }
            }
            result.found = result.completed > 0;
        }
        JobPayload::Move(requests) => {
            result.requested = requests.len() as u64;
            for request in requests {
                if engine.move_block(
                    &request.key,
                    request.source_index,
                    request.destination_index,
                    request.remove_source,
                )? {
                    result.completed = result.completed.saturating_add(1);
                } else {
                    result.missed = result.missed.saturating_add(1);
                }
            }
            result.found = result.completed > 0;
        }
        JobPayload::Prefetch {
            keys,
            destination_index,
        } => {
            let report = engine.prefetch_to(keys, destination_index).wait()?;
            result.requested = report.requested;
            result.completed = report.populated;
            result.missed = report.missed;
            result.found = report.populated > 0;
        }
        JobPayload::Invalidate(keys) => {
            result.requested = keys.len() as u64;
            for key in keys {
                engine.invalidate(&key)?;
                result.completed = result.completed.saturating_add(1);
            }
        }
        JobPayload::Pin { keys, pinned } => {
            result.requested = keys.len() as u64;
            for key in keys {
                if engine.set_pinned(&key, pinned)? {
                    result.completed = result.completed.saturating_add(1);
                } else {
                    result.missed = result.missed.saturating_add(1);
                }
            }
            result.found = result.completed > 0;
        }
        JobPayload::Clear => {
            result.requested = 1;
            engine.clear()?;
            result.completed = 1;
        }
        JobPayload::Health => {
            result.requested = 1;
            result.health = engine.health();
            result.found = result.health.iter().all(|health| health.healthy);
            if result.found {
                result.completed = 1;
            } else {
                result.missed = 1;
            }
        }
    }
    result.elapsed_micros = elapsed_micros(started);
    Ok(result)
}

fn elapsed_micros(started: Instant) -> u64 {
    started
        .elapsed()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

fn invalid<T>(message: &str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvBlockRange, KvTier, KvTierEntry};
    use std::collections::HashMap;

    #[derive(Default)]
    struct MemoryTier {
        values: Mutex<HashMap<String, KvTierEntry>>,
    }

    impl KvTier for MemoryTier {
        fn name(&self) -> &str {
            "memory"
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

    fn block(byte: u8) -> KvBlock {
        let tokens = [1, 2, 3, 4];
        KvBlock::new(
            KvBlockKey::from_prefix(
                "model",
                &tokens,
                KvBlockRange {
                    block_index: byte as u32,
                    token_start: 0,
                    token_count: 4,
                    layer_start: 0,
                    layer_count: 8,
                    layout_version: 1,
                },
            ),
            vec![byte; 64],
        )
        .unwrap()
    }

    #[test]
    fn store_and_retrieve_are_waitable() {
        let engine = KvEngine::builder().tier(MemoryTier::default()).build().unwrap();
        let pipeline = AsyncKvPipeline::new(engine, 2, 8, None).unwrap();
        let original = block(7);
        let store = pipeline
            .submit_store(vec![original.clone()], None, false)
            .unwrap();
        assert_eq!(
            pipeline.wait(store, Duration::from_secs(2)).unwrap().state,
            AsyncKvJobState::Completed
        );
        let retrieve = pipeline.submit_retrieve(vec![original.key.clone()]).unwrap();
        let result = pipeline
            .wait(retrieve, Duration::from_secs(2))
            .unwrap()
            .result
            .unwrap();
        assert_eq!(result.blocks, vec![original]);
        assert!(pipeline.finish(Duration::from_secs(1)).unwrap());
        assert_eq!(pipeline.stats().inflight, 0);
    }

    #[test]
    fn batch_move_reports_misses_without_failing_job() {
        let source: Arc<dyn KvTier> = Arc::new(MemoryTier::default());
        let destination: Arc<dyn KvTier> = Arc::new(MemoryTier::default());
        let engine = KvEngine::builder()
            .tier_arc(source)
            .tier_arc(destination)
            .build()
            .unwrap();
        let pipeline = AsyncKvPipeline::new(engine.clone(), 1, 4, None).unwrap();
        let present = block(1);
        let missing = block(2);
        engine.put_to(0, present.clone(), None, false).unwrap();
        let id = pipeline
            .submit_move_many(
                vec![present.key.clone(), missing.key],
                0,
                1,
                false,
            )
            .unwrap();
        let result = pipeline
            .wait(id, Duration::from_secs(2))
            .unwrap()
            .result
            .unwrap();
        assert_eq!(result.requested, 2);
        assert_eq!(result.completed, 1);
        assert_eq!(result.missed, 1);
    }
}