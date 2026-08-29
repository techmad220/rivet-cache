#[path = "lib.rs"]
mod legacy;

pub use legacy::*;

mod codec;
mod connectors;
mod control;
mod distributed;
mod gpu_direct;
mod memory;
mod observability;
mod plugins;
mod quality;

pub use codec::{CodecKvTier, CodecRegistry, IdentityCodec, PayloadCodec, RleCodec};
pub use connectors::{
    HttpClient, HttpRequest, HttpResponse, RedisAuth, RedisDialer, RedisKvTier, RedisStream,
    S3Clock, S3Config, S3Credentials, S3KvTier, SystemS3Clock, TcpHttpClient, TcpRedisDialer,
};
pub use control::{
    CacheController, ControllerServer, FleetNode, FleetRegistry, QuotaManager, RequestLease,
    StorageReservation, TenantQuota, TenantSnapshot, TenantUsage,
};
pub use distributed::{
    DeviceTransferFn, DeviceTransferHealthFn, DeviceTransferProvider, DirectTransferRequest,
    DisaggregatedKvRouter, FfiDeviceTransferOps, FfiDeviceTransferProvider, KvHandoffReport,
    TransportCapabilities, TransportKind, WorkerRole,
};
pub use gpu_direct::{
    FfiGpuDirectIo, FfiGpuDirectOps, GpuCopyFn, GpuDirectCapabilities, GpuDirectHealthFn,
    GpuDirectIo, GpuFileReadFn, GpuFileWriteFn,
};
pub use memory::{
    HeapPageAllocator, NativePinnedAllocator, PageAllocator, PinnedLease, PinnedMemoryPool,
    PinnedPoolStats,
};
pub use observability::{InstrumentedKvTier, PrometheusRegistry};
pub use plugins::{
    CodecPluginFactory, KvTierPluginFactory, PluginComponent, PluginManifest, PluginRegistry,
    TransportPluginFactory,
};
pub use quality::{
    apply_quality_reuse, plan_quality_reuse, QualityAwareRuntimeKvAdapter, QualityReusePlan,
    QualityReusePolicy, TokenRange,
};
