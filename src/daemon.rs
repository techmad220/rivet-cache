use crate::{
    CacheController, ContextCache, ContextCacheTier, ControllerServer, InstrumentedKvTier, KvTier,
    PrometheusRegistry, RemoteLimits, TcpKvServer,
};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const MIB: u64 = 1024 * 1024;
const DAEMON_TIER: &str = "daemon-cache";

/// Configuration for a managed RivetCache service process.
///
/// The data plane and control plane are intentionally separate listeners. Both
/// default to loopback-only endpoints so deployments must explicitly opt into
/// wider network exposure.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub data_bind: String,
    pub control_bind: String,
    pub root: PathBuf,
    pub memory_capacity_bytes: u64,
    pub persistent_capacity_bytes: u64,
    pub remote_limits: RemoteLimits,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            data_bind: "127.0.0.1:65432".to_owned(),
            control_bind: "127.0.0.1:65433".to_owned(),
            root: PathBuf::from("rivet-cache-data"),
            memory_capacity_bytes: 512 * MIB,
            persistent_capacity_bytes: 8192 * MIB,
            remote_limits: RemoteLimits::default(),
        }
    }
}

impl DaemonConfig {
    fn validate(&self) -> io::Result<()> {
        if self.data_bind.trim().is_empty() || self.control_bind.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "daemon bind addresses must not be empty",
            ));
        }
        if self.memory_capacity_bytes == 0 || self.persistent_capacity_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "daemon memory and persistent capacities must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// A running RivetCache data plane plus control/metrics plane.
///
/// Dropping the value shuts both listeners down. Call [`RivetDaemon::stop`] to
/// surface shutdown failures to the caller instead of discarding them in Drop.
pub struct RivetDaemon {
    data_server: Option<TcpKvServer>,
    control_server: Option<ControllerServer>,
    data_addr: SocketAddr,
    control_addr: String,
    metrics: Arc<PrometheusRegistry>,
}

impl RivetDaemon {
    pub fn spawn(config: DaemonConfig) -> io::Result<Self> {
        config.validate()?;

        let metrics = Arc::new(PrometheusRegistry::new());
        register_daemon_metrics(&metrics, &config)?;

        let cache = Arc::new(ContextCache::new(
            Some(config.root),
            config.memory_capacity_bytes,
            config.persistent_capacity_bytes,
            Duration::ZERO,
        )?);
        let cache_tier: Arc<dyn KvTier> = Arc::new(ContextCacheTier::new(DAEMON_TIER, cache)?);
        let tier: Arc<dyn KvTier> = Arc::new(InstrumentedKvTier::new(
            DAEMON_TIER,
            cache_tier,
            Arc::clone(&metrics),
        )?);

        // Start the data plane first. If control-plane startup fails, the data
        // server is dropped on this error path and its listener is shut down.
        let data_server = TcpKvServer::spawn(&config.data_bind, tier, config.remote_limits)?;
        let data_addr = data_server.local_addr();

        let controller = CacheController::new(Arc::clone(&metrics));
        let control_server = ControllerServer::spawn(&config.control_bind, controller)?;
        let control_addr = control_server.local_addr().to_owned();

        // Readiness becomes true only after both listeners and shared telemetry
        // exist. A failure to publish readiness tears the just-created service
        // back down through the local server values' Drop implementations.
        metrics.set_gauge("rivet_daemon_ready", &[], 1)?;

        Ok(Self {
            data_server: Some(data_server),
            control_server: Some(control_server),
            data_addr,
            control_addr,
            metrics,
        })
    }

    pub fn data_addr(&self) -> SocketAddr {
        self.data_addr
    }

    pub fn control_addr(&self) -> &str {
        &self.control_addr
    }

    pub fn metrics(&self) -> Arc<PrometheusRegistry> {
        Arc::clone(&self.metrics)
    }

    pub fn stop(mut self) -> io::Result<()> {
        let readiness_result = self.metrics.set_gauge("rivet_daemon_ready", &[], 0);
        let control_result = self
            .control_server
            .take()
            .map(ControllerServer::shutdown)
            .transpose();
        let data_result = self.data_server.take().map(TcpKvServer::stop).transpose();

        match (control_result, data_result, readiness_result) {
            (Err(error), _, _) => Err(error),
            (_, Err(error), _) => Err(error),
            (_, _, Err(error)) => Err(error),
            (Ok(_), Ok(_), Ok(())) => Ok(()),
        }
    }
}

fn register_daemon_metrics(
    metrics: &PrometheusRegistry,
    config: &DaemonConfig,
) -> io::Result<()> {
    metrics.set_gauge(
        "rivet_daemon_build_info",
        &[("tier", DAEMON_TIER), ("version", env!("CARGO_PKG_VERSION"))],
        1,
    )?;
    metrics.set_gauge(
        "rivet_daemon_capacity_bytes",
        &[("tier", "memory")],
        gauge_bytes(config.memory_capacity_bytes),
    )?;
    metrics.set_gauge(
        "rivet_daemon_capacity_bytes",
        &[("tier", "persistent")],
        gauge_bytes(config.persistent_capacity_bytes),
    )?;
    metrics.set_gauge("rivet_daemon_ready", &[], 0)?;
    Ok(())
}

fn gauge_bytes(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TcpKvTier;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("rivet-daemon-{}-{nonce}", std::process::id()))
    }

    fn controller_get(address: &str, path: &str) -> io::Result<String> {
        let mut stream = TcpStream::connect(address)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )?;
        stream.flush()?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    #[test]
    fn daemon_starts_both_planes_and_stops_cleanly() -> io::Result<()> {
        let root = temp_root();
        let daemon = RivetDaemon::spawn(DaemonConfig {
            data_bind: "127.0.0.1:0".to_owned(),
            control_bind: "127.0.0.1:0".to_owned(),
            root: root.clone(),
            memory_capacity_bytes: 2 * MIB,
            persistent_capacity_bytes: 8 * MIB,
            remote_limits: RemoteLimits {
                io_timeout: Duration::from_secs(2),
                connect_timeout: Duration::from_secs(2),
                ..RemoteLimits::default()
            },
        })?;

        let client = TcpKvTier::new("daemon-probe", daemon.data_addr(), RemoteLimits::default())?;
        client.ping()?;

        let health = controller_get(daemon.control_addr(), "/health")?;
        assert!(health.starts_with("HTTP/1.1 200 OK"));
        assert!(health.contains("{\"ok\":true}"));

        let metrics = controller_get(daemon.control_addr(), "/metrics")?;
        assert!(metrics.starts_with("HTTP/1.1 200 OK"));
        assert!(metrics.contains("rivet_kv_tier_requests_total"));
        assert!(metrics.contains("operation=\"health\""));
        assert!(metrics.contains("rivet_daemon_ready 1"));
        assert!(metrics.contains("rivet_daemon_capacity_bytes{tier=\"memory\"} 2097152"));
        assert!(metrics.contains("rivet_daemon_capacity_bytes{tier=\"persistent\"} 8388608"));
        assert!(metrics.contains("rivet_daemon_build_info"));
        assert!(metrics.contains("tier=\"daemon-cache\""));
        assert!(metrics.contains(&format!("version=\"{}\"", env!("CARGO_PKG_VERSION"))));

        daemon.stop()?;
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn daemon_rejects_zero_capacity() {
        let config = DaemonConfig {
            memory_capacity_bytes: 0,
            ..DaemonConfig::default()
        };
        let error = match RivetDaemon::spawn(config) {
            Ok(_) => panic!("zero-capacity daemon unexpectedly started"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn byte_gauge_saturates_at_prometheus_integer_limit() {
        assert_eq!(gauge_bytes(u64::MAX), i64::MAX);
    }
}
