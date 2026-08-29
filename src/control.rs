use crate::{PrometheusRegistry, WorkerRole};
use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantQuota {
    pub max_bytes: u64,
    pub max_entries: u64,
    pub max_inflight: u64,
}

impl TenantQuota {
    fn validate(self) -> io::Result<Self> {
        if self.max_bytes == 0 || self.max_entries == 0 || self.max_inflight == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tenant quota limits must all be greater than zero",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TenantUsage {
    pub used_bytes: u64,
    pub used_entries: u64,
    pub reserved_bytes: u64,
    pub reserved_entries: u64,
    pub inflight: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantSnapshot {
    pub tenant: String,
    pub quota: TenantQuota,
    pub usage: TenantUsage,
}

#[derive(Debug, Clone, Copy)]
struct TenantState {
    quota: TenantQuota,
    usage: TenantUsage,
}

#[derive(Default)]
struct QuotaInner {
    tenants: Mutex<BTreeMap<String, TenantState>>,
}

#[derive(Clone, Default)]
pub struct QuotaManager {
    inner: Arc<QuotaInner>,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_quota(&self, tenant: impl Into<String>, quota: TenantQuota) -> io::Result<()> {
        let tenant = validate_id(tenant.into(), "tenant")?;
        let quota = quota.validate()?;
        let mut tenants = self
            .inner
            .tenants
            .lock()
            .map_err(|_| io::Error::other("quota manager lock poisoned"))?;
        if let Some(state) = tenants.get(&tenant) {
            if state
                .usage
                .used_bytes
                .saturating_add(state.usage.reserved_bytes)
                > quota.max_bytes
                || state
                    .usage
                    .used_entries
                    .saturating_add(state.usage.reserved_entries)
                    > quota.max_entries
                || state.usage.inflight > quota.max_inflight
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "new quota is below current tenant usage",
                ));
            }
        }
        tenants
            .entry(tenant)
            .and_modify(|state| state.quota = quota)
            .or_insert(TenantState {
                quota,
                usage: TenantUsage::default(),
            });
        Ok(())
    }

    pub fn begin_request(&self, tenant: &str) -> io::Result<RequestLease> {
        let mut tenants = self
            .inner
            .tenants
            .lock()
            .map_err(|_| io::Error::other("quota manager lock poisoned"))?;
        let state = tenants.get_mut(tenant).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("tenant {tenant} has no quota"),
            )
        })?;
        if state.usage.inflight >= state.quota.max_inflight {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("tenant {tenant} exceeded max_inflight"),
            ));
        }
        state.usage.inflight += 1;
        Ok(RequestLease {
            inner: Arc::clone(&self.inner),
            tenant: tenant.to_owned(),
            released: false,
        })
    }

    pub fn reserve_storage(
        &self,
        tenant: &str,
        bytes: u64,
        entries: u64,
    ) -> io::Result<StorageReservation> {
        if bytes == 0 || entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "storage reservations require non-zero bytes and entries",
            ));
        }
        let mut tenants = self
            .inner
            .tenants
            .lock()
            .map_err(|_| io::Error::other("quota manager lock poisoned"))?;
        let state = tenants.get_mut(tenant).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("tenant {tenant} has no quota"),
            )
        })?;
        let total_bytes = state
            .usage
            .used_bytes
            .saturating_add(state.usage.reserved_bytes)
            .saturating_add(bytes);
        let total_entries = state
            .usage
            .used_entries
            .saturating_add(state.usage.reserved_entries)
            .saturating_add(entries);
        if total_bytes > state.quota.max_bytes || total_entries > state.quota.max_entries {
            return Err(io::Error::other(format!(
                "tenant {tenant} exceeded storage quota"
            )));
        }
        state.usage.reserved_bytes = state.usage.reserved_bytes.saturating_add(bytes);
        state.usage.reserved_entries = state.usage.reserved_entries.saturating_add(entries);
        Ok(StorageReservation {
            inner: Arc::clone(&self.inner),
            tenant: tenant.to_owned(),
            bytes,
            entries,
            state: ReservationState::Pending,
        })
    }

    pub fn release_storage(&self, tenant: &str, bytes: u64, entries: u64) -> io::Result<()> {
        let mut tenants = self
            .inner
            .tenants
            .lock()
            .map_err(|_| io::Error::other("quota manager lock poisoned"))?;
        let state = tenants.get_mut(tenant).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("tenant {tenant} has no quota"),
            )
        })?;
        state.usage.used_bytes = state.usage.used_bytes.saturating_sub(bytes);
        state.usage.used_entries = state.usage.used_entries.saturating_sub(entries);
        Ok(())
    }

    pub fn snapshots(&self) -> io::Result<Vec<TenantSnapshot>> {
        Ok(self
            .inner
            .tenants
            .lock()
            .map_err(|_| io::Error::other("quota manager lock poisoned"))?
            .iter()
            .map(|(tenant, state)| TenantSnapshot {
                tenant: tenant.clone(),
                quota: state.quota,
                usage: state.usage,
            })
            .collect())
    }
}

pub struct RequestLease {
    inner: Arc<QuotaInner>,
    tenant: String,
    released: bool,
}

impl RequestLease {
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        if let Ok(mut tenants) = self.inner.tenants.lock() {
            if let Some(state) = tenants.get_mut(&self.tenant) {
                state.usage.inflight = state.usage.inflight.saturating_sub(1);
            }
        }
        self.released = true;
    }
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReservationState {
    Pending,
    Committed,
    Released,
}

pub struct StorageReservation {
    inner: Arc<QuotaInner>,
    tenant: String,
    bytes: u64,
    entries: u64,
    state: ReservationState,
}

impl StorageReservation {
    pub fn commit(mut self) -> io::Result<()> {
        let mut tenants = self
            .inner
            .tenants
            .lock()
            .map_err(|_| io::Error::other("quota manager lock poisoned"))?;
        let state = tenants.get_mut(&self.tenant).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "tenant disappeared during reservation",
            )
        })?;
        state.usage.reserved_bytes = state.usage.reserved_bytes.saturating_sub(self.bytes);
        state.usage.reserved_entries = state.usage.reserved_entries.saturating_sub(self.entries);
        state.usage.used_bytes = state.usage.used_bytes.saturating_add(self.bytes);
        state.usage.used_entries = state.usage.used_entries.saturating_add(self.entries);
        self.state = ReservationState::Committed;
        Ok(())
    }

    fn rollback(&mut self) {
        if self.state != ReservationState::Pending {
            return;
        }
        if let Ok(mut tenants) = self.inner.tenants.lock() {
            if let Some(state) = tenants.get_mut(&self.tenant) {
                state.usage.reserved_bytes = state.usage.reserved_bytes.saturating_sub(self.bytes);
                state.usage.reserved_entries =
                    state.usage.reserved_entries.saturating_sub(self.entries);
            }
        }
        self.state = ReservationState::Released;
    }
}

impl Drop for StorageReservation {
    fn drop(&mut self) {
        self.rollback();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetNode {
    pub id: String,
    pub endpoint: String,
    pub role: WorkerRole,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub last_seen_epoch_secs: u64,
}

#[derive(Clone, Default)]
pub struct FleetRegistry {
    nodes: Arc<Mutex<BTreeMap<String, FleetNode>>>,
}

impl FleetRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn heartbeat(
        &self,
        id: impl Into<String>,
        endpoint: impl Into<String>,
        role: WorkerRole,
        capacity_bytes: u64,
        used_bytes: u64,
    ) -> io::Result<FleetNode> {
        let id = validate_id(id.into(), "node")?;
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() || capacity_bytes == 0 || used_bytes > capacity_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid fleet heartbeat",
            ));
        }
        let node = FleetNode {
            id: id.clone(),
            endpoint,
            role,
            capacity_bytes,
            used_bytes,
            last_seen_epoch_secs: now_epoch_secs()?,
        };
        self.nodes
            .lock()
            .map_err(|_| io::Error::other("fleet registry lock poisoned"))?
            .insert(id, node.clone());
        Ok(node)
    }

    pub fn nodes(&self) -> io::Result<Vec<FleetNode>> {
        Ok(self
            .nodes
            .lock()
            .map_err(|_| io::Error::other("fleet registry lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }

    pub fn prune_stale(&self, max_age: Duration) -> io::Result<usize> {
        let now = now_epoch_secs()?;
        let mut nodes = self
            .nodes
            .lock()
            .map_err(|_| io::Error::other("fleet registry lock poisoned"))?;
        let before = nodes.len();
        nodes.retain(|_, node| now.saturating_sub(node.last_seen_epoch_secs) <= max_age.as_secs());
        Ok(before.saturating_sub(nodes.len()))
    }
}

#[derive(Clone)]
pub struct CacheController {
    pub quotas: QuotaManager,
    pub fleet: FleetRegistry,
    pub metrics: Arc<PrometheusRegistry>,
}

impl CacheController {
    pub fn new(metrics: Arc<PrometheusRegistry>) -> Self {
        Self {
            quotas: QuotaManager::new(),
            fleet: FleetRegistry::new(),
            metrics,
        }
    }

    pub fn refresh_metrics(&self) -> io::Result<()> {
        let tenants = self.quotas.snapshots()?;
        self.metrics
            .set_gauge("rivet_controller_tenants", &[], tenants.len() as i64)?;
        for tenant in tenants {
            self.metrics.set_gauge(
                "rivet_tenant_used_bytes",
                &[("tenant", &tenant.tenant)],
                tenant.usage.used_bytes.min(i64::MAX as u64) as i64,
            )?;
            self.metrics.set_gauge(
                "rivet_tenant_inflight",
                &[("tenant", &tenant.tenant)],
                tenant.usage.inflight.min(i64::MAX as u64) as i64,
            )?;
        }
        let nodes = self.fleet.nodes()?;
        self.metrics
            .set_gauge("rivet_controller_nodes", &[], nodes.len() as i64)?;
        Ok(())
    }
}

pub struct ControllerServer {
    local_addr: String,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ControllerServer {
    pub fn spawn(address: impl Into<String>, controller: CacheController) -> io::Result<Self> {
        let address = address.into();
        let listener = TcpListener::bind(&address)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?.to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("rivet-controller".to_owned())
            .spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let _ = handle_connection(stream, &controller);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(20));
                        }
                        Err(_) => thread::sleep(Duration::from_millis(50)),
                    }
                }
            })?;
        Ok(Self {
            local_addr,
            stop,
            handle: Some(handle),
        })
    }

    pub fn local_addr(&self) -> &str {
        &self.local_addr
    }

    pub fn shutdown(mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        wake_listener(&self.local_addr);
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| io::Error::other("controller server thread panicked"))?;
        }
        Ok(())
    }
}

impl Drop for ControllerServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        wake_listener(&self.local_addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn handle_connection(mut stream: TcpStream, controller: &CacheController) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut first = String::new();
    if reader.read_line(&mut first)? == 0 || first.len() > 8192 {
        return Ok(());
    }
    let mut header_bytes = first.len();
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        header_bytes = header_bytes.saturating_add(read);
        if header_bytes > 32 * 1024 {
            return write_response(
                &mut stream,
                431,
                "text/plain",
                b"request headers too large\n",
            );
        }
        if line == "\r\n" {
            break;
        }
    }
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let params = parse_query(query)?;

    let response = route_request(method, path, &params, controller);
    match response {
        Ok((status, content_type, body)) => {
            write_response(&mut stream, status, content_type, body.as_bytes())
        }
        Err(error) => write_response(
            &mut stream,
            400,
            "application/json",
            format!("{{\"error\":\"{}\"}}", json_escape(&error.to_string())).as_bytes(),
        ),
    }
}

fn route_request(
    method: &str,
    path: &str,
    params: &BTreeMap<String, String>,
    controller: &CacheController,
) -> io::Result<(u16, &'static str, String)> {
    match (method, path) {
        ("GET", "/health") => Ok((200, "application/json", "{\"ok\":true}".to_owned())),
        ("GET", "/metrics") => {
            controller.refresh_metrics()?;
            Ok((
                200,
                "text/plain; version=0.0.4",
                controller.metrics.render()?,
            ))
        }
        ("GET", "/v1/nodes") => Ok((
            200,
            "application/json",
            nodes_json(&controller.fleet.nodes()?),
        )),
        ("GET", "/v1/tenants") => Ok((
            200,
            "application/json",
            tenants_json(&controller.quotas.snapshots()?),
        )),
        ("POST", "/v1/nodes/heartbeat") => {
            let role = match required(params, "role")?.as_str() {
                "prefill" => WorkerRole::Prefill,
                "decode" => WorkerRole::Decode,
                "hybrid" => WorkerRole::Hybrid,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid worker role",
                    ))
                }
            };
            let node = controller.fleet.heartbeat(
                required(params, "id")?,
                required(params, "endpoint")?,
                role,
                parse_u64(params, "capacity_bytes")?,
                parse_u64(params, "used_bytes")?,
            )?;
            Ok((200, "application/json", node_json(&node)))
        }
        _ if method == "PUT" && path.starts_with("/v1/tenants/") => {
            let tenant = percent_decode(&path["/v1/tenants/".len()..])?;
            controller.quotas.set_quota(
                tenant.clone(),
                TenantQuota {
                    max_bytes: parse_u64(params, "max_bytes")?,
                    max_entries: parse_u64(params, "max_entries")?,
                    max_inflight: parse_u64(params, "max_inflight")?,
                },
            )?;
            Ok((
                200,
                "application/json",
                format!("{{\"tenant\":\"{}\",\"ok\":true}}", json_escape(&tenant)),
            ))
        }
        _ => Ok((
            404,
            "application/json",
            "{\"error\":\"not found\"}".to_owned(),
        )),
    }
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        431 => "Request Header Fields Too Large",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()
}

fn parse_query(query: &str) -> io::Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    if query.is_empty() {
        return Ok(values);
    }
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        if key.is_empty() || values.insert(key.clone(), value).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate or empty query key {key}"),
            ));
        }
    }
    Ok(values)
}

fn percent_decode(value: &str) -> io::Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "truncated percent escape",
                    ));
                }
                let high = hex_value(bytes[index + 1])?;
                let low = hex_value(bytes[index + 2])?;
                decoded.push(high * 16 + low);
                index += 3;
            }
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "query value is not UTF-8"))
}

fn hex_value(byte: u8) -> io::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid percent escape",
        )),
    }
}

fn required(params: &BTreeMap<String, String>, name: &str) -> io::Result<String> {
    params
        .get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}")))
}

fn parse_u64(params: &BTreeMap<String, String>, name: &str) -> io::Result<u64> {
    required(params, name)?
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {name}")))
}

fn nodes_json(nodes: &[FleetNode]) -> String {
    format!(
        "[{}]",
        nodes.iter().map(node_json).collect::<Vec<_>>().join(",")
    )
}

fn node_json(node: &FleetNode) -> String {
    let role = match node.role {
        WorkerRole::Prefill => "prefill",
        WorkerRole::Decode => "decode",
        WorkerRole::Hybrid => "hybrid",
    };
    format!(
        "{{\"id\":\"{}\",\"endpoint\":\"{}\",\"role\":\"{}\",\"capacity_bytes\":{},\"used_bytes\":{},\"last_seen_epoch_secs\":{}}}",
        json_escape(&node.id),
        json_escape(&node.endpoint),
        role,
        node.capacity_bytes,
        node.used_bytes,
        node.last_seen_epoch_secs
    )
}

fn tenants_json(tenants: &[TenantSnapshot]) -> String {
    let values = tenants
        .iter()
        .map(|tenant| {
            format!(
                "{{\"tenant\":\"{}\",\"max_bytes\":{},\"max_entries\":{},\"max_inflight\":{},\"used_bytes\":{},\"used_entries\":{},\"reserved_bytes\":{},\"reserved_entries\":{},\"inflight\":{}}}",
                json_escape(&tenant.tenant),
                tenant.quota.max_bytes,
                tenant.quota.max_entries,
                tenant.quota.max_inflight,
                tenant.usage.used_bytes,
                tenant.usage.used_entries,
                tenant.usage.reserved_bytes,
                tenant.usage.reserved_entries,
                tenant.usage.inflight
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{values}]")
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn validate_id(value: String, field: &str) -> io::Result<String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {field} id"),
        ));
    }
    Ok(value)
}

fn now_epoch_secs() -> io::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| io::Error::other("system clock is before Unix epoch"))?
        .as_secs())
}

fn wake_listener(address: &str) {
    if let Ok(mut addresses) = address.to_socket_addrs() {
        if let Some(address) = addresses.next() {
            let _ = TcpStream::connect_timeout(&address, Duration::from_millis(50));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_reservation_is_transactional() {
        let quotas = QuotaManager::new();
        quotas
            .set_quota(
                "tenant-a",
                TenantQuota {
                    max_bytes: 100,
                    max_entries: 10,
                    max_inflight: 2,
                },
            )
            .unwrap();
        {
            let reservation = quotas.reserve_storage("tenant-a", 60, 2).unwrap();
            let snapshot = quotas.snapshots().unwrap().remove(0);
            assert_eq!(snapshot.usage.reserved_bytes, 60);
            reservation.commit().unwrap();
        }
        let snapshot = quotas.snapshots().unwrap().remove(0);
        assert_eq!(snapshot.usage.used_bytes, 60);
        assert!(quotas.reserve_storage("tenant-a", 50, 1).is_err());
        quotas.release_storage("tenant-a", 60, 2).unwrap();
        assert_eq!(quotas.snapshots().unwrap()[0].usage.used_bytes, 0);
    }

    #[test]
    fn request_lease_enforces_inflight_limit() {
        let quotas = QuotaManager::new();
        quotas
            .set_quota(
                "tenant-a",
                TenantQuota {
                    max_bytes: 100,
                    max_entries: 10,
                    max_inflight: 1,
                },
            )
            .unwrap();
        let first = quotas.begin_request("tenant-a").unwrap();
        assert_eq!(
            quotas
                .begin_request("tenant-a")
                .err()
                .expect("inflight quota error")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(first);
        quotas.begin_request("tenant-a").unwrap();
    }

    #[test]
    fn controller_routes_quota_and_heartbeat_calls() {
        let controller = CacheController::new(Arc::new(PrometheusRegistry::new()));
        let quota_params = parse_query("max_bytes=1000&max_entries=20&max_inflight=3").unwrap();
        assert_eq!(
            route_request("PUT", "/v1/tenants/team-a", &quota_params, &controller)
                .unwrap()
                .0,
            200
        );
        let heartbeat = parse_query(
            "id=node-1&endpoint=127.0.0.1%3A65432&role=prefill&capacity_bytes=10000&used_bytes=100",
        )
        .unwrap();
        assert_eq!(
            route_request("POST", "/v1/nodes/heartbeat", &heartbeat, &controller)
                .unwrap()
                .0,
            200
        );
        let nodes = route_request("GET", "/v1/nodes", &BTreeMap::new(), &controller)
            .unwrap()
            .2;
        assert!(nodes.contains("node-1"));
    }
}
