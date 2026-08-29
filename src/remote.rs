use crate::kv::{KvBlock, KvBlockKey, KvTier, KvTierEntry};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAGIC: &[u8; 8] = b"RKVNET1\n";
const OP_GET: u8 = 1;
const OP_PUT: u8 = 2;
const OP_REMOVE: u8 = 3;
const OP_CLEAR: u8 = 4;
const OP_PING: u8 = 5;
const STATUS_OK: u8 = 0;
const STATUS_MISS: u8 = 1;
const STATUS_ERROR: u8 = 2;
const MAX_ERROR_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct RemoteLimits {
    pub max_payload_bytes: usize,
    pub max_model_bytes: usize,
    pub max_inflight_connections: usize,
    pub connect_timeout: Duration,
    pub io_timeout: Duration,
}

impl Default for RemoteLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 512 * 1024 * 1024,
            max_model_bytes: 64 * 1024,
            max_inflight_connections: 64,
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
pub struct TcpKvTier {
    name: String,
    endpoint: SocketAddr,
    limits: RemoteLimits,
}

impl TcpKvTier {
    pub fn new(
        name: impl Into<String>,
        endpoint: impl ToSocketAddrs,
        limits: RemoteLimits,
    ) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "remote tier name must not be empty",
            ));
        }
        validate_limits(limits)?;
        Ok(Self {
            name,
            endpoint: resolve_one(endpoint)?,
            limits,
        })
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub fn ping(&self) -> io::Result<()> {
        let mut stream = self.connect()?;
        write_request_header(&mut stream, OP_PING)?;
        stream.flush()?;
        expect_simple_response(&mut stream)
    }

    fn connect(&self) -> io::Result<TcpStream> {
        let stream = TcpStream::connect_timeout(&self.endpoint, self.limits.connect_timeout)?;
        configure_stream(&stream, self.limits.io_timeout)?;
        Ok(stream)
    }
}

impl KvTier for TcpKvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let mut stream = self.connect()?;
        write_request_header(&mut stream, OP_GET)?;
        write_key(&mut stream, key, self.limits)?;
        stream.flush()?;
        match read_status(&mut stream)? {
            STATUS_OK => read_entry(&mut stream, key.clone(), self.limits).map(Some),
            STATUS_MISS => Ok(None),
            STATUS_ERROR => Err(read_remote_error(&mut stream)?),
            status => Err(protocol_error(format!("invalid remote status {status}"))),
        }
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        if entry.block.bytes.len() > self.limits.max_payload_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "KV payload exceeds configured remote limit",
            ));
        }
        let mut stream = self.connect()?;
        write_request_header(&mut stream, OP_PUT)?;
        write_key(&mut stream, &entry.block.key, self.limits)?;
        write_entry_body(&mut stream, entry, self.limits)?;
        stream.flush()?;
        expect_simple_response(&mut stream)
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        let mut stream = self.connect()?;
        write_request_header(&mut stream, OP_REMOVE)?;
        write_key(&mut stream, key, self.limits)?;
        stream.flush()?;
        expect_simple_response(&mut stream)
    }

    fn clear(&self) -> io::Result<()> {
        let mut stream = self.connect()?;
        write_request_header(&mut stream, OP_CLEAR)?;
        stream.flush()?;
        expect_simple_response(&mut stream)
    }

    fn health(&self) -> io::Result<()> {
        self.ping()
    }
}

pub struct TcpKvServer {
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<io::Result<()>>>,
}

impl TcpKvServer {
    pub fn spawn(
        bind: impl ToSocketAddrs,
        tier: Arc<dyn KvTier>,
        limits: RemoteLimits,
    ) -> io::Result<Self> {
        validate_limits(limits)?;
        let listener = TcpListener::bind(resolve_one(bind)?)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_worker = shutdown.clone();
        let inflight = Arc::new(AtomicUsize::new(0));
        let handle = thread::Builder::new()
            .name("rivet-cache-tcp-server".to_string())
            .spawn(move || {
                while !shutdown_worker.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _peer)) => {
                            if inflight.load(Ordering::Acquire) >= limits.max_inflight_connections {
                                let mut stream = stream;
                                configure_stream(&stream, limits.io_timeout)?;
                                let _ = write_error_response(
                                    &mut stream,
                                    "remote KV server is at its connection limit",
                                );
                                continue;
                            }
                            inflight.fetch_add(1, Ordering::AcqRel);
                            let tier = tier.clone();
                            let inflight_worker = inflight.clone();
                            thread::spawn(move || {
                                let _guard = InflightGuard(inflight_worker);
                                let _ = handle_connection(stream, tier.as_ref(), limits);
                            });
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(())
            })?;

        Ok(Self {
            address,
            shutdown,
            handle: Some(handle),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub fn stop(mut self) -> io::Result<()> {
        self.signal_shutdown();
        if let Some(handle) = self.handle.take() {
            return handle
                .join()
                .map_err(|_| io::Error::other("remote KV server thread panicked"))?;
        }
        Ok(())
    }

    fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_millis(100));
    }
}

impl Drop for TcpKvServer {
    fn drop(&mut self) {
        self.signal_shutdown();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct InflightGuard(Arc<AtomicUsize>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn handle_connection(
    mut stream: TcpStream,
    tier: &dyn KvTier,
    limits: RemoteLimits,
) -> io::Result<()> {
    configure_stream(&stream, limits.io_timeout)?;
    let result = (|| {
        let op = read_request_header(&mut stream)?;
        match op {
            OP_GET => {
                let key = read_key(&mut stream, limits)?;
                match tier.get(&key)? {
                    Some(entry) => {
                        write_status(&mut stream, STATUS_OK)?;
                        write_entry_body(&mut stream, &entry, limits)?;
                    }
                    None => write_status(&mut stream, STATUS_MISS)?,
                }
            }
            OP_PUT => {
                let key = read_key(&mut stream, limits)?;
                let entry = read_entry(&mut stream, key, limits)?;
                tier.put(&entry)?;
                write_status(&mut stream, STATUS_OK)?;
            }
            OP_REMOVE => {
                let key = read_key(&mut stream, limits)?;
                tier.remove(&key)?;
                write_status(&mut stream, STATUS_OK)?;
            }
            OP_CLEAR => {
                tier.clear()?;
                write_status(&mut stream, STATUS_OK)?;
            }
            OP_PING => {
                tier.health()?;
                write_status(&mut stream, STATUS_OK)?;
            }
            _ => return Err(protocol_error(format!("unsupported remote opcode {op}"))),
        }
        stream.flush()
    })();

    if let Err(error) = result {
        let _ = write_error_response(&mut stream, &error.to_string());
        return Err(error);
    }
    Ok(())
}

fn validate_limits(limits: RemoteLimits) -> io::Result<()> {
    if limits.max_payload_bytes == 0
        || limits.max_model_bytes == 0
        || limits.max_inflight_connections == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "remote limits must be greater than zero",
        ));
    }
    Ok(())
}

fn resolve_one(address: impl ToSocketAddrs) -> io::Result<SocketAddr> {
    address.to_socket_addrs()?.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "address resolved to no endpoints",
        )
    })
}

fn configure_stream(stream: &TcpStream, timeout: Duration) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))
}

fn write_request_header(stream: &mut TcpStream, op: u8) -> io::Result<()> {
    stream.write_all(MAGIC)?;
    stream.write_all(&[op])
}

fn read_request_header(stream: &mut TcpStream) -> io::Result<u8> {
    let mut magic = [0_u8; MAGIC.len()];
    stream.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(protocol_error("invalid remote protocol magic"));
    }
    read_u8(stream)
}

fn write_status(stream: &mut TcpStream, status: u8) -> io::Result<()> {
    stream.write_all(MAGIC)?;
    stream.write_all(&[status])
}

fn read_status(stream: &mut TcpStream) -> io::Result<u8> {
    let mut magic = [0_u8; MAGIC.len()];
    stream.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(protocol_error("invalid remote response magic"));
    }
    read_u8(stream)
}

fn expect_simple_response(stream: &mut TcpStream) -> io::Result<()> {
    match read_status(stream)? {
        STATUS_OK => Ok(()),
        STATUS_ERROR => Err(read_remote_error(stream)?),
        status => Err(protocol_error(format!("unexpected remote status {status}"))),
    }
}

fn write_error_response(stream: &mut TcpStream, message: &str) -> io::Result<()> {
    let bytes = message.as_bytes();
    let bytes = &bytes[..bytes.len().min(MAX_ERROR_BYTES)];
    write_status(stream, STATUS_ERROR)?;
    write_u32(stream, bytes.len() as u32)?;
    stream.write_all(bytes)?;
    stream.flush()
}

fn read_remote_error(stream: &mut TcpStream) -> io::Result<io::Error> {
    let len = read_u32(stream)? as usize;
    if len > MAX_ERROR_BYTES {
        return Err(protocol_error(
            "remote error message exceeds protocol limit",
        ));
    }
    let mut bytes = vec![0_u8; len];
    stream.read_exact(&mut bytes)?;
    let message = String::from_utf8(bytes)
        .map_err(|_| protocol_error("remote error message is not UTF-8"))?;
    Ok(io::Error::other(message))
}

fn write_key(stream: &mut TcpStream, key: &KvBlockKey, limits: RemoteLimits) -> io::Result<()> {
    let model = key.model_fingerprint.as_bytes();
    if model.is_empty() || model.len() > limits.max_model_bytes || model.len() > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "model fingerprint exceeds remote protocol limits",
        ));
    }
    write_u32(stream, model.len() as u32)?;
    stream.write_all(model)?;
    stream.write_all(&key.sequence_hash)?;
    for value in [
        key.block_index,
        key.token_start,
        key.token_count,
        key.layer_start,
        key.layer_count,
        key.layout_version,
    ] {
        write_u32(stream, value)?;
    }
    Ok(())
}

fn read_key(stream: &mut TcpStream, limits: RemoteLimits) -> io::Result<KvBlockKey> {
    let model_len = read_u32(stream)? as usize;
    if model_len == 0 || model_len > limits.max_model_bytes {
        return Err(protocol_error("invalid model fingerprint length"));
    }
    let mut model = vec![0_u8; model_len];
    stream.read_exact(&mut model)?;
    let model_fingerprint =
        String::from_utf8(model).map_err(|_| protocol_error("model fingerprint is not UTF-8"))?;
    let mut sequence_hash = [0_u8; 32];
    stream.read_exact(&mut sequence_hash)?;
    let block_index = read_u32(stream)?;
    let token_start = read_u32(stream)?;
    let token_count = read_u32(stream)?;
    let layer_start = read_u32(stream)?;
    let layer_count = read_u32(stream)?;
    let layout_version = read_u32(stream)?;
    if token_count == 0 || layer_count == 0 {
        return Err(protocol_error(
            "remote KV key has an empty token or layer range",
        ));
    }
    Ok(KvBlockKey {
        model_fingerprint,
        sequence_hash,
        block_index,
        token_start,
        token_count,
        layer_start,
        layer_count,
        layout_version,
    })
}

fn write_entry_body(
    stream: &mut TcpStream,
    entry: &KvTierEntry,
    limits: RemoteLimits,
) -> io::Result<()> {
    let len = entry.block.bytes.len();
    if len == 0 || len > limits.max_payload_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KV payload exceeds remote protocol limits",
        ));
    }
    write_u64(stream, entry.expires_at)?;
    stream.write_all(&[u8::from(entry.pinned)])?;
    write_u64(stream, len as u64)?;
    stream.write_all(&entry.block.bytes)
}

fn read_entry(
    stream: &mut TcpStream,
    key: KvBlockKey,
    limits: RemoteLimits,
) -> io::Result<KvTierEntry> {
    let expires_at = read_u64(stream)?;
    let pinned = match read_u8(stream)? {
        0 => false,
        1 => true,
        _ => return Err(protocol_error("invalid remote pin flag")),
    };
    let len_u64 = read_u64(stream)?;
    let len = usize::try_from(len_u64).map_err(|_| protocol_error("payload length overflow"))?;
    if len == 0 || len > limits.max_payload_bytes {
        return Err(protocol_error("remote payload exceeds configured limit"));
    }
    let mut bytes = vec![0_u8; len];
    stream.read_exact(&mut bytes)?;
    Ok(KvTierEntry {
        block: KvBlock { key, bytes },
        expires_at,
        pinned,
    })
}

fn write_u32(stream: &mut TcpStream, value: u32) -> io::Result<()> {
    stream.write_all(&value.to_le_bytes())
}

fn write_u64(stream: &mut TcpStream, value: u64) -> io::Result<()> {
    stream.write_all(&value.to_le_bytes())
}

fn read_u8(stream: &mut TcpStream) -> io::Result<u8> {
    let mut bytes = [0_u8; 1];
    stream.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn read_u32(stream: &mut TcpStream) -> io::Result<u32> {
    let mut bytes = [0_u8; 4];
    stream.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(stream: &mut TcpStream) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    stream.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn protocol_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContextCache, ContextCacheTier, KvBlockRange};

    fn key() -> KvBlockKey {
        KvBlockKey::from_prefix(
            "remote-test-model",
            &[1, 2, 3, 4],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 4,
                layer_start: 0,
                layer_count: 16,
                layout_version: 1,
            },
        )
    }

    #[test]
    fn tcp_tier_round_trips_and_clears() {
        let cache = Arc::new(
            ContextCache::builder()
                .memory_capacity(1024 * 1024)
                .build()
                .expect("cache"),
        );
        let local: Arc<dyn KvTier> = Arc::new(ContextCacheTier::new("local", cache).expect("tier"));
        let server =
            TcpKvServer::spawn("127.0.0.1:0", local, RemoteLimits::default()).expect("server");
        let remote =
            TcpKvTier::new("remote", server.local_addr(), RemoteLimits::default()).expect("remote");
        remote.ping().expect("ping");

        let entry = KvTierEntry {
            block: KvBlock {
                key: key(),
                bytes: vec![4, 5, 6],
            },
            expires_at: 1234,
            pinned: true,
        };
        remote.put(&entry).expect("put");
        assert_eq!(
            remote.get(&entry.block.key).expect("get"),
            Some(entry.clone())
        );
        remote.clear().expect("clear");
        assert!(remote
            .get(&entry.block.key)
            .expect("get after clear")
            .is_none());
        server.stop().expect("stop");
    }
}
