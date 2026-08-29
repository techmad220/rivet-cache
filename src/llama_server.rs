use crate::ContextCache;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const LLAMA_SERVER_SLOT_NAMESPACE: &str = "LLAMA_SERVER_SLOT_V1";
const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024;
static SLOT_TEMP_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlamaServerSlotAction {
    pub slot_id: u32,
    pub filename: String,
    pub token_count: Option<u64>,
    pub bytes: Option<u64>,
    pub response_body: String,
}

pub trait LlamaServerSlotControl: Send + Sync {
    fn save(&self, slot_id: u32, filename: &str) -> io::Result<LlamaServerSlotAction>;
    fn restore(&self, slot_id: u32, filename: &str) -> io::Result<LlamaServerSlotAction>;
    fn erase(&self, slot_id: u32) -> io::Result<LlamaServerSlotAction>;
}

#[derive(Debug, Clone)]
pub struct HttpLlamaServerSlotControl {
    address: SocketAddr,
    host_header: String,
    timeout: Duration,
    max_response_bytes: usize,
}

impl HttpLlamaServerSlotControl {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            host_header: address.to_string(),
            timeout: Duration::from_secs(30),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> io::Result<Self> {
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "llama-server timeout must be greater than zero",
            ));
        }
        self.timeout = timeout;
        Ok(self)
    }

    pub fn with_max_response_bytes(mut self, bytes: usize) -> io::Result<Self> {
        if bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "llama-server maximum response size must be greater than zero",
            ));
        }
        self.max_response_bytes = bytes;
        Ok(self)
    }

    fn post_action(
        &self,
        slot_id: u32,
        action: &str,
        filename: Option<&str>,
    ) -> io::Result<LlamaServerSlotAction> {
        if !matches!(action, "save" | "restore" | "erase") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported llama-server slot action",
            ));
        }

        let filename = match filename {
            Some(value) => {
                validate_slot_filename(value)?;
                value
            }
            None => "",
        };
        let body = if filename.is_empty() {
            "{}".to_string()
        } else {
            format!("{{\"filename\":\"{filename}\"}}")
        };
        let path = format!("/slots/{slot_id}?action={action}");
        let response = self.request(&path, &body)?;
        let (token_field, byte_field) = match action {
            "save" => ("n_saved", "n_written"),
            "restore" => ("n_restored", "n_read"),
            "erase" => ("n_erased", ""),
            _ => unreachable!(),
        };

        Ok(LlamaServerSlotAction {
            slot_id,
            filename: filename.to_string(),
            token_count: json_u64_field(&response, token_field),
            bytes: if byte_field.is_empty() {
                None
            } else {
                json_u64_field(&response, byte_field)
            },
            response_body: response,
        })
    }

    fn request(&self, path: &str, body: &str) -> io::Result<String> {
        let mut stream = TcpStream::connect_timeout(&self.address, self.timeout)?;
        stream.set_read_timeout(Some(self.timeout))?;
        stream.set_write_timeout(Some(self.timeout))?;

        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.host_header,
            body.len()
        );
        stream.write_all(request.as_bytes())?;
        stream.flush()?;
        let _ = stream.shutdown(Shutdown::Write);

        let mut limited = stream.take((self.max_response_bytes as u64).saturating_add(1));
        let mut response = Vec::new();
        limited.read_to_end(&mut response)?;
        if response.len() > self.max_response_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "llama-server HTTP response exceeded configured limit",
            ));
        }
        parse_http_response(&response)
    }
}

impl LlamaServerSlotControl for HttpLlamaServerSlotControl {
    fn save(&self, slot_id: u32, filename: &str) -> io::Result<LlamaServerSlotAction> {
        self.post_action(slot_id, "save", Some(filename))
    }

    fn restore(&self, slot_id: u32, filename: &str) -> io::Result<LlamaServerSlotAction> {
        self.post_action(slot_id, "restore", Some(filename))
    }

    fn erase(&self, slot_id: u32) -> io::Result<LlamaServerSlotAction> {
        self.post_action(slot_id, "erase", None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlamaServerSlotReceipt {
    pub cache_key: String,
    pub state_bytes: u64,
    pub state_sha256: String,
    pub action: LlamaServerSlotAction,
}

pub struct LlamaServerSlotBridge {
    cache: Arc<ContextCache>,
    control: Arc<dyn LlamaServerSlotControl>,
    slot_root: PathBuf,
    max_state_bytes: u64,
}

impl LlamaServerSlotBridge {
    pub fn new(
        cache: Arc<ContextCache>,
        control: Arc<dyn LlamaServerSlotControl>,
        slot_root: impl Into<PathBuf>,
        max_state_bytes: u64,
    ) -> io::Result<Self> {
        if max_state_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "llama-server maximum state size must be greater than zero",
            ));
        }
        let slot_root = slot_root.into();
        fs::create_dir_all(&slot_root)?;
        let slot_root = fs::canonicalize(slot_root)?;
        if !slot_root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "llama-server slot root is not a directory",
            ));
        }
        Ok(Self {
            cache,
            control,
            slot_root,
            max_state_bytes,
        })
    }

    pub fn cache_key(&self, model_fingerprint: &str, logical_identity: &str) -> io::Result<String> {
        if model_fingerprint.trim().is_empty() || logical_identity.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "model fingerprint and logical slot identity must not be empty",
            ));
        }
        Ok(self.cache.make_key(
            LLAMA_SERVER_SLOT_NAMESPACE,
            model_fingerprint,
            logical_identity,
        ))
    }

    pub fn capture(
        &self,
        slot_id: u32,
        model_fingerprint: &str,
        logical_identity: &str,
        pinned: bool,
    ) -> io::Result<LlamaServerSlotReceipt> {
        let key = self.cache_key(model_fingerprint, logical_identity)?;
        let filename = slot_filename_for_key(&key);
        self.remove_slot_file_if_regular(&filename)?;

        let action = self.control.save(slot_id, &filename)?;
        let result = (|| {
            let bytes = self.read_slot_file(&filename)?;
            if let Some(reported) = action.bytes {
                if reported != bytes.len() as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "llama-server reported {reported} saved bytes but slot file contains {}",
                            bytes.len()
                        ),
                    ));
                }
            }
            self.cache.put(&key, &bytes, None, pinned)?;
            Ok(receipt_for(key, bytes, action))
        })();
        let cleanup = self.remove_slot_file_if_regular(&filename);
        match (result, cleanup) {
            (Ok(receipt), Ok(())) => Ok(receipt),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    pub fn restore(
        &self,
        slot_id: u32,
        model_fingerprint: &str,
        logical_identity: &str,
    ) -> io::Result<LlamaServerSlotReceipt> {
        let key = self.cache_key(model_fingerprint, logical_identity)?;
        let bytes = self.cache.get(&key)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "requested llama-server slot state is not present in RivetCache",
            )
        })?;
        self.validate_state_len(bytes.len() as u64)?;
        let filename = slot_filename_for_key(&key);
        self.remove_slot_file_if_regular(&filename)?;
        self.write_slot_file_atomic(&filename, &bytes)?;

        let result = (|| {
            let action = self.control.restore(slot_id, &filename)?;
            if let Some(reported) = action.bytes {
                if reported != bytes.len() as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "llama-server reported {reported} restored bytes but RivetCache materialized {}",
                            bytes.len()
                        ),
                    ));
                }
            }
            Ok(receipt_for(key, bytes, action))
        })();
        let cleanup = self.remove_slot_file_if_regular(&filename);
        match (result, cleanup) {
            (Ok(receipt), Ok(())) => Ok(receipt),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    pub fn erase(&self, slot_id: u32) -> io::Result<LlamaServerSlotAction> {
        self.control.erase(slot_id)
    }

    pub fn contains(&self, model_fingerprint: &str, logical_identity: &str) -> io::Result<bool> {
        let key = self.cache_key(model_fingerprint, logical_identity)?;
        Ok(self.cache.get(&key)?.is_some())
    }

    fn validate_state_len(&self, bytes: u64) -> io::Result<()> {
        if bytes == 0 || bytes > self.max_state_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "llama-server slot state size {bytes} is outside configured range 1..={} bytes",
                    self.max_state_bytes
                ),
            ));
        }
        Ok(())
    }

    fn path_for(&self, filename: &str) -> io::Result<PathBuf> {
        validate_slot_filename(filename)?;
        Ok(self.slot_root.join(filename))
    }

    fn read_slot_file(&self, filename: &str) -> io::Result<Vec<u8>> {
        let path = self.path_for(filename)?;
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "llama-server slot state is not a regular non-symlink file",
            ));
        }
        self.validate_state_len(metadata.len())?;
        let canonical = fs::canonicalize(&path)?;
        if canonical.parent() != Some(self.slot_root.as_path()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "llama-server slot state resolved outside the configured slot root",
            ));
        }
        let mut file = File::open(canonical)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut bytes)?;
        self.validate_state_len(bytes.len() as u64)?;
        Ok(bytes)
    }

    fn write_slot_file_atomic(&self, filename: &str, bytes: &[u8]) -> io::Result<()> {
        self.validate_state_len(bytes.len() as u64)?;
        let final_path = self.path_for(filename)?;
        let nonce = SLOT_TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let temp_name = format!("tmp-{filename}.{}.{}.tmp", std::process::id(), nonce);
        let temp_path = self.path_for(&temp_name)?;

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        let write_result = (|| {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok::<(), io::Error>(())
        })();
        drop(file);
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }

        if final_path.exists() {
            self.remove_slot_file_if_regular(filename)?;
        }
        if let Err(error) = fs::rename(&temp_path, &final_path) {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }
        Ok(())
    }

    fn remove_slot_file_if_regular(&self, filename: &str) -> io::Result<()> {
        let path = self.path_for(filename)?;
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "refusing to remove non-regular or symlink slot path",
            ));
        }
        fs::remove_file(path)
    }
}

fn receipt_for(
    cache_key: String,
    bytes: Vec<u8>,
    action: LlamaServerSlotAction,
) -> LlamaServerSlotReceipt {
    let state_sha256 = hex::encode(Sha256::digest(&bytes));
    LlamaServerSlotReceipt {
        cache_key,
        state_bytes: bytes.len() as u64,
        state_sha256,
        action,
    }
}

fn slot_filename_for_key(key: &str) -> String {
    format!("rivet-{key}.ggsq")
}

fn validate_slot_filename(filename: &str) -> io::Result<()> {
    let valid = !filename.is_empty()
        && filename.len() <= 160
        && filename != "."
        && filename != ".."
        && !filename.starts_with('.')
        && filename
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid || filename.contains("..") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "llama-server slot filename must be a simple ASCII basename",
        ));
    }
    Ok(())
}

fn json_u64_field(body: &str, field: &str) -> Option<u64> {
    if field.is_empty() {
        return None;
    }
    let needle = format!("\"{field}\"");
    let start = body.find(&needle)?.saturating_add(needle.len());
    let tail = &body[start..];
    let colon = tail.find(':')?;
    let digits = tail[colon + 1..].trim_start();
    let end = digits
        .find(|value: char| !value.is_ascii_digit())
        .unwrap_or(digits.len());
    if end == 0 {
        return None;
    }
    digits[..end].parse().ok()
}

fn parse_http_response(response: &[u8]) -> io::Result<String> {
    let separator = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP response"))?;
    let headers = std::str::from_utf8(&response[..separator])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 HTTP headers"))?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP status line"))?;
    let raw_body = &response[separator + 4..];
    let body = if headers.lines().any(|line| {
        line.split_once(':')
            .map(|(name, value)| {
                name.eq_ignore_ascii_case("transfer-encoding")
                    && value.trim().eq_ignore_ascii_case("chunked")
            })
            .unwrap_or(false)
    }) {
        decode_chunked(raw_body)?
    } else {
        raw_body.to_vec()
    };
    let body = String::from_utf8(body)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 HTTP body"))?;
    if !(200..300).contains(&status) {
        return Err(io::Error::other(format!(
            "llama-server HTTP {status}: {}",
            body.trim()
        )));
    }
    Ok(body)
}

fn decode_chunked(mut input: &[u8]) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    loop {
        let line_end = input
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk header"))?;
        let line = std::str::from_utf8(&input[..line_end])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))?;
        let size_text = line.split(';').next().unwrap_or(line).trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))?;
        input = &input[line_end + 2..];
        if size == 0 {
            break;
        }
        if input.len() < size.saturating_add(2) || &input[size..size + 2] != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated HTTP chunk",
            ));
        }
        output.extend_from_slice(&input[..size]);
        input = &input[size + 2..];
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct MockControl {
        slot_root: PathBuf,
        payload: Vec<u8>,
        restored: Mutex<Vec<u8>>,
    }

    impl LlamaServerSlotControl for MockControl {
        fn save(&self, slot_id: u32, filename: &str) -> io::Result<LlamaServerSlotAction> {
            fs::write(self.slot_root.join(filename), &self.payload)?;
            Ok(LlamaServerSlotAction {
                slot_id,
                filename: filename.to_string(),
                token_count: Some(7),
                bytes: Some(self.payload.len() as u64),
                response_body: "mock-save".to_string(),
            })
        }

        fn restore(&self, slot_id: u32, filename: &str) -> io::Result<LlamaServerSlotAction> {
            let bytes = fs::read(self.slot_root.join(filename))?;
            *self
                .restored
                .lock()
                .map_err(|_| io::Error::other("mock restore lock poisoned"))? = bytes.clone();
            Ok(LlamaServerSlotAction {
                slot_id,
                filename: filename.to_string(),
                token_count: Some(7),
                bytes: Some(bytes.len() as u64),
                response_body: "mock-restore".to_string(),
            })
        }

        fn erase(&self, slot_id: u32) -> io::Result<LlamaServerSlotAction> {
            Ok(LlamaServerSlotAction {
                slot_id,
                filename: String::new(),
                token_count: Some(7),
                bytes: None,
                response_body: "mock-erase".to_string(),
            })
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rivet-{name}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn slot_bridge_round_trips_saved_state_through_context_cache() {
        let root = temp_root("llama-slot");
        let slot_root = root.join("slots");
        let cache_root = root.join("cache");
        fs::create_dir_all(&slot_root).expect("slot root");
        let cache = Arc::new(
            ContextCache::new(Some(cache_root), 0, 4 * 1024 * 1024, Duration::ZERO).expect("cache"),
        );
        let control = Arc::new(MockControl {
            slot_root: slot_root.clone(),
            payload: vec![1, 3, 3, 7, 9, 9, 4, 2],
            restored: Mutex::new(Vec::new()),
        });
        let bridge = LlamaServerSlotBridge::new(cache, control.clone(), &slot_root, 1024 * 1024)
            .expect("bridge");

        let captured = bridge
            .capture(0, "model-sha", "prompt-sha", true)
            .expect("capture");
        assert_eq!(captured.state_bytes, 8);
        assert!(bridge
            .contains("model-sha", "prompt-sha")
            .expect("contains"));
        assert!(!slot_root
            .join(slot_filename_for_key(&captured.cache_key))
            .exists());

        let restored = bridge
            .restore(1, "model-sha", "prompt-sha")
            .expect("restore");
        assert_eq!(restored.cache_key, captured.cache_key);
        assert_eq!(restored.state_sha256, captured.state_sha256);
        assert_eq!(
            *control.restored.lock().expect("restored"),
            vec![1, 3, 3, 7, 9, 9, 4, 2]
        );
        assert!(!slot_root
            .join(slot_filename_for_key(&captured.cache_key))
            .exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn http_control_uses_slot_action_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            stream.read_to_end(&mut request).expect("request");
            let request = String::from_utf8(request).expect("utf8 request");
            assert!(request.starts_with("POST /slots/3?action=save HTTP/1.1\r\n"));
            assert!(request.contains("{\"filename\":\"slot.ggsq\"}"));
            let body =
                "{\"id_slot\":3,\"filename\":\"slot.ggsq\",\"n_saved\":12,\"n_written\":345}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).expect("response");
        });

        let control = HttpLlamaServerSlotControl::new(address)
            .with_timeout(Duration::from_secs(5))
            .expect("timeout");
        let result = control.save(3, "slot.ggsq").expect("save");
        assert_eq!(result.token_count, Some(12));
        assert_eq!(result.bytes, Some(345));
        server.join().expect("server");
    }

    #[test]
    fn filename_validation_rejects_traversal_and_hidden_paths() {
        for invalid in ["../state", "..", ".hidden", "a/b", "a\\b", ""] {
            assert!(validate_slot_filename(invalid).is_err(), "{invalid}");
        }
        assert!(validate_slot_filename("rivet-0123.ggsq").is_ok());
    }

    #[test]
    fn chunked_response_decoder_works() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n3\r\n123\r\n0\r\n\r\n";
        assert_eq!(parse_http_response(response).expect("response"), "test123");
    }
}
