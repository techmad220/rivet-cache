use crate::{KvBlock, KvBlockKey, KvTier, KvTierEntry};
use sha2::{Digest, Sha256};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const KV_ENVELOPE_MAGIC: &[u8; 6] = b"RKV01\n";

pub trait RedisStream: Read + Write + Send {}
impl<T: Read + Write + Send> RedisStream for T {}

pub trait RedisDialer: Send + Sync {
    fn connect(
        &self,
        address: &str,
        connect_timeout: Duration,
        io_timeout: Duration,
    ) -> io::Result<Box<dyn RedisStream>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TcpRedisDialer;

impl RedisDialer for TcpRedisDialer {
    fn connect(
        &self,
        address: &str,
        connect_timeout: Duration,
        io_timeout: Duration,
    ) -> io::Result<Box<dyn RedisStream>> {
        let mut addresses = address.to_socket_addrs()?;
        let address = addresses.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Redis address did not resolve")
        })?;
        let stream = TcpStream::connect_timeout(&address, connect_timeout)?;
        stream.set_read_timeout(Some(io_timeout))?;
        stream.set_write_timeout(Some(io_timeout))?;
        stream.set_nodelay(true)?;
        Ok(Box::new(stream))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisAuth {
    pub username: Option<String>,
    pub password: String,
}

pub struct RedisKvTier {
    name: String,
    address: String,
    namespace: String,
    auth: Option<RedisAuth>,
    dialer: Arc<dyn RedisDialer>,
    connect_timeout: Duration,
    io_timeout: Duration,
    max_value_bytes: usize,
}

impl RedisKvTier {
    pub fn new(
        name: impl Into<String>,
        address: impl Into<String>,
        namespace: impl Into<String>,
        max_value_bytes: usize,
    ) -> io::Result<Self> {
        Self::with_dialer(
            name,
            address,
            namespace,
            max_value_bytes,
            Arc::new(TcpRedisDialer),
        )
    }

    pub fn with_dialer(
        name: impl Into<String>,
        address: impl Into<String>,
        namespace: impl Into<String>,
        max_value_bytes: usize,
        dialer: Arc<dyn RedisDialer>,
    ) -> io::Result<Self> {
        let name = name.into();
        let address = address.into();
        let namespace = namespace.into();
        if name.trim().is_empty()
            || address.trim().is_empty()
            || namespace.trim().is_empty()
            || max_value_bytes == 0
            || namespace
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte == b' ')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Redis tier requires valid name/address/namespace and a non-zero value limit",
            ));
        }
        Ok(Self {
            name,
            address,
            namespace,
            auth: None,
            dialer,
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(30),
            max_value_bytes,
        })
    }

    pub fn with_auth(mut self, auth: RedisAuth) -> io::Result<Self> {
        if auth.password.is_empty()
            || auth
                .username
                .as_deref()
                .is_some_and(|username| username.is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Redis authentication requires a non-empty password and username when supplied",
            ));
        }
        self.auth = Some(auth);
        Ok(self)
    }

    pub fn timeouts(mut self, connect: Duration, io_timeout: Duration) -> io::Result<Self> {
        if connect.is_zero() || io_timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Redis timeouts must be greater than zero",
            ));
        }
        self.connect_timeout = connect;
        self.io_timeout = io_timeout;
        Ok(self)
    }

    fn redis_key(&self, key: &KvBlockKey) -> Vec<u8> {
        format!("{}:{}", self.namespace, key.cache_key()).into_bytes()
    }

    fn command(&self, arguments: &[Vec<u8>]) -> io::Result<RespValue> {
        let stream = self
            .dialer
            .connect(&self.address, self.connect_timeout, self.io_timeout)?;
        let mut stream = BufReader::new(stream);
        if let Some(auth) = &self.auth {
            let mut arguments = vec![b"AUTH".to_vec()];
            if let Some(username) = &auth.username {
                arguments.push(username.as_bytes().to_vec());
            }
            arguments.push(auth.password.as_bytes().to_vec());
            write_resp_command(stream.get_mut(), &arguments)?;
            expect_simple_ok(read_resp(&mut stream, self.max_value_bytes, 0)?, "AUTH")?;
        }
        write_resp_command(stream.get_mut(), arguments)?;
        read_resp(&mut stream, self.max_value_bytes, 0)
    }
}

impl KvTier for RedisKvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let result = self.command(&[b"GET".to_vec(), self.redis_key(key)])?;
        match result {
            RespValue::Bulk(None) => Ok(None),
            RespValue::Bulk(Some(bytes)) => {
                decode_entry(key.clone(), &bytes, self.max_value_bytes).map(Some)
            }
            other => Err(protocol_error(format!("GET returned {other:?}"))),
        }
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        let encoded = encode_entry(entry)?;
        if encoded.len() > self.max_value_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Redis KV entry exceeds configured value limit",
            ));
        }
        let result = self.command(&[b"SET".to_vec(), self.redis_key(&entry.block.key), encoded])?;
        expect_simple_ok(result, "SET")
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        match self.command(&[b"DEL".to_vec(), self.redis_key(key)])? {
            RespValue::Integer(removed) if removed >= 0 => Ok(()),
            other => Err(protocol_error(format!("DEL returned {other:?}"))),
        }
    }

    fn clear(&self) -> io::Result<()> {
        let pattern = format!("{}:*", self.namespace).into_bytes();
        let mut cursor = b"0".to_vec();
        for _ in 0..100_000 {
            let response = self.command(&[
                b"SCAN".to_vec(),
                cursor.clone(),
                b"MATCH".to_vec(),
                pattern.clone(),
                b"COUNT".to_vec(),
                b"256".to_vec(),
            ])?;
            let RespValue::Array(Some(mut outer)) = response else {
                return Err(protocol_error("SCAN returned an unexpected response"));
            };
            if outer.len() != 2 {
                return Err(protocol_error("SCAN response does not have two fields"));
            }
            let keys = match outer.pop().unwrap() {
                RespValue::Array(Some(values)) => values,
                _ => return Err(protocol_error("SCAN key list is invalid")),
            };
            cursor = match outer.pop().unwrap() {
                RespValue::Bulk(Some(value)) => value,
                RespValue::Simple(value) => value.into_bytes(),
                _ => return Err(protocol_error("SCAN cursor is invalid")),
            };
            let mut delete = vec![b"DEL".to_vec()];
            for key in keys {
                match key {
                    RespValue::Bulk(Some(key)) => delete.push(key),
                    _ => return Err(protocol_error("SCAN returned a non-bulk key")),
                }
            }
            if delete.len() > 1 {
                match self.command(&delete)? {
                    RespValue::Integer(removed) if removed >= 0 => {}
                    other => return Err(protocol_error(format!("DEL batch returned {other:?}"))),
                }
            }
            if cursor == b"0" {
                return Ok(());
            }
        }
        Err(io::Error::other(
            "Redis clear exceeded the bounded SCAN iteration limit",
        ))
    }

    fn health(&self) -> io::Result<()> {
        match self.command(&[b"PING".to_vec()])? {
            RespValue::Simple(value) if value.eq_ignore_ascii_case("PONG") => Ok(()),
            other => Err(protocol_error(format!("PING returned {other:?}"))),
        }
    }
}

#[derive(Debug)]
enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

fn write_resp_command(stream: &mut dyn RedisStream, arguments: &[Vec<u8>]) -> io::Result<()> {
    if arguments.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty Redis command",
        ));
    }
    write!(stream, "*{}\r\n", arguments.len())?;
    for argument in arguments {
        write!(stream, "${}\r\n", argument.len())?;
        stream.write_all(argument)?;
        stream.write_all(b"\r\n")?;
    }
    stream.flush()
}

fn read_resp<R: BufRead>(reader: &mut R, max_bytes: usize, depth: usize) -> io::Result<RespValue> {
    if depth > 8 {
        return Err(protocol_error("RESP nesting exceeds limit"));
    }
    let mut prefix = [0_u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' => Ok(RespValue::Simple(read_resp_line(reader, 4096)?)),
        b'-' => Ok(RespValue::Error(read_resp_line(reader, 8192)?)),
        b':' => {
            let line = read_resp_line(reader, 64)?;
            let value = line
                .parse::<i64>()
                .map_err(|_| protocol_error("invalid RESP integer"))?;
            Ok(RespValue::Integer(value))
        }
        b'$' => {
            let line = read_resp_line(reader, 64)?;
            let length = line
                .parse::<i64>()
                .map_err(|_| protocol_error("invalid RESP bulk length"))?;
            if length == -1 {
                return Ok(RespValue::Bulk(None));
            }
            let length =
                usize::try_from(length).map_err(|_| protocol_error("negative RESP bulk length"))?;
            if length > max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESP bulk value exceeds limit",
                ));
            }
            let mut bytes = vec![0_u8; length];
            reader.read_exact(&mut bytes)?;
            expect_crlf(reader)?;
            Ok(RespValue::Bulk(Some(bytes)))
        }
        b'*' => {
            let line = read_resp_line(reader, 64)?;
            let count = line
                .parse::<i64>()
                .map_err(|_| protocol_error("invalid RESP array length"))?;
            if count == -1 {
                return Ok(RespValue::Array(None));
            }
            let count =
                usize::try_from(count).map_err(|_| protocol_error("negative RESP array length"))?;
            if count > 4096 {
                return Err(protocol_error("RESP array exceeds element limit"));
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(read_resp(reader, max_bytes, depth + 1)?);
            }
            Ok(RespValue::Array(Some(values)))
        }
        _ => Err(protocol_error("unsupported RESP type")),
    }
}

fn read_resp_line<R: BufRead>(reader: &mut R, max: usize) -> io::Result<String> {
    let mut line = Vec::new();
    let count = reader.read_until(b'\n', &mut line)?;
    if count == 0 || count > max || !line.ends_with(b"\r\n") {
        return Err(protocol_error("invalid RESP line"));
    }
    line.truncate(line.len() - 2);
    String::from_utf8(line).map_err(|_| protocol_error("RESP line is not UTF-8"))
}

fn expect_crlf<R: Read>(reader: &mut R) -> io::Result<()> {
    let mut ending = [0_u8; 2];
    reader.read_exact(&mut ending)?;
    if ending != *b"\r\n" {
        return Err(protocol_error("RESP bulk value missing CRLF"));
    }
    Ok(())
}

fn expect_simple_ok(value: RespValue, operation: &str) -> io::Result<()> {
    match value {
        RespValue::Simple(value) if value.eq_ignore_ascii_case("OK") => Ok(()),
        RespValue::Error(error) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("Redis {operation} failed: {error}"),
        )),
        other => Err(protocol_error(format!(
            "Redis {operation} returned {other:?}"
        ))),
    }
}

fn protocol_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: String,
    pub scheme: String,
    pub authority: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub trait HttpClient: Send + Sync {
    fn execute(&self, request: &HttpRequest) -> io::Result<HttpResponse>;
}

#[derive(Debug, Clone)]
pub struct TcpHttpClient {
    pub connect_timeout: Duration,
    pub io_timeout: Duration,
    pub max_response_bytes: usize,
}

impl Default for TcpHttpClient {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(30),
            max_response_bytes: 512 * 1024 * 1024,
        }
    }
}

impl HttpClient for TcpHttpClient {
    fn execute(&self, request: &HttpRequest) -> io::Result<HttpResponse> {
        if request.scheme != "http" {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "built-in TCP HTTP client supports http only; inject a TLS HttpClient for HTTPS/S3",
            ));
        }
        let address = if request.authority.contains(':') {
            request.authority.clone()
        } else {
            format!("{}:80", request.authority)
        };
        let socket = address.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTP authority did not resolve",
            )
        })?;
        let mut stream = TcpStream::connect_timeout(&socket, self.connect_timeout)?;
        stream.set_read_timeout(Some(self.io_timeout))?;
        stream.set_write_timeout(Some(self.io_timeout))?;
        write!(stream, "{} {} HTTP/1.1\r\n", request.method, request.path)?;
        let mut has_host = false;
        let mut has_length = false;
        for (name, value) in &request.headers {
            if name.eq_ignore_ascii_case("host") {
                has_host = true;
            }
            if name.eq_ignore_ascii_case("content-length") {
                has_length = true;
            }
            write!(stream, "{name}: {value}\r\n")?;
        }
        if !has_host {
            write!(stream, "Host: {}\r\n", request.authority)?;
        }
        if !has_length {
            write!(stream, "Content-Length: {}\r\n", request.body.len())?;
        }
        stream.write_all(b"Connection: close\r\n\r\n")?;
        stream.write_all(&request.body)?;
        stream.flush()?;

        let mut reader = BufReader::new(stream);
        let status_line = read_http_line(&mut reader, 8192)?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| protocol_error("HTTP response missing status"))?
            .parse::<u16>()
            .map_err(|_| protocol_error("HTTP status is invalid"))?;
        let mut headers = Vec::new();
        let mut content_length = None;
        let mut chunked = false;
        loop {
            let line = read_http_line(&mut reader, 64 * 1024)?;
            if line.is_empty() {
                break;
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| protocol_error("HTTP header is invalid"))?;
            let value = value.trim().to_owned();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| protocol_error("bad content-length"))?,
                );
            }
            if name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            }
            headers.push((name.trim().to_owned(), value));
        }
        let body = if chunked {
            read_chunked(&mut reader, self.max_response_bytes)?
        } else if let Some(length) = content_length {
            if length > self.max_response_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP response exceeds limit",
                ));
            }
            let mut body = vec![0_u8; length];
            reader.read_exact(&mut body)?;
            body
        } else {
            let mut body = Vec::new();
            reader
                .take((self.max_response_bytes as u64).saturating_add(1))
                .read_to_end(&mut body)?;
            if body.len() > self.max_response_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP response exceeds limit",
                ));
            }
            body
        };
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

pub trait S3Clock: Send + Sync {
    fn timestamp(&self) -> io::Result<(String, String)>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemS3Clock;

impl S3Clock for SystemS3Clock {
    fn timestamp(&self) -> io::Result<(String, String)> {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| io::Error::other("system clock is before Unix epoch"))?
            .as_secs();
        aws_timestamp(seconds)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub max_value_bytes: usize,
}

pub struct S3KvTier {
    name: String,
    endpoint: Endpoint,
    config: S3Config,
    credentials: S3Credentials,
    client: Arc<dyn HttpClient>,
    clock: Arc<dyn S3Clock>,
}

impl S3KvTier {
    pub fn new(
        name: impl Into<String>,
        config: S3Config,
        credentials: S3Credentials,
        client: Arc<dyn HttpClient>,
    ) -> io::Result<Self> {
        Self::with_clock(name, config, credentials, client, Arc::new(SystemS3Clock))
    }

    pub fn with_clock(
        name: impl Into<String>,
        config: S3Config,
        credentials: S3Credentials,
        client: Arc<dyn HttpClient>,
        clock: Arc<dyn S3Clock>,
    ) -> io::Result<Self> {
        let name = name.into();
        if name.trim().is_empty()
            || config.region.trim().is_empty()
            || config.bucket.trim().is_empty()
            || config.max_value_bytes == 0
            || credentials.access_key.is_empty()
            || credentials.secret_key.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid S3 tier configuration",
            ));
        }
        let endpoint = Endpoint::parse(&config.endpoint)?;
        Ok(Self {
            name,
            endpoint,
            config,
            credentials,
            client,
            clock,
        })
    }

    fn object_path(&self, key: &KvBlockKey) -> String {
        let mut parts = Vec::new();
        if !self.endpoint.base_path.is_empty() {
            parts.push(self.endpoint.base_path.trim_matches('/').to_owned());
        }
        parts.push(percent_path_segment(&self.config.bucket));
        for part in self
            .config
            .prefix
            .split('/')
            .filter(|part| !part.is_empty())
        {
            parts.push(percent_path_segment(part));
        }
        parts.push(key.cache_key());
        format!("/{}", parts.join("/"))
    }

    fn bucket_path(&self) -> String {
        let mut parts = Vec::new();
        if !self.endpoint.base_path.is_empty() {
            parts.push(self.endpoint.base_path.trim_matches('/').to_owned());
        }
        parts.push(percent_path_segment(&self.config.bucket));
        format!("/{}", parts.join("/"))
    }

    fn request(&self, method: &str, path: String, body: Vec<u8>) -> io::Result<HttpResponse> {
        let (date, amz_date) = self.clock.timestamp()?;
        let payload_hash = hex::encode(Sha256::digest(&body));
        let mut canonical_headers = vec![
            ("host".to_owned(), self.endpoint.authority.clone()),
            ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
            ("x-amz-date".to_owned(), amz_date.clone()),
        ];
        if let Some(token) = &self.credentials.session_token {
            canonical_headers.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        canonical_headers.sort_by(|left, right| left.0.cmp(&right.0));
        let signed_headers = canonical_headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let mut canonical_header_text = String::new();
        for (name, value) in &canonical_headers {
            canonical_header_text.push_str(name);
            canonical_header_text.push(':');
            canonical_header_text.push_str(value.trim());
            canonical_header_text.push('\n');
        }
        let canonical_request = format!(
            "{method}\n{path}\n\n{canonical_header_text}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{}/{}/s3/aws4_request", date, self.config.region);
        let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_hash}");
        let signature = s3_signature(
            &self.credentials.secret_key,
            &date,
            &self.config.region,
            string_to_sign.as_bytes(),
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.credentials.access_key,
            scope,
            signed_headers,
            hex::encode(signature)
        );
        let mut headers = canonical_headers;
        headers.push(("Authorization".to_owned(), authorization));
        self.client.execute(&HttpRequest {
            method: method.to_owned(),
            scheme: self.endpoint.scheme.clone(),
            authority: self.endpoint.authority.clone(),
            path,
            headers,
            body,
        })
    }
}

impl KvTier for S3KvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let response = self.request("GET", self.object_path(key), Vec::new())?;
        match response.status {
            200 => decode_entry(key.clone(), &response.body, self.config.max_value_bytes).map(Some),
            404 => Ok(None),
            status => Err(io::Error::other(format!("S3 GET returned HTTP {status}"))),
        }
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        let body = encode_entry(entry)?;
        if body.len() > self.config.max_value_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "S3 KV entry exceeds configured limit",
            ));
        }
        let response = self.request("PUT", self.object_path(&entry.block.key), body)?;
        if matches!(response.status, 200 | 201 | 204) {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "S3 PUT returned HTTP {}",
                response.status
            )))
        }
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        let response = self.request("DELETE", self.object_path(key), Vec::new())?;
        if matches!(response.status, 200 | 204 | 404) {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "S3 DELETE returned HTTP {}",
                response.status
            )))
        }
    }

    fn clear(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "S3 tier intentionally does not bulk-delete a bucket/prefix through KvTier::clear",
        ))
    }

    fn health(&self) -> io::Result<()> {
        let response = self.request("HEAD", self.bucket_path(), Vec::new())?;
        if matches!(response.status, 200 | 204) {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "S3 health returned HTTP {}",
                response.status
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Endpoint {
    scheme: String,
    authority: String,
    base_path: String,
}

impl Endpoint {
    fn parse(value: &str) -> io::Result<Self> {
        let (scheme, rest) = value.split_once("://").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "S3 endpoint must include scheme://",
            )
        })?;
        if !matches!(scheme, "http" | "https") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported S3 endpoint scheme",
            ));
        }
        let (authority, base_path) = rest.split_once('/').unwrap_or((rest, ""));
        if authority.is_empty()
            || authority
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte == b' ')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid S3 endpoint authority",
            ));
        }
        Ok(Self {
            scheme: scheme.to_owned(),
            authority: authority.to_owned(),
            base_path: base_path.trim_matches('/').to_owned(),
        })
    }
}

fn s3_signature(secret: &str, date: &str, region: &str, string_to_sign: &[u8]) -> [u8; 32] {
    let key_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let key_region = hmac_sha256(&key_date, region.as_bytes());
    let key_service = hmac_sha256(&key_region, b"s3");
    let key_signing = hmac_sha256(&key_service, b"aws4_request");
    hmac_sha256(&key_signing, string_to_sign)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut normalized = [0_u8; 64];
    if key.len() > 64 {
        let digest: [u8; 32] = Sha256::digest(key).into();
        normalized[..32].copy_from_slice(&digest);
    } else {
        normalized[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36_u8; 64];
    let mut outer_pad = [0x5c_u8; 64];
    for index in 0..64 {
        inner_pad[index] ^= normalized[index];
        outer_pad[index] ^= normalized[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(data);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn aws_timestamp(seconds: u64) -> io::Result<(String, String)> {
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    if !(1970..=9999).contains(&year) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "S3 timestamp year is out of range",
        ));
    }
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;
    let date = format!("{year:04}{month:02}{day:02}");
    Ok((
        date.clone(),
        format!("{date}T{hour:02}{minute:02}{second:02}Z"),
    ))
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn percent_path_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn encode_entry(entry: &KvTierEntry) -> io::Result<Vec<u8>> {
    if entry.block.bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KV entry payload must not be empty",
        ));
    }
    let mut encoded = Vec::with_capacity(KV_ENVELOPE_MAGIC.len() + 17 + entry.block.bytes.len());
    encoded.extend_from_slice(KV_ENVELOPE_MAGIC);
    encoded.extend_from_slice(&entry.expires_at.to_le_bytes());
    encoded.push(u8::from(entry.pinned));
    encoded.extend_from_slice(&(entry.block.bytes.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&entry.block.bytes);
    Ok(encoded)
}

fn decode_entry(
    key: KvBlockKey,
    encoded: &[u8],
    max_value_bytes: usize,
) -> io::Result<KvTierEntry> {
    if encoded.len() > max_value_bytes || encoded.len() < KV_ENVELOPE_MAGIC.len() + 17 {
        return Err(protocol_error("KV envelope size is invalid"));
    }
    if &encoded[..KV_ENVELOPE_MAGIC.len()] != KV_ENVELOPE_MAGIC {
        return Err(protocol_error("KV envelope magic mismatch"));
    }
    let mut cursor = KV_ENVELOPE_MAGIC.len();
    let expires_at = read_u64(encoded, &mut cursor)?;
    let pinned = match encoded.get(cursor).copied() {
        Some(0) => false,
        Some(1) => true,
        _ => return Err(protocol_error("KV envelope pin flag is invalid")),
    };
    cursor += 1;
    let payload_len = usize::try_from(read_u64(encoded, &mut cursor)?)
        .map_err(|_| protocol_error("KV envelope payload length exceeds usize"))?;
    if payload_len == 0 || encoded.len().saturating_sub(cursor) != payload_len {
        return Err(protocol_error("KV envelope payload length mismatch"));
    }
    Ok(KvTierEntry {
        block: KvBlock::new(key, encoded[cursor..].to_vec())?,
        expires_at,
        pinned,
    })
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor.saturating_add(8);
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| protocol_error("KV envelope truncated"))?;
    *cursor = end;
    let mut value = [0_u8; 8];
    value.copy_from_slice(slice);
    Ok(u64::from_le_bytes(value))
}

fn read_http_line<R: BufRead>(reader: &mut R, max: usize) -> io::Result<String> {
    let mut line = Vec::new();
    let count = reader.read_until(b'\n', &mut line)?;
    if count == 0 || count > max || !line.ends_with(b"\r\n") {
        return Err(protocol_error("invalid HTTP line"));
    }
    line.truncate(line.len() - 2);
    String::from_utf8(line).map_err(|_| protocol_error("HTTP line is not UTF-8"))
}

fn read_chunked<R: BufRead>(reader: &mut R, max: usize) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let line = read_http_line(reader, 128)?;
        let size_text = line.split(';').next().unwrap_or("");
        let size = usize::from_str_radix(size_text.trim(), 16)
            .map_err(|_| protocol_error("invalid HTTP chunk size"))?;
        if size == 0 {
            loop {
                if read_http_line(reader, 8192)?.is_empty() {
                    break;
                }
            }
            return Ok(body);
        }
        if body.len().saturating_add(size) > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunked HTTP body exceeds limit",
            ));
        }
        let old = body.len();
        body.resize(old + size, 0);
        reader.read_exact(&mut body[old..])?;
        let mut ending = [0_u8; 2];
        reader.read_exact(&mut ending)?;
        if ending != *b"\r\n" {
            return Err(protocol_error("HTTP chunk missing CRLF"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvBlockRange;
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Mutex;

    struct FixedClock;

    impl S3Clock for FixedClock {
        fn timestamp(&self) -> io::Result<(String, String)> {
            Ok(("20260829".to_owned(), "20260829T010203Z".to_owned()))
        }
    }

    #[derive(Default)]
    struct MockS3 {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        saw_auth: Mutex<bool>,
    }

    impl HttpClient for MockS3 {
        fn execute(&self, request: &HttpRequest) -> io::Result<HttpResponse> {
            *self.saw_auth.lock().unwrap() = request.headers.iter().any(|(name, value)| {
                name == "Authorization" && value.starts_with("AWS4-HMAC-SHA256")
            });
            let mut objects = self.objects.lock().unwrap();
            let response = match request.method.as_str() {
                "PUT" => {
                    objects.insert(request.path.clone(), request.body.clone());
                    HttpResponse {
                        status: 200,
                        headers: vec![],
                        body: vec![],
                    }
                }
                "GET" => match objects.get(&request.path) {
                    Some(body) => HttpResponse {
                        status: 200,
                        headers: vec![],
                        body: body.clone(),
                    },
                    None => HttpResponse {
                        status: 404,
                        headers: vec![],
                        body: vec![],
                    },
                },
                "DELETE" => {
                    objects.remove(&request.path);
                    HttpResponse {
                        status: 204,
                        headers: vec![],
                        body: vec![],
                    }
                }
                "HEAD" => HttpResponse {
                    status: 200,
                    headers: vec![],
                    body: vec![],
                },
                _ => HttpResponse {
                    status: 405,
                    headers: vec![],
                    body: vec![],
                },
            };
            Ok(response)
        }
    }

    fn key() -> KvBlockKey {
        KvBlockKey::from_prefix(
            "model",
            &[1, 2, 3, 4],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 4,
                layer_start: 0,
                layer_count: 4,
                layout_version: 1,
            },
        )
    }

    #[test]
    fn resp_parser_handles_nested_scan_response() {
        let bytes = b"*2\r\n$1\r\n0\r\n*2\r\n$3\r\na:1\r\n$3\r\na:2\r\n";
        let mut reader = BufReader::new(Cursor::new(bytes.as_slice()));
        let response = read_resp(&mut reader, 1024, 0).unwrap();
        let RespValue::Array(Some(values)) = response else {
            panic!("array")
        };
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn s3_connector_signs_and_round_trips_entries() {
        let client = Arc::new(MockS3::default());
        let tier = S3KvTier::with_clock(
            "s3",
            S3Config {
                endpoint: "https://s3.example.test".to_owned(),
                region: "us-east-1".to_owned(),
                bucket: "rivet-cache".to_owned(),
                prefix: "kv/prod".to_owned(),
                max_value_bytes: 1024 * 1024,
            },
            S3Credentials {
                access_key: "AKIDEXAMPLE".to_owned(),
                secret_key: "secret".to_owned(),
                session_token: None,
            },
            client.clone(),
            Arc::new(FixedClock),
        )
        .unwrap();
        let entry = KvTierEntry {
            block: KvBlock::new(key(), vec![5; 128]).unwrap(),
            expires_at: 44,
            pinned: true,
        };
        tier.put(&entry).unwrap();
        assert_eq!(tier.get(&entry.block.key).unwrap().unwrap(), entry);
        assert!(*client.saw_auth.lock().unwrap());
        tier.remove(&entry.block.key).unwrap();
        assert!(tier.get(&entry.block.key).unwrap().is_none());
    }

    #[test]
    fn aws_epoch_timestamp_is_correct() {
        assert_eq!(
            aws_timestamp(0).unwrap(),
            ("19700101".to_owned(), "19700101T000000Z".to_owned())
        );
    }
}
