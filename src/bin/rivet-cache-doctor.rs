use rivet_cache::{RemoteLimits, TcpKvTier};
use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::time::Duration;

const MAX_HTTP_RESPONSE_BYTES: u64 = 64 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
struct DoctorConfig {
    data_endpoint: String,
    control_endpoint: String,
    json: bool,
}

impl Default for DoctorConfig {
    fn default() -> Self {
        Self {
            data_endpoint: "127.0.0.1:65432".to_owned(),
            control_endpoint: "127.0.0.1:65433".to_owned(),
            json: false,
        }
    }
}

#[derive(Debug)]
struct ProbeResult {
    ok: bool,
    detail: String,
}

fn main() -> ExitCode {
    let config = match parse_args(env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("rivet-cache-doctor: {error}");
            return ExitCode::FAILURE;
        }
    };

    let data = probe_data(&config.data_endpoint);
    let control = probe_control(&config.control_endpoint);
    let healthy = data.ok && control.ok;

    if config.json {
        println!(
            "{{\"ok\":{},\"data\":{{\"endpoint\":\"{}\",\"ok\":{},\"detail\":\"{}\"}},\"control\":{{\"endpoint\":\"{}\",\"ok\":{},\"detail\":\"{}\"}}}}",
            healthy,
            json_escape(&config.data_endpoint),
            data.ok,
            json_escape(&data.detail),
            json_escape(&config.control_endpoint),
            control.ok,
            json_escape(&control.detail),
        );
    } else {
        println!(
            "RivetCache doctor: {}",
            if healthy { "PASS" } else { "FAIL" }
        );
        println!(
            "data    {}  {}  {}",
            if data.ok { "PASS" } else { "FAIL" },
            config.data_endpoint,
            data.detail
        );
        println!(
            "control {}  {}  {}",
            if control.ok { "PASS" } else { "FAIL" },
            config.control_endpoint,
            control.detail
        );
    }

    if healthy {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn probe_data(endpoint: &str) -> ProbeResult {
    let limits = RemoteLimits {
        connect_timeout: PROBE_TIMEOUT,
        io_timeout: PROBE_TIMEOUT,
        ..RemoteLimits::default()
    };
    match TcpKvTier::new("doctor-probe", endpoint, limits).and_then(|client| client.ping()) {
        Ok(()) => ProbeResult {
            ok: true,
            detail: "KV protocol ping succeeded".to_owned(),
        },
        Err(error) => ProbeResult {
            ok: false,
            detail: error.to_string(),
        },
    }
}

fn probe_control(endpoint: &str) -> ProbeResult {
    let result = (|| -> io::Result<()> {
        let health = http_get(endpoint, "/health")?;
        ensure_http_ok(&health, "/health")?;
        let metrics = http_get(endpoint, "/metrics")?;
        ensure_http_ok(&metrics, "/metrics")?;
        Ok(())
    })();

    match result {
        Ok(()) => ProbeResult {
            ok: true,
            detail: "health and metrics endpoints reachable".to_owned(),
        },
        Err(error) => ProbeResult {
            ok: false,
            detail: error.to_string(),
        },
    }
}

fn http_get(endpoint: &str, path: &str) -> io::Result<String> {
    let address = resolve_one(endpoint)?;
    let mut stream = TcpStream::connect_timeout(&address, PROBE_TIMEOUT)?;
    stream.set_read_timeout(Some(PROBE_TIMEOUT))?;
    stream.set_write_timeout(Some(PROBE_TIMEOUT))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;

    let mut response = String::new();
    stream
        .take(MAX_HTTP_RESPONSE_BYTES)
        .read_to_string(&mut response)?;
    if response.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("empty response from {path}"),
        ));
    }
    Ok(response)
}

fn ensure_http_ok(response: &str, path: &str) -> io::Result<()> {
    if response.starts_with("HTTP/1.1 200 ") {
        Ok(())
    } else {
        let status = response.lines().next().unwrap_or("invalid HTTP response");
        Err(io::Error::other(format!("{path} returned {status}")))
    }
}

fn resolve_one(endpoint: &str) -> io::Result<SocketAddr> {
    endpoint.to_socket_addrs()?.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("endpoint {endpoint:?} resolved to no addresses"),
        )
    })
}

fn parse_args<I>(args: I) -> io::Result<DoctorConfig>
where
    I: IntoIterator<Item = String>,
{
    let mut config = DoctorConfig::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" => config.data_endpoint = required_value(&mut args, "--data")?,
            "--control" => config.control_endpoint = required_value(&mut args, "--control")?,
            "--json" => config.json = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--version" | "-V" => {
                println!("rivet-cache-doctor {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument {arg:?}; use --help for usage"),
                ))
            }
        }
    }

    if config.data_endpoint.trim().is_empty() || config.control_endpoint.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "doctor endpoints must not be empty",
        ));
    }
    Ok(config)
}

fn required_value<I>(args: &mut I, flag: &str) -> io::Result<String>
where
    I: Iterator<Item = String>,
{
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} requires a value"),
        )
    })
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn print_usage() {
    println!(
        "Usage: rivet-cache-doctor [OPTIONS]\n\
         \n\
         Options:\n\
           --data <ADDR>     KV data-plane endpoint [default: 127.0.0.1:65432]\n\
           --control <ADDR>  Control/metrics endpoint [default: 127.0.0.1:65433]\n\
           --json            Emit one JSON result object\n\
           -V, --version     Print version\n\
           -h, --help        Print help"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_endpoints_and_json_mode() -> io::Result<()> {
        let config = parse_args([
            "--data".to_owned(),
            "localhost:7001".to_owned(),
            "--control".to_owned(),
            "localhost:7002".to_owned(),
            "--json".to_owned(),
        ])?;
        assert_eq!(config.data_endpoint, "localhost:7001");
        assert_eq!(config.control_endpoint, "localhost:7002");
        assert!(config.json);
        Ok(())
    }

    #[test]
    fn escapes_json_control_characters() {
        assert_eq!(json_escape("a\"b\\c\n"), "a\\\"b\\\\c\\n");
    }
}
