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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DaemonStatus {
    tier: String,
    version: String,
    memory_capacity_bytes: u64,
    persistent_capacity_bytes: u64,
}

#[derive(Debug)]
struct ControlProbeResult {
    probe: ProbeResult,
    daemon: Option<DaemonStatus>,
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
    let healthy = data.ok && control.probe.ok;

    if config.json {
        let daemon_json = control
            .daemon
            .as_ref()
            .map(daemon_status_json)
            .unwrap_or_else(|| "null".to_owned());
        println!(
            "{{\"ok\":{},\"data\":{{\"endpoint\":\"{}\",\"ok\":{},\"detail\":\"{}\"}},\"control\":{{\"endpoint\":\"{}\",\"ok\":{},\"detail\":\"{}\"}},\"daemon\":{}}}",
            healthy,
            json_escape(&config.data_endpoint),
            data.ok,
            json_escape(&data.detail),
            json_escape(&config.control_endpoint),
            control.probe.ok,
            json_escape(&control.probe.detail),
            daemon_json,
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
            if control.probe.ok { "PASS" } else { "FAIL" },
            config.control_endpoint,
            control.probe.detail
        );
        if let Some(status) = &control.daemon {
            println!(
                "daemon  READY tier={} version={} memory={} MiB persistent={} MiB",
                status.tier,
                status.version,
                status.memory_capacity_bytes / (1024 * 1024),
                status.persistent_capacity_bytes / (1024 * 1024),
            );
        }
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

fn probe_control(endpoint: &str) -> ControlProbeResult {
    let result = (|| -> io::Result<DaemonStatus> {
        let health = http_get(endpoint, "/health")?;
        ensure_http_ok(&health, "/health")?;
        let metrics = http_get(endpoint, "/metrics")?;
        ensure_http_ok(&metrics, "/metrics")?;
        inspect_daemon_metrics(http_body(&metrics)?)
    })();

    match result {
        Ok(daemon) => ControlProbeResult {
            probe: ProbeResult {
                ok: true,
                detail: "health, metrics, and daemon readiness checks succeeded".to_owned(),
            },
            daemon: Some(daemon),
        },
        Err(error) => ControlProbeResult {
            probe: ProbeResult {
                ok: false,
                detail: error.to_string(),
            },
            daemon: None,
        },
    }
}

fn inspect_daemon_metrics(metrics: &str) -> io::Result<DaemonStatus> {
    let ready = metric_value(metrics, "rivet_daemon_ready", &[])?;
    if ready != 1 {
        return Err(io::Error::other(format!(
            "daemon readiness metric is {ready}, expected 1"
        )));
    }

    let memory_capacity_bytes = metric_value(
        metrics,
        "rivet_daemon_capacity_bytes",
        &[("tier", "memory")],
    )?;
    let persistent_capacity_bytes = metric_value(
        metrics,
        "rivet_daemon_capacity_bytes",
        &[("tier", "persistent")],
    )?;

    let (series, build_value) = metric_sample(metrics, "rivet_daemon_build_info", &[])?;
    if build_value != 1 {
        return Err(io::Error::other(format!(
            "daemon build-info metric is {build_value}, expected 1"
        )));
    }
    let tier = label_value(series, "tier").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daemon build-info metric is missing tier label",
        )
    })?;
    let version = label_value(series, "version").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daemon build-info metric is missing version label",
        )
    })?;

    Ok(DaemonStatus {
        tier,
        version,
        memory_capacity_bytes,
        persistent_capacity_bytes,
    })
}

fn metric_value(metrics: &str, name: &str, labels: &[(&str, &str)]) -> io::Result<u64> {
    metric_sample(metrics, name, labels).map(|(_, value)| value)
}

fn metric_sample<'a>(
    metrics: &'a str,
    name: &str,
    labels: &[(&str, &str)],
) -> io::Result<(&'a str, u64)> {
    for line in metrics.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let series_name = series.split_once('{').map(|(value, _)| value).unwrap_or(series);
        if series_name != name {
            continue;
        }
        if !labels.iter().all(|(key, expected)| {
            label_value(series, key).as_deref() == Some(*expected)
        }) {
            continue;
        }
        let value = value.parse::<u64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("metric {name} has non-integer value {value:?}"),
            )
        })?;
        return Ok((series, value));
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("required metric {name} not found"),
    ))
}

fn label_value(series: &str, key: &str) -> Option<String> {
    let (_, labels) = series.split_once('{')?;
    let labels = labels.strip_suffix('}')?;
    for pair in labels.split(',') {
        let (name, raw) = pair.split_once('=')?;
        if name != key {
            continue;
        }
        let raw = raw.strip_prefix('"')?.strip_suffix('"')?;
        return prometheus_unescape(raw);
    }
    None
}

fn prometheus_unescape(value: &str) -> Option<String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        match chars.next()? {
            '\\' => output.push('\\'),
            '"' => output.push('"'),
            'n' => output.push('\n'),
            _ => return None,
        }
    }
    Some(output)
}

fn daemon_status_json(status: &DaemonStatus) -> String {
    format!(
        "{{\"ready\":true,\"tier\":\"{}\",\"version\":\"{}\",\"memory_capacity_bytes\":{},\"persistent_capacity_bytes\":{}}}",
        json_escape(&status.tier),
        json_escape(&status.version),
        status.memory_capacity_bytes,
        status.persistent_capacity_bytes,
    )
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

fn http_body(response: &str) -> io::Result<&str> {
    response.split_once("\r\n\r\n").map(|(_, body)| body).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "HTTP response is missing header/body separator",
        )
    })
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

    const METRICS: &str = "# TYPE rivet_daemon_build_info gauge\n\
rivet_daemon_build_info{tier=\"daemon-cache\",version=\"0.8.0\"} 1\n\
# TYPE rivet_daemon_capacity_bytes gauge\n\
rivet_daemon_capacity_bytes{tier=\"memory\"} 536870912\n\
rivet_daemon_capacity_bytes{tier=\"persistent\"} 8589934592\n\
# TYPE rivet_daemon_ready gauge\n\
rivet_daemon_ready 1\n";

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
    fn inspects_required_daemon_metrics() -> io::Result<()> {
        let status = inspect_daemon_metrics(METRICS)?;
        assert_eq!(status.tier, "daemon-cache");
        assert_eq!(status.version, "0.8.0");
        assert_eq!(status.memory_capacity_bytes, 536_870_912);
        assert_eq!(status.persistent_capacity_bytes, 8_589_934_592);
        Ok(())
    }

    #[test]
    fn rejects_not_ready_daemon() {
        let metrics = METRICS.replace("rivet_daemon_ready 1", "rivet_daemon_ready 0");
        let error = inspect_daemon_metrics(&metrics).expect_err("not-ready daemon must fail");
        assert!(error.to_string().contains("expected 1"));
    }

    #[test]
    fn parses_escaped_prometheus_label() {
        assert_eq!(
            label_value("metric{value=\"a\\\\b\\\"c\\n\"}", "value").as_deref(),
            Some("a\\b\"c\n")
        );
    }

    #[test]
    fn escapes_json_control_characters() {
        assert_eq!(json_escape("a\"b\\c\n"), "a\\\"b\\\\c\\n");
    }
}
