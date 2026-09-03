use rivet_cache::{DaemonConfig, RivetDaemon};
use std::env;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

const MIB: u64 = 1024 * 1024;

fn main() -> io::Result<()> {
    let config = parse_args(env::args().skip(1))?;
    let daemon = RivetDaemon::spawn(config)?;

    println!(
        "RIVET_CACHE_DAEMON_READY data_addr={} control_addr={}",
        daemon.data_addr(),
        daemon.control_addr()
    );
    println!("RIVET_CACHE_DAEMON_VERSION={}", env!("CARGO_PKG_VERSION"));
    println!(
        "security=loopback-by-default; protect non-loopback listeners at an authenticated boundary"
    );

    // The managed RivetDaemon API exposes explicit clean shutdown to embedding
    // supervisors. This standalone process intentionally stays alive until its
    // supervisor or service manager terminates it.
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn parse_args<I>(args: I) -> io::Result<DaemonConfig>
where
    I: IntoIterator<Item = String>,
{
    let mut config = DaemonConfig::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-bind" => config.data_bind = required_value(&mut args, "--data-bind")?,
            "--control-bind" => config.control_bind = required_value(&mut args, "--control-bind")?,
            "--root" => config.root = PathBuf::from(required_value(&mut args, "--root")?),
            "--memory-mib" => {
                config.memory_capacity_bytes =
                    parse_mib(&required_value(&mut args, "--memory-mib")?, "--memory-mib")?
            }
            "--disk-mib" => {
                config.persistent_capacity_bytes =
                    parse_mib(&required_value(&mut args, "--disk-mib")?, "--disk-mib")?
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--version" | "-V" => {
                println!("rivet-cache-daemon {}", env!("CARGO_PKG_VERSION"));
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

fn parse_mib(value: &str, flag: &str) -> io::Result<u64> {
    let mib = value.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} must be an unsigned integer"),
        )
    })?;
    if mib == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} must be greater than zero"),
        ));
    }
    mib.checked_mul(MIB).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} capacity overflows u64"),
        )
    })
}

fn print_usage() {
    println!(
        "Usage: rivet-cache-daemon [OPTIONS]\n\
         \n\
         Options:\n\
           --data-bind <ADDR>     KV data-plane bind address [default: 127.0.0.1:65432]\n\
           --control-bind <ADDR>  Control/metrics bind address [default: 127.0.0.1:65433]\n\
           --root <PATH>          Persistent cache directory [default: rivet-cache-data]\n\
           --memory-mib <MIB>     In-memory capacity [default: 512]\n\
           --disk-mib <MIB>       Persistent capacity [default: 8192]\n\
           -V, --version          Print version\n\
           -h, --help             Print help"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_daemon_flags() -> io::Result<()> {
        let config = parse_args([
            "--data-bind".to_owned(),
            "127.0.0.1:10001".to_owned(),
            "--control-bind".to_owned(),
            "127.0.0.1:10002".to_owned(),
            "--root".to_owned(),
            "target/test-cache".to_owned(),
            "--memory-mib".to_owned(),
            "64".to_owned(),
            "--disk-mib".to_owned(),
            "256".to_owned(),
        ])?;
        assert_eq!(config.data_bind, "127.0.0.1:10001");
        assert_eq!(config.control_bind, "127.0.0.1:10002");
        assert_eq!(config.memory_capacity_bytes, 64 * MIB);
        assert_eq!(config.persistent_capacity_bytes, 256 * MIB);
        Ok(())
    }

    #[test]
    fn rejects_zero_capacity() {
        let error = parse_args(["--memory-mib".to_owned(), "0".to_owned()])
            .expect_err("zero capacity must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
