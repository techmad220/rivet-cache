use rivet_cache::{CacheController, ControllerServer, PrometheusRegistry};
use std::io;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> io::Result<()> {
    let bind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:65433".to_owned());
    let metrics = Arc::new(PrometheusRegistry::new());
    let controller = CacheController::new(metrics);
    let server = ControllerServer::spawn(bind, controller)?;
    println!("RIVET_CONTROLLER_READY addr={}", server.local_addr());
    println!("Controller API is unauthenticated; expose it only on a trusted admin network or authenticated tunnel.");
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
