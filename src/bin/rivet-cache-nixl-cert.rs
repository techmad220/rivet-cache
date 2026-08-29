#[cfg(not(target_os = "linux"))]
compile_error!("rivet-cache-nixl-cert is supported only on Linux because upstream NIXL is Linux-only");

#[path = "../nixl_native.rs"]
mod nixl_native;

use nixl_native::{NixlEndpoint, NixlHostBuffer};
use std::io;

const DEFAULT_BYTES: usize = 8 * 1024 * 1024;

fn main() -> io::Result<()> {
    let bytes = std::env::var("RIVET_NIXL_CERT_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BYTES);
    if bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RIVET_NIXL_CERT_BYTES must be greater than zero",
        ));
    }

    let source = NixlEndpoint::new("rivet-prefill", "UCX")?;
    let target = NixlEndpoint::new("rivet-decode", "UCX")?;

    let mut payload = vec![0_u8; bytes];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = ((index.wrapping_mul(131).wrapping_add(0x5a)) % 251) as u8;
    }
    let source_buffer = NixlHostBuffer::from_bytes(&source, &payload)?;
    let target_buffer = NixlHostBuffer::new(&target, bytes)?;

    // Registrations must exist before metadata is captured so the remote agent
    // receives the memory registration records required by UCX.
    let source_metadata = source.local_metadata()?;
    let target_metadata = target.local_metadata()?;
    let target_name = source.load_remote_metadata(&target_metadata)?;
    let source_name = target.load_remote_metadata(&source_metadata)?;

    if target_name != target.agent_name() || source_name != source.agent_name() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "NIXL metadata resolved to unexpected agent identities",
        ));
    }

    let receipt = source.write_remote(
        &source_buffer,
        &target_name,
        target_buffer.region()?,
        bytes,
    )?;

    if target_buffer.as_slice() != payload.as_slice() {
        let mismatch = target_buffer
            .as_slice()
            .iter()
            .zip(payload.iter())
            .position(|(actual, expected)| actual != expected)
            .unwrap_or(0);
        return Err(io::Error::other(format!(
            "NIXL UCX payload mismatch at byte {mismatch}"
        )));
    }

    println!(
        "RIVET_NIXL_UCX=PASS source={} target={} bytes={} elapsed_us={}",
        source.agent_name(),
        target.agent_name(),
        receipt.bytes,
        receipt.elapsed.as_micros()
    );
    println!("RIVET_NIXL_METADATA=PASS source_bytes={} target_bytes={}", source_metadata.len(), target_metadata.len());
    println!("RIVET_V08_NIXL_CERT=PASS");
    Ok(())
}
