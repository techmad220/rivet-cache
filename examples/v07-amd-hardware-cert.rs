use rivet_cache::{DeviceMemory, GpuDirectIo, HipDeviceMemory};
use std::io;

const DEFAULT_BYTES: usize = 64 * 1024 * 1024;

fn main() -> io::Result<()> {
    let bytes = std::env::var("RIVET_AMD_CERT_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BYTES);
    if bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RIVET_AMD_CERT_BYTES must be greater than zero",
        ));
    }

    let memory = HipDeviceMemory::load()?;
    memory.health()?;
    let direct = memory.direct_io()?;
    direct.health()?;
    if !direct.capabilities().device_copy {
        return Err(io::Error::other(
            "native HIP provider did not advertise device-copy support",
        ));
    }

    let source = memory.allocate(bytes)?;
    let destination = match memory.allocate(bytes) {
        Ok(buffer) => buffer,
        Err(error) => {
            let _ = memory.free(source);
            return Err(error);
        }
    };

    let result = (|| {
        let mut expected = vec![0_u8; bytes];
        for (index, byte) in expected.iter_mut().enumerate() {
            *byte = ((index.wrapping_mul(131).wrapping_add(0x5a)) % 251) as u8;
        }
        memory.upload(source, &expected)?;
        direct.copy_device(source, destination, bytes)?;
        let actual = memory.download(destination)?;
        if actual != expected {
            let mismatch = actual
                .iter()
                .zip(expected.iter())
                .position(|(actual, expected)| actual != expected)
                .unwrap_or(0);
            return Err(io::Error::other(format!(
                "HIP D2D verification mismatch at byte {mismatch}"
            )));
        }
        println!("RIVET_V07_HIP_D2D=PASS bytes={bytes}");
        println!(
            "RIVET_V07_HIP_CAPS device_copy={} storage_read={} storage_write={} zero_copy={}",
            direct.capabilities().device_copy,
            direct.capabilities().storage_read,
            direct.capabilities().storage_write,
            direct.capabilities().zero_copy
        );
        Ok(())
    })();

    let destination_free = memory.free(destination);
    let source_free = memory.free(source);
    result?;
    destination_free?;
    source_free?;
    println!("RIVET_V07_AMD_HARDWARE_CERT=PASS");
    Ok(())
}
