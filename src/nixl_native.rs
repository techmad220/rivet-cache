use nixl_sys::{
    is_stub, Agent, Backend, MemType, MemoryRegion, NixlDescriptor, NixlRegistration,
    SystemStorage, XferDescList, XferOp, XferStatus,
};
use std::io;
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NixlRemoteRegion {
    pub address: u64,
    pub len: usize,
    pub device_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NixlTransferReceipt {
    pub backend: String,
    pub remote_agent: String,
    pub bytes: usize,
    pub elapsed: Duration,
}

/// Registered NIXL DRAM owned by RivetCache. The registration handle is retained
/// inside upstream `SystemStorage` and is deregistered through its RAII teardown.
pub struct NixlHostBuffer {
    storage: SystemStorage,
}

impl NixlHostBuffer {
    pub fn new(endpoint: &NixlEndpoint, len: usize) -> io::Result<Self> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NIXL host buffer length must be greater than zero",
            ));
        }
        let mut storage = SystemStorage::new(len).map_err(nixl_error)?;
        storage
            .register(&endpoint.agent, None)
            .map_err(nixl_error)?;
        Ok(Self { storage })
    }

    pub fn from_bytes(endpoint: &NixlEndpoint, bytes: &[u8]) -> io::Result<Self> {
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NIXL host buffer payload must not be empty",
            ));
        }
        let mut storage = SystemStorage::new(bytes.len()).map_err(nixl_error)?;
        // SAFETY: `storage` is uniquely borrowed here, its Vec allocation is writable for
        // exactly `storage.size()` bytes, and registration has not occurred yet.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), storage.as_ptr().cast_mut(), bytes.len());
        }
        storage
            .register(&endpoint.agent, None)
            .map_err(nixl_error)?;
        Ok(Self { storage })
    }

    pub fn len(&self) -> usize {
        self.storage.size()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        self.storage.as_slice()
    }

    pub fn fill(&mut self, value: u8) {
        self.storage.memset(value);
    }

    pub fn region(&self) -> io::Result<NixlRemoteRegion> {
        let address = unsafe { self.storage.as_ptr() } as usize;
        Ok(NixlRemoteRegion {
            address: u64::try_from(address).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "NIXL host pointer does not fit the wire descriptor",
                )
            })?,
            len: self.storage.size(),
            device_id: self.storage.device_id(),
        })
    }
}

/// Concrete NIXL endpoint for registered DRAM transfers. Metadata exchange is
/// deliberately caller-controlled so deployments can carry the opaque metadata
/// over their existing authenticated control plane.
pub struct NixlEndpoint {
    agent: Agent,
    _backend: Backend,
    backend_name: String,
    transfer_timeout: Duration,
}

impl NixlEndpoint {
    pub fn new(agent_name: &str, backend_name: &str) -> io::Result<Self> {
        Self::with_timeout(agent_name, backend_name, DEFAULT_TRANSFER_TIMEOUT)
    }

    pub fn with_timeout(
        agent_name: &str,
        backend_name: &str,
        transfer_timeout: Duration,
    ) -> io::Result<Self> {
        if agent_name.trim().is_empty() || backend_name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NIXL agent/backend names must not be empty",
            ));
        }
        if transfer_timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NIXL transfer timeout must be greater than zero",
            ));
        }
        if is_stub() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "nixl-sys is using its compile-only stub API; install a real NIXL runtime",
            ));
        }

        let agent = Agent::new(agent_name).map_err(nixl_error)?;
        let (_, params) = agent.get_plugin_params(backend_name).map_err(nixl_error)?;
        let backend = agent
            .create_backend(backend_name, &params)
            .map_err(nixl_error)?;
        Ok(Self {
            agent,
            _backend: backend,
            backend_name: backend_name.to_owned(),
            transfer_timeout,
        })
    }

    pub fn agent_name(&self) -> String {
        self.agent.name()
    }

    pub fn backend_name(&self) -> &str {
        &self.backend_name
    }

    pub fn local_metadata(&self) -> io::Result<Vec<u8>> {
        self.agent.get_local_md().map_err(nixl_error)
    }

    pub fn load_remote_metadata(&self, metadata: &[u8]) -> io::Result<String> {
        if metadata.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NIXL remote metadata must not be empty",
            ));
        }
        self.agent.load_remote_md(metadata).map_err(nixl_error)
    }

    pub fn write_remote(
        &self,
        source: &NixlHostBuffer,
        remote_agent: &str,
        destination: NixlRemoteRegion,
        bytes: usize,
    ) -> io::Result<NixlTransferReceipt> {
        validate_transfer(source, remote_agent, destination, bytes)?;

        let source_address = unsafe { source.storage.as_ptr() } as usize;
        let destination_address = usize::try_from(destination.address).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "remote NIXL address does not fit this process address width",
            )
        })?;

        let mut source_desc = XferDescList::new(MemType::Dram).map_err(nixl_error)?;
        source_desc
            .add_desc(source_address, bytes, source.storage.device_id())
            .map_err(nixl_error)?;
        let mut destination_desc = XferDescList::new(MemType::Dram).map_err(nixl_error)?;
        destination_desc
            .add_desc(destination_address, bytes, destination.device_id)
            .map_err(nixl_error)?;

        let request = self
            .agent
            .create_xfer_req(
                XferOp::Write,
                &source_desc,
                &destination_desc,
                remote_agent,
                None,
            )
            .map_err(nixl_error)?;

        let started = Instant::now();
        let in_progress = self
            .agent
            .post_xfer_req(&request, None)
            .map_err(nixl_error)?;
        if in_progress {
            loop {
                match self.agent.get_xfer_status(&request).map_err(nixl_error)? {
                    XferStatus::Success => break,
                    XferStatus::InProgress if started.elapsed() < self.transfer_timeout => {
                        thread::sleep(POLL_INTERVAL);
                    }
                    XferStatus::InProgress => {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "NIXL {} transfer to {remote_agent} exceeded {:?}",
                                self.backend_name, self.transfer_timeout
                            ),
                        ));
                    }
                }
            }
        }

        Ok(NixlTransferReceipt {
            backend: self.backend_name.clone(),
            remote_agent: remote_agent.to_owned(),
            bytes,
            elapsed: started.elapsed(),
        })
    }
}

fn validate_transfer(
    source: &NixlHostBuffer,
    remote_agent: &str,
    destination: NixlRemoteRegion,
    bytes: usize,
) -> io::Result<()> {
    if remote_agent.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "remote NIXL agent name must not be empty",
        ));
    }
    if bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NIXL transfer size must be greater than zero",
        ));
    }
    if destination.address == 0 || bytes > source.len() || bytes > destination.len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NIXL transfer range exceeds the registered source or destination region",
        ));
    }
    Ok(())
}

fn nixl_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("NIXL: {error}"))
}
