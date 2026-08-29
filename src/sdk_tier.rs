use crate::{KvBlock, KvBlockKey, KvTier, KvTierEntry};
use sha2::{Digest, Sha256};
use std::io;
use std::sync::Arc;

const FRAME_MAGIC: &[u8; 8] = b"RIVSDK1\n";

pub trait NativeKvSdk: Send + Sync {
    fn name(&self) -> &str;
    fn get(&self, key: &str, max_bytes: usize) -> io::Result<Option<Vec<u8>>>;
    fn put(&self, key: &str, value: &[u8]) -> io::Result<()>;
    fn remove(&self, key: &str) -> io::Result<()>;
    fn clear_prefix(&self, prefix: &str) -> io::Result<()>;
    fn health(&self) -> io::Result<()>;
}

pub struct NativeSdkKvTier {
    name: String,
    namespace: String,
    max_value_bytes: usize,
    sdk: Arc<dyn NativeKvSdk>,
}

impl NativeSdkKvTier {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        max_value_bytes: usize,
        sdk: Arc<dyn NativeKvSdk>,
    ) -> io::Result<Self> {
        let name = name.into();
        let namespace = namespace.into();
        validate_identifier(&name, "tier name")?;
        validate_identifier(&namespace, "namespace")?;
        if max_value_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native SDK value limit must be greater than zero",
            ));
        }
        sdk.health()?;
        Ok(Self {
            name,
            namespace,
            max_value_bytes,
            sdk,
        })
    }

    pub fn sdk_name(&self) -> &str {
        self.sdk.name()
    }

    fn object_key(&self, key: &KvBlockKey) -> String {
        format!("rivet:{}:{}", self.namespace, key.cache_key())
    }

    fn prefix(&self) -> String {
        format!("rivet:{}:", self.namespace)
    }
}

impl KvTier for NativeSdkKvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let Some(raw) = self.sdk.get(&self.object_key(key), self.max_value_bytes)? else {
            return Ok(None);
        };
        decode_entry(key.clone(), &raw, self.max_value_bytes).map(Some)
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        let encoded = encode_entry(entry)?;
        if encoded.len() > self.max_value_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native SDK KV entry exceeds configured limit",
            ));
        }
        self.sdk.put(&self.object_key(&entry.block.key), &encoded)
    }

    fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
        self.sdk.remove(&self.object_key(key))
    }

    fn clear(&self) -> io::Result<()> {
        self.sdk.clear_prefix(&self.prefix())
    }

    fn health(&self) -> io::Result<()> {
        self.sdk.health()
    }
}

fn encode_entry(entry: &KvTierEntry) -> io::Result<Vec<u8>> {
    if entry.block.bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "native SDK KV payload must not be empty",
        ));
    }
    let digest: [u8; 32] = Sha256::digest(&entry.block.bytes).into();
    let mut out = Vec::with_capacity(57 + entry.block.bytes.len());
    out.extend_from_slice(FRAME_MAGIC);
    out.extend_from_slice(&entry.expires_at.to_le_bytes());
    out.push(u8::from(entry.pinned));
    out.extend_from_slice(&(entry.block.bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(&digest);
    out.extend_from_slice(&entry.block.bytes);
    Ok(out)
}

fn decode_entry(key: KvBlockKey, bytes: &[u8], max: usize) -> io::Result<KvTierEntry> {
    const HEADER: usize = 8 + 8 + 1 + 8 + 32;
    if bytes.len() < HEADER || bytes.len() > max || &bytes[..8] != FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid native SDK RivetCache frame",
        ));
    }
    let mut cursor = 8;
    let expires_at = read_u64(bytes, &mut cursor)?;
    let pinned = match bytes[cursor] {
        0 => false,
        1 => true,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid native SDK pin flag",
            ))
        }
    };
    cursor += 1;
    let payload_len = usize::try_from(read_u64(bytes, &mut cursor)?).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "native SDK payload length overflow",
        )
    })?;
    let checksum = bytes
        .get(cursor..cursor + 32)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated SDK checksum"))?;
    cursor += 32;
    if payload_len == 0 || bytes.len().saturating_sub(cursor) != payload_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native SDK payload length mismatch",
        ));
    }
    let payload = bytes[cursor..].to_vec();
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    if digest.as_slice() != checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native SDK payload checksum mismatch",
        ));
    }
    Ok(KvTierEntry {
        block: KvBlock::new(key, payload)?,
        expires_at,
        pinned,
    })
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor.saturating_add(8);
    let raw = bytes.get(*cursor..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "native SDK frame is truncated",
        )
    })?;
    *cursor = end;
    let mut value = [0_u8; 8];
    value.copy_from_slice(raw);
    Ok(u64::from_le_bytes(value))
}

fn validate_identifier(value: &str, name: &str) -> io::Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid native SDK {name}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvBlockRange;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockSdk(Mutex<HashMap<String, Vec<u8>>>);

    impl NativeKvSdk for MockSdk {
        fn name(&self) -> &str {
            "mock"
        }
        fn get(&self, key: &str, _max_bytes: usize) -> io::Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        fn put(&self, key: &str, value: &[u8]) -> io::Result<()> {
            self.0
                .lock()
                .unwrap()
                .insert(key.to_owned(), value.to_vec());
            Ok(())
        }
        fn remove(&self, key: &str) -> io::Result<()> {
            self.0.lock().unwrap().remove(key);
            Ok(())
        }
        fn clear_prefix(&self, prefix: &str) -> io::Result<()> {
            self.0
                .lock()
                .unwrap()
                .retain(|key, _| !key.starts_with(prefix));
            Ok(())
        }
        fn health(&self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn generic_sdk_tier_preserves_metadata() {
        let key = KvBlockKey::from_prefix(
            "model",
            &[1, 2],
            KvBlockRange {
                block_index: 0,
                token_start: 0,
                token_count: 2,
                layer_start: 0,
                layer_count: 8,
                layout_version: 1,
            },
        );
        let tier = NativeSdkKvTier::new("sdk", "ns", 1024, Arc::new(MockSdk::default())).unwrap();
        let entry = KvTierEntry {
            block: KvBlock::new(key.clone(), vec![9; 64]).unwrap(),
            expires_at: 42,
            pinned: true,
        };
        tier.put(&entry).unwrap();
        assert_eq!(tier.get(&key).unwrap(), Some(entry));
    }
}
