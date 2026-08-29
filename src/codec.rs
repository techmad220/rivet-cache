use crate::{KvBlock, KvTier, KvTierEntry};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

const FRAME_MAGIC: &[u8; 6] = b"RKC01\n";

pub trait PayloadCodec: Send + Sync {
    fn name(&self) -> &str;
    fn encode(&self, input: &[u8]) -> io::Result<Vec<u8>>;
    fn decode(&self, input: &[u8], expected_len: usize, max_output: usize) -> io::Result<Vec<u8>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IdentityCodec;

impl PayloadCodec for IdentityCodec {
    fn name(&self) -> &str {
        "identity"
    }

    fn encode(&self, input: &[u8]) -> io::Result<Vec<u8>> {
        Ok(input.to_vec())
    }

    fn decode(&self, input: &[u8], expected_len: usize, max_output: usize) -> io::Result<Vec<u8>> {
        if expected_len > max_output || input.len() != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "identity payload length is invalid",
            ));
        }
        Ok(input.to_vec())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RleCodec;

impl PayloadCodec for RleCodec {
    fn name(&self) -> &str {
        "rle-v1"
    }

    fn encode(&self, input: &[u8]) -> io::Result<Vec<u8>> {
        if input.is_empty() {
            return Ok(Vec::new());
        }
        let mut encoded = Vec::with_capacity(input.len());
        let mut index = 0;
        while index < input.len() {
            let byte = input[index];
            let mut run = 1_usize;
            while index + run < input.len() && input[index + run] == byte && run < u8::MAX as usize
            {
                run += 1;
            }
            encoded.push(run as u8);
            encoded.push(byte);
            index += run;
        }
        Ok(encoded)
    }

    fn decode(&self, input: &[u8], expected_len: usize, max_output: usize) -> io::Result<Vec<u8>> {
        if expected_len > max_output || input.len() % 2 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RLE payload framing is invalid",
            ));
        }
        let mut output = Vec::with_capacity(expected_len.min(max_output));
        for pair in input.chunks_exact(2) {
            let run = pair[0] as usize;
            if run == 0 || output.len().saturating_add(run) > expected_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RLE run exceeds the declared output size",
                ));
            }
            output.resize(output.len() + run, pair[1]);
        }
        if output.len() != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RLE decoded length does not match the frame",
            ));
        }
        Ok(output)
    }
}

#[derive(Default)]
pub struct CodecRegistry {
    codecs: Mutex<BTreeMap<String, Arc<dyn PayloadCodec>>>,
}

impl CodecRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtins() -> io::Result<Self> {
        let registry = Self::new();
        registry.register(Arc::new(IdentityCodec))?;
        registry.register(Arc::new(RleCodec))?;
        Ok(registry)
    }

    pub fn register(&self, codec: Arc<dyn PayloadCodec>) -> io::Result<()> {
        validate_name(codec.name())?;
        let mut codecs = self
            .codecs
            .lock()
            .map_err(|_| io::Error::other("codec registry lock poisoned"))?;
        if codecs.contains_key(codec.name()) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("codec {} is already registered", codec.name()),
            ));
        }
        codecs.insert(codec.name().to_owned(), codec);
        Ok(())
    }

    pub fn get(&self, name: &str) -> io::Result<Option<Arc<dyn PayloadCodec>>> {
        Ok(self
            .codecs
            .lock()
            .map_err(|_| io::Error::other("codec registry lock poisoned"))?
            .get(name)
            .cloned())
    }

    pub fn names(&self) -> io::Result<Vec<String>> {
        Ok(self
            .codecs
            .lock()
            .map_err(|_| io::Error::other("codec registry lock poisoned"))?
            .keys()
            .cloned()
            .collect())
    }
}

pub struct CodecKvTier {
    name: String,
    inner: Arc<dyn KvTier>,
    registry: Arc<CodecRegistry>,
    write_codec: String,
    max_decoded_bytes: usize,
}

impl CodecKvTier {
    pub fn new(
        name: impl Into<String>,
        inner: Arc<dyn KvTier>,
        registry: Arc<CodecRegistry>,
        write_codec: impl Into<String>,
        max_decoded_bytes: usize,
    ) -> io::Result<Self> {
        let name = name.into();
        let write_codec = write_codec.into();
        if name.trim().is_empty() || max_decoded_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "codec tier requires a name and non-zero decode limit",
            ));
        }
        if registry.get(&write_codec)?.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("write codec {write_codec} is not registered"),
            ));
        }
        Ok(Self {
            name,
            inner,
            registry,
            write_codec,
            max_decoded_bytes,
        })
    }

    pub fn write_codec(&self) -> &str {
        &self.write_codec
    }
}

impl KvTier for CodecKvTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &crate::KvBlockKey) -> io::Result<Option<KvTierEntry>> {
        let Some(mut entry) = self.inner.get(key)? else {
            return Ok(None);
        };
        let bytes = decode_frame(&self.registry, &entry.block.bytes, self.max_decoded_bytes)?;
        entry.block = KvBlock::new(entry.block.key, bytes)?;
        Ok(Some(entry))
    }

    fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
        let codec = self.registry.get(&self.write_codec)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "configured write codec disappeared",
            )
        })?;
        let framed = encode_frame(codec.as_ref(), &entry.block.bytes)?;
        let encoded_entry = KvTierEntry {
            block: KvBlock::new(entry.block.key.clone(), framed)?,
            expires_at: entry.expires_at,
            pinned: entry.pinned,
        };
        self.inner.put(&encoded_entry)
    }

    fn remove(&self, key: &crate::KvBlockKey) -> io::Result<()> {
        self.inner.remove(key)
    }

    fn clear(&self) -> io::Result<()> {
        self.inner.clear()
    }

    fn health(&self) -> io::Result<()> {
        self.inner.health()
    }
}

fn encode_frame(codec: &dyn PayloadCodec, input: &[u8]) -> io::Result<Vec<u8>> {
    if input.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "codec frame payload must not be empty",
        ));
    }
    validate_name(codec.name())?;
    if codec.name().len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "codec name is too long",
        ));
    }
    let encoded = codec.encode(input)?;
    let digest: [u8; 32] = Sha256::digest(input).into();
    let mut frame = Vec::with_capacity(6 + 2 + codec.name().len() + 8 + 32 + encoded.len());
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&(codec.name().len() as u16).to_le_bytes());
    frame.extend_from_slice(codec.name().as_bytes());
    frame.extend_from_slice(&(input.len() as u64).to_le_bytes());
    frame.extend_from_slice(&digest);
    frame.extend_from_slice(&encoded);
    Ok(frame)
}

fn decode_frame(
    registry: &CodecRegistry,
    frame: &[u8],
    max_decoded_bytes: usize,
) -> io::Result<Vec<u8>> {
    if frame.len() < FRAME_MAGIC.len() + 2 + 8 + 32 || &frame[..FRAME_MAGIC.len()] != FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid codec frame",
        ));
    }
    let mut cursor = FRAME_MAGIC.len();
    let name_len = read_u16(frame, &mut cursor)? as usize;
    if name_len == 0 || frame.len().saturating_sub(cursor) < name_len + 8 + 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid codec name length",
        ));
    }
    let name = std::str::from_utf8(&frame[cursor..cursor + name_len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "codec name is not UTF-8"))?;
    cursor += name_len;
    let expected_len_u64 = read_u64(frame, &mut cursor)?;
    let expected_len = usize::try_from(expected_len_u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded payload length exceeds usize",
        )
    })?;
    if expected_len == 0
        || expected_len > max_decoded_bytes
        || frame.len().saturating_sub(cursor) < 32
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded payload exceeds configured limit",
        ));
    }
    let expected_digest = &frame[cursor..cursor + 32];
    cursor += 32;
    let codec = registry.get(name)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("codec {name} is not registered"),
        )
    })?;
    let decoded = codec.decode(&frame[cursor..], expected_len, max_decoded_bytes)?;
    let actual_digest: [u8; 32] = Sha256::digest(&decoded).into();
    if actual_digest.as_slice() != expected_digest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded payload checksum mismatch",
        ));
    }
    Ok(decoded)
}

fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "codec names must be 1-128 ASCII identifier characters",
        ));
    }
    Ok(())
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> io::Result<u16> {
    let end = cursor.saturating_add(2);
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "codec frame truncated"))?;
    *cursor = end;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor.saturating_add(8);
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "codec frame truncated"))?;
    *cursor = end;
    let mut value = [0_u8; 8];
    value.copy_from_slice(slice);
    Ok(u64::from_le_bytes(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvBlockKey, KvBlockRange};
    use std::collections::HashMap;

    #[derive(Default)]
    struct MemoryTier {
        entries: Mutex<HashMap<String, KvTierEntry>>,
    }

    impl KvTier for MemoryTier {
        fn name(&self) -> &str {
            "memory"
        }

        fn get(&self, key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
            Ok(self.entries.lock().unwrap().get(&key.cache_key()).cloned())
        }

        fn put(&self, entry: &KvTierEntry) -> io::Result<()> {
            self.entries
                .lock()
                .unwrap()
                .insert(entry.block.key.cache_key(), entry.clone());
            Ok(())
        }

        fn remove(&self, key: &KvBlockKey) -> io::Result<()> {
            self.entries.lock().unwrap().remove(&key.cache_key());
            Ok(())
        }

        fn clear(&self) -> io::Result<()> {
            self.entries.lock().unwrap().clear();
            Ok(())
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
                layer_count: 8,
                layout_version: 1,
            },
        )
    }

    #[test]
    fn rle_round_trip_and_checksum() {
        let registry = Arc::new(CodecRegistry::with_builtins().unwrap());
        let inner: Arc<dyn KvTier> = Arc::new(MemoryTier::default());
        let tier =
            CodecKvTier::new("compressed", Arc::clone(&inner), registry, "rle-v1", 1024).unwrap();
        let original = KvTierEntry {
            block: KvBlock::new(key(), vec![7; 128]).unwrap(),
            expires_at: 123,
            pinned: true,
        };
        tier.put(&original).unwrap();
        let raw = inner.get(&original.block.key).unwrap().unwrap();
        assert_ne!(raw.block.bytes, original.block.bytes);
        assert!(raw.block.bytes.len() < original.block.bytes.len());
        assert_eq!(tier.get(&original.block.key).unwrap().unwrap(), original);
    }

    #[test]
    fn decode_limit_fails_closed() {
        let registry = Arc::new(CodecRegistry::with_builtins().unwrap());
        let inner: Arc<dyn KvTier> = Arc::new(MemoryTier::default());
        let writer = CodecKvTier::new(
            "writer",
            Arc::clone(&inner),
            Arc::clone(&registry),
            "rle-v1",
            1024,
        )
        .unwrap();
        let entry = KvTierEntry {
            block: KvBlock::new(key(), vec![9; 64]).unwrap(),
            expires_at: 0,
            pinned: false,
        };
        writer.put(&entry).unwrap();
        let reader = CodecKvTier::new("reader", inner, registry, "identity", 16).unwrap();
        assert_eq!(
            reader.get(&entry.block.key).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
