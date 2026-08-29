use crate::{DeviceTransferProvider, KvTier, PayloadCodec};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginComponent {
    Tier,
    Codec,
    Transport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginManifest {
    pub name: String,
    pub component: PluginComponent,
    pub kind: String,
    pub properties: BTreeMap<String, String>,
    pub source: Option<PathBuf>,
}

impl PluginManifest {
    pub fn parse(text: &str) -> io::Result<Self> {
        let mut properties = BTreeMap::new();
        for (line_number, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line.split_once('=').ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("plugin manifest line {} is not key=value", line_number + 1),
                )
            })?;
            let key = key.trim();
            let value = value.trim();
            validate_identifier(key, "manifest property")?;
            if value.is_empty()
                || properties
                    .insert(key.to_owned(), value.to_owned())
                    .is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("plugin manifest has invalid/duplicate property {key}"),
                ));
            }
        }
        let name = properties.remove("name").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "plugin manifest is missing name",
            )
        })?;
        let component = match properties.remove("component").as_deref() {
            Some("tier") => PluginComponent::Tier,
            Some("codec") => PluginComponent::Codec,
            Some("transport") => PluginComponent::Transport,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "plugin component must be tier, codec, or transport",
                ))
            }
        };
        let kind = properties.remove("kind").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "plugin manifest is missing kind",
            )
        })?;
        validate_identifier(&name, "plugin name")?;
        validate_identifier(&kind, "plugin kind")?;
        Ok(Self {
            name,
            component,
            kind,
            properties,
            source: None,
        })
    }
}

pub trait KvTierPluginFactory: Send + Sync {
    fn kind(&self) -> &str;
    fn create(&self, manifest: &PluginManifest) -> io::Result<Arc<dyn KvTier>>;
}

pub trait CodecPluginFactory: Send + Sync {
    fn kind(&self) -> &str;
    fn create(&self, manifest: &PluginManifest) -> io::Result<Arc<dyn PayloadCodec>>;
}

pub trait TransportPluginFactory: Send + Sync {
    fn kind(&self) -> &str;
    fn create(&self, manifest: &PluginManifest) -> io::Result<Arc<dyn DeviceTransferProvider>>;
}

#[derive(Default)]
pub struct PluginRegistry {
    tier_factories: Mutex<BTreeMap<String, Arc<dyn KvTierPluginFactory>>>,
    codec_factories: Mutex<BTreeMap<String, Arc<dyn CodecPluginFactory>>>,
    transport_factories: Mutex<BTreeMap<String, Arc<dyn TransportPluginFactory>>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_tier_factory(&self, factory: Arc<dyn KvTierPluginFactory>) -> io::Result<()> {
        let kind = factory.kind().to_owned();
        register_factory(&self.tier_factories, &kind, factory)
    }

    pub fn register_codec_factory(&self, factory: Arc<dyn CodecPluginFactory>) -> io::Result<()> {
        let kind = factory.kind().to_owned();
        register_factory(&self.codec_factories, &kind, factory)
    }

    pub fn register_transport_factory(
        &self,
        factory: Arc<dyn TransportPluginFactory>,
    ) -> io::Result<()> {
        let kind = factory.kind().to_owned();
        register_factory(&self.transport_factories, &kind, factory)
    }

    pub fn discover_dir(&self, directory: &Path) -> io::Result<Vec<PluginManifest>> {
        let mut manifests = Vec::new();
        if !directory.exists() {
            return Ok(manifests);
        }
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file()
                || path.extension().and_then(|value| value.to_str()) != Some("rivet-plugin")
            {
                continue;
            }
            let metadata = fs::metadata(&path)?;
            if metadata.len() > 1024 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("plugin manifest {} exceeds 1 MiB", path.display()),
                ));
            }
            let mut manifest = PluginManifest::parse(&fs::read_to_string(&path)?)?;
            manifest.source = Some(path);
            manifests.push(manifest);
        }
        manifests.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(manifests)
    }

    pub fn create_tier(&self, manifest: &PluginManifest) -> io::Result<Arc<dyn KvTier>> {
        if manifest.component != PluginComponent::Tier {
            return Err(component_error("tier", manifest));
        }
        let factory = self
            .tier_factories
            .lock()
            .map_err(|_| io::Error::other("plugin tier registry lock poisoned"))?
            .get(&manifest.kind)
            .cloned()
            .ok_or_else(|| factory_missing(&manifest.kind))?;
        factory.create(manifest)
    }

    pub fn create_codec(&self, manifest: &PluginManifest) -> io::Result<Arc<dyn PayloadCodec>> {
        if manifest.component != PluginComponent::Codec {
            return Err(component_error("codec", manifest));
        }
        let factory = self
            .codec_factories
            .lock()
            .map_err(|_| io::Error::other("plugin codec registry lock poisoned"))?
            .get(&manifest.kind)
            .cloned()
            .ok_or_else(|| factory_missing(&manifest.kind))?;
        factory.create(manifest)
    }

    pub fn create_transport(
        &self,
        manifest: &PluginManifest,
    ) -> io::Result<Arc<dyn DeviceTransferProvider>> {
        if manifest.component != PluginComponent::Transport {
            return Err(component_error("transport", manifest));
        }
        let factory = self
            .transport_factories
            .lock()
            .map_err(|_| io::Error::other("plugin transport registry lock poisoned"))?
            .get(&manifest.kind)
            .cloned()
            .ok_or_else(|| factory_missing(&manifest.kind))?;
        factory.create(manifest)
    }
}

fn register_factory<T: ?Sized>(
    registry: &Mutex<BTreeMap<String, Arc<T>>>,
    kind: &str,
    factory: Arc<T>,
) -> io::Result<()> {
    validate_identifier(kind, "plugin factory kind")?;
    let mut registry = registry
        .lock()
        .map_err(|_| io::Error::other("plugin factory registry lock poisoned"))?;
    if registry.contains_key(kind) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("plugin factory {kind} is already registered"),
        ));
    }
    registry.insert(kind.to_owned(), factory);
    Ok(())
}

fn validate_identifier(value: &str, field: &str) -> io::Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field} is not a valid plugin identifier"),
        ));
    }
    Ok(())
}

fn component_error(expected: &str, manifest: &PluginManifest) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("plugin {} is not a {expected} plugin", manifest.name),
    )
}

fn factory_missing(kind: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("no plugin factory is registered for kind {kind}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvBlockKey, KvTierEntry};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct NullTier;

    impl KvTier for NullTier {
        fn name(&self) -> &str {
            "null"
        }
        fn get(&self, _key: &KvBlockKey) -> io::Result<Option<KvTierEntry>> {
            Ok(None)
        }
        fn put(&self, _entry: &KvTierEntry) -> io::Result<()> {
            Ok(())
        }
        fn remove(&self, _key: &KvBlockKey) -> io::Result<()> {
            Ok(())
        }
    }

    struct NullFactory;

    impl KvTierPluginFactory for NullFactory {
        fn kind(&self) -> &str {
            "null-tier"
        }
        fn create(&self, _manifest: &PluginManifest) -> io::Result<Arc<dyn KvTier>> {
            Ok(Arc::new(NullTier))
        }
    }

    #[test]
    fn discovers_and_instantiates_manifest_plugins() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("rivet-plugin-test-{unique}"));
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("cache.rivet-plugin"),
            "name=cache-a\ncomponent=tier\nkind=null-tier\nendpoint=local\n",
        )
        .unwrap();
        let registry = PluginRegistry::new();
        registry
            .register_tier_factory(Arc::new(NullFactory))
            .unwrap();
        let manifests = registry.discover_dir(&directory).unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].properties.get("endpoint").unwrap(), "local");
        assert_eq!(registry.create_tier(&manifests[0]).unwrap().name(), "null");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn duplicate_manifest_keys_fail_closed() {
        assert!(
            PluginManifest::parse("name=a\ncomponent=tier\nkind=null-tier\nkind=again\n").is_err()
        );
    }
}
