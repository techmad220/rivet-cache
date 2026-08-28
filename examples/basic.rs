use rivet_cache::ContextCache;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let root = std::env::temp_dir().join("rivet-cache-example");
    let cache = ContextCache::new(
        Some(root.clone()),
        1024 * 1024,
        16 * 1024 * 1024,
        Duration::from_secs(60),
    )?;

    let key = ContextCache::key("completion/v1", "demo-model", "hello");
    cache.put(&key, b"world", None, false)?;
    println!("{:?}", cache.get(&key)?);
    println!("{:?}", cache.stats()?);
    cache.clear()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
