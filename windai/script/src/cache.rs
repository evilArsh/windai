use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use rquickjs::{CaughtError, Context, Ctx, Module, Runtime, WriteOptions};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;

const MAGIC: &[u8; 8] = b"WINDBC01";
const DIGEST_LEN: usize = 32;
const HEADER_LEN: usize = MAGIC.len() + DIGEST_LEN;
const EXTENSION: &str = "bin";

#[derive(Clone)]
pub(crate) enum Image {
    Bytecode(Arc<[u8]>),
    Source(Arc<str>),
}

#[derive(Default)]
struct Entry {
    image: Option<Image>,
    source: Option<Arc<str>>,
    path: Option<PathBuf>,
}

#[derive(Default)]
struct Slot {
    ready: AtomicBool,
    entry: Mutex<Entry>,
}

type MemoryKey = (PathBuf, String, String);

pub(crate) struct BytecodeCache {
    disk: Option<PathBuf>,
    engine: String,
    memory_limit: usize,
    entries: Mutex<HashMap<MemoryKey, Arc<Slot>>>,
    poisoned: Mutex<HashSet<PathBuf>>,
}

impl BytecodeCache {
    pub(crate) fn new(disk: Option<PathBuf>, engine: String) -> Self {
        Self {
            disk,
            engine,
            memory_limit: 64 * 1024 * 1024,
            entries: Mutex::new(HashMap::new()),
            poisoned: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn with_memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = limit;
        self
    }

    fn entry(&self, root: &Path, mod_name: &str, module: &str) -> Arc<Slot> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry((root.to_path_buf(), mod_name.to_string(), module.to_string()))
            .or_default()
            .clone()
    }

    pub(crate) fn resolve(
        &self,
        root: &Path,
        mod_name: &str,
        module: &str,
        cold: &AtomicBool,
        read: impl FnOnce() -> Result<String, String>,
    ) -> Result<Image, String> {
        let slot = self.entry(root, mod_name, module);
        if !slot.ready.load(Ordering::Acquire) {
            cold.store(true, Ordering::Release);
        }
        let mut entry = slot.entry.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(image) = &entry.image {
            return Ok(image.clone());
        }
        let source: Arc<str> = read()?.into();
        let key = key(module, &source);
        let path = self.path(mod_name, &key);
        let image = if let Some(path) = &path {
            if self
                .poisoned
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(path)
            {
                Image::Source(source.clone())
            } else {
                match std::fs::read(path) {
                    Ok(bytes) => match self.decode(&key, &bytes) {
                        Some(bytes) => Image::Bytecode(bytes.into()),
                        None => {
                            self.remove(path);
                            Image::Source(source.clone())
                        }
                    },
                    Err(error) => {
                        if error.kind() != io::ErrorKind::NotFound {
                            log::warn!("[script] cannot read {}: {error}", path.display());
                        }
                        let bytes = self.compile(module, &source)?;
                        if let Err(error) = self.write(path, &key, &bytes) {
                            log::warn!("[script] cannot write {}: {error}", path.display());
                        }
                        Image::Bytecode(bytes.into())
                    }
                }
            }
        } else {
            Image::Bytecode(self.compile(module, &source)?.into())
        };
        entry.source = Some(source);
        entry.path = path;
        entry.image = Some(image.clone());
        slot.ready.store(true, Ordering::Release);
        Ok(image)
    }

    pub(crate) fn poison(&self, root: &Path, mod_name: &str, module: &str) {
        let slot = self.entry(root, mod_name, module);
        let mut entry = slot.entry.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(path) = &entry.path {
            self.remove(path);
        }
        entry.image = entry.source.clone().map(Image::Source);
    }

    fn remove(&self, path: &Path) {
        self.poisoned
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path.to_path_buf());
        if let Err(error) = std::fs::remove_file(path)
            && error.kind() != io::ErrorKind::NotFound
        {
            log::warn!("[script] cannot remove {}: {error}", path.display());
        }
    }

    fn path(&self, name: &str, key: &str) -> Option<PathBuf> {
        let root = self.disk.as_ref()?;
        if !component(name) || !component(&self.engine) {
            log::warn!("[script] invalid cache directory component; using memory only");
            return None;
        }
        Some(
            root.join(name)
                .join(&self.engine)
                .join(format!("{key}.{EXTENSION}")),
        )
    }

    fn compile(&self, module: &str, source: &str) -> Result<Vec<u8>, String> {
        let runtime = Runtime::new().map_err(|e| e.to_string())?;
        runtime.set_memory_limit(self.memory_limit);
        runtime.set_loader(CompileResolver, CompileLoader);
        let context = Context::full(&runtime).map_err(|e| e.to_string())?;
        context.with(|ctx| {
            Module::declare(ctx.clone(), module, source)
                .and_then(|module| module.write(WriteOptions::default()))
                .map_err(|error| CaughtError::from_error(&ctx, error).to_string())
        })
    }

    fn digest(&self, key: &str, bytes: &[u8]) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(self.engine.as_bytes());
        hash.update([0]);
        hash.update(key.as_bytes());
        hash.update(bytes);
        hash.finalize().into()
    }

    fn decode<'a>(&self, key: &str, bytes: &'a [u8]) -> Option<&'a [u8]> {
        let header = bytes.get(..MAGIC.len())?;
        let digest = bytes.get(MAGIC.len()..HEADER_LEN)?;
        let payload = bytes.get(HEADER_LEN..)?;
        (header == MAGIC && !payload.is_empty() && digest == self.digest(key, payload))
            .then_some(payload)
    }

    fn write(&self, path: &Path, key: &str, bytes: &[u8]) -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("missing cache parent"))?;
        std::fs::create_dir_all(parent)?;
        let mut file = NamedTempFile::new_in(parent)?;
        file.write_all(MAGIC)?;
        file.write_all(&self.digest(key, bytes))?;
        file.write_all(bytes)?;
        file.persist(path).map_err(|error| error.error)?;
        Ok(())
    }
}

struct CompileResolver;
impl Resolver for CompileResolver {
    fn resolve<'js>(
        &mut self,
        _ctx: &Ctx<'js>,
        base: &str,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<String> {
        Ok(format!("{base}#{name}"))
    }
}

struct CompileLoader;
impl Loader for CompileLoader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<Module<'js>> {
        Module::declare(ctx.clone(), name, "export {};")
    }
}

fn component(value: &str) -> bool {
    !value.is_empty() && !value.ends_with(['.', ' ']) && !value.contains(['/', '\\', ':', '\0'])
}

fn key(module: &str, source: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(module.as_bytes());
    hash.update([0]);
    hash.update(source.as_bytes());
    format!("{:x}", hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    const SOURCE: &str = "export function run() { return 1; }";
    fn cache(disk: &Path) -> BytecodeCache {
        BytecodeCache::new(Some(disk.to_path_buf()), "engine".to_string())
    }
    fn files(disk: &Path, name: &str, engine: &str) -> Vec<PathBuf> {
        std::fs::read_dir(disk.join(name).join(engine))
            .expect("artifact dir")
            .map(|entry| entry.expect("entry").path())
            .collect()
    }
    fn resolve(cache: &BytecodeCache, root: &str, name: &str, module: &str, source: &str) -> Image {
        cache
            .resolve(
                Path::new(root),
                name,
                module,
                &AtomicBool::new(false),
                || Ok(source.to_string()),
            )
            .expect("image")
    }
    #[test]
    fn a_second_resolve_reuses_the_memory_image() {
        let cache = BytecodeCache::new(None, "engine".into());
        let first = resolve(&cache, "v1", "demo", "scripts/index.js", SOURCE);
        let second = cache
            .resolve(
                Path::new("v1"),
                "demo",
                "scripts/index.js",
                &AtomicBool::new(false),
                || panic!("unexpected source read"),
            )
            .expect("image");
        let (Image::Bytecode(a), Image::Bytecode(b)) = (first, second) else {
            panic!("expected bytecode")
        };
        assert!(Arc::ptr_eq(&a, &b));
    }
    #[test]
    fn a_new_version_root_reuses_the_disk_image() {
        let disk = TempDir::new().expect("temp dir");
        let cache = cache(disk.path());
        resolve(&cache, "v1", "demo", "scripts/index.js", SOURCE);
        let artifact = files(disk.path(), "demo", "engine")
            .pop()
            .expect("artifact");
        let before = std::fs::metadata(&artifact)
            .expect("metadata")
            .modified()
            .expect("mtime");
        resolve(&cache, "v2", "demo", "scripts/index.js", SOURCE);
        assert_eq!(
            before,
            std::fs::metadata(artifact)
                .expect("metadata")
                .modified()
                .expect("mtime")
        );
        assert_eq!(files(disk.path(), "demo", "engine").len(), 1);
    }
    #[test]
    fn a_changed_source_produces_a_different_key() {
        let disk = TempDir::new().expect("temp dir");
        resolve(
            &cache(disk.path()),
            "v1",
            "demo",
            "scripts/index.js",
            SOURCE,
        );
        resolve(
            &cache(disk.path()),
            "v1",
            "demo",
            "scripts/index.js",
            "export const value = 2;",
        );
        assert_eq!(files(disk.path(), "demo", "engine").len(), 2);
    }
    #[test]
    fn two_modules_with_the_same_source_get_different_keys() {
        let disk = TempDir::new().expect("temp dir");
        let cache = cache(disk.path());
        for module in ["scripts/a.js", "scripts/b.js"] {
            resolve(&cache, "v1", "demo", module, SOURCE);
        }
        assert_eq!(files(disk.path(), "demo", "engine").len(), 2);
    }
    #[test]
    fn two_mods_with_the_same_module_get_separate_files() {
        let disk = TempDir::new().expect("temp dir");
        let cache = cache(disk.path());
        for name in ["demo-a", "demo-b"] {
            resolve(&cache, "v1", name, "scripts/index.js", SOURCE);
            assert_eq!(files(disk.path(), name, "engine").len(), 1);
        }
    }
    #[test]
    fn a_different_engine_id_never_shares_files() {
        let disk = TempDir::new().expect("temp dir");
        for engine in ["old", "new"] {
            let cache = BytecodeCache::new(Some(disk.path().into()), engine.into());
            resolve(&cache, "v1", "demo", "scripts/index.js", SOURCE);
            assert_eq!(files(disk.path(), "demo", engine).len(), 1);
        }
    }
    #[test]
    fn poison_deletes_the_image_and_falls_back_to_source() {
        let disk = TempDir::new().expect("temp dir");
        let cache = cache(disk.path());
        resolve(&cache, "v1", "demo", "scripts/index.js", SOURCE);
        cache.poison(Path::new("v1"), "demo", "scripts/index.js");
        let image = cache
            .resolve(
                Path::new("v1"),
                "demo",
                "scripts/index.js",
                &AtomicBool::new(false),
                || panic!("reuse original source"),
            )
            .expect("source");
        assert!(matches!(image, Image::Source(_)));
        assert!(files(disk.path(), "demo", "engine").is_empty());
    }
    #[test]
    fn a_disabled_disk_store_writes_nothing() {
        let disk = TempDir::new().expect("temp dir");
        let cache = BytecodeCache::new(None, "engine".into());
        assert!(matches!(
            cache
                .resolve(
                    disk.path(),
                    "demo",
                    "scripts/index.js",
                    &AtomicBool::new(false),
                    || Ok(SOURCE.into())
                )
                .expect("image"),
            Image::Bytecode(_)
        ));
        assert_eq!(std::fs::read_dir(disk.path()).expect("dir").count(), 0);
    }
    #[test]
    fn a_mod_name_that_escapes_the_bytecode_root_is_never_written() {
        let disk = TempDir::new().expect("temp dir");
        let cache = cache(disk.path());
        for name in [
            "../evil", "/abs", "", "..", ".", "a/b", "a\\b", "C:\\evil", ".. ", "...", "demo.",
        ] {
            assert!(matches!(
                resolve(&cache, name, name, "scripts/index.js", SOURCE),
                Image::Bytecode(_)
            ));
        }
        assert_eq!(std::fs::read_dir(disk.path()).expect("dir").count(), 0);
    }
    #[test]
    fn concurrent_resolves_compile_once() {
        let cache = BytecodeCache::new(None, "engine".into());
        let reads = AtomicUsize::new(0);
        let images = std::thread::scope(|scope| {
            (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        cache
                            .resolve(
                                Path::new("v1"),
                                "demo",
                                "scripts/index.js",
                                &AtomicBool::new(false),
                                || {
                                    reads.fetch_add(1, Ordering::Relaxed);
                                    Ok(SOURCE.into())
                                },
                            )
                            .expect("image")
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|t| t.join().expect("thread"))
                .collect::<Vec<_>>()
        });
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        let Image::Bytecode(first) = &images[0] else {
            panic!("bytecode")
        };
        for image in &images {
            let Image::Bytecode(bytes) = image else {
                panic!("bytecode")
            };
            assert!(Arc::ptr_eq(first, bytes));
        }
    }
    #[test]
    fn a_read_failure_can_be_retried() {
        let cache = BytecodeCache::new(None, "engine".into());
        assert!(
            cache
                .resolve(
                    Path::new("v1"),
                    "demo",
                    "scripts/index.js",
                    &AtomicBool::new(false),
                    || Err("missing".into())
                )
                .is_err()
        );
        assert!(matches!(
            resolve(&cache, "v1", "demo", "scripts/index.js", SOURCE),
            Image::Bytecode(_)
        ));
    }
    #[test]
    fn damaged_artifacts_fall_back_without_loading_untrusted_bytes() {
        for damaged in [vec![], vec![1, 2, 3], b"not bytecode".to_vec()] {
            let disk = TempDir::new().expect("temp dir");
            resolve(
                &cache(disk.path()),
                "v1",
                "demo",
                "scripts/index.js",
                SOURCE,
            );
            let artifact = files(disk.path(), "demo", "engine")
                .pop()
                .expect("artifact");
            std::fs::write(&artifact, damaged).expect("damage");
            let cache = cache(disk.path());
            assert!(matches!(
                resolve(&cache, "v1", "demo", "scripts/index.js", SOURCE),
                Image::Source(_)
            ));
            assert!(!artifact.exists());
        }
    }
}
