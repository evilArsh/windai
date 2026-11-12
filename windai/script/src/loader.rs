use super::cache::{BytecodeCache, Image};
use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use rquickjs::{Ctx, Module};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use wind_wire::MOD_SCRIPT_DIR;

/// 模块名或文件不合法
#[derive(Debug, thiserror::Error)]
pub(crate) enum LoadError {
    #[error("the module specifier must be relative")]
    NotRelative,
    #[error("a bare module specifier is not supported")]
    Bare,
    #[error("the module specifier leaves the scripts directory")]
    Escapes,
    #[error("the module specifier must stay inside the scripts directory")]
    NotHostSide,
    #[error("the module specifier must end with .js")]
    Extension,
    #[error("the scripts directory is missing")]
    Missing,
    #[error("the module is outside the scripts directory")]
    Outside,
    #[error("{0}")]
    Io(String),
}

/// 把导入说明符折成相对 mod 根的模块名
fn normalize_specifier(base: &str, specifier: &str) -> Result<String, LoadError> {
    if specifier.starts_with('/') || specifier.contains(':') {
        return Err(LoadError::NotRelative);
    }
    if !specifier.starts_with("./") && !specifier.starts_with("../") {
        return Err(LoadError::Bare);
    }
    let Some(parent) = Path::new(base).parent() else {
        return Err(LoadError::Escapes);
    };
    let mut parts: Vec<String> = Vec::new();
    for component in parent.join(specifier).components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or(LoadError::NotHostSide)?;
                parts.push(part.to_string());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                parts.pop().ok_or(LoadError::Escapes)?;
            }
            Component::RootDir | Component::Prefix(_) => return Err(LoadError::NotRelative),
        }
    }
    if parts.first().map(String::as_str) != Some(MOD_SCRIPT_DIR) {
        return Err(LoadError::NotHostSide);
    }
    let name = parts.join("/");
    if !name.ends_with(".js") {
        return Err(LoadError::Extension);
    }
    Ok(name)
}

/// 把导入说明符规范化成模块名
pub(crate) struct ModResolver;

impl Resolver for ModResolver {
    fn resolve<'js>(
        &mut self,
        _ctx: &Ctx<'js>,
        base: &str,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<String> {
        normalize_specifier(base, name)
            .map_err(|error| rquickjs::Error::new_resolving_message(base, name, error.to_string()))
    }
}

/// 从 mod 包读取模块源码
#[derive(Clone)]
pub(crate) struct ModLoader {
    root: PathBuf,
    mod_name: String,
    cache: Arc<BytecodeCache>,
    cold: Arc<AtomicBool>,
}

impl ModLoader {
    pub(crate) fn new(
        root: &Path,
        mod_name: String,
        cache: Arc<BytecodeCache>,
        cold: Arc<AtomicBool>,
    ) -> Self {
        Self {
            root: root.to_path_buf(),
            mod_name,
            cache,
            cold,
        }
    }

    pub(crate) fn image(&self, name: &str) -> Result<Image, LoadError> {
        self.cache
            .resolve(&self.root, &self.mod_name, name, &self.cold, || {
                self.source(name).map_err(|error| error.to_string())
            })
            .map_err(LoadError::Io)
    }

    pub(crate) fn cached_module<'js>(
        &self,
        ctx: &Ctx<'js>,
        name: &str,
        image: Image,
    ) -> rquickjs::Result<Module<'js>> {
        match image {
            Image::Source(source) => {
                self.cold.store(true, Ordering::Release);
                Module::declare(ctx.clone(), name, source.as_ref())
            }
            Image::Bytecode(bytes) => {
                // SAFETY: 字节码来自同引擎编译或通过完整性校验的可信本地缓存
                match unsafe { Module::load(ctx.clone(), &bytes) } {
                    Ok(module) => Ok(module),
                    Err(error) => {
                        log::warn!("[script] cannot load cached module {name}: {error}");
                        if error.is_exception() {
                            let _ = ctx.catch();
                        }
                        self.cache.poison(&self.root, &self.mod_name, name);
                        let image = self.image(name).map_err(|error| {
                            rquickjs::Error::new_loading_message(name, error.to_string())
                        })?;
                        match image {
                            Image::Source(source) => {
                                Module::declare(ctx.clone(), name, source.as_ref())
                            }
                            Image::Bytecode(_) => Err(error),
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn source(&self, name: &str) -> Result<String, LoadError> {
        let normalized = !name.is_empty()
            && Path::new(name)
                .components()
                .all(|component| matches!(component, Component::Normal(_)));
        if !normalized {
            return Err(LoadError::Outside);
        }
        let root = self.root.canonicalize().map_err(|_| LoadError::Missing)?;
        let scripts = root.join(MOD_SCRIPT_DIR);
        let path = root.join(name);
        let canonical = path.canonicalize().map_err(|_| LoadError::Missing)?;
        if !canonical.starts_with(&scripts) {
            return Err(LoadError::Outside);
        }
        std::fs::read_to_string(&canonical).map_err(|error| LoadError::Io(error.to_string()))
    }
}

impl Loader for ModLoader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<Module<'js>> {
        let image = self
            .image(name)
            .map_err(|error| rquickjs::Error::new_loading_message(name, error.to_string()))?;
        self.cached_module(ctx, name, image)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rquickjs::{Context, Runtime};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[test]
    fn waiting_for_initial_compilation_marks_the_call_cold() {
        let cache = Arc::new(BytecodeCache::new(None, "engine".into()));
        let cold = Arc::new(AtomicBool::new(false));
        let loader = ModLoader::new(Path::new("v1"), "demo".into(), cache.clone(), cold.clone());
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let compiler = cache.clone();
            let first = scope.spawn(move || {
                compiler.resolve(
                    Path::new("v1"),
                    "demo",
                    "scripts/index.js",
                    &AtomicBool::new(false),
                    || {
                        started_tx.send(()).expect("started");
                        release_rx.recv().expect("release");
                        Ok("export const value = 1;".into())
                    },
                )
            });
            started_rx.recv().expect("initializing");
            let waiter = scope.spawn(|| loader.image("scripts/index.js"));
            let deadline = Instant::now() + Duration::from_millis(100);
            while !cold.load(Ordering::Acquire) && Instant::now() < deadline {
                std::thread::yield_now();
            }
            let was_cold = cold.load(Ordering::Acquire);
            release_tx.send(()).expect("release");
            first.join().expect("first thread").expect("first image");
            waiter.join().expect("waiter thread").expect("waiter image");
            assert!(
                was_cold,
                "initialization wait must receive the cold-call exemption"
            );
        });
    }

    #[test]
    fn declaring_a_cached_source_image_marks_every_call_cold() {
        let cache = Arc::new(BytecodeCache::new(None, "engine".into()));
        cache
            .resolve(
                Path::new("v1"),
                "demo",
                "scripts/index.js",
                &AtomicBool::new(false),
                || Ok("export const value = 1;".into()),
            )
            .expect("cache");
        cache.poison(Path::new("v1"), "demo", "scripts/index.js");
        for _ in 0..2 {
            let cold = Arc::new(AtomicBool::new(false));
            let loader =
                ModLoader::new(Path::new("v1"), "demo".into(), cache.clone(), cold.clone());
            let image = loader.image("scripts/index.js").expect("source");
            let runtime = Runtime::new().expect("runtime");
            let context = Context::full(&runtime).expect("context");
            context.with(|ctx| {
                loader
                    .cached_module(&ctx, "scripts/index.js", image)
                    .map(|_| ())
                    .expect("declare")
            });
            assert!(
                cold.load(Ordering::Acquire),
                "source recompilation must receive the cold-call exemption"
            );
        }
    }
}
