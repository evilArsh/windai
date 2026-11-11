use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use rquickjs::{Ctx, Module};
use std::path::{Component, Path, PathBuf};
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
pub(crate) struct ModLoader {
    root: PathBuf,
    scripts: PathBuf,
}

impl ModLoader {
    pub(crate) fn new(root: &Path) -> Self {
        let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        Self {
            scripts: root.join(MOD_SCRIPT_DIR),
            root,
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
        let path = self.root.join(name);
        let canonical = path.canonicalize().map_err(|_| LoadError::Missing)?;
        if !canonical.starts_with(&self.scripts) {
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
        let source = self
            .source(name)
            .map_err(|error| rquickjs::Error::new_loading_message(name, error.to_string()))?;
        Module::declare(ctx.clone(), name, source)
    }
}
