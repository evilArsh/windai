use rquickjs::Error;
use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use rquickjs::module::Declared;
use rquickjs::{Ctx, Module, Result as JsResult};
use std::collections::BTreeMap;
use std::fs::read_to_string;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use wind_mod::package::SCRIPT_DIR;

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("{specifier} is not a relative specifier")]
    NotRelative { specifier: String },
    #[error("{specifier} is a bare specifier")]
    Bare { specifier: String },
    #[error("{specifier} escapes the install root")]
    Escapes { specifier: String },
    #[error("{specifier} resolves to the install root, which is not a module")]
    NotAModule { specifier: String },
    #[error("{specifier} resolves into a directory that is not host-side code")]
    NotHostSide { specifier: String },
    #[error("{name} is not a normalized module name")]
    NotNormalized { name: String },
    #[error("module {name} is not present under the install root")]
    Missing { name: String },
    #[error("module {name} resolves outside the install root")]
    Outside { name: String },
    #[error("cannot read module {name}: {reason}")]
    Io { name: String, reason: String },
}

pub fn normalize_specifier(base: &str, specifier: &str) -> Result<String, LoadError> {
    debug_assert!(
        is_normalized(base),
        "expect a normalized module name: {base}"
    );

    if !(specifier.starts_with("./") || specifier.starts_with("../")) {
        return Err(if specifier.contains(':') || specifier.starts_with('/') {
            LoadError::NotRelative {
                specifier: specifier.to_string(),
            }
        } else {
            LoadError::Bare {
                specifier: specifier.to_string(),
            }
        });
    }

    let mut segments: Vec<String> = base
        .rsplit_once('/')
        .map(|(dir, _)| dir.split('/').map(str::to_string).collect())
        .unwrap_or_default();

    for component in Path::new(specifier).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => segments.push(part.to_string_lossy().into_owned()),
            Component::ParentDir => {
                if segments.pop().is_none() {
                    return Err(LoadError::Escapes {
                        specifier: specifier.to_string(),
                    });
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(LoadError::NotRelative {
                    specifier: specifier.to_string(),
                });
            }
        }
    }

    match segments.first() {
        None => {
            return Err(LoadError::NotAModule {
                specifier: specifier.to_string(),
            });
        }
        Some(first) if !(SCRIPT_DIR == first.as_str()) => {
            return Err(LoadError::NotHostSide {
                specifier: specifier.to_string(),
            });
        }
        Some(_) => {}
    }
    Ok(segments.join("/"))
}

fn is_normalized(name: &str) -> bool {
    !name.is_empty()
        && Path::new(name)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

#[derive(Debug, Default)]
pub struct ModuleResolver;

impl ModuleResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Resolver for ModuleResolver {
    fn resolve<'js>(
        &mut self,
        _ctx: &Ctx<'js>,
        base: &str,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> JsResult<String> {
        normalize_specifier(base, name)
            .map_err(|error| Error::new_resolving_message(base, name, error.to_string()))
    }
}

/// 一次调用里改读过源码的模块名
pub type Fallbacks = Vec<String>;

#[derive(Debug)]
pub struct ModuleLoader {
    root: Option<PathBuf>,
    compiled: BTreeMap<String, Vec<u8>>,
    fell_back: Fallbacks,
}

impl ModuleLoader {
    pub fn new(root: PathBuf, compiled: BTreeMap<String, Vec<u8>>) -> Self {
        Self {
            root: root.canonicalize().ok(),
            compiled,
            fell_back: Vec::new(),
        }
    }

    pub fn fell_back(&self) -> &[String] {
        &self.fell_back
    }

    fn source(&self, name: &str) -> Result<String, LoadError> {
        if !is_normalized(name) {
            return Err(LoadError::NotNormalized {
                name: name.to_string(),
            });
        }
        let Some(root) = self.root.as_ref() else {
            return Err(LoadError::Missing {
                name: name.to_string(),
            });
        };
        let path = root.join(name);
        let real = match path.canonicalize() {
            Ok(real) => real,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(LoadError::Missing {
                    name: name.to_string(),
                });
            }
            Err(error) => {
                return Err(LoadError::Io {
                    name: name.to_string(),
                    reason: error.to_string(),
                });
            }
        };
        if !real.starts_with(root) {
            return Err(LoadError::Outside {
                name: name.to_string(),
            });
        }
        read_to_string(&real).map_err(|error| LoadError::Io {
            name: name.to_string(),
            reason: error.to_string(),
        })
    }
}

impl Loader for ModuleLoader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> JsResult<Module<'js, Declared>> {
        if let Some(bytes) = self.compiled.get(name) {
            // SAFETY: 字节码必须由本引擎产出，由调用方保证
            let loaded = unsafe { Module::load(ctx.clone(), bytes) };
            match loaded {
                Ok(module) => return Ok(module),
                Err(error) => {
                    log::warn!(
                        "[script] cached bytecode of {name} is unreadable, falling back to source: {error}"
                    );
                    self.fell_back.push(name.to_string());
                }
            }
        }
        let source = self
            .source(name)
            .map_err(|error| Error::new_loading_message(name, error.to_string()))?;
        Module::declare(ctx.clone(), name, source)
    }
}
