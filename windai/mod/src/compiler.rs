//! 安装期的编译接口

use std::collections::BTreeSet;
use std::path::Path;
pub use wind_core::models::CompiledScripts;

pub trait Compiler: Send + Sync {
    fn compile(
        &self,
        root: &Path,
        handlers: &BTreeSet<String>,
    ) -> Result<Option<CompiledScripts>, String>;
}

pub struct NoCompiler;

impl Compiler for NoCompiler {
    fn compile(
        &self,
        _root: &Path,
        handlers: &BTreeSet<String>,
    ) -> Result<Option<CompiledScripts>, String> {
        if handlers.is_empty() {
            Ok(None)
        } else {
            Err("this deployment has no script runtime".to_string())
        }
    }
}
