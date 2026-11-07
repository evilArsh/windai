use std::collections::HashSet;
use std::path::Path;

use wind_wire::CompiledScripts;

pub mod dirs;
pub mod error;
pub mod events;
pub mod manifest;
pub mod package;
pub mod roster;
pub mod installer;

pub trait Compiler: Send + Sync {
    fn compile(
        &self,
        root: &Path,
        handlers: &HashSet<String>,
    ) -> Result<Option<CompiledScripts>, String>;
}
