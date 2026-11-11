use std::collections::HashSet;
use std::path::Path;

use wind_wire::CompiledScripts;

pub mod error;
pub mod events;
pub mod installer;
pub mod manifest;
pub mod package;
pub mod resources;
pub mod roster;
pub mod schema;

pub use resources::{ModResources, Resources};
pub use schema::manifest_schema;

pub trait Compiler: Send + Sync {
    fn compile(
        &self,
        root: &Path,
        handlers: &HashSet<String>,
    ) -> Result<Option<CompiledScripts>, String>;
}
