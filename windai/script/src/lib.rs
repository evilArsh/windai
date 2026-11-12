mod cache;
mod circuit;
mod engine;
mod error;
mod facade;
mod loader;
mod service;
mod threads;
pub use engine::{Config, quickjs};
pub use error::ScriptsError;
pub use service::{ScriptCall, ScriptHost, ScriptScope, ScriptService};

pub fn engine_id() -> String {
    let endian = if cfg!(target_endian = "little") {
        "le"
    } else {
        "be"
    };
    format!(
        "rquickjs-sys={};arch={};ptr={};endian={endian}",
        env!("WIND_SCRIPT_ENGINE_VERSION"),
        std::env::consts::ARCH,
        usize::BITS
    )
}
