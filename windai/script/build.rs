use std::fs;
use std::path::Path;

const ENGINE_PACKAGE: &str = "rquickjs-sys";
const LOCK_FILE: &str = "../../Cargo.lock";

fn main() {
    let lock = Path::new(LOCK_FILE);
    println!("cargo:rerun-if-changed={}", lock.display());
    let version = read_version(lock, ENGINE_PACKAGE).unwrap_or_else(|| {
        panic!(
            "cannot read the resolved version of {ENGINE_PACKAGE} from {}",
            lock.display()
        )
    });
    println!("cargo:rustc-env=WIND_SCRIPT_ENGINE_VERSION={version}");
}

fn read_version(lock: &Path, name: &str) -> Option<String> {
    let text = fs::read_to_string(lock).ok()?;
    let lines: Vec<&str> = text.lines().collect();
    let mut index = 0;
    while index < lines.len() {
        if lines[index].trim() != "[[package]]" {
            index += 1;
            continue;
        }
        index += 1;
        let mut package = None;
        let mut version = None;
        while index < lines.len() && !lines[index].trim().starts_with('[') {
            let field = lines[index].trim();
            if let Some(value) = field.strip_prefix("name = ") {
                package = Some(value.trim_matches('"').to_string());
            } else if let Some(value) = field.strip_prefix("version = ") {
                version = Some(value.trim_matches('"').to_string());
            }
            index += 1;
        }
        if package.as_deref() == Some(name) {
            return version;
        }
    }
    None
}
