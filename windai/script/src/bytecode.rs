use super::runtime::interpreter;
use rquickjs::{Context, Module, WriteOptions};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use wind_mod::package::{SCRIPT_DIR, SCRIPT_ENTRY, SCRIPT_EXT};

/// 一次安装期编译的时间预算
const COMPILE_BUDGET: Duration = Duration::from_millis(10_000);

/// 中断生效之后调用方多等的余量
const COMPILE_GRACE: Duration = Duration::from_millis(2_000);

pub use wind_core::models::CompiledScripts as Compiled;

pub fn engine_fingerprint() -> String {
    format!(
        "rquickjs-sys={};arch={};ptr={};endian={}",
        env!("WIND_SCRIPT_ENGINE_VERSION"),
        env::consts::ARCH,
        usize::BITS,
        if cfg!(target_endian = "little") {
            "le"
        } else {
            "be"
        },
    )
}

pub fn matches_current_engine(compiled: &Compiled) -> bool {
    compiled.fingerprint == engine_fingerprint()
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("cannot walk {path}: {reason}")]
    Walk { path: PathBuf, reason: String },
    #[error("module {name} does not compile: {reason}")]
    Module { name: String, reason: String },
    #[error("the manifest names a handler {name} that the scripts do not export")]
    MissingHandler { name: String },
    #[error("the compile of {path} did not finish in time")]
    Timeout { path: PathBuf },
    #[error("the compiler could not start an interpreter: {0}")]
    Interpreter(String),
    #[error("the compiler thread failed: {0}")]
    Thread(String),
}

/// 将脚本编译成字节码
///
/// `handlers` js导出的函数名字集合
pub fn compile_scripts(root: &Path, handlers: &BTreeSet<String>) -> Result<Compiled, CompileError> {
    compile_scripts_within(root, handlers, COMPILE_BUDGET)
}

fn compile_scripts_within(
    root: &Path,
    handlers: &BTreeSet<String>,
    budget: Duration,
) -> Result<Compiled, CompileError> {
    let root = root.to_path_buf();
    let handlers = handlers.clone();
    let deadline = Instant::now() + budget;
    let path = root.clone();
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name("wind-script-compile".to_string())
        .spawn(move || {
            let _ = tx.send(compile_on_this_thread(&root, &handlers, deadline));
        })
        .map_err(|error| CompileError::Thread(error.to_string()))?;
    match rx.recv_timeout(budget + COMPILE_GRACE) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(CompileError::Timeout { path }),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(CompileError::Thread(
            "it stopped without an answer".to_string(),
        )),
    }
}

fn compile_on_this_thread(
    root: &Path,
    handlers: &BTreeSet<String>,
    deadline: Instant,
) -> Result<Compiled, CompileError> {
    let sources = collect_sources(root)?;
    if !sources.contains_key(SCRIPT_ENTRY) {
        if handlers.is_empty() {
            return Ok(Compiled {
                modules: BTreeMap::new(),
                fingerprint: engine_fingerprint(),
            });
        }
        return Err(CompileError::MissingHandler {
            name: handlers.iter().next().cloned().unwrap_or_default(),
        });
    }

    let runtime =
        interpreter(deadline).map_err(|error| CompileError::Interpreter(error.to_string()))?;
    let context =
        Context::full(&runtime).map_err(|error| CompileError::Interpreter(error.to_string()))?;

    let mut modules = BTreeMap::new();
    let outcome = context.with(|ctx| {
        // 将源代码转换成字节码
        for (name, source) in &sources {
            // 把源码编译成模块对象
            let module = Module::declare(ctx.clone(), name.as_str(), source.as_str())
                .map_err(|error| module_error(name, error))?;
            let bytes = module
                .write(WriteOptions::default())
                .map_err(|error| module_error(name, error))?;
            modules.insert(name.clone(), bytes);
        }

        let entry = Module::declare(
            ctx.clone(),
            SCRIPT_ENTRY,
            sources.get(SCRIPT_ENTRY).cloned().unwrap_or_default(),
        )
        .map_err(|error| module_error(SCRIPT_ENTRY, error))?;

        let (module, promise) = entry
            .eval()
            .map_err(|error| module_error(SCRIPT_ENTRY, error))?;
        let _ = promise;

        while ctx.execute_pending_job() {}
        let namespace = module
            .namespace()
            .map_err(|error| module_error(SCRIPT_ENTRY, error))?;
        let mut exported = BTreeSet::new();
        for key in namespace.keys::<String>() {
            exported.insert(key.map_err(|error| module_error(SCRIPT_ENTRY, error))?);
        }
        Ok::<BTreeSet<String>, CompileError>(exported)
    });
    if Instant::now() >= deadline {
        return Err(CompileError::Timeout {
            path: root.to_path_buf(),
        });
    }
    let exported = outcome?;

    if let Some(missing) = handlers.iter().find(|name| !exported.contains(*name)) {
        return Err(CompileError::MissingHandler {
            name: missing.clone(),
        });
    }
    Ok(Compiled {
        modules,
        fingerprint: engine_fingerprint(),
    })
}

fn module_error(name: &str, reason: impl Display) -> CompileError {
    CompileError::Module {
        name: name.to_string(),
        reason: reason.to_string(),
    }
}

/// 搜集 js 源代码，如下格式
///
/// ```js
/// {
///    "scripts/index.js":    "import { hn } from './lib/hn.js';\nexport const run = () => hn;\n",
///    "scripts/lib/hn.js":   "export const hn = 1;\n",
///    "scripts/xxx.js":      "",
///  }
/// ```
fn collect_sources(root: &Path) -> Result<BTreeMap<String, String>, CompileError> {
    let mut out = BTreeMap::new();
    let scripts = root.join(SCRIPT_DIR);
    if !scripts.is_dir() {
        return Ok(out);
    }
    let mut stack = vec![scripts];
    while let Some(dir) = stack.pop() {
        let read = fs::read_dir(&dir).map_err(|error| CompileError::Walk {
            path: dir.clone(),
            reason: error.to_string(),
        })?;
        for entry in read {
            let entry = entry.map_err(|error| CompileError::Walk {
                path: dir.clone(),
                reason: error.to_string(),
            })?;
            let path = entry.path();
            let kind = entry.file_type().map_err(|error| CompileError::Walk {
                path: path.clone(),
                reason: error.to_string(),
            })?;
            if kind.is_dir() {
                stack.push(path);
            } else if kind.is_file() && path.extension().is_some_and(|ext| ext == SCRIPT_EXT) {
                let name = path
                    .strip_prefix(root)
                    .map_err(|_| CompileError::Walk {
                        path: path.clone(),
                        reason: "it is not under the install root".to_string(),
                    })?
                    .to_string_lossy()
                    .replace('\\', "/");
                let source = fs::read_to_string(&path).map_err(|error| CompileError::Walk {
                    path: path.clone(),
                    reason: error.to_string(),
                })?;
                out.insert(name, source);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 建一个只有入口的包
    fn package(body: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("create temp dir");
        let scripts = dir.path().join("scripts");
        fs::create_dir_all(&scripts).expect("create dir");
        fs::write(scripts.join("index.js"), body).expect("write file");
        dir
    }

    #[test]
    fn only_javascript_under_the_scripts_directory_is_collected() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let files = [
            ("app.js", "root-level"),
            ("ui/index.js", "browser"),
            ("mcp/tool.js", "asset"),
            ("scripts/index.js", "entry"),
            ("scripts/xxx.js", "module"),
            ("scripts/lib/hn.js", "nested"),
            ("scripts/notes.txt", "not js"),
        ];
        for (relative, body) in files {
            let path = dir.path().join(relative);
            fs::create_dir_all(path.parent().expect("has parent")).expect("create dir");
            fs::write(&path, body).expect("write file");
        }
        let sources = collect_sources(dir.path()).expect("walk");
        assert_eq!(
            sources.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["scripts/index.js", "scripts/lib/hn.js", "scripts/xxx.js"]
        );
    }

    #[test]
    fn a_top_level_loop_is_cut_off_by_the_budget() {
        let dir = package("export const run = () => 1;\nwhile (true) {}\n");
        let handlers = BTreeSet::from(["run".to_string()]);
        let started = Instant::now();
        let error = compile_scripts_within(dir.path(), &handlers, Duration::from_millis(200))
            .expect_err("cut off a top-level loop");
        assert!(matches!(error, CompileError::Timeout { .. }), "{error:?}");
        // 解释器到点就被打断，因此不该等到调用方用尽余量
        let waited = started.elapsed();
        assert!(waited < Duration::from_secs(1), "waited {waited:?}");
    }
}
