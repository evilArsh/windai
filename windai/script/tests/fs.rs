//! `ctx.fs` 的形状与私有目录的边界
//!
//! ctx 不判路径边界，只把相对路径交给宿主；测试替身按一个根建边界，读写落在
//! 磁盘上

use rquickjs::{Context, FromJs, Runtime};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use wind_core::sandbox::{Sandbox, SandboxError};
use wind_event::envelope::Scope;
use wind_event::host::CallMode;
use wind_mod::capability::{Capability, available};
use wind_script::facade::{
    FacadeCall, FacadeError, HostApi, HttpRequest, ModIdentity, TranscriptEntry, build_ctx,
};

/// 一个把私有目录落到磁盘上的宿主实现
///
/// 只有 `fs_*` 是真的，其余方法只为满足接口
struct Local {
    /// 私有目录，即路径边界的根
    root: PathBuf,
    /// 路径边界
    sandbox: Sandbox,
    /// 到达宿主的方法与参数，按调用顺序记下来
    calls: Mutex<Vec<String>>,
}

impl Local {
    /// 以一个私有目录建一个宿主实现
    fn new(root: PathBuf) -> Self {
        let sandbox = Sandbox::new(vec![root.clone()]);
        Self {
            root,
            sandbox,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// 记一次到达宿主的调用
    fn record(&self, entry: String) {
        self.calls.lock().expect("lock the state").push(entry);
    }

    /// 把相对路径解析到私有目录之内；界外折成路径失败
    fn resolve(&self, path: &str) -> Result<PathBuf, FacadeError> {
        self.sandbox
            .resolve(&self.root.join(path))
            .map_err(|error| match error {
                SandboxError::NotAllowed(path) => FacadeError::Path { path },
                other => FacadeError::Failed(other.to_string()),
            })
    }
}

impl HostApi for Local {
    fn state_read(&self, _key: &str) -> Option<Value> {
        None
    }
    fn state_write(&self, _key: &str, _value: Value) -> Result<(), FacadeError> {
        Ok(())
    }
    fn state_delete(&self, _key: &str) {}
    fn state_keys(&self) -> Vec<String> {
        Vec::new()
    }
    fn storage_read(&self, _key: &str) -> Option<Value> {
        None
    }
    fn storage_write(&self, _key: &str, _value: Value) {}
    fn storage_delete(&self, _key: &str) {}
    fn storage_keys(&self) -> Vec<String> {
        Vec::new()
    }
    fn http_fetch(&self, _init: HttpRequest) -> Result<Vec<u8>, FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn http_fetch_many(&self, requests: Vec<HttpRequest>) -> Vec<Result<Vec<u8>, FacadeError>> {
        // 这个替身不出网，每个地址各得一份与 `http_fetch` 相同的失败
        requests
            .into_iter()
            .map(|request| self.http_fetch(request))
            .collect()
    }
    fn agent_prompt(&self, _scope: &Scope, _text: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn agent_draft(&self, _scope: &Scope, _text: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn agent_spawn(&self, _scope: &Scope, _mode: &str, _task: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn agent_transcript(&self, _scope: &Scope, _limit: Option<u32>) -> Vec<TranscriptEntry> {
        Vec::new()
    }
    fn fs_dir(&self, _name: &str) -> PathBuf {
        self.root.clone()
    }
    fn fs_read(&self, _name: &str, path: &str) -> Result<String, FacadeError> {
        self.record(format!("readText:{path}"));
        let target = self.resolve(path)?;
        std::fs::read_to_string(&target).map_err(|error| FacadeError::Failed(error.to_string()))
    }
    fn fs_write(&self, _name: &str, path: &str, data: &str) -> Result<(), FacadeError> {
        self.record(format!("writeText:{path}"));
        let target = self.resolve(path)?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| FacadeError::Failed(error.to_string()))?;
        }
        std::fs::write(&target, data).map_err(|error| FacadeError::Failed(error.to_string()))
    }
    fn fs_list(&self, _name: &str, path: &str) -> Result<Vec<String>, FacadeError> {
        self.record(format!("list:{path}"));
        let dir = self.resolve(path)?;
        let entries =
            std::fs::read_dir(&dir).map_err(|error| FacadeError::Failed(error.to_string()))?;
        let mut names: Vec<String> = entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .map(|path| match path.strip_prefix(&self.root) {
                Ok(relative) => relative.to_string_lossy().into_owned(),
                Err(_) => path.to_string_lossy().into_owned(),
            })
            .collect();
        names.sort();
        Ok(names)
    }
    fn fs_remove(&self, _name: &str, path: &str) -> Result<(), FacadeError> {
        self.record(format!("remove:{path}"));
        let target = self.resolve(path)?;
        std::fs::remove_file(&target)
            .or_else(|_| std::fs::remove_dir(&target))
            .map_err(|error| FacadeError::Failed(error.to_string()))
    }
    fn emit(&self, _scope: &Scope, _name: &str, _payload: Value) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn notify(&self, _scope: &Scope, _level: &str, _text: &str) -> Result<(), FacadeError> {
        Err(FacadeError::Unsupported)
    }
    fn log(&self, _level: &str, _message: &str) {}
}

/// 一次只带私有目录能力的 ctx 调用
fn call_with(host: Arc<dyn HostApi>) -> FacadeCall {
    FacadeCall {
        identity: ModIdentity {
            name: String::from("demo"),
            version: 1,
        },
        scope: Scope::Topic { topic_id: 7 },
        props: available(&[Capability::Fs], CallMode::Request),
        host,
    }
}

/// 一个临时目录里的私有目录，旁边留出界外的位置
struct Private {
    /// 承载私有目录的临时目录
    base: TempDir,
    /// 私有目录本身
    root: PathBuf,
}

impl Private {
    /// 建一个空的私有目录
    fn new() -> Self {
        let base = tempfile::tempdir().expect("create temp dir");
        let root = base.path().join("data");
        std::fs::create_dir_all(&root).expect("create the private dir");
        Self { base, root }
    }

    /// 以这个私有目录建一个宿主实现
    fn host(&self) -> Arc<Local> {
        Arc::new(Local::new(self.root.clone()))
    }

    /// 以这个私有目录建一次调用的输入
    fn call(&self) -> FacadeCall {
        call_with(self.host())
    }
}

/// 在解释器里跑一段脚本并取出返回值
///
/// 解释器必须跑在没有 tokio 运行时的线程上
fn run<T>(call: &FacadeCall, source: &str) -> T
where
    T: for<'js> FromJs<'js> + Send + 'static,
{
    let call = call.clone_for_thread();
    let source = source.to_string();
    std::thread::spawn(move || {
        let rt = Runtime::new().expect("create runtime");
        let ctx = Context::full(&rt).expect("build context");
        ctx.with(|ctx| {
            let object = build_ctx(&ctx, &call).expect("assemble");
            ctx.globals().set("ctx", object).expect("attach");
            ctx.eval::<T, _>(source.as_str()).expect("evaluate")
        })
    })
    .join()
    .expect("the thread shuts down")
}

#[test]
fn fs_writes_and_reads_inside_the_private_directory() {
    let private = Private::new();
    let call = private.call();

    let read_back: String = run(
        &call,
        r#"ctx.fs.writeText("notes/a.txt", "hi");
           ctx.fs.readText("notes/a.txt")"#,
    );

    assert_eq!(read_back, "hi");
    assert_eq!(
        std::fs::read_to_string(private.root.join("notes").join("a.txt"))
            .expect("read the written file"),
        "hi"
    );
}

#[test]
fn fs_hands_the_relative_path_over_untouched() {
    let private = Private::new();
    let host = private.host();
    let call = call_with(host.clone());

    let _: String = run(
        &call,
        r#"ctx.fs.writeText("notes/a.txt", "hi");
           ctx.fs.readText("./notes/a.txt")"#,
    );

    assert_eq!(
        host.calls.lock().expect("lock the state").clone(),
        vec![
            String::from("writeText:notes/a.txt"),
            String::from("readText:./notes/a.txt"),
        ],
        "ctx neither rewrites the path nor judges the boundary itself: what reaches the host is exactly what was written"
    );
}

#[test]
fn fs_refuses_a_path_that_escapes_the_private_directory() {
    let private = Private::new();
    let call = private.call();
    let name: String = run(
        &call,
        r#"try { ctx.fs.readText("../../etc/passwd"); "no throw" }
           catch (error) { error.name }"#,
    );
    assert_eq!(name, "PathError");
}

#[test]
fn fs_refuses_a_write_that_lands_outside_the_private_directory() {
    let private = Private::new();
    let outside = private.base.path().join("outside.txt");
    std::fs::write(&outside, "untouched").expect("write the outside file");
    let call = private.call();

    let name: String = run(
        &call,
        r#"try { ctx.fs.writeText("../outside.txt", "owned"); "no throw" }
           catch (error) { error.name }"#,
    );

    assert_eq!(name, "PathError");
    assert_eq!(
        std::fs::read_to_string(&outside).expect("read the outside file"),
        "untouched",
        "a refused write must not touch files outside the boundary"
    );
}

#[test]
fn fs_refuses_a_removal_that_escapes_the_private_directory() {
    let private = Private::new();
    let outside = private.base.path().join("outside.txt");
    std::fs::write(&outside, "untouched").expect("write the outside file");
    let call = private.call();

    let name: String = run(
        &call,
        r#"try { ctx.fs.remove("../outside.txt"); "no throw" }
           catch (error) { error.name }"#,
    );

    assert_eq!(name, "PathError");
    assert!(
        outside.exists(),
        "a refused removal must not touch files outside the boundary"
    );
}

#[test]
fn fs_keeps_the_private_directory_in_a_read_only_property() {
    let private = Private::new();
    let call = private.call();

    let dir: String = run(&call, "ctx.fs.dir");
    assert_eq!(
        dir,
        private.root.to_string_lossy(),
        "the absolute path of the private directory is supplied by the host"
    );

    let name: String = run(
        &call,
        r#"try { ctx.fs.dir = "/tmp"; "no throw" } catch (error) { error.name }"#,
    );
    assert_eq!(
        name, "TypeError",
        "the private directory is a read-only property"
    );
}

#[test]
fn fs_lists_relative_paths_under_the_private_directory() {
    let private = Private::new();
    let call = private.call();

    let nested: Vec<String> = run(
        &call,
        r#"ctx.fs.writeText("notes/a.txt", "a");
           ctx.fs.writeText("notes/b.txt", "b");
           ctx.fs.list("notes")"#,
    );
    assert_eq!(nested, ["notes/a.txt", "notes/b.txt"]);

    let top: Vec<String> = run(&call, "ctx.fs.list()");
    assert_eq!(
        top,
        ["notes"],
        "omitting the argument lists the private directory itself"
    );

    let undefined: Vec<String> = run(&call, "ctx.fs.list(undefined)");
    assert_eq!(
        undefined,
        ["notes"],
        "explicitly passing undefined is equivalent to omitting it"
    );
}

#[test]
fn fs_removes_a_file_inside_the_private_directory() {
    let private = Private::new();
    let call = private.call();

    let name: String = run(
        &call,
        r#"ctx.fs.writeText("a.txt", "a");
           ctx.fs.remove("a.txt");
           try { ctx.fs.readText("a.txt"); "no throw" }
           catch (error) { error.name }"#,
    );

    assert_eq!(name, "FacadeError");
    assert!(
        !private.root.join("a.txt").exists(),
        "a deleted file must not remain in the private directory"
    );
}

#[test]
fn fs_exposes_a_directory_and_exactly_four_methods() {
    let private = Private::new();
    let call = private.call();
    let keys: Vec<String> = run(&call, "Object.getOwnPropertyNames(ctx.fs).sort()");
    assert_eq!(keys, ["dir", "list", "readText", "remove", "writeText"]);
}

#[test]
fn fs_refuses_a_call_that_leaves_out_a_required_path() {
    let private = Private::new();
    let call = private.call();
    let names: Vec<String> = run(
        &call,
        r#"["readText", "writeText", "remove"].map((method) => {
               try { ctx.fs[method](); return "no throw" }
               catch (error) { return error.name }
           })"#,
    );
    assert_eq!(
        names,
        ["TypeError", "TypeError", "TypeError"],
        "only the path of list is optional; the three methods must report a missing argument"
    );
}
