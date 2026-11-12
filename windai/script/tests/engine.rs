use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wind_script::{
    Config, ScriptCall, ScriptHost, ScriptScope, ScriptService, ScriptsError, quickjs,
};

fn service() -> Arc<dyn ScriptService> {
    quickjs(Config::default()).expect("the engine must start")
}

fn write_entry(root: &Path, source: &str) -> PathBuf {
    let path = root.join("scripts/index.js");
    std::fs::create_dir_all(path.parent().expect("entry parent")).expect("scripts dir");
    std::fs::write(&path, source).expect("entry source");
    root.to_path_buf()
}

#[test]
fn factory_returns_the_script_service_trait() {
    let _: Arc<dyn ScriptService> = service();
}

struct NoHost;

impl ScriptHost for NoHost {
    fn emit(&self, _channel: &str, _payload: Value) -> Result<(), ScriptsError> {
        Ok(())
    }
    fn notify(&self, _level: &str, _text: &str) -> Result<(), ScriptsError> {
        Ok(())
    }
}

fn call(root: PathBuf, handler: &str, input: Value) -> ScriptCall {
    ScriptCall {
        mod_name: "demo".to_string(),
        mod_version: 3,
        scope: ScriptScope::Global,
        root,
        handler: handler.to_string(),
        deadline: Instant::now() + Duration::from_secs(5),
        cancel: CancellationToken::new(),
        input,
        host: Arc::new(NoHost),
    }
}

#[tokio::test]
async fn call_returns_the_serialized_value() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input) { return { ok: input.n + 1 }; }",
    );
    let result = service()
        .call(call(root, "onEvent", json!({ "n": 2 })))
        .await
        .expect("the handler must run");
    assert_eq!(result, Some(json!({ "ok": 3 })));
}

#[tokio::test]
async fn undefined_and_null_return_none() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input) { return input.what; }",
    );
    assert_eq!(
        service()
            .call(call(root.clone(), "onEvent", json!({ "what": null })))
            .await
            .expect("null is accepted"),
        None
    );
    assert_eq!(
        service()
            .call(call(root, "onEvent", json!({})))
            .await
            .expect("undefined is accepted"),
        None
    );
}

#[tokio::test]
async fn missing_handler_is_a_handler_error() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function other() {}");
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("the handler is missing");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[tokio::test]
async fn a_non_function_export_is_a_handler_error() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export const onEvent = 1;");
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("the export is not callable");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[tokio::test]
async fn a_thrown_handler_is_a_handler_error() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent() { throw new Error('boom'); }",
    );
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("the handler throws");
    assert!(matches!(error, ScriptsError::Handler(_)));
    assert!(error.to_string().contains("boom"));
}

#[tokio::test]
async fn imports_resolve_relative_to_scripts() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("scripts/lib")).expect("scripts dir");
    std::fs::write(
        root.join("scripts/index.js"),
        "import { value } from './bar.js';\nexport function onEvent() { return value; }",
    )
    .expect("entry");
    std::fs::write(
        root.join("scripts/bar.js"),
        "import { n } from './lib/util.js';\nexport const value = { n };",
    )
    .expect("bar");
    std::fs::write(root.join("scripts/lib/util.js"), "export const n = 7;").expect("util");
    let result = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect("imports must resolve");
    assert_eq!(result, Some(json!({ "n": 7 })));
}

#[tokio::test]
async fn parent_specifiers_stay_inside_scripts() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("scripts/lib")).expect("scripts dir");
    std::fs::write(
        root.join("scripts/index.js"),
        "import { n } from './lib/../bar.js';\nexport function onEvent() { return n; }",
    )
    .expect("entry");
    std::fs::write(root.join("scripts/bar.js"), "export const n = 4;").expect("bar");
    let result = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect("a parent step inside scripts is legal");
    assert_eq!(result, Some(json!(4)));
}

#[tokio::test]
async fn relative_specifiers_must_end_with_js() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "import './bar';\nexport function onEvent() { return 1; }",
    );
    std::fs::write(dir.path().join("scripts/bar.js"), "export const n = 1;").expect("bar");
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("an extensionless specifier is refused");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[tokio::test]
async fn bare_absolute_and_escaping_specifiers_are_refused() {
    for specifier in ["\"lodash\"", "\"/scripts/bar.js\"", "\"../escape.js\""] {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().to_path_buf();
        std::fs::write(root.join("escape.js"), "export const n = 1;").expect("outside");
        let source =
            format!("import {{ n }} from {specifier};\nexport function onEvent() {{ return n; }}");
        write_entry(&root, &source);
        let error = service()
            .call(call(root, "onEvent", json!({})))
            .await
            .expect_err("the specifier must be refused");
        assert!(matches!(error, ScriptsError::Handler(_)), "{specifier}");
    }
}

#[tokio::test]
async fn missing_imports_are_handler_errors() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "import './missing.js';\nexport function onEvent() { return 1; }",
    );
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("the import target is missing");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[tokio::test]
async fn imported_module_syntax_errors_are_handler_errors() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "import './bad.js';\nexport function onEvent() { return 1; }",
    );
    std::fs::write(dir.path().join("scripts/bad.js"), "export const n = ;").expect("bad module");
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("the imported module does not compile");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_escapes_are_refused() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("scripts")).expect("scripts dir");
    std::fs::write(root.join("escape.js"), "export const n = 1;").expect("outside");
    symlink(root.join("escape.js"), root.join("scripts/link.js")).expect("symlink");
    std::fs::write(
        root.join("scripts/index.js"),
        "import { n } from './link.js';\nexport function onEvent() { return n; }",
    )
    .expect("entry");
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("the symlink leaves scripts");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_scripts_directory_is_refused() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().expect("temp dir");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&outside).expect("outside dir");
    std::fs::write(
        outside.join("index.js"),
        "import { n } from './secret.js';\nexport function onEvent() { return n; }",
    )
    .expect("entry");
    std::fs::write(outside.join("secret.js"), "export const n = 42;").expect("secret");
    let root = dir.path().join("mod");
    std::fs::create_dir_all(&root).expect("mod root");
    symlink(&outside, root.join("scripts")).expect("scripts symlink");
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("a symlinked scripts directory leaves the package");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[tokio::test]
async fn ctx_exposes_identity_and_scope() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { return { name: ctx.mod.name, version: ctx.mod.version, kind: ctx.scope.kind, topic_id: ctx.scope.topic_id, instance_id: ctx.scope.instance_id }; }",
    );
    let mut request = call(root, "onEvent", json!({}));
    request.scope = ScriptScope::Topic {
        topic_id: 7,
        instance_id: 9,
    };
    let result = service()
        .call(request)
        .await
        .expect("the context must be built");
    assert_eq!(
        result,
        Some(json!({
            "name": "demo",
            "version": 3,
            "kind": "topic",
            "topic_id": 7,
            "instance_id": 9,
        }))
    );
}

#[derive(Default)]
struct RecordingHost {
    emits: Mutex<Vec<(String, Value)>>,
    notifies: Mutex<Vec<(String, String)>>,
}

impl ScriptHost for RecordingHost {
    fn emit(&self, channel: &str, payload: Value) -> Result<(), ScriptsError> {
        self.emits
            .lock()
            .expect("emit log")
            .push((channel.to_string(), payload));
        Ok(())
    }
    fn notify(&self, level: &str, text: &str) -> Result<(), ScriptsError> {
        self.notifies
            .lock()
            .expect("notify log")
            .push((level.to_string(), text.to_string()));
        Ok(())
    }
}

fn host_call(root: PathBuf, handler: &str, host: Arc<RecordingHost>) -> ScriptCall {
    ScriptCall {
        host,
        ..call(root, handler, json!({}))
    }
}

#[tokio::test]
async fn ctx_emit_reaches_the_host() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { ctx.emit('state', { n: 1 }); return null; }",
    );
    let host = Arc::new(RecordingHost::default());
    service()
        .call(host_call(root, "onEvent", Arc::clone(&host)))
        .await
        .expect("the handler must run");
    assert_eq!(
        *host.emits.lock().expect("emit log"),
        vec![("state".to_string(), json!({ "n": 1 }))]
    );
}

#[tokio::test]
async fn ctx_notify_uses_the_default_and_given_level() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { ctx.notify({ text: 'hello' }); ctx.notify({ text: 'careful', level: 'warn' }); }",
    );
    let host = Arc::new(RecordingHost::default());
    service()
        .call(host_call(root, "onEvent", Arc::clone(&host)))
        .await
        .expect("the handler must run");
    assert_eq!(
        *host.notifies.lock().expect("notify log"),
        vec![
            ("info".to_string(), "hello".to_string()),
            ("warn".to_string(), "careful".to_string()),
        ]
    );
}

#[tokio::test]
async fn ctx_notify_requires_text_and_a_known_level() {
    for source in [
        "export function onEvent(input, ctx) { ctx.notify({}); }",
        "export function onEvent(input, ctx) { ctx.notify({ text: 'x', level: 'trace' }); }",
    ] {
        let dir = TempDir::new().expect("temp dir");
        let root = write_entry(dir.path(), source);
        let host = Arc::new(RecordingHost::default());
        let error = service()
            .call(host_call(root, "onEvent", host))
            .await
            .expect_err("the notify call is invalid");
        assert!(matches!(error, ScriptsError::Handler(_)));
    }
}

#[tokio::test]
async fn ctx_log_accepts_known_levels_and_rejects_others() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { ctx.log('debug', 'a'); ctx.log('info', 'b'); ctx.log('warn', 'c'); ctx.log('error', 'd'); }",
    );
    service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect("known levels are accepted");

    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { ctx.log('trace', 'a'); }",
    );
    let error = service()
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("trace is not a known level");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[tokio::test]
async fn a_non_serializable_emit_payload_is_a_handler_error() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { ctx.emit('state', () => {}); }",
    );
    let host = Arc::new(RecordingHost::default());
    let error = service()
        .call(host_call(root, "onEvent", host))
        .await
        .expect_err("a function is not a payload");
    assert!(matches!(error, ScriptsError::Handler(_)));
}

#[tokio::test]
async fn oversized_emit_and_notify_payloads_are_refused() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { ctx.emit('state', 'x'.repeat(70000)); }",
    );
    let host = Arc::new(RecordingHost::default());
    let error = service()
        .call(host_call(root, "onEvent", host))
        .await
        .expect_err("the emit payload is too large");
    assert!(matches!(
        error,
        ScriptsError::PayloadTooLarge { limit: 65536 }
    ));

    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(
        dir.path(),
        "export function onEvent(input, ctx) { ctx.notify({ text: 'x'.repeat(70000) }); }",
    );
    let host = Arc::new(RecordingHost::default());
    let error = service()
        .call(host_call(root, "onEvent", host))
        .await
        .expect_err("the notify text is too large");
    assert!(matches!(
        error,
        ScriptsError::PayloadTooLarge { limit: 65536 }
    ));
}

#[tokio::test]
async fn a_timeout_returns_timeout() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { while (true) {} }");
    let mut request = call(root, "onEvent", json!({}));
    request.deadline = Instant::now() + Duration::from_millis(100);
    let error = service()
        .call(request)
        .await
        .expect_err("the handler must time out");
    assert!(matches!(error, ScriptsError::Timeout));
}

#[tokio::test]
async fn a_cancelled_call_is_internal() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { while (true) {} }");
    let request = call(root, "onEvent", json!({}));
    request.cancel.cancel();
    let error = service()
        .call(request)
        .await
        .expect_err("the call must be cancelled");
    assert!(matches!(error, ScriptsError::Internal(_)));
}

#[tokio::test]
async fn a_busy_pool_returns_busy() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { while (true) {} }");
    let service = quickjs(Config {
        threads: 1,
        ..Config::default()
    })
    .expect("single-threaded engine");
    let mut first = call(root.clone(), "onEvent", json!({}));
    first.deadline = Instant::now() + Duration::from_millis(300);
    let running = tokio::spawn({
        let service = Arc::clone(&service);
        async move { service.call(first).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut second = call(root, "onEvent", json!({}));
    second.deadline = Instant::now() + Duration::from_millis(100);
    let error = service
        .call(second)
        .await
        .expect_err("the only thread is busy");
    assert!(matches!(error, ScriptsError::Busy));
    let _ = running.await.expect("the first call must finish");
}

#[tokio::test]
async fn three_consecutive_timeouts_suspend_the_mod() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { while (true) {} }");
    let service = service();
    for _ in 0..4 {
        let mut request = call(root.clone(), "onEvent", json!({}));
        request.deadline = Instant::now() + Duration::from_millis(50);
        let error = service
            .call(request)
            .await
            .expect_err("the handler times out");
        assert!(matches!(error, ScriptsError::Timeout));
    }
    let error = service
        .call(call(root, "onEvent", json!({})))
        .await
        .expect_err("the mod is suspended");
    assert!(matches!(error, ScriptsError::Suspended));
}

#[tokio::test]
async fn a_handler_error_resets_the_timeout_streak() {
    let dir = TempDir::new().expect("temp dir");
    let slow = write_entry(dir.path(), "export function onEvent() { while (true) {} }");
    let failing_dir = TempDir::new().expect("temp dir");
    let failing = write_entry(
        failing_dir.path(),
        "export function onEvent() { throw new Error('boom'); }",
    );
    let fast_dir = TempDir::new().expect("temp dir");
    let fast = write_entry(fast_dir.path(), "export function onEvent() { return 1; }");
    let service = service();
    for _ in 0..2 {
        let mut request = call(slow.clone(), "onEvent", json!({}));
        request.deadline = Instant::now() + Duration::from_millis(50);
        assert!(matches!(
            service.call(request).await,
            Err(ScriptsError::Timeout)
        ));
    }
    assert!(matches!(
        service.call(call(failing, "onEvent", json!({}))).await,
        Err(ScriptsError::Handler(_))
    ));
    for _ in 0..2 {
        let mut request = call(slow.clone(), "onEvent", json!({}));
        request.deadline = Instant::now() + Duration::from_millis(50);
        assert!(matches!(
            service.call(request).await,
            Err(ScriptsError::Timeout)
        ));
    }
    service
        .call(call(fast, "onEvent", json!({})))
        .await
        .expect("the streak was reset");
}

fn artifacts(root: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(root.join("demo").join(wind_script::engine_id()))
        .expect("artifact dir")
        .map(|entry| entry.expect("entry").path())
        .collect()
}

#[tokio::test]
async fn a_second_call_does_not_recompile() {
    let disk = TempDir::new().expect("temp dir");
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { return 1; }");
    let service = quickjs(Config {
        cache: Some(disk.path().into()),
        ..Config::default()
    })
    .expect("engine");
    assert_eq!(
        service
            .call(call(root.clone(), "onEvent", json!({})))
            .await
            .expect("first"),
        Some(json!(1))
    );
    assert_eq!(artifacts(disk.path()).len(), 1);
    std::fs::remove_dir_all(root.join("scripts")).expect("remove source");
    assert_eq!(
        service
            .call(call(root, "onEvent", json!({})))
            .await
            .expect("second"),
        Some(json!(1))
    );
}

#[tokio::test]
async fn damaged_images_fall_back_to_source() {
    for damage in [vec![], vec![1, 2, 3], b"not bytecode".to_vec()] {
        let disk = TempDir::new().expect("temp dir");
        let dir = TempDir::new().expect("temp dir");
        let root = write_entry(dir.path(), "export function onEvent() { return 7; }");
        let config = Config {
            cache: Some(disk.path().into()),
            ..Config::default()
        };
        quickjs(config.clone())
            .expect("engine")
            .call(call(root.clone(), "onEvent", json!({})))
            .await
            .expect("first");
        let artifact = artifacts(disk.path()).pop().expect("artifact");
        std::fs::write(&artifact, damage).expect("damage");
        let service = quickjs(config).expect("restart");
        assert_eq!(
            service
                .call(call(root.clone(), "onEvent", json!({})))
                .await
                .expect("fallback"),
            Some(json!(7))
        );
        assert!(!artifact.exists());
        std::fs::remove_file(root.join("scripts/index.js")).expect("remove source");
        assert_eq!(
            service
                .call(call(root, "onEvent", json!({})))
                .await
                .expect("memory source"),
            Some(json!(7))
        );
    }
}

#[tokio::test]
async fn an_unwritable_cache_dir_still_serves_the_call() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { return 1; }");
    let service = quickjs(Config {
        cache: Some(root.join("scripts/index.js")),
        ..Config::default()
    })
    .expect("engine");
    assert_eq!(
        service
            .call(call(root, "onEvent", json!({})))
            .await
            .expect("call"),
        Some(json!(1))
    );
}

#[tokio::test]
async fn editing_the_source_does_not_change_behavior_until_restart() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { return 1; }");
    let service = service();
    assert_eq!(
        service
            .call(call(root.clone(), "onEvent", json!({})))
            .await
            .expect("first"),
        Some(json!(1))
    );
    write_entry(&root, "export function onEvent() { return 2; }");
    assert_eq!(
        service
            .call(call(root.clone(), "onEvent", json!({})))
            .await
            .expect("cached"),
        Some(json!(1))
    );
    assert_eq!(
        quickjs(Config::default())
            .expect("restart")
            .call(call(root, "onEvent", json!({})))
            .await
            .expect("new"),
        Some(json!(2))
    );
}

#[tokio::test]
async fn a_new_version_root_picks_up_the_new_source() {
    let dir = TempDir::new().expect("temp dir");
    let service = service();
    for version in [1, 2] {
        let root = write_entry(
            &dir.path().join(version.to_string()),
            &format!("export function onEvent() {{ return {version}; }}"),
        );
        assert_eq!(
            service
                .call(call(root, "onEvent", json!({})))
                .await
                .expect("version"),
            Some(json!(version))
        );
    }
}

#[tokio::test]
async fn changing_one_module_does_not_recompile_the_others() {
    let dir = TempDir::new().expect("temp dir");
    let disk = TempDir::new().expect("temp dir");
    let config = Config {
        cache: Some(disk.path().into()),
        ..Config::default()
    };
    let service = quickjs(config.clone()).expect("engine");
    let source = "import { value } from './lib.js'; export function onEvent() { return value; }";
    let first = write_entry(&dir.path().join("1"), source);
    std::fs::write(first.join("scripts/lib.js"), "export const value = 1;").expect("lib");
    assert_eq!(
        service
            .call(call(first, "onEvent", json!({})))
            .await
            .expect("first"),
        Some(json!(1))
    );
    let before: Vec<_> = artifacts(disk.path())
        .into_iter()
        .map(|p| {
            let time = std::fs::metadata(&p)
                .expect("meta")
                .modified()
                .expect("mtime");
            (p, time)
        })
        .collect();
    assert_eq!(before.len(), 2);
    let second = write_entry(&dir.path().join("2"), source);
    std::fs::write(second.join("scripts/lib.js"), "export const value = 2;").expect("lib");
    assert_eq!(
        service
            .call(call(second.clone(), "onEvent", json!({})))
            .await
            .expect("second"),
        Some(json!(2))
    );
    assert_eq!(artifacts(disk.path()).len(), 3);
    for (path, time) in before {
        assert_eq!(
            std::fs::metadata(path)
                .expect("meta")
                .modified()
                .expect("mtime"),
            time
        );
    }
    assert_eq!(
        quickjs(config)
            .expect("restart")
            .call(call(second, "onEvent", json!({})))
            .await
            .expect("disk imports"),
        Some(json!(2))
    );
}

#[tokio::test]
async fn an_entry_syntax_error_is_a_handler_error() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent( {");
    assert!(matches!(
        service().call(call(root, "onEvent", json!({}))).await,
        Err(ScriptsError::Handler(_))
    ));
}

#[tokio::test]
async fn a_cold_call_does_not_count_toward_the_breaker() {
    let dir = TempDir::new().expect("temp dir");
    let root = write_entry(dir.path(), "export function onEvent() { while (true) {} }");
    let service = service();
    for _ in 0..4 {
        let mut request = call(root.clone(), "onEvent", json!({}));
        request.deadline = Instant::now() + Duration::from_millis(100);
        assert!(matches!(
            service.call(request).await,
            Err(ScriptsError::Timeout)
        ));
    }
    assert!(matches!(
        service.call(call(root, "onEvent", json!({}))).await,
        Err(ScriptsError::Suspended)
    ));
}
