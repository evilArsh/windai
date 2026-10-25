//! 组合根：按序建立部件、接上接口，事件流只有一份

use std::sync::Arc;
use std::time::Duration;
use wind_event::context::DispatchCx;
use wind_event::envelope::{Origin, Scope, Source};
use wind_mod::dirs::AppDirs;
use wind_mod::events::{CORE_MOD_DELETED, ModDeleted};
use wind_mod::installer::NoServers;
use wind_runtime::Assembly;
use wind_script::assembly::ScriptCompiler;

/// 一个作用在话题 1 上的顶层操作上下文
fn cx() -> DispatchCx {
    DispatchCx::root(
        Origin::Agent,
        Scope::Topic { topic_id: 1 },
        Source::core("mod"),
        Duration::from_millis(50),
    )
}

/// 派发一条会写入日志的事件，并把日志写入磁盘
async fn log_one_event(assembly: &Assembly) {
    assembly
        .bus()
        .notify(
            &CORE_MOD_DELETED,
            &cx(),
            ModDeleted {
                name: "demo".to_string(),
                version: 1,
            },
        )
        .await;
    assembly.hub().flush().await.expect("write the state");
}

#[tokio::test]
async fn a_bare_deployment_builds_without_a_script_runtime() {
    // 不接脚本运行时的装配同样合法，宿主用 `NoHost`，编译器用 `NoCompiler`
    let tmp = tempfile::tempdir().expect("create temp dir");
    let assembly = Assembly::open(AppDirs::under(tmp.path()), None, None, None)
        .await
        .expect("assemble");
    assert_eq!(
        assembly.bus().directory().len(),
        wind_runtime::directory::merged().len(),
        "the bus must receive the merged set of eight"
    );
}

#[tokio::test]
async fn a_full_deployment_builds_with_a_script_runtime() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let assembly = Assembly::open(
        AppDirs::under(tmp.path()),
        Some(Arc::new(NoServers)),
        None,
        Some(Arc::new(ScriptCompiler)),
    )
    .await
    .expect("assemble");
    assert!(
        assembly
            .installer()
            .get("demo")
            .await
            .expect("query")
            .is_none(),
        "the installer must already be wired to storage"
    );
    assert_eq!(
        assembly.bus().directory().len(),
        wind_runtime::directory::merged().len(),
        "the bus must receive the merged set of eight"
    );
}

#[tokio::test]
async fn a_restart_continues_the_sequence_the_log_left_off() {
    // 不以日志水位作起点时，重启会把序号从零重新发一遍，与库里已有的日志行重号
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    let first = Assembly::open(dirs.clone(), None, None, None)
        .await
        .expect("assemble");
    log_one_event(&first).await;
    let watermark = first
        .storage()
        .event_log_max_seq()
        .await
        .expect("read the max log sequence");
    assert!(
        watermark > 0,
        "the dispatched entry must already be in the log"
    );
    let high = first.hub().high_water();
    drop(first);

    let again = Assembly::open(dirs, None, None, None)
        .await
        .expect("assemble");
    assert!(
        again.bus().current_seq().0 >= watermark,
        "after a restart the bus-issued sequence must continue from the numbers already in the database, actual {}",
        again.bus().current_seq()
    );
    assert!(
        again.hub().high_water() >= high,
        "the frame facility watermark must likewise continue from where it was before the restart"
    );
}
