//! 话题启用过的 mod 对请求链的三个投影

use tempfile::TempDir;
use wind_core::db::Db;
use wind_core::llm::ports::{Tools, TopicMods};
use wind_core::storage::Storage;
use wind_event::def::Directory;
use wind_mod::catalogue::Catalogue;
use wind_mod::manifest::{self, Manifest};

/// 一个建好表、落在临时目录里的空库
async fn store() -> (TempDir, Storage) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("t.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    (dir, Storage::new(db))
}

/// 安装路径使用的目录
///
/// 这里手工拼出与安装时交给 `Installer` 的总线目录相同的合并，次序也相同。只含
/// 六条 mod 生命周期事件的窄目录解不开清单里其他的订阅名，用这种目录读会让合法
/// mod 的清单解析失败。
fn directory() -> Directory {
    wind_core::usecase::directory()
        .merge(wind_core::agent::events::directory())
        .merge(wind_core::conversation::directory())
        .merge(wind_mod::events::directory())
}

/// 解析一份清单
fn manifest(raw: &str) -> Manifest {
    manifest::parse(raw, &directory()).expect("the manifest is valid")
}

/// 一个 mod，一个 request 交给 agent，另一个只交给界面
///
/// 清单里除了 mod 生命周期事件还有别的订阅，读侧的目录必须解得开
const TWO_REQUESTS: &str = r#"{"name":"alpha","version":1,
    "requests":{
        "fetch":{"handler":"fetchTop","summary":"fetch once","input":{"type":"object"},
                 "callableBy":["agent"]},
        "preview":{"handler":"previewList","callableBy":["ui"]}},
    "events":{"on":{"core/mod.install":{"handler":"onInstall"}}}}"#;

#[test]
fn the_declared_fragments_come_in_a_stable_order() {
    // 键有序，因此同一份清单每次给出同样的顺序
    let one = manifest(
        r#"{"name":"alpha","version":1,"prompts":{
        "b":{"content":"beta"},"a":{"content":"alpha"}}}"#,
    );
    let fragments = wind_mod::prompt::declared_fragments(&[one]);
    assert_eq!(fragments, vec!["alpha".to_string(), "beta".to_string()]);
}

#[test]
fn a_mod_without_prompts_contributes_nothing() {
    let one = manifest(r#"{"name":"alpha","version":1}"#);
    assert!(wind_mod::prompt::declared_fragments(&[one]).is_empty());
}

#[tokio::test]
async fn only_the_requests_marked_agent_callable_become_tools() {
    let (_dir, storage) = store().await;
    // 装一个 mod，其中一个 request 声明 callableBy: ["agent"]，另一个声明 ["ui"]
    let id = storage
        .mod_install("alpha", TWO_REQUESTS, None)
        .await
        .expect("install");
    storage.mod_enable(1, id).await.expect("enable");
    let catalogue = Catalogue::new(storage, directory());
    let tools = catalogue.agent_tools(1).await.expect("read the manifest");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].origin.exposed(), "m_alpha__fetch");
    match &tools[0].declaration {
        Tools::Function(function) => {
            assert_eq!(
                function.name, "m_alpha__fetch",
                "the declared name is the same as the name the source provides"
            );
            assert_eq!(
                function.description.as_deref(),
                Some("fetch once"),
                "the one-line summary of a request is the tool description"
            );
            assert_eq!(
                function.parameters,
                Some(serde_json::json!({ "type": "object" })),
                "the input schema declared by a request is the tool parameter schema"
            );
        }
    }
    assert_eq!(
        catalogue
            .version_of("alpha")
            .await
            .expect("read the current version"),
        1
    );
    assert!(
        catalogue.version_of("absent").await.is_err(),
        "an absent mod must report an error: falling back to a wrong version number would silently run a different piece of code"
    );
}

#[tokio::test]
async fn a_fragment_or_tool_read_that_fails_is_reported_rather_than_emptied() {
    // 读不到清单时不能静默返回空。那会让 mod 的提示词与工具凭空消失
    let (_dir, storage) = store().await;
    let catalogue = Catalogue::new(storage.clone(), directory());
    // 让库不可用（关掉底层池）之后读，必须报错
    storage.pool().close().await;
    assert!(catalogue.prompt_fragments(1).await.is_err());
    assert!(catalogue.agent_tools(1).await.is_err());
}
