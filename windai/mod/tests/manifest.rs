//! 清单的形状与校验

use wind_event::def::{Directory, EventDef, Mount, Shape};
use wind_mod::manifest::{Delivery, Events, Manifest, ManifestError, Phase, parse};

#[test]
fn a_minimal_manifest_parses() {
    let raw = r#"{"name":"demo","version":1}"#;
    let manifest: Manifest = serde_json::from_str(raw).expect("parse the minimal manifest");
    assert_eq!(manifest.name, "demo");
    assert_eq!(manifest.version, 1);
    assert!(manifest.capabilities.is_empty());
    assert!(manifest.prompts.is_empty());
    assert!(manifest.mcp_servers.is_empty());
    assert!(manifest.requests.is_empty());
    assert_eq!(manifest.events, Events::default());
    assert!(manifest.defs.is_empty());
}

#[test]
fn the_wire_names_are_camel_case() {
    let raw = r#"{
        "name": "demo",
        "version": 1,
        "mcpServers": { "fetch": { "command": "uvx", "args": ["mcp-server-fetch"] } },
        "requests": { "top": { "handler": "top", "callableBy": ["agent"], "timeoutMs": 1500 } },
        "events": {
            "on": { "core/mcp.tool.call": { "handler": "guard", "phase": "guard" } },
            "emit": { "notify": { "delivery": "stateful", "stateKey": "items" } }
        }
    }"#;
    let manifest: Manifest = serde_json::from_str(raw).expect("valid");
    assert!(manifest.mcp_servers.contains_key("fetch"));
    assert_eq!(
        manifest.requests["top"].callable_by,
        vec![wind_mod::manifest::CallableBy::Agent]
    );
    assert_eq!(manifest.requests["top"].timeout_ms, Some(1500));
    assert_eq!(
        manifest.events.on["core/mcp.tool.call"].phase,
        Some(Phase::Guard)
    );
    assert_eq!(manifest.events.emit["notify"].delivery, Delivery::Stateful);
    assert_eq!(
        manifest.events.emit["notify"].state_key.as_deref(),
        Some("items")
    );
}

#[test]
fn an_unknown_top_level_field_is_rejected() {
    let raw = r#"{"name":"demo","version":1,"agents":{}}"#;
    let error = serde_json::from_str::<Manifest>(raw).expect_err("accept an unknown field");
    assert!(error.to_string().contains("agents"), "{error}");
}

/// 一份可被引用的目录，用来检验订阅未定义的事件
fn directory() -> Directory {
    fn schema() -> serde_json::Value {
        serde_json::json!({ "type": "object" })
    }
    const TOOL_CALL: EventDef<serde_json::Value> = EventDef::new(
        "core/mcp.tool.call",
        Shape::Intercept,
        true,
        Mount::Subsystem,
        schema,
    );
    const CRASH: EventDef<serde_json::Value> = EventDef::new(
        "core/mcp.crash",
        Shape::Observe,
        true,
        Mount::Subsystem,
        schema,
    );
    Directory::from_metas([TOOL_CALL.meta(), CRASH.meta()])
}

/// 二十三条反例，每条只违反一条约定
fn rejections() -> Vec<(&'static str, String)> {
    vec![
        ("missing name", r#"{"version":1}"#.into()),
        ("missing version", r#"{"name":"demo"}"#.into()),
        (
            "name starts with a digit",
            r#"{"name":"1demo","version":1}"#.into(),
        ),
        (
            "name first character is uppercase",
            r#"{"name":"Demo","version":1}"#.into(),
        ),
        (
            "name last character is uppercase",
            r#"{"name":"deMo","version":1}"#.into(),
        ),
        (
            "name exceeds the length limit",
            format!(r#"{{"name":"{}","version":1}}"#, "a".repeat(64)),
        ),
        ("version is zero", r#"{"name":"demo","version":0}"#.into()),
        (
            "description exceeds the length limit",
            format!(
                r#"{{"name":"demo","version":1,"description":"{}"}}"#,
                "x".repeat(501)
            ),
        ),
        (
            "extra top-level field",
            r#"{"name":"demo","version":1,"scope":"topic"}"#.into(),
        ),
        (
            "unknown capability value",
            r#"{"name":"demo","version":1,"capabilities":["network"]}"#.into(),
        ),
        (
            "duplicate capability",
            r#"{"name":"demo","version":1,"capabilities":["http","http"]}"#.into(),
        ),
        (
            "prompt missing content",
            r#"{"name":"demo","version":1,"prompts":{"a":{}}}"#.into(),
        ),
        (
            "server has both command and url",
            r#"{
            "name":"demo","version":1,
            "mcpServers":{"x":{"command":"uvx","url":"https://mcp.example.com/v1"}}
        }"#
            .into(),
        ),
        (
            "remote server has args",
            r#"{
            "name":"demo","version":1,
            "mcpServers":{"x":{"url":"https://mcp.example.com/v1","args":["--x"]}}
        }"#
            .into(),
        ),
        (
            "remote url is not a URI",
            r#"{
            "name":"demo","version":1,
            "mcpServers":{"x":{"url":"not a uri at all"}}
        }"#
            .into(),
        ),
        (
            "command is an empty string",
            r#"{"name":"demo","version":1,"mcpServers":{"x":{"command":""}}}"#.into(),
        ),
        (
            "server has neither command nor url",
            r#"{"name":"demo","version":1,"mcpServers":{"x":{}}}"#.into(),
        ),
        (
            "entry missing handler",
            r#"{
            "name":"demo","version":1,"requests":{"top":{"summary":"x"}}
        }"#
            .into(),
        ),
        (
            "handler name is not an identifier",
            r#"{
            "name":"demo","version":1,"requests":{"top":{"handler":"has-dash"}}
        }"#
            .into(),
        ),
        (
            "caller contains an unknown value",
            r#"{
            "name":"demo","version":1,
            "requests":{"top":{"handler":"top","callableBy":["mod"]}}
        }"#
            .into(),
        ),
        (
            "timeout exceeds the limit",
            r#"{
            "name":"demo","version":1,
            "requests":{"top":{"handler":"top","timeoutMs":300001}}
        }"#
            .into(),
        ),
        (
            "stateful delivery missing payload",
            r#"{
            "name":"demo","version":1,
            "events":{"emit":{"notify":{"delivery":"stateful","stateKey":"items"}}}
        }"#
            .into(),
        ),
        (
            "subscription timeout exceeds the limit",
            r#"{
            "name":"demo","version":1,
            "events":{"on":{"core/mcp.crash":{"handler":"h","timeoutMs":60001}}}
        }"#
            .into(),
        ),
    ]
}

#[test]
fn the_twenty_three_rejections_are_refused() {
    let directory = directory();
    for (label, raw) in rejections() {
        assert!(
            parse(&raw, &directory).is_err(),
            "the counterexample \"{label}\" must be refused"
        );
    }
}

/// 目录之外的约定，两条不属于形状但同样要拒绝
#[test]
fn the_two_directory_level_rejections_are_refused() {
    let directory = directory();
    let cases = [
        (
            "subscribed to a nonexistent event",
            r#"{"name":"demo","version":1,
                "events":{"on":{"core/mcp.restart":{"handler":"h"}}}}"#,
        ),
        (
            "an observe event declared a phase",
            r#"{"name":"demo","version":1,
                "events":{"on":{"core/mcp.crash":{"handler":"h","phase":"guard"}}}}"#,
        ),
    ];
    for (label, raw) in cases {
        assert!(
            parse(raw, &directory).is_err(),
            "the counterexample \"{label}\" must be refused"
        );
    }
}

/// 合批由宿主统一执行，清单不再提供开关
#[test]
fn batch_is_not_a_manifest_field() {
    let raw = r#"{"name":"demo","version":1,
        "events":{"on":{"core/mcp.crash":{"handler":"h","batch":true}}}}"#;
    let error: ManifestError = parse(raw, &directory()).expect_err("batch is no longer a field");
    assert!(
        error.to_string().contains("batch"),
        "the error names the unknown field: {error}"
    );
}

#[test]
fn the_error_names_the_offending_field() {
    let directory = directory();
    let error: ManifestError =
        parse(r#"{"name":"demo","version":0}"#, &directory).expect_err("must be rejected");
    let text = error.to_string();
    assert!(text.contains("version"), "{text}");
}

#[test]
fn an_invalid_name_is_refused_in_words() {
    let directory = directory();
    for raw in [
        r#"{"name":"1demo","version":1}"#,
        r#"{"name":"de mo","version":1}"#,
        r#"{"name":"demo","version":1,"prompts":{"a b":{}}}"#,
    ] {
        let error: ManifestError = parse(raw, &directory).expect_err("must be rejected");
        let text = error.to_string();
        assert!(
            !text.contains('^') && !text.contains("]{1,"),
            "the reason must be a single sentence, with the character set left in the comment: {text}"
        );
    }
}

#[test]
fn a_full_manifest_passes() {
    let raw = r#"{
        "name": "research-kit",
        "version": 1,
        "description": "researcher toolbox",
        "capabilities": ["state", "http"],
        "prompts": { "brief": { "content": "answers must cite sources" } },
        "mcpServers": {
            "fetch": { "command": "uvx", "args": ["mcp-server-fetch"] },
            "search": { "command": "./mcp/search/bin/server" },
            "remote": { "url": "https://mcp.example.com/v1" }
        },
        "requests": {
            "fetch": {
                "handler": "fetchTop",
                "summary": "fetch headlines",
                "callableBy": ["agent", "ui"],
                "timeoutMs": 15000
            }
        },
        "events": {
            "on": {
                "core/mcp.tool.call": { "handler": "guard", "phase": "guard", "timeoutMs": 50 },
                "core/mcp.crash": { "handler": "onCrash" }
            },
            "emit": {
                "notify": { "delivery": "stateful", "stateKey": "items", "payload": { "type": "array" } }
            }
        },
        "defs": { "StoryList": { "type": "array" } }
    }"#;
    let manifest = parse(raw, &directory()).expect("valid");
    assert_eq!(manifest.requests["fetch"].handler, "fetchTop");
}

#[test]
fn a_stateful_channel_without_a_key_is_refused() {
    let raw = r#"{"name":"demo","version":1,
        "events":{"emit":{"notify":{"delivery":"stateful","payload":{"type":"array"}}}}}"#;
    assert!(
        parse(raw, &directory()).is_err(),
        "stateful delivery must provide a key"
    );
}

/// 状态投递的存储键与订阅名同为事件的完整名字
#[test]
fn a_stateful_channel_is_kept_under_its_full_event_name() {
    let raw = r#"{"name":"demo","version":1,
        "capabilities":["state"],
        "events":{"emit":{
            "notify":{"delivery":"stateful","stateKey":"items","payload":{"type":"array"}},
            "ping":{"delivery":"notice","payload":{"type":"array"}}}}}"#;
    let manifest = parse(raw, &directory()).expect("valid");
    let channels: Vec<(String, String)> = manifest
        .stateful_channels()
        .map(|(name, key)| (name.to_string(), key))
        .collect();
    assert_eq!(
        channels,
        vec![("notify".to_string(), "m_demo/notify".to_string())],
        "the storage key matches the subscription name, and notice channels are not included"
    );
}

#[test]
fn an_on_timeout_field_is_refused() {
    let raw = r#"{"name":"demo","version":1,
        "events":{"on":{"core/mcp.crash":{"handler":"h","onTimeout":"deny"}}}}"#;
    let error: ManifestError = parse(raw, &directory()).expect_err("onTimeout was removed");
    assert!(error.to_string().contains("onTimeout"), "{error}");
}

/// 名字的下界是两个字符，单字符名字不合法
#[test]
fn a_two_character_name_is_accepted_and_a_single_character_one_is_not() {
    let directory = directory();
    let manifest =
        parse(r#"{"name":"ab","version":1}"#, &directory).expect("parse a two character name");
    assert_eq!(manifest.name, "ab");
    assert!(
        parse(r#"{"name":"a","version":1}"#, &directory).is_err(),
        "a single-character name must be refused"
    );
}

/// 五个能力取值的拼写能往返，`exec` 已不在能力集合内
#[test]
fn the_capability_values_round_trip() {
    let directory = directory();
    for spelling in ["state", "storage", "http", "agent", "fs"] {
        let raw = format!(r#"{{"name":"demo","version":1,"capabilities":["{spelling}"]}}"#);
        let manifest = parse(&raw, &directory).expect("parse the capability value");
        let wire =
            serde_json::to_value(&manifest.capabilities).expect("serialize the capabilities");
        assert_eq!(
            wire,
            serde_json::json!([spelling]),
            "the spelling of capability value {spelling} should round-trip"
        );
    }
    let raw = r#"{"name":"demo","version":1,"capabilities":["exec"]}"#;
    let error: ManifestError = parse(raw, &directory).expect_err("parse exec as a capability");
    assert!(error.to_string().contains("exec"), "{error}");
}

/// 订阅的时间预算用 camelCase 键名，写成下划线形式即为未知字段
#[test]
fn the_subscription_timeout_key_is_camel_case() {
    let directory = directory();
    let raw = r#"{"name":"demo","version":1,
        "events":{"on":{"core/mcp.tool.call":{"handler":"h","phase":"guard","timeoutMs":50}}}}"#;
    let manifest = parse(raw, &directory).expect("valid");
    let sub = &manifest.events.on["core/mcp.tool.call"];
    assert_eq!(sub.timeout_ms, Some(50));

    let raw = r#"{"name":"demo","version":1,
        "events":{"on":{"core/mcp.tool.call":{"handler":"h","phase":"guard","timeout_ms":50}}}}"#;
    let error: ManifestError = parse(raw, &directory).expect_err("accept a snake case key");
    assert!(error.to_string().contains("timeout_ms"), "{error}");
}

/// 一条状态键只对应一条通道
///
/// 两条通道声明同一个键时，调用边界写入的键无法判断该投给谁。投给其中一条会让
/// 另一条的订阅方永远停在旧值上，而两条都投会推出两条名字不同的帧
#[test]
fn two_stateful_channels_declaring_one_key_are_refused() {
    let raw = r#"{"name":"demo","version":1,
        "events":{"emit":{
            "notify":{"delivery":"stateful","stateKey":"items","payload":{"type":"array"}},
            "ping":{"delivery":"stateful","stateKey":"items","payload":{"type":"array"}}}}}"#;
    let error = parse(raw, &directory()).expect_err("declare one state key twice");
    assert!(error.to_string().contains("stateKey"), "{error}");
}

/// 同一个键出现在只推的通道里不算冲突，因为该通道没有留存的当前值
#[test]
fn a_notice_channel_does_not_collide_with_a_state_key() {
    let raw = r#"{"name":"demo","version":1,
        "events":{"emit":{
            "notify":{"delivery":"stateful","stateKey":"items","payload":{"type":"array"}},
            "ping":{"delivery":"notice","stateKey":"items","payload":{"type":"array"}}}}}"#;
    assert!(
        parse(raw, &directory()).is_ok(),
        "notice-only channels do not participate in state key mapping"
    );
}
