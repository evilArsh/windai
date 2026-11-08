//! MCP 配置、内建工具与完成事件的 schema

use schemars::JsonSchema;
use serde_json::{Map, Value, json};
use wind_wire::McpDefinition;

use crate::builtin::{
    ExecRequest, ExecResult, ListDirRequest, ListDirResult, ReadFileRequest, ReadFileResult,
    SkillsListRequest, SkillsListResult, WriteFileRequest, WriteFileResult,
};
use crate::events::{McpFailed, McpStarted, McpStopped};

const NAME_PATTERN: &str = "^[a-zA-Z_][a-zA-Z0-9_-]{0,62}$";
const COMMAND_PATTERN: &str = "^(?=.*\\S)[^\\x00]*$";
const NUL_FREE_PATTERN: &str = "^[^\\x00]*$";
const ENV_KEY_PATTERN: &str = "^[^=\\x00]+$";
const URL_PATTERN: &str = "^[hH][tT][tT][pP][sS]?://[^/\\s?#]+";

/// 导出独立配置、内建工具输入输出与完成事件的 schema
pub fn mcp_schema() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(McpDefinition))
        .expect("the derived mcp schema is always serializable");
    let object = schema
        .as_object_mut()
        .expect("the derived mcp schema is an object");
    object.insert("title".into(), json!("windai MCP 配置"));
    let defs = object
        .entry("$defs")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("the mcp definitions are an object");
    insert_definition::<ListDirRequest>(defs);
    insert_definition::<ListDirResult>(defs);
    insert_definition::<ReadFileRequest>(defs);
    insert_definition::<ReadFileResult>(defs);
    insert_definition::<WriteFileRequest>(defs);
    insert_definition::<WriteFileResult>(defs);
    insert_definition::<ExecRequest>(defs);
    insert_definition::<ExecResult>(defs);
    insert_definition::<SkillsListRequest>(defs);
    insert_definition::<SkillsListResult>(defs);
    insert_definition::<McpStarted>(defs);
    insert_definition::<McpStopped>(defs);
    insert_definition::<McpFailed>(defs);
    apply_constraints(&mut schema);
    schema
}

fn insert_definition<T: JsonSchema>(defs: &mut Map<String, Value>) {
    let value =
        serde_json::to_value(schemars::SchemaGenerator::default().into_root_schema_for::<T>())
            .expect("the derived definition schema is always serializable");
    let mut object = value
        .as_object()
        .expect("the derived definition schema is an object")
        .clone();
    object.remove("$schema");
    if let Some(Value::Object(nested)) = object.remove("$defs") {
        for (name, schema) in nested {
            insert_unique(defs, name, schema);
        }
    }
    insert_unique(defs, T::schema_name().into_owned(), Value::Object(object));
}

fn insert_unique(defs: &mut Map<String, Value>, name: String, schema: Value) {
    match defs.get(&name) {
        Some(existing) => assert_eq!(
            existing, &schema,
            "the definition {name} must have one shape"
        ),
        None => {
            defs.insert(name, schema);
        }
    }
}

fn apply_constraints(schema: &mut Value) {
    let object = schema.as_object_mut().expect("the mcp schema is an object");
    let name = object
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .and_then(|properties| properties.get_mut("name"))
        .expect("the mcp schema declares the name property");
    merge(
        name,
        json!({
            "minLength": 1,
            "maxLength": 63,
            "pattern": NAME_PATTERN,
        }),
    );

    let branches = object
        .get_mut("$defs")
        .and_then(Value::as_object_mut)
        .and_then(|defs| defs.get_mut("McpConfig"))
        .and_then(|config| config.get_mut("oneOf"))
        .and_then(Value::as_array_mut)
        .expect("the mcp config schema declares its branches");
    for branch in branches {
        let tag = branch
            .pointer("/properties/type/const")
            .and_then(Value::as_str)
            .map(str::to_string);
        let properties = branch
            .get_mut("properties")
            .and_then(Value::as_object_mut)
            .expect("a config branch declares its properties");
        match tag.as_deref() {
            Some("stdio") => {
                let command = properties
                    .get_mut("command")
                    .expect("a stdio branch declares the command");
                merge(
                    command,
                    json!({
                        "minLength": 1,
                        "pattern": COMMAND_PATTERN,
                    }),
                );
                let items = properties
                    .get_mut("args")
                    .and_then(|args| args.get_mut("items"))
                    .expect("a stdio branch declares the arguments");
                merge(items, json!({ "pattern": NUL_FREE_PATTERN }));
                let env = properties
                    .get_mut("env")
                    .expect("a stdio branch declares the environment");
                merge(
                    env,
                    json!({ "propertyNames": { "pattern": ENV_KEY_PATTERN } }),
                );
            }
            Some("streamable_http") => {
                let url = properties
                    .get_mut("url")
                    .expect("a remote branch declares the url");
                merge(
                    url,
                    json!({
                        "minLength": 8,
                        "pattern": URL_PATTERN,
                        "description": "HTTP 或 HTTPS 地址，连接前由配置校验完成完整解析",
                    }),
                );
            }
            other => panic!("unexpected mcp config branch: {other:?}"),
        }
    }
}

fn merge(target: &mut Value, patch: Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                match target.get_mut(&key) {
                    Some(existing) => merge(existing, value),
                    None => {
                        target.insert(key, value);
                    }
                }
            }
        }
        (target, patch) => *target = patch,
    }
}

#[cfg(test)]
mod tests {
    use super::mcp_schema;
    use crate::config::validate_definition;
    use jsonschema::Validator;
    use serde_json::{Value, json};
    use wind_wire::{McpConfig, McpDefinition};

    fn validator() -> Validator {
        jsonschema::draft202012::new(&mcp_schema()).expect("the mcp schema compiles")
    }

    fn assert_verdict(validator: &Validator, value: &Value, accepted: bool) {
        let runtime = serde_json::from_value::<McpDefinition>(value.clone())
            .is_ok_and(|definition| validate_definition(&definition).is_ok());
        assert_eq!(
            validator.is_valid(value),
            accepted,
            "unexpected schema verdict for {value}"
        );
        assert_eq!(runtime, accepted, "unexpected parser verdict for {value}");
    }

    fn stdio(name: &str, command: &str) -> Value {
        json!({
            "name": name,
            "config": {
                "type": "stdio",
                "command": command,
                "cwd": std::env::temp_dir().to_string_lossy(),
            },
        })
    }

    #[test]
    fn schema_requires_name_and_config() {
        let validator = validator();
        assert_verdict(&validator, &json!({}), false);
        assert_verdict(
            &validator,
            &json!({"config": {"type": "stdio", "command": "node", "cwd": "/tmp"}}),
            false,
        );
        assert_verdict(&validator, &json!({"name": "files"}), false);
        assert_verdict(&validator, &stdio("files", "node"), true);
    }

    #[test]
    fn omitted_optional_fields_take_defaults() {
        let validator = validator();
        let value = stdio("files", "node");
        assert_verdict(&validator, &value, true);
        let definition: McpDefinition =
            serde_json::from_value(value).expect("the definition must parse");
        assert_eq!(definition.description, None);
        match definition.config {
            McpConfig::Stdio { args, env, .. } => {
                assert!(args.is_empty(), "args default to an empty list");
                assert!(env.is_empty(), "env defaults to an empty map");
            }
            other => panic!("unexpected config: {other:?}"),
        }
    }

    #[test]
    fn schema_rejects_unknown_fields() {
        let validator = validator();
        let mut extra = stdio("files", "node");
        extra["extra"] = json!(true);
        assert_verdict(&validator, &extra, false);

        let mut branch = stdio("files", "node");
        branch["config"]["extra"] = json!(true);
        assert_verdict(&validator, &branch, false);

        let mut remote = json!({
            "name": "remote",
            "config": {"type": "streamable_http", "url": "https://example.com/mcp"},
        });
        remote["config"]["args"] = json!([]);
        assert_verdict(&validator, &remote, false);
    }

    #[test]
    fn schema_rejects_conflicting_or_missing_branches() {
        let validator = validator();
        assert_verdict(
            &validator,
            &json!({"name": "files", "config": {"command": "node", "cwd": "/tmp"}}),
            false,
        );
        assert_verdict(
            &validator,
            &json!({"name": "files", "config": {"type": "socket"}}),
            false,
        );
        assert_verdict(
            &validator,
            &json!({
                "name": "files",
                "config": {
                    "type": "stdio",
                    "command": "node",
                    "cwd": "/tmp",
                    "url": "https://example.com/mcp",
                },
            }),
            false,
        );
        assert_verdict(
            &validator,
            &json!({
                "name": "files",
                "config": {"type": "streamable_http", "url": "https://example.com/mcp", "cwd": "/tmp"},
            }),
            false,
        );
    }

    #[test]
    fn schema_enforces_names_and_connection_boundaries() {
        let validator = validator();
        assert_verdict(&validator, &stdio("a", "node"), true);
        assert_verdict(&validator, &stdio("_files-2", "node"), true);
        assert_verdict(&validator, &stdio(&"a".repeat(63), "node"), true);
        assert_verdict(&validator, &stdio("", "node"), false);
        assert_verdict(&validator, &stdio(&"a".repeat(64), "node"), false);
        assert_verdict(&validator, &stdio("1files", "node"), false);
        assert_verdict(&validator, &stdio("with space", "node"), false);
        assert_verdict(&validator, &stdio("files", ""), false);
        assert_verdict(&validator, &stdio("files", "   "), false);
        assert_verdict(&validator, &stdio("files", "no\u{0}de"), false);
        assert_verdict(
            &validator,
            &json!({
                "name": "remote",
                "config": {"type": "streamable_http", "url": "https://example.com/mcp"},
            }),
            true,
        );
        assert_verdict(
            &validator,
            &json!({
                "name": "remote",
                "config": {"type": "streamable_http", "url": "http://127.0.0.1:8080/mcp"},
            }),
            true,
        );
        assert_verdict(
            &validator,
            &json!({
                "name": "remote",
                "config": {"type": "streamable_http", "url": "ftp://example.com/mcp"},
            }),
            false,
        );
        assert_verdict(
            &validator,
            &json!({
                "name": "remote",
                "config": {"type": "streamable_http", "url": "https://"},
            }),
            false,
        );
    }

    fn def_validator(name: &str) -> Validator {
        let schema = mcp_schema();
        let root = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": schema["$defs"],
            "$ref": format!("#/$defs/{name}"),
        });
        jsonschema::draft202012::new(&root).expect("the definition schema compiles")
    }

    fn assert_def(name: &str, value: &Value, accepted: bool) {
        let validator = def_validator(name);
        assert_eq!(
            validator.is_valid(value),
            accepted,
            "unexpected verdict for {name} with {value}"
        );
    }

    #[test]
    fn schema_exports_builtin_tools_and_events() {
        let schema = mcp_schema();
        let defs = schema["$defs"]
            .as_object()
            .expect("the schema must have definitions");
        for name in [
            "McpConfig",
            "ListDirRequest",
            "ListDirResult",
            "DirEntry",
            "EntryKind",
            "ReadFileRequest",
            "ReadFileResult",
            "WriteFileRequest",
            "WriteFileResult",
            "ExecRequest",
            "ExecResult",
            "SkillsListRequest",
            "SkillsListResult",
            "SkillsMeta",
            "SkillError",
            "McpStarted",
            "McpStopped",
            "McpFailed",
        ] {
            assert!(defs.contains_key(name), "missing definition {name}");
        }

        assert_def("ListDirRequest", &json!({"path": "/tmp"}), true);
        assert_def(
            "ListDirRequest",
            &json!({"path": "/tmp", "recursive": false, "max_depth": 3}),
            true,
        );
        assert_def("ListDirRequest", &json!({}), false);
        assert_def("ListDirRequest", &json!({"path": 1}), false);

        assert_def(
            "ListDirResult",
            &json!({
                "path": "/tmp",
                "entries": [{"name": "a.txt", "kind": "file", "size": 1, "mtime": 2}],
                "recursive": true,
                "truncated": false,
                "total": 1,
            }),
            true,
        );
        assert_def(
            "ListDirResult",
            &json!({"path": "/tmp", "entries": [], "recursive": false}),
            false,
        );
        assert_def("DirEntry", &json!({"name": "a", "kind": "symlink"}), true);
        assert_def("DirEntry", &json!({"name": "a", "kind": "other"}), false);

        assert_def(
            "ReadFileRequest",
            &json!({"path": "/tmp/a", "offset": 2}),
            true,
        );
        assert_def(
            "ReadFileRequest",
            &json!({"path": "/tmp/a", "offset": -1}),
            false,
        );
        assert_def(
            "ReadFileResult",
            &json!({
                "path": "/tmp/a",
                "content": "x",
                "offset": 0,
                "limit": 1,
                "truncated": false,
                "bytes": 1,
            }),
            true,
        );

        assert_def(
            "WriteFileRequest",
            &json!({"path": "/tmp/a", "data": "x"}),
            true,
        );
        assert_def("WriteFileRequest", &json!({"path": "/tmp/a"}), false);
        assert_def(
            "WriteFileResult",
            &json!({"path": "/tmp/a", "bytes": 1}),
            true,
        );

        assert_def(
            "ExecRequest",
            &json!({"command": "ls", "cwd": "/tmp", "timeout": 1000}),
            true,
        );
        assert_def(
            "ExecRequest",
            &json!({"command": "ls", "cwd": "/tmp", "timeout": 1000, "args": ["-l"]}),
            true,
        );
        assert_def(
            "ExecRequest",
            &json!({"command": "ls", "cwd": "/tmp"}),
            false,
        );
        assert_def(
            "ExecResult",
            &json!({
                "stdout": "",
                "stderr": "",
                "timed_out": false,
                "cmd": "ls",
                "cwd": "/tmp",
                "truncated_out": false,
                "truncated_err": false,
            }),
            true,
        );

        assert_def("SkillsListRequest", &json!({"dir": "/tmp"}), true);
        assert_def("SkillsListRequest", &json!({}), false);
        assert_def(
            "SkillsListResult",
            &json!({
                "skills": [{
                    "source": "workspace",
                    "skill_dir": "/tmp/skills/a",
                    "name": "a",
                    "description": "the a skill",
                }],
                "errors": [],
            }),
            true,
        );
        assert_def("SkillsListResult", &json!({"skills": []}), false);
        assert_def(
            "SkillsMeta",
            &json!({"source": "workspace", "skill_dir": "/tmp", "name": "a", "description": "a"}),
            true,
        );
        assert_def("SkillError", &json!({"path": "/tmp", "error": "bad"}), true);

        assert_def(
            "McpStarted",
            &json!({"topic_id": 1, "source": "mods", "key": "k"}),
            true,
        );
        assert_def(
            "McpStarted",
            &json!({"topic_id": 1, "source": "mods"}),
            false,
        );
        assert_def(
            "McpStopped",
            &json!({"topic_id": 1, "source": "configured", "key": "1"}),
            true,
        );
        assert_def(
            "McpFailed",
            &json!({"topic_id": 1, "source": "mods", "key": "k", "error": "boom"}),
            true,
        );
        assert_def(
            "McpFailed",
            &json!({"topic_id": 1, "source": "mods", "key": "k"}),
            false,
        );
    }
}
