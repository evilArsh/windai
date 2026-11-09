use std::fs;
use std::path::{Path, PathBuf};

use jsonschema::Validator;
use serde_json::{Value, json};
use tempfile::TempDir;
use wind_event::{Directory, EventMeta, Shape};
use wind_mod::Resources;
use wind_mod::dirs::AppDirs;
use wind_mod::error::ModError;
use wind_mod::manifest::Manifest;
use wind_mod::package::Package;
use wind_storage::Storage;
use wind_wire::McpConfig;

fn validator() -> Validator {
    jsonschema::draft202012::new(&wind_mod::manifest_schema())
        .expect("the manifest schema compiles")
}

fn assert_verdict(validator: &Validator, manifest: &Value, accepted: bool) {
    let schema_ok = validator.is_valid(manifest);
    let parse_ok = Manifest::parse(&manifest.to_string()).is_ok();
    assert_eq!(schema_ok, accepted, "schema verdict for {manifest}");
    assert_eq!(parse_ok, accepted, "parser verdict for {manifest}");
}

#[test]
fn manifest_schema_matches_parser() {
    let validator = validator();
    assert_verdict(&validator, &json!({"name": "ab", "version": 1}), true);
    assert_verdict(
        &validator,
        &json!({"name": "a-b_9", "version": 4294967295_u32}),
        true,
    );
    assert_verdict(&validator, &json!({"name": "a", "version": 1}), false);
    assert_verdict(&validator, &json!({"name": "A", "version": 1}), false);
    assert_verdict(&validator, &json!({"name": "a b", "version": 1}), false);
    assert_verdict(
        &validator,
        &json!({"name": format!("a{}", "b".repeat(62)), "version": 1}),
        true,
    );
    assert_verdict(
        &validator,
        &json!({"name": format!("a{}", "b".repeat(63)), "version": 1}),
        false,
    );
    assert_verdict(&validator, &json!({"name": "ab", "version": 0}), false);
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "description": "a".repeat(500)}),
        true,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "description": "a".repeat(501)}),
        false,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "description": null}),
        true,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "capabilities": ["http", "fs"]}),
        true,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "capabilities": ["http", "http"]}),
        false,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "capabilities": ["nope"]}),
        false,
    );

    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {"server": {"command": "./run"}}
        }),
        true,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {"bad key": {"command": "./run"}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {"server": {"command": "./run", "url": "http://example.com/mcp"}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "mcpServers": {"server": {}}}),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {"server": {"command": ""}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {"server": {"url": "http://example.com/mcp"}}
        }),
        true,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {"server": {"url": "short"}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {"server": {"url": "zzzzzzzz"}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "mcpServers": {
                "server": {"url": "http://example.com/mcp", "args": ["--x"]}
            }
        }),
        false,
    );

    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"run": {"handler": "run"}}
        }),
        true,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"run": {"handler": "1run"}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"bad key": {"handler": "run"}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"run": {"handler": "run", "timeoutMs": 300000}}
        }),
        true,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"run": {"handler": "run", "timeoutMs": 300001}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"run": {"handler": "run", "timeoutMs": 0}}
        }),
        false,
    );

    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {
                "on": {"core/ready": {"handler": "run", "timeoutMs": 60000}},
                "emit": {
                    "changed": {
                        "retention": "journal",
                        "eventKey": "changed",
                        "payload": {"type": "object"}
                    }
                }
            }
        }),
        true,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {"on": {"core/ready": {"handler": "run", "timeoutMs": 60001}}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {"on": {"ready": {"handler": "run"}}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {
                "emit": {"changed": {"retention": "journal", "payload": {"type": "object"}}}
            }
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {
                "emit": {
                    "changed": {
                        "retention": "journal",
                        "eventKey": "",
                        "payload": {"type": "object"}
                    }
                }
            }
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {
                "emit": {"changed": {"retention": "live", "payload": true}}
            }
        }),
        true,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {
                "emit": {"changed": {"retention": "live"}}
            }
        }),
        false,
    );

    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "defs": {"point": {"type": "object"}}}),
        true,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "defs": {"point": 5}}),
        false,
    );
    assert_verdict(
        &validator,
        &json!({"name": "ab", "version": 1, "prompts": {}}),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"run": {"handler": "run", "input": {}}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "requests": {"run": {"handler": "run", "output": {}}}
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {
                "emit": {
                    "changed": {
                        "retention": "live",
                        "delivery": "journal",
                        "payload": {"type": "object"}
                    }
                }
            }
        }),
        false,
    );
    assert_verdict(
        &validator,
        &json!({
            "name": "ab",
            "version": 1,
            "events": {"on": {"core/ready": {"handler": "run", "phase": "before"}}}
        }),
        false,
    );
}

fn manifest_with_input(input: Value) -> String {
    json!({
        "name": "ab",
        "version": 1,
        "requests": {"run": {"handler": "run", "inputSchema": input}}
    })
    .to_string()
}

#[test]
fn schema_local_refs_are_validated() {
    assert!(Manifest::parse(&manifest_with_input(json!({"type": "object"}))).is_ok());
    assert!(Manifest::parse(&manifest_with_input(json!(true))).is_ok());
    assert!(Manifest::parse(&manifest_with_input(json!(false))).is_ok());
    assert!(
        Manifest::parse(&manifest_with_input(json!({
            "$ref": "#/defs/point"
        })))
        .is_err()
    );
    assert!(
        Manifest::parse(
            &json!({
                "name": "ab",
                "version": 1,
                "defs": {"point": {"type": "object"}},
                "requests": {"run": {"handler": "run", "inputSchema": {"$ref": "#/defs/point"}}}
            })
            .to_string()
        )
        .is_ok()
    );
    assert!(
        Manifest::parse(
            &json!({
                "name": "ab",
                "version": 1,
                "defs": {
                    "node": {
                        "type": "object",
                        "properties": {"next": {"$ref": "#/defs/node"}}
                    }
                },
                "requests": {"run": {"handler": "run", "inputSchema": {"$ref": "#/defs/node"}}}
            })
            .to_string()
        )
        .is_ok()
    );
    assert!(
        Manifest::parse(
            &json!({
                "name": "ab",
                "version": 1,
                "requests": {
                    "run": {
                        "handler": "run",
                        "inputSchema": {
                            "$defs": {"value": {"type": "string"}},
                            "type": "object",
                            "properties": {"value": {"$ref": "#/$defs/value"}}
                        }
                    }
                }
            })
            .to_string()
        )
        .is_ok()
    );
    assert!(Manifest::parse(&manifest_with_input(json!(42))).is_err());
    assert!(
        Manifest::parse(&manifest_with_input(
            json!({"$ref": "https://example.com/schema.json"})
        ))
        .is_err()
    );
    assert!(Manifest::parse(&manifest_with_input(json!({"type": 5}))).is_err());
    assert!(
        Manifest::parse(
            &json!({
                "name": "ab",
                "version": 1,
                "defs": {"broken": {"$ref": "#/defs/missing"}}
            })
            .to_string()
        )
        .is_err()
    );
    assert!(
        Manifest::parse(&json!({"name": "ab", "version": 1, "defs": {"broken": 5}}).to_string())
            .is_err()
    );
}

#[test]
fn subscriptions_reject_unknown_events() {
    let manifest = Manifest::parse(
        &json!({
            "name": "ab",
            "version": 1,
            "events": {"on": {"core/missing": {"handler": "run"}}}
        })
        .to_string(),
    )
    .expect("manifest parses");

    let error = manifest
        .subscriptions(&Directory::default(), 1000)
        .expect_err("the event is not in the directory");
    assert!(matches!(error, ModError::ManifestUnknownEvent { .. }));

    let meta = EventMeta {
        name: "core/missing",
        shape: Shape::Observe,
        log: false,
        schema: || json!({}),
    };
    let directory = Directory::from_metas([meta]);
    let subscriptions = manifest
        .subscriptions(&directory, 1000)
        .expect("the event exists");
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].event, "core/missing");
    assert_eq!(subscriptions[0].timeout_ms, 1000);
}

struct Fixture {
    _dir: TempDir,
    dirs: AppDirs,
    storage: Storage,
}

async fn fixture() -> Fixture {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("app");
    fs::create_dir_all(root.join("mods")).expect("create mods root");
    let dirs = AppDirs::under(root);
    let storage = Storage::open(&dirs.db_file()).await.expect("open database");
    Fixture {
        _dir: dir,
        dirs,
        storage,
    }
}

fn write_version(dirs: &AppDirs, name: &str, manifest: &Value) -> PathBuf {
    let version = manifest["version"].as_u64().expect("version");
    let root = dirs.mod_version(name, version as u32);
    fs::create_dir_all(&root).expect("create version root");
    fs::write(root.join("mod.json"), manifest.to_string()).expect("write manifest");
    root
}

async fn install(storage: &Storage, manifest: &Value) -> i64 {
    let name = manifest["name"].as_str().expect("name");
    storage
        .install_mod(name, &manifest.to_string(), None)
        .await
        .expect("install mod")
}

async fn enable_topic(storage: &Storage, label: &str, mod_id: i64) -> i64 {
    let topic_id = storage
        .create_topic(None, label, None, None, None)
        .await
        .expect("create topic");
    storage
        .enable_mod(topic_id, mod_id)
        .await
        .expect("enable mod");
    topic_id
}

fn write_prompts(root: &Path, files: &[(&str, &str)]) {
    let dir = root.join("prompts");
    fs::create_dir_all(&dir).expect("create prompts");
    for (name, content) in files {
        fs::write(dir.join(name), content).expect("write prompt");
    }
}

#[tokio::test]
async fn topic_resources_use_published_versions() {
    let fixture = fixture().await;
    let first = json!({"name": "math", "version": 1});
    let second = json!({"name": "math", "version": 2});
    let first_root = write_version(&fixture.dirs, "math", &first);
    write_prompts(&first_root, &[("01.md", "one")]);
    let second_root = write_version(&fixture.dirs, "math", &second);
    write_prompts(&second_root, &[("01.md", "two")]);

    let id = install(&fixture.storage, &first).await;
    let topic_id = enable_topic(&fixture.storage, "topic", id).await;
    let resources = Resources::new(fixture.storage.clone(), fixture.dirs.clone());

    let listed = resources.topic(topic_id).await.expect("topic resources");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "math");
    assert_eq!(listed[0].version, 1);
    assert_eq!(listed[0].prompts, vec!["one"]);

    let error = resources
        .version("math", 2)
        .await
        .expect_err("version two is not published");
    assert!(matches!(error, ModError::VersionNotPublished { .. }));

    fixture
        .storage
        .update_mod(id, 1, 2, &second.to_string(), None)
        .await
        .expect("update to version two");
    let listed = resources.topic(topic_id).await.expect("topic resources");
    assert_eq!(listed[0].version, 2);
    assert_eq!(listed[0].prompts, vec!["two"]);
}

#[tokio::test]
async fn prompt_and_mcp_resources_are_complete() {
    let fixture = fixture().await;
    let manifest = json!({
        "name": "math",
        "version": 1,
        "mcpServers": {
            "tool": {
                "command": "./bin/tool",
                "args": ["--flag", "value"],
                "env": {"KEY": "VALUE"},
                "description": "local tool"
            },
            "remote": {"url": "http://127.0.0.1:9/mcp?token=a&mode=b"}
        },
        "requests": {"lookup": {"handler": "lookup"}}
    });
    let root = write_version(&fixture.dirs, "math", &manifest);
    write_prompts(
        &root,
        &[
            ("20.md", "B"),
            ("10.MD", "A"),
            ("notes.txt", "C"),
            ("page.html", "D"),
            ("image.png", "E"),
        ],
    );
    fs::create_dir_all(root.join("skills")).expect("create skills");
    fs::create_dir_all(root.join("ui")).expect("create ui");
    fs::write(root.join("ui/index.js"), "export {};").expect("write ui");
    fs::create_dir_all(root.join("bin")).expect("create bin");
    fs::write(root.join("bin/tool"), "").expect("write tool");
    install(&fixture.storage, &manifest).await;

    let resources = Resources::new(fixture.storage.clone(), fixture.dirs.clone());
    let loaded = resources.version("math", 1).await.expect("resources");
    assert_eq!(loaded.prompts, vec!["A", "B", "C", "D"]);
    assert!(loaded.skills.is_some());
    assert!(loaded.ui.is_some());
    assert!(loaded.requires_scripts);
    assert_eq!(
        loaded.manifest.description, None,
        "the manifest description is optional"
    );

    let tool = &loaded.mcp["tool"];
    assert_eq!(tool.name, "tool");
    assert_eq!(tool.description.as_deref(), Some("local tool"));
    let canonical_root = fs::canonicalize(&root).expect("canonical root");
    let canonical_tool = fs::canonicalize(root.join("bin/tool")).expect("canonical tool");
    match &tool.config {
        McpConfig::Stdio {
            command,
            args,
            env,
            cwd,
        } => {
            assert_eq!(command, &canonical_tool.to_string_lossy());
            assert_eq!(args, &vec!["--flag".to_string(), "value".to_string()]);
            assert_eq!(env["KEY"], "VALUE");
            assert_eq!(cwd, &canonical_root);
        }
        other => panic!("expected a stdio server, found {other:?}"),
    }
    let remote = &loaded.mcp["remote"];
    assert_eq!(remote.description, None);
    match &remote.config {
        McpConfig::StreamableHttp { url } => {
            assert_eq!(url, "http://127.0.0.1:9/mcp?token=a&mode=b");
        }
        other => panic!("expected an http server, found {other:?}"),
    }
}

#[tokio::test]
async fn empty_resource_directories_are_empty() {
    let fixture = fixture().await;
    let manifest = json!({"name": "math", "version": 1});
    write_version(&fixture.dirs, "math", &manifest);
    install(&fixture.storage, &manifest).await;

    let resources = Resources::new(fixture.storage.clone(), fixture.dirs.clone());
    let loaded = resources.version("math", 1).await.expect("resources");
    assert!(loaded.prompts.is_empty());
    assert!(loaded.skills.is_none());
    assert!(loaded.ui.is_none());
    assert!(loaded.mcp.is_empty());
    assert!(!loaded.requires_scripts);
}

#[tokio::test]
async fn broken_package_files_are_reported() {
    let fixture = fixture().await;
    let manifest = json!({"name": "math", "version": 1});
    install(&fixture.storage, &manifest).await;
    let resources = Resources::new(fixture.storage.clone(), fixture.dirs.clone());

    let error = resources
        .version("math", 1)
        .await
        .expect_err("the version root is missing");
    assert!(matches!(error, ModError::Resource { .. }));

    let root = write_version(&fixture.dirs, "math", &manifest);
    fs::create_dir_all(root.join("prompts")).expect("create prompts");
    fs::write(root.join("prompts/broken.md"), [0xff, 0xfe]).expect("write invalid utf-8");
    let error = resources
        .version("math", 1)
        .await
        .expect_err("the prompt is not valid utf-8");
    assert!(matches!(error, ModError::Resource { .. }));

    fs::remove_dir_all(root.join("prompts")).expect("remove prompts");
    fs::write(root.join("prompts"), "not a directory").expect("write prompts file");
    let error = resources
        .version("math", 1)
        .await
        .expect_err("prompts must be a directory");
    assert!(matches!(error, ModError::Resource { .. }));
}

#[tokio::test]
async fn declared_package_paths_cannot_escape_the_version_root() {
    let fixture = fixture().await;
    let manifest = json!({
        "name": "math",
        "version": 1,
        "mcpServers": {"tool": {"command": "./../outside"}}
    });
    let root = write_version(&fixture.dirs, "math", &manifest);
    fs::write(root.parent().expect("mod root").join("outside"), "").expect("write outside");
    install(&fixture.storage, &manifest).await;

    let resources = Resources::new(fixture.storage.clone(), fixture.dirs.clone());
    let error = resources
        .version("math", 1)
        .await
        .expect_err("the command leaves the version root");
    assert!(matches!(error, ModError::Resource { .. }));
}

#[cfg(unix)]
fn symlink_dir(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn symlink_dir(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

#[tokio::test]
async fn symlinked_resources_cannot_escape_the_version_root() {
    let fixture = fixture().await;
    let manifest = json!({"name": "math", "version": 1});
    let root = write_version(&fixture.dirs, "math", &manifest);
    install(&fixture.storage, &manifest).await;
    let outside = fixture.dirs.root().join("outside-skills");
    fs::create_dir_all(&outside).expect("create outside dir");
    if symlink_dir(&outside, &root.join("skills")).is_err() {
        return;
    }

    let resources = Resources::new(fixture.storage.clone(), fixture.dirs.clone());
    let error = resources
        .version("math", 1)
        .await
        .expect_err("the skills symlink leaves the version root");
    assert!(matches!(error, ModError::Resource { .. }));
}

#[tokio::test]
async fn same_named_mcp_servers_keep_their_sources() {
    let fixture = fixture().await;
    let alpha = json!({
        "name": "alpha",
        "version": 1,
        "mcpServers": {"shared": {"command": "./bin/server"}}
    });
    let beta = json!({
        "name": "beta",
        "version": 1,
        "mcpServers": {"shared": {"command": "./bin/server"}}
    });
    let alpha_root = write_version(&fixture.dirs, "alpha", &alpha);
    let beta_root = write_version(&fixture.dirs, "beta", &beta);
    for root in [&alpha_root, &beta_root] {
        fs::create_dir_all(root.join("bin")).expect("create bin");
        fs::write(root.join("bin/server"), "").expect("write server");
    }
    let alpha_id = install(&fixture.storage, &alpha).await;
    let beta_id = install(&fixture.storage, &beta).await;
    let topic_id = enable_topic(&fixture.storage, "topic", alpha_id).await;
    fixture
        .storage
        .enable_mod(topic_id, beta_id)
        .await
        .expect("enable beta");

    let resources = Resources::new(fixture.storage.clone(), fixture.dirs.clone());
    let listed = resources.topic(topic_id).await.expect("topic resources");
    assert_eq!(listed.len(), 2);
    let names: Vec<&str> = listed.iter().map(|entry| entry.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "beta"]);
    let alpha_command = match &listed[0].mcp["shared"].config {
        McpConfig::Stdio { command, .. } => command.clone(),
        other => panic!("expected stdio, found {other:?}"),
    };
    let beta_command = match &listed[1].mcp["shared"].config {
        McpConfig::Stdio { command, .. } => command.clone(),
        other => panic!("expected stdio, found {other:?}"),
    };
    assert_ne!(alpha_command, beta_command);
    assert!(alpha_command.starts_with(&*listed[0].root.to_string_lossy()));
    assert!(beta_command.starts_with(&*listed[1].root.to_string_lossy()));
}

#[test]
fn package_identity_comes_from_the_manifest() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("mods").join("math").join("1");
    fs::create_dir_all(&root).expect("create package root");
    fs::write(
        root.join("mod.json"),
        json!({"name": "math", "version": 1}).to_string(),
    )
    .expect("write manifest");

    let package = Package::open(&root).expect("open package");
    assert_eq!(package.name(), "math");
    assert_eq!(package.version(), 1);
    assert_eq!(package.manifest.description, None);

    let wrong_name = dir.path().join("mods").join("other").join("1");
    fs::create_dir_all(&wrong_name).expect("create wrong name root");
    fs::write(
        wrong_name.join("mod.json"),
        json!({"name": "math", "version": 1}).to_string(),
    )
    .expect("write manifest");
    let error = Package::open(&wrong_name).expect_err("the directory name must match");
    assert!(matches!(error, ModError::Mismatch { .. }));

    let wrong_version = dir.path().join("mods").join("math").join("2");
    fs::create_dir_all(&wrong_version).expect("create wrong version root");
    fs::write(
        wrong_version.join("mod.json"),
        json!({"name": "math", "version": 1}).to_string(),
    )
    .expect("write manifest");
    let error = Package::open(&wrong_version).expect_err("the version directory must match");
    assert!(matches!(error, ModError::Mismatch { .. }));
}
