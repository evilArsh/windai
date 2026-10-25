//! 安装期的编译、导出校验与指纹失配后的改读源码

use rquickjs::{Context, Function, Module, Runtime};
use std::collections::BTreeSet;
use std::env::consts::ARCH;
use std::fs;
use wind_script::bytecode::{
    CompileError, Compiled, compile_scripts, engine_fingerprint, matches_current_engine,
};

/// 建一个包目录，返回它的根
fn package(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("create temp dir");
    for (relative, body) in files {
        let path = dir.path().join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create dir");
        }
        fs::write(&path, body).expect("write file");
    }
    dir
}

/// 一份清单指认的处理函数集合
fn handlers(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

#[test]
fn every_module_of_the_entry_graph_is_compiled() {
    let dir = package(&[
        (
            "scripts/index.js",
            "import { top } from './hn.js';\nexport const run = () => top;\n",
        ),
        ("scripts/hn.js", "export const top = 3;\n"),
    ]);
    let compiled = compile_scripts(dir.path(), &handlers(&["run"])).expect("compile");
    let mut names: Vec<&str> = compiled.modules.keys().map(String::as_str).collect();
    names.sort();
    assert_eq!(names, vec!["scripts/hn.js", "scripts/index.js"]);
    assert!(
        !compiled.fingerprint.is_empty(),
        "the fingerprint must be recorded"
    );
}

#[test]
fn a_handler_the_manifest_names_must_be_exported() {
    let dir = package(&[("scripts/index.js", "export const run = () => 1;\n")]);
    let error = compile_scripts(dir.path(), &handlers(&["run", "onMcpCall"]))
        .expect_err("reject a missing export");
    match error {
        CompileError::MissingHandler { name } => assert_eq!(name, "onMcpCall"),
        other => panic!("expected a missing handler, got {other:?}"),
    }
}

#[test]
fn a_module_that_does_not_parse_is_refused_at_install_time() {
    let dir = package(&[("scripts/index.js", "export const run = () => { ;\n")]);
    let error = compile_scripts(dir.path(), &handlers(&["run"])).expect_err("must be rejected");
    assert!(matches!(error, CompileError::Module { .. }), "{error:?}");
}

#[test]
fn a_package_without_a_scripts_directory_compiles_to_nothing() {
    let dir = package(&[("mod.json", "{}")]);
    let compiled = compile_scripts(dir.path(), &handlers(&[])).expect("compile");
    assert!(compiled.modules.is_empty());
}

#[test]
fn the_engine_fingerprint_names_the_resolved_engine_and_the_target() {
    let fingerprint = engine_fingerprint();
    assert!(
        fingerprint.contains(&format!(
            "rquickjs-sys={}",
            env!("WIND_SCRIPT_ENGINE_VERSION")
        )),
        "{fingerprint}"
    );
    assert!(fingerprint.contains(ARCH), "{fingerprint}");
    assert!(
        fingerprint.contains(&format!("ptr={}", usize::BITS)),
        "{fingerprint}"
    );
}

#[test]
fn a_fingerprint_from_another_engine_is_not_reused() {
    let dir = package(&[("scripts/index.js", "export const run = () => 1;\n")]);
    let compiled = compile_scripts(dir.path(), &handlers(&["run"])).expect("compile");
    let mut stale = compiled.clone();
    stale.fingerprint = "rquickjs-sys=0.0.0;arch=none;ptr=8;endian=le".to_string();
    assert!(
        !matches_current_engine(&stale),
        "a different fingerprint must be treated as unusable and fall back to reading the source"
    );
    assert!(matches_current_engine(&compiled));
}

#[test]
fn the_same_source_compiles_to_the_same_module_names_on_every_run() {
    let dir = package(&[("scripts/index.js", "export const run = () => 1;\n")]);
    let first: Compiled = compile_scripts(dir.path(), &handlers(&["run"])).expect("compile");
    let second: Compiled = compile_scripts(dir.path(), &handlers(&["run"])).expect("compile");
    let names = |set: &Compiled| set.modules.keys().cloned().collect::<Vec<_>>();
    assert_eq!(
        names(&first),
        names(&second),
        "module names must be determined only by file location"
    );
}

#[test]
fn code_outside_the_scripts_directory_is_not_compiled() {
    let dir = package(&[
        ("scripts/index.js", "export const run = () => 1;\n"),
        ("lib/x.js", "export const run = () => 2;\n"),
    ]);
    let compiled = compile_scripts(dir.path(), &handlers(&["run"])).expect("compile");
    let names: Vec<&str> = compiled.modules.keys().map(String::as_str).collect();
    assert_eq!(names, vec!["scripts/index.js"]);
}

#[test]
fn the_browser_half_and_the_binary_assets_are_not_host_side_code() {
    let dir = package(&[
        ("scripts/index.js", "export const run = () => 1;\n"),
        (
            "ui/index.js",
            "import react from 'react';\nexport default react;\n",
        ),
        ("mcp/tool.js", "export const name = 'tool';\n"),
    ]);
    let compiled = compile_scripts(dir.path(), &handlers(&["run"])).expect("compile");
    let names: Vec<&str> = compiled.modules.keys().map(String::as_str).collect();
    assert_eq!(
        names,
        vec!["scripts/index.js"],
        "the browser half runs in the browser and the assets are binary, so neither enters the host-side graph"
    );
}

#[test]
fn the_written_bytecode_reloads_and_runs() {
    let dir = package(&[("scripts/index.js", "export const run = () => 3;\n")]);
    let compiled = compile_scripts(dir.path(), &handlers(&["run"])).expect("compile");
    let entry = compiled
        .modules
        .get("scripts/index.js")
        .expect("find the entry module");

    // 换一个解释器：调用期只有字节码可用，源码不再参与
    let runtime = Runtime::new().expect("create runtime");
    let context = Context::full(&runtime).expect("build context");
    let value: i32 = context.with(|ctx| {
        // 字节码由本进程刚刚写出，引擎指纹也来自同一个构建
        let module = unsafe { Module::load(ctx.clone(), entry) }.expect("load the bytecode");
        let (module, _promise) = module.eval().expect("evaluate");
        let run: Function = module.get("run").expect("find the export");
        run.call::<(), i32>(()).expect("call")
    });
    assert_eq!(value, 3);
}
