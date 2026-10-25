//! 模块说明符的白名单与模块的加载

use rquickjs::loader::{Loader, Resolver};
use rquickjs::{Context, Module, Runtime};
use std::collections::BTreeMap;
use std::fs::{create_dir_all, write};
use std::path::PathBuf;
use wind_script::loader::{LoadError, ModuleLoader, ModuleResolver, normalize_specifier};

#[test]
fn a_relative_specifier_resolves_against_the_referrer() {
    assert_eq!(
        normalize_specifier("scripts/index.js", "./hn.js").expect("parse"),
        "scripts/hn.js"
    );
    assert_eq!(
        normalize_specifier("scripts/a/b/c.js", "../../d.js").expect("parse"),
        "scripts/d.js"
    );
}

#[test]
fn a_bare_specifier_is_refused() {
    let error = normalize_specifier("scripts/index.js", "lodash").expect_err("must be rejected");
    assert!(matches!(error, LoadError::Bare { .. }), "{error:?}");
}

#[test]
fn a_url_or_absolute_specifier_is_refused() {
    for specifier in [
        "https://cdn.example.com/x.js",
        "data:text/javascript,1",
        "node:fs",
        "/etc/passwd",
        "C:/x.js",
    ] {
        let error =
            normalize_specifier("scripts/index.js", specifier).expect_err("must be rejected");
        assert!(
            matches!(error, LoadError::NotRelative { .. }),
            "{specifier} should be classified as not a relative specifier, got {error:?}"
        );
    }
}

#[test]
fn an_escape_out_of_the_install_root_is_refused() {
    let error =
        normalize_specifier("scripts/index.js", "../../outside.js").expect_err("must be rejected");
    assert!(matches!(error, LoadError::Escapes { .. }), "{error:?}");
}

#[test]
fn a_specifier_into_a_non_host_directory_is_refused() {
    for specifier in ["../ui/x.js", "../mcp/server.js", "../lib/x.js", "../ui"] {
        let error =
            normalize_specifier("scripts/index.js", specifier).expect_err("must be rejected");
        assert!(
            matches!(error, LoadError::NotHostSide { .. }),
            "{specifier} resolves into a directory that is not host-side code, got {error:?}"
        );
    }
    // 只看根目录下的直接子目录。嵌在别处的同名目录仍是宿主侧代码的一部分
    assert_eq!(
        normalize_specifier("scripts/index.js", "./ui/x.js").expect("parse"),
        "scripts/ui/x.js"
    );
    assert_eq!(
        normalize_specifier("scripts/ui/a.js", "./mcp/b.js").expect("parse"),
        "scripts/ui/mcp/b.js"
    );
}

#[test]
fn a_specifier_that_resolves_to_the_install_root_is_refused() {
    let error = normalize_specifier("scripts/index.js", "./..").expect_err("must be rejected");
    assert!(matches!(error, LoadError::NotAModule { .. }), "{error:?}");
}

#[test]
fn the_resolver_normalizes_through_the_same_rule() {
    let mut resolver = ModuleResolver::new();
    let rt = Runtime::new().expect("create runtime");
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        let resolved = resolver
            .resolve(&ctx, "scripts/index.js", "./hn.js", None)
            .expect("parse");
        assert_eq!(resolved, "scripts/hn.js");
        let refused = resolver.resolve(&ctx, "scripts/index.js", "lodash", None);
        assert!(
            refused.is_err(),
            "a bare specifier must not exist at the resolution stage"
        );
    });
}

#[test]
fn a_module_is_loaded_from_the_source_when_no_bytecode_is_cached() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let scripts = dir.path().join("scripts");
    create_dir_all(&scripts).expect("create dir");
    write(scripts.join("hn.js"), "export const top = 3;").expect("write file");

    let rt = Runtime::new().expect("create runtime");
    let loader = ModuleLoader::new(dir.path().to_path_buf(), BTreeMap::new());
    rt.set_loader(ModuleResolver::new(), loader);
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        let module = {
            let mut loader = ModuleLoader::new(dir.path().to_path_buf(), BTreeMap::new());
            loader
                .load(&ctx, "scripts/hn.js", None)
                .expect("load the module")
        };
        let (_, promise) = module.eval().expect("evaluate");
        let _ = promise;
        let _ = &ctx;
    });
}

#[test]
fn a_module_that_is_not_under_the_install_root_is_refused() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let rt = Runtime::new().expect("create runtime");
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        let mut loader = ModuleLoader::new(PathBuf::from(dir.path()), BTreeMap::new());
        // 拒绝它的是名字形状那道判定，文件系统不会被碰。根外确实存在同名文件的情况
        // 由 a_name_that_escapes_the_install_root_is_refused_even_when_the_file_is_there 覆盖
        let error = loader.load(&ctx, "../outside.js", None);
        assert!(
            error.is_err(),
            "a name that escapes the install root must not be loaded"
        );
    });
}

#[test]
fn a_normalized_name_with_no_file_behind_it_is_reported_as_missing() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let rt = Runtime::new().expect("create runtime");
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        let mut loader = ModuleLoader::new(dir.path().to_path_buf(), BTreeMap::new());
        let error = loader
            .load(&ctx, "scripts/nope.js", None)
            .expect_err("load a missing module");
        assert!(
            error
                .to_string()
                .contains("is not present under the install root"),
            "a missing file must be reported as a missing module, not as a generic I/O reason, got {error}"
        );
    });
}

#[test]
fn a_loader_without_an_install_root_does_not_fall_back_to_the_process_directory() {
    let rt = Runtime::new().expect("create runtime");
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        // 空路径解不出安装根；`src` 是包目录下确实存在的一项，退回相对路径就会去问它
        let mut loader = ModuleLoader::new(PathBuf::new(), BTreeMap::new());
        let error = loader
            .load(&ctx, "src", None)
            .expect_err("load without an install root");
        assert!(
            error
                .to_string()
                .contains("is not present under the install root"),
            "without an install root it must not read anything from the process working directory, got {error}"
        );
    });
}

#[test]
fn a_name_that_escapes_the_install_root_is_refused_even_when_the_file_is_there() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let root = dir.path().join("install");
    create_dir_all(&root).expect("create dir");
    write(dir.path().join("outside.js"), "export const top = 3;").expect("write file");

    let rt = Runtime::new().expect("create runtime");
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        let mut loader = ModuleLoader::new(root.clone(), BTreeMap::new());
        let error = loader.load(&ctx, "../outside.js", None);
        assert!(
            error.is_err(),
            "even when a readable file really exists outside the install root, it must not be read as a module"
        );
    });
}

#[cfg(unix)]
#[test]
fn a_module_reached_through_a_link_out_of_the_install_root_is_refused() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("create temp dir");
    let root = dir.path().join("install");
    let scripts = root.join("scripts");
    create_dir_all(&scripts).expect("create dir");
    let outside = dir.path().join("outside.js");
    write(&outside, "export const top = 3;").expect("write file");
    symlink(&outside, scripts.join("linked.js")).expect("create the symlink");

    let rt = Runtime::new().expect("create runtime");
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        let mut loader = ModuleLoader::new(root.clone(), BTreeMap::new());
        let error = loader.load(&ctx, "scripts/linked.js", None);
        assert!(
            error.is_err(),
            "when the file itself is a link pointing outside the root, it must not be read as a module"
        );
    });
}

#[cfg(unix)]
#[test]
fn a_module_under_a_linked_directory_out_of_the_install_root_is_refused() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("create temp dir");
    let root = dir.path().join("install");
    create_dir_all(&root).expect("create dir");
    let elsewhere = dir.path().join("elsewhere");
    create_dir_all(&elsewhere).expect("create dir");
    write(elsewhere.join("hn.js"), "export const top = 3;").expect("write file");
    symlink(&elsewhere, root.join("scripts")).expect("create the symlink");

    let rt = Runtime::new().expect("create runtime");
    let ctx = Context::full(&rt).expect("build context");
    ctx.with(|ctx| {
        let mut loader = ModuleLoader::new(root.clone(), BTreeMap::new());
        let error = loader.load(&ctx, "scripts/hn.js", None);
        assert!(
            error.is_err(),
            "when the directory itself is a link pointing outside the root, modules under it must not be read either"
        );
    });
}

#[test]
fn an_import_of_a_neighbour_module_resolves_and_loads_through_the_runtime() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let scripts = dir.path().join("scripts");
    create_dir_all(&scripts).expect("create dir");
    write(scripts.join("hn.js"), "export const top = 3;").expect("write file");

    let rt = Runtime::new().expect("create runtime");
    rt.set_loader(
        ModuleResolver::new(),
        ModuleLoader::new(dir.path().to_path_buf(), BTreeMap::new()),
    );
    let ctx = Context::full(&rt).expect("build context");
    let top: i32 = ctx.with(|ctx| {
        let entry = Module::declare(
            ctx.clone(),
            "scripts/index.js",
            "import { top } from './hn.js';\nexport const n = top;",
        )
        .expect("declare the entry");
        let (module, _promise) = entry.eval().expect("evaluate");
        while ctx.execute_pending_job() {}
        module
            .namespace()
            .expect("read the namespace")
            .get("n")
            .expect("read the export")
    });
    assert_eq!(
        top, 3,
        "the entry's imports are loaded by the resolver and loader passed into the runtime"
    );
}
