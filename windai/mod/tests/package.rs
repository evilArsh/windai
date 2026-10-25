//! 包布局与资产指纹

use std::fs;
use tempfile::TempDir;
use wind_mod::dirs::AppDirs;
use wind_mod::package::Package;

/// 造一个最小可用的包
fn package(root: &std::path::Path, name: &str, version: i64) -> std::path::PathBuf {
    let dir = root.join(format!("{name}-{version}"));
    fs::create_dir_all(dir.join("scripts")).expect("create dir");
    fs::write(
        dir.join("mod.json"),
        format!("{{\"name\":\"{name}\",\"version\":{version}}}"),
    )
    .expect("write the manifest");
    dir
}

#[test]
fn the_layout_uses_path_buf_so_both_separators_work() {
    let tmp = TempDir::new().expect("create temp dir");
    let dirs = AppDirs::under(tmp.path());
    assert!(
        dirs.mod_version("a", 2)
            .ends_with(std::path::Path::new("mods").join("a").join("2"))
    );
    assert!(
        dirs.mod_data("a")
            .ends_with(std::path::Path::new("mods").join("a").join("data"))
    );
    assert!(dirs.public_skills().ends_with("skills"));
}

#[test]
fn opening_a_package_reads_the_manifest() {
    let tmp = TempDir::new().expect("create temp dir");
    let dir = package(tmp.path(), "demo", 3);
    let pkg = Package::open(dir).expect("open");
    assert_eq!(pkg.name, "demo");
    assert_eq!(pkg.version, 3);
    assert!(
        pkg.script_entry()
            .ends_with(std::path::Path::new("scripts").join("index.js"))
    );
    assert!(
        pkg.ui_entry()
            .ends_with(std::path::Path::new("ui").join("index.js"))
    );
}

#[test]
fn a_package_without_a_manifest_is_rejected() {
    let tmp = TempDir::new().expect("create temp dir");
    let dir = tmp.path().join("empty");
    fs::create_dir_all(&dir).expect("create dir");
    assert!(Package::open(dir).is_err());
}

#[test]
fn the_fingerprint_follows_content_and_not_location() {
    let first = TempDir::new().expect("create temp dir");
    let second = TempDir::new().expect("create temp dir");
    let installed = [first.path(), second.path()].map(|root| {
        let dir = package(root, "demo", 1);
        // 资产随包分发，因此落在包根目录之下
        let asset = dir.join("mcp").join("search");
        fs::create_dir_all(&asset).expect("create dir");
        fs::write(asset.join("bin"), b"same").expect("write");
        dir
    });
    let a = Package::open(installed[0].clone()).expect("open");
    let b = Package::open(installed[1].clone()).expect("open");
    assert_eq!(
        a.asset_fingerprint("search").expect("compute"),
        b.asset_fingerprint("search").expect("compute"),
        "identical content means an identical fingerprint, regardless of directory"
    );

    fs::write(
        installed[0].join("mcp").join("search").join("bin"),
        b"changed",
    )
    .expect("write");
    let a = Package::open(installed[0].clone()).expect("open");
    assert_ne!(
        a.asset_fingerprint("search").expect("compute"),
        b.asset_fingerprint("search").expect("compute"),
        "changed content must change the fingerprint"
    );
}

#[test]
fn a_missing_asset_has_no_fingerprint() {
    let tmp = TempDir::new().expect("create temp dir");
    let pkg = Package::open(package(tmp.path(), "demo", 1)).expect("open");
    assert!(pkg.asset_fingerprint("absent").is_err());
}

#[test]
fn an_asset_name_that_leaves_the_asset_directory_has_no_fingerprint() {
    let tmp = TempDir::new().expect("create temp dir");
    let pkg = Package::open(package(tmp.path(), "demo", 1)).expect("open");
    // 取一个确实存在的绝对路径，它指向包外
    let rooted = tmp.path().display().to_string();

    assert!(
        pkg.asset_fingerprint(&rooted).is_err(),
        "an asset name containing an absolute path component must be refused"
    );
    assert!(
        pkg.asset_fingerprint("..").is_err(),
        "an asset name that escapes the asset directory on the first step must be refused"
    );
    assert!(
        pkg.asset_fingerprint("search/../../escape").is_err(),
        "an asset name that escapes the asset directory after accumulation must be refused"
    );
}

#[cfg(unix)]
#[test]
fn a_symlink_inside_an_asset_stays_out_of_its_fingerprint() {
    use std::os::unix::fs::symlink;

    let tmp = TempDir::new().expect("create temp dir");
    let dir = package(tmp.path(), "demo", 1);
    let asset = dir.join("mcp").join("search");
    fs::create_dir_all(&asset).expect("create dir");
    fs::write(asset.join("bin"), b"same").expect("write");

    let pkg = Package::open(dir).expect("open");
    let before = pkg.asset_fingerprint("search").expect("compute");
    // 指向自身的链接。跟随它会让遍历不终止
    symlink(&asset, asset.join("loop")).expect("create the symlink");

    assert_eq!(
        pkg.asset_fingerprint("search").expect("compute"),
        before,
        "a symlink is not a file distributed with the package and does not enter the fingerprint"
    );
}

#[test]
fn a_relative_command_resolves_under_the_package_root() {
    let tmp = TempDir::new().expect("create temp dir");
    let dir = package(tmp.path(), "demo", 1);
    let server = dir.join("mcp").join("search").join("bin").join("server");
    fs::create_dir_all(server.parent().expect("get the parent dir")).expect("create dir");
    fs::write(&server, b"#!/bin/sh\n").expect("write");

    let pkg = Package::open(dir).expect("open");
    assert_eq!(
        pkg.resolve_asset("./mcp/search/bin/server").expect("parse"),
        server,
        "a relative command resolves against that version's install directory"
    );
}

#[test]
fn a_command_the_host_does_not_resolve_has_no_path() {
    let tmp = TempDir::new().expect("create temp dir");
    let pkg = Package::open(package(tmp.path(), "demo", 1)).expect("open");
    let sep = std::path::MAIN_SEPARATOR;
    let rooted = format!("./{sep}etc");

    assert!(
        pkg.resolve_asset("uvx").is_none(),
        "a command not starting with ./ is looked up on the system PATH"
    );
    assert!(
        pkg.resolve_asset(&rooted).is_none(),
        "a command containing an absolute path component must be refused"
    );
    assert!(
        pkg.resolve_asset("./../escape").is_none(),
        "a command that escapes the install directory on the first step must be refused"
    );
    assert!(
        pkg.resolve_asset("./mcp/../../escape").is_none(),
        "a command that escapes the install directory after accumulation must be refused"
    );
}

#[test]
fn a_step_back_inside_the_package_is_allowed() {
    let tmp = TempDir::new().expect("create temp dir");
    let dir = package(tmp.path(), "demo", 1);
    let pkg = Package::open(dir.clone()).expect("open");

    assert_eq!(
        pkg.resolve_asset("./a/../b").expect("parse"),
        dir.join("b"),
        "stepping back inside the install directory is allowed"
    );
    assert_eq!(
        pkg.resolve_asset("./a/./b").expect("parse"),
        dir.join("a").join("b"),
        "empty path segments are normalized"
    );
}
