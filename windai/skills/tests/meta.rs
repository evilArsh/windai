mod common;

use common::TempDir;
use wind_skills::{Error, from_path};

#[test]
fn parse_minimal_skill() {
    let dir = TempDir::new("parse-minimal");
    dir.write(
        "SKILL.md",
        "---\nname: my-skill\ndescription: does things\n---\n# body\n",
    );

    let meta = from_path(dir.path().to_path_buf()).expect("parse minimal skill");

    assert_eq!(meta.name, "my-skill");
    assert_eq!(meta.description, "does things");
    assert_eq!(meta.license, None);
    assert_eq!(meta.compatibility, None);
    assert_eq!(meta.metadata, None);
    assert_eq!(meta.allowed_tools, None);
    // skill_dir 是规范化绝对路径
    assert!(meta.skill_dir.starts_with('/'));
    assert!(meta.skill_dir.contains("parse-minimal"));
}

#[test]
fn parse_all_fields() {
    let dir = TempDir::new("parse-all");
    dir.write(
        "SKILL.md",
        "---\nname: full-skill\ndescription: a full skill\nlicense: MIT\ncompatibility: linux\nmetadata:\n  author: arsh\n  version: \"1.0\"\nallowed-tools: \"read write\"\n---\n",
    );

    let meta = from_path(dir.path().to_path_buf()).expect("parse full skill");

    assert_eq!(meta.name, "full-skill");
    assert_eq!(meta.license.as_deref(), Some("MIT"));
    assert_eq!(meta.compatibility.as_deref(), Some("linux"));
    let md = meta.metadata.expect("metadata parsed");
    assert_eq!(md.get("author").map(String::as_str), Some("arsh"));
    assert_eq!(md.get("version").map(String::as_str), Some("1.0"));
    assert_eq!(meta.allowed_tools.as_deref(), Some("read write"));
}

#[test]
fn parse_truncates_overlong_fields() {
    let dir = TempDir::new("parse-truncate");
    let long_name = "a".repeat(100);
    let long_desc = "b".repeat(2000);
    let long_compat = "c".repeat(800);
    dir.write(
        "SKILL.md",
        &format!(
            "---\nname: {long_name}\ndescription: {long_desc}\ncompatibility: {long_compat}\n---\n"
        ),
    );

    let meta = from_path(dir.path().to_path_buf()).expect("parse truncate skill");

    assert_eq!(meta.name.chars().count(), 64);
    assert_eq!(meta.description.chars().count(), 1024);
    assert_eq!(meta.compatibility.expect("compat").chars().count(), 500);
}

#[test]
fn parse_missing_description_is_error() {
    let dir = TempDir::new("parse-missing-desc");
    dir.write("SKILL.md", "---\nname: no-desc\n---\n");

    let err = from_path(dir.path().to_path_buf()).expect_err("missing description");
    assert!(matches!(err, Error::MissingField(_)));
}

#[test]
fn parse_bad_yaml_is_error() {
    let dir = TempDir::new("parse-bad-yaml");
    dir.write("SKILL.md", "---\nname: [unclosed\n---\n");

    let err = from_path(dir.path().to_path_buf()).expect_err("bad yaml");
    assert!(matches!(err, Error::Yaml(_)));
}

#[test]
fn parse_no_frontmatter_is_error() {
    let dir = TempDir::new("parse-no-fm");
    dir.write("SKILL.md", "just a body, no frontmatter\n");

    let err = from_path(dir.path().to_path_buf()).expect_err("no frontmatter");
    assert!(matches!(err, Error::FrontmatterNotFound));
}

#[test]
fn parse_missing_skill_file() {
    let dir = TempDir::new("parse-no-file");

    let err = from_path(dir.path().to_path_buf()).expect_err("missing SKILL.md");
    assert!(matches!(err, Error::SKILLmdNotFound));
}

#[test]
fn parse_with_bom() {
    let dir = TempDir::new("parse-bom");
    dir.write(
        "SKILL.md",
        "\u{feff}---\nname: bom-skill\ndescription: has bom\n---\n",
    );

    let meta = from_path(dir.path().to_path_buf()).expect("parse bom skill");
    assert_eq!(meta.name, "bom-skill");
}
