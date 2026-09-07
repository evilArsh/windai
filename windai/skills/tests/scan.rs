mod common;

use common::TempDir;
use wind_skills::scan;

fn names(metas: &[wind_skills::SkillsMeta]) -> Vec<&str> {
    let mut v: Vec<&str> = metas.iter().map(|m| m.name.as_str()).collect();
    v.sort_unstable();
    v
}

#[test]
fn scan_stops_at_first_skill() {
    let dir = TempDir::new("scan-nested");
    dir.write("SKILL.md", "---\nname: parent\ndescription: p\n---\n");
    // 子目录里的 SKILL.md 不应被扫描（父已命中，不再深入）
    dir.write("sub/SKILL.md", "---\nname: child\ndescription: c\n---\n");

    let metas = scan(dir.path().to_path_buf(), None, None);
    assert_eq!(names(&metas), vec!["parent"]);
}

#[test]
fn scan_collects_multiple_skills() {
    let dir = TempDir::new("scan-multiple");
    dir.write("a/SKILL.md", "---\nname: skill-a\ndescription: a\n---\n");
    dir.write("b/SKILL.md", "---\nname: skill-b\ndescription: b\n---\n");
    dir.write("c/note.txt", "not a skill");

    let metas = scan(dir.path().to_path_buf(), None, None);
    assert_eq!(names(&metas), vec!["skill-a", "skill-b"]);
}

#[test]
fn scan_deduplicates_by_name() {
    let dir = TempDir::new("scan-dup");
    dir.write("x/SKILL.md", "---\nname: same\ndescription: x\n---\n");
    dir.write("y/SKILL.md", "---\nname: same\ndescription: y\n---\n");

    let metas = scan(dir.path().to_path_buf(), None, None);
    // 按路径排序先扫到 x，保留先到者
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].name, "same");
    assert!(metas[0].skill_dir.ends_with("x"));
}

#[test]
fn scan_respects_max_depth() {
    let dir = TempDir::new("scan-depth");
    dir.write("a/SKILL.md", "---\nname: depth-one\ndescription: a\n---\n");
    dir.write(
        "a/b/SKILL.md",
        "---\nname: depth-two\ndescription: b\n---\n",
    );

    // max_depth=0：只扫根目录本身，根目录无 SKILL.md → 空
    let metas = scan(dir.path().to_path_buf(), None, Some(0));
    assert!(metas.is_empty());

    // max_depth=1：扫到直接子目录 a，不再深入 a/b
    let metas = scan(dir.path().to_path_buf(), None, Some(1));
    assert_eq!(names(&metas), vec!["depth-one"]);
}

#[test]
fn scan_non_recursive() {
    let dir = TempDir::new("scan-nonrec");
    dir.write("SKILL.md", "---\nname: root\ndescription: r\n---\n");
    dir.write("sub/SKILL.md", "---\nname: sub\ndescription: s\n---\n");

    let metas = scan(dir.path().to_path_buf(), Some(false), None);
    assert_eq!(names(&metas), vec!["root"]);
}

#[test]
fn scan_skips_bad_skills() {
    let dir = TempDir::new("scan-skip");
    dir.write("bad/SKILL.md", "---\nname: [unclosed\n---\n");
    dir.write("good/SKILL.md", "---\nname: good\ndescription: ok\n---\n");

    let metas = scan(dir.path().to_path_buf(), None, None);
    assert_eq!(names(&metas), vec!["good"]);
}

#[test]
fn scan_nonexistent_dir_returns_empty() {
    let dir = TempDir::new("scan-missing");
    let missing = dir.path().join("does-not-exist");

    let metas = scan(missing, None, None);
    assert!(metas.is_empty());
}
