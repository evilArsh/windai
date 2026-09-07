use std::path::PathBuf;

use wind_skills::{SkillsMeta, scan};

/// 递归扫描目录，收集所有 skill 的元数据。
pub fn skills_list(
    dir: PathBuf,
    recursive: Option<bool>,
    max_depth: Option<u16>,
) -> Vec<SkillsMeta> {
    scan(dir, recursive, max_depth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn skills_list_scans_directory() {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("wind-skills-list-{}-{id}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        fs::write(
            dir.join("SKILL.md"),
            "---\nname: demo\ndescription: a demo skill\n---\n",
        )
        .expect("write SKILL.md");

        let metas = skills_list(dir.clone(), None, None);
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].name, "demo");

        let _ = fs::remove_dir_all(&dir);
    }
}
