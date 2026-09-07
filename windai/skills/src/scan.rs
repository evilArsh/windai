//! 技能目录扫描。

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use crate::{SKILL_NAME, SkillsMeta, from_path};

/// 递归扫描目录，收集所有 skill 的元数据。
///
/// 深扫规则：目录含 `SKILL.md` 即命中为 skill 根，解析后不再向更深层找新 skill；
/// 更深层内容均视为该 skill 的资源。
///
/// - `recursive`：是否递归子目录，默认 `true`。
/// - `max_depth`：允许递归的子目录层数（`0` 只扫 `dir` 本身），默认不限。
/// - 同名冲突：先到先得（目录按路径排序保证确定性），后到者跳过并告警。
/// - 单个 skill 解析失败（坏 YAML / 缺字段）跳过，不中断整体扫描。
pub fn scan(dir: PathBuf, recursive: Option<bool>, max_depth: Option<u16>) -> Vec<SkillsMeta> {
    if !dir.is_dir() {
        return Vec::new();
    }

    let recursive = recursive.unwrap_or(true);
    let max_depth = max_depth.unwrap_or(u16::MAX);

    let mut metas = Vec::new();
    let mut seen = HashSet::new();
    walk(&dir, recursive, max_depth, 0, &mut metas, &mut seen);
    metas
}

fn walk(
    dir: &Path,
    recursive: bool,
    max_depth: u16,
    depth: u16,
    metas: &mut Vec<SkillsMeta>,
    seen: &mut HashSet<String>,
) {
    // 当前目录含 SKILL.md → 命中为 skill 根，解析后不再深入
    if dir.join(SKILL_NAME).is_file() {
        match from_path(dir.to_path_buf()) {
            Ok(meta) => {
                if seen.insert(meta.name.clone()) {
                    metas.push(meta);
                } else {
                    log::warn!("Ignore dup skill \"{}\": {}", meta.name, dir.display());
                }
            }
            Err(e) => {
                log::warn!("Parse `SKILL.md` failed {}: {}", dir.display(), e);
            }
        }
        return;
    }

    // 非递归或已达深度上限，不再深入子目录
    if !recursive || depth >= max_depth {
        return;
    }

    let mut entries: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(it) => it.filter_map(|e| e.ok()).map(|e| e.path()).collect(),
        Err(_) => return,
    };
    // 排序保证"先到先得"的确定性
    entries.sort();

    for path in entries {
        if path.is_dir() {
            walk(&path, recursive, max_depth, depth + 1, metas, seen);
        }
    }
}
