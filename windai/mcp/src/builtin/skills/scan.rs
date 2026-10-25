use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use yaml_rust2::Yaml;
use yaml_rust2::YamlLoader;
use yaml_rust2::yaml::Hash;

pub const SKILL_NAME: &str = "SKILL.md";
/// 技能名的字符上限
const NAME_MAX: usize = 64;
/// 技能描述的字符上限
const DESCRIPTION_MAX: usize = 1024;
/// 兼容性说明的字符上限
const COMPATIBILITY_MAX: usize = 500;

#[derive(Debug, thiserror::Error)]
pub enum SkillsError {
    #[error("SKILL.md not found")]
    SkillFileNotFound,

    #[error("frontmatter block not found or malformed")]
    FrontmatterNotFound,

    #[error("missing required field `{0}`")]
    MissingField(String),

    #[error("invalid YAML frontmatter: {0}")]
    Yaml(String),

    #[error("IO error: {0}")]
    Io(String),
}

pub type Result<T> = std::result::Result<T, SkillsError>;

/// 技能的元数据
///
/// 字段与取值约束由技能目录的约定给出
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SkillsMeta {
    /// 技能所在目录的绝对路径，由代码填充，不来自 frontmatter
    #[serde(skip_deserializing)]
    pub skill_dir: String,
    /// 技能名称，仅小写字母、数字与连字符，最多 64 字符
    pub name: String,
    /// 技能描述，非空，最多 1024 字符
    pub description: String,
    /// 许可证的名称或对随附许可证文件的引用
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// 兼容性说明，最多 500 字符
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    /// 附加的任意键值对
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    /// 预先批准的工具，空格分隔
    #[serde(rename = "allowed-tools", skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<String>,
}

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
    if dir.join(SKILL_NAME).is_file() {
        match from_path(dir.to_path_buf()) {
            Ok(meta) => {
                if seen.insert(meta.name.clone()) {
                    metas.push(meta);
                } else {
                    log::warn!("ignore dup skill \"{}\": {}", meta.name, dir.display());
                }
            }
            Err(error) => {
                log::warn!("parse `SKILL.md` failed {}: {}", dir.display(), error);
            }
        }
        return;
    }

    if !recursive || depth >= max_depth {
        return;
    }

    let mut entries: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_type().ok()?.is_dir().then(|| entry.path()))
            .collect(),
        Err(_) => return,
    };
    entries.sort();

    for path in entries {
        walk(&path, recursive, max_depth, depth + 1, metas, seen);
    }
}

pub fn from_path(dir: PathBuf) -> Result<SkillsMeta> {
    let skill_file = dir.join(SKILL_NAME);
    if !skill_file.is_file() {
        return Err(SkillsError::SkillFileNotFound);
    }

    let content =
        fs::read_to_string(&skill_file).map_err(|error| SkillsError::Io(error.to_string()))?;

    let frontmatter = extract_frontmatter(&content)?;

    let mut meta = parse_frontmatter(&frontmatter)?;
    meta.skill_dir = absolute_dir(&dir)?;
    Ok(meta)
}

fn parse_frontmatter(frontmatter: &str) -> Result<SkillsMeta> {
    let docs = YamlLoader::load_from_str(frontmatter)
        .map_err(|error| SkillsError::Yaml(error.to_string()))?;
    let doc = docs
        .first()
        .ok_or_else(|| SkillsError::Yaml("empty YAML document".to_string()))?;

    let hash = doc
        .as_hash()
        .ok_or_else(|| SkillsError::Yaml("frontmatter is not a mapping".to_string()))?;

    let name = get_str(hash, "name").unwrap_or_default();
    let description = get_str(hash, "description").unwrap_or_default();

    if name.is_empty() {
        return Err(SkillsError::MissingField("name".to_string()));
    }
    if description.is_empty() {
        return Err(SkillsError::MissingField("description".to_string()));
    }

    Ok(SkillsMeta {
        skill_dir: String::new(),
        name: truncate(name, NAME_MAX),
        description: truncate(description, DESCRIPTION_MAX),
        license: get_str(hash, "license").map(str::to_string),
        compatibility: get_str(hash, "compatibility")
            .map(|value| truncate(value, COMPATIBILITY_MAX)),
        metadata: get_metadata(hash),
        allowed_tools: get_str(hash, "allowed-tools").map(str::to_string),
    })
}

fn get_str<'a>(hash: &'a Hash, key: &str) -> Option<&'a str> {
    hash.get(&Yaml::String(key.to_string()))
        .and_then(Yaml::as_str)
}

fn get_metadata(hash: &Hash) -> Option<HashMap<String, String>> {
    let metadata = hash.get(&Yaml::String("metadata".to_string()))?.as_hash()?;
    let map = metadata
        .iter()
        .filter_map(|(key, value)| Some((key.as_str()?.to_string(), value.as_str()?.to_string())))
        .collect();
    Some(map)
}

fn extract_frontmatter(content: &str) -> Result<String> {
    let content = content.strip_prefix('\u{feff}').unwrap_or(content);
    let mut lines = content.lines();

    if lines.next().map(str::trim_end) != Some("---") {
        return Err(SkillsError::FrontmatterNotFound);
    }

    let mut frontmatter = String::new();
    for line in lines {
        if line.trim_end() == "---" {
            return Ok(frontmatter);
        }
        frontmatter.push_str(line);
        frontmatter.push('\n');
    }
    Err(SkillsError::FrontmatterNotFound)
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() > max {
        text.chars().take(max).collect()
    } else {
        text.to_string()
    }
}

fn absolute_dir(dir: &PathBuf) -> Result<String> {
    fs::canonicalize(dir)
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|error| SkillsError::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// 目录名计数器，保证并发运行的测试各取一个目录
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// 用完即删的临时目录
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        /// 按标签创建临时目录
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir()
                .join(format!("wind-skills-{label}-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        /// 写一个文件，父目录一并创建
        fn write(&self, relative: &str, content: &str) {
            let path = self.path.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent dir");
            }
            fs::write(path, content).expect("write file");
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// 生成合法的 `SKILL.md` 正文
    fn skill(name: &str, description: &str) -> String {
        format!("---\nname: {name}\ndescription: {description}\n---\n")
    }

    #[test]
    fn scan_collects_a_skill_at_the_root() {
        let dir = TempDir::new("root");
        dir.write("SKILL.md", &skill("demo", "a demo skill"));

        let metas = scan(dir.path().to_path_buf(), None, None);

        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].name, "demo");
        assert_eq!(metas[0].description, "a demo skill");
        assert_eq!(
            metas[0].skill_dir,
            dir.path().canonicalize().expect("parse").to_string_lossy()
        );
    }

    #[test]
    fn scan_finds_nested_skills_and_stops_at_a_skill_root() {
        let dir = TempDir::new("nested");
        dir.write("one/SKILL.md", &skill("one", "first"));
        // 技能根之下的内容不再当作技能
        dir.write("one/resources/two/SKILL.md", &skill("two", "second"));

        let metas = scan(dir.path().to_path_buf(), None, None);

        let names: Vec<&str> = metas.iter().map(|meta| meta.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["one"],
            "no new skills are looked for below a skill root"
        );
    }

    #[test]
    fn scan_skips_a_directory_that_is_not_a_directory() {
        let dir = TempDir::new("missing");
        let missing = dir.path().join("nowhere");
        assert!(
            scan(missing, None, None).is_empty(),
            "a missing directory scans as an empty list"
        );
    }

    #[test]
    fn scan_ignores_a_broken_skill_md() {
        let dir = TempDir::new("broken");
        dir.write("good/SKILL.md", &skill("good", "normal"));
        dir.write("bad/SKILL.md", "---\nname: bad\n---\n");

        let metas = scan(dir.path().to_path_buf(), None, None);

        let names: Vec<&str> = metas.iter().map(|meta| meta.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["good"],
            "the broken one is skipped without aborting the scan"
        );
    }

    #[test]
    fn a_duplicate_name_keeps_the_first_one() {
        let dir = TempDir::new("dup");
        dir.write("a/SKILL.md", &skill("same", "first"));
        dir.write("b/SKILL.md", &skill("same", "second"));

        let metas = scan(dir.path().to_path_buf(), None, None);

        assert_eq!(metas.len(), 1);
        assert_eq!(
            metas[0].description, "first",
            "the first one wins for a duplicate name"
        );
    }

    #[test]
    fn a_skill_without_frontmatter_is_refused() {
        let dir = TempDir::new("no-frontmatter");
        dir.write("SKILL.md", "# no frontmatter\n");

        assert!(from_path(dir.path().to_path_buf()).is_err());
    }
}
