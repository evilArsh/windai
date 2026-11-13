use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::Serialize;
use yaml_rust2::Yaml;
use yaml_rust2::YamlLoader;
use yaml_rust2::yaml::Hash;

use super::SkillRoot;

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
    #[error("io error: {0}")]
    Io(String),
}

pub type Result<T> = std::result::Result<T, SkillsError>;

/// 一个技能的元数据
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SkillsMeta {
    /// 技能来源, None 时表示来自公共路径
    pub source: Option<String>,
    /// 技能所在目录的绝对路径
    pub skill_dir: String,
    /// 技能名称，最多 64 字符
    pub name: String,
    /// 技能描述，最多 1024 字符
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

/// 一个无法解析的技能
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SkillError {
    /// 技能目录
    pub path: String,
    /// 失败原因
    pub error: String,
}

/// 技能扫描的结果
#[derive(Debug, Serialize, JsonSchema)]
pub struct SkillsListResult {
    /// 解析成功的技能
    pub skills: Vec<SkillsMeta>,
    /// 解析失败的技能
    pub errors: Vec<SkillError>,
}

pub fn scan(
    roots: &[SkillRoot],
    recursive: Option<bool>,
    max_depth: Option<u16>,
) -> SkillsListResult {
    let mut result = SkillsListResult {
        skills: Vec::new(),
        errors: Vec::new(),
    };
    let recursive = recursive.unwrap_or(true);
    let max_depth = max_depth.unwrap_or(u16::MAX);
    for root in roots {
        if !root.root.is_dir() {
            continue;
        }
        walk(root, &root.root, recursive, max_depth, 0, &mut result);
    }
    result
}

fn walk(
    root: &SkillRoot,
    dir: &Path,
    recursive: bool,
    max_depth: u16,
    depth: u16,
    result: &mut SkillsListResult,
) {
    if dir.join(SKILL_NAME).is_file() {
        match from_path(dir, root.source.as_deref()) {
            Ok(meta) => result.skills.push(meta),
            Err(error) => result.errors.push(SkillError {
                path: dir.to_string_lossy().into_owned(),
                error: error.to_string(),
            }),
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
        walk(root, &path, recursive, max_depth, depth + 1, result);
    }
}

pub fn from_path(dir: &Path, source: Option<&str>) -> Result<SkillsMeta> {
    let skill_file = dir.join(SKILL_NAME);
    if !skill_file.is_file() {
        return Err(SkillsError::SkillFileNotFound);
    }
    let content =
        fs::read_to_string(&skill_file).map_err(|error| SkillsError::Io(error.to_string()))?;
    let frontmatter = extract_frontmatter(&content)?;
    let mut meta = parse_frontmatter(&frontmatter)?;
    meta.source = source.map(|s| s.to_string());
    meta.skill_dir = absolute_dir(dir)?;
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
        source: None,
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

fn absolute_dir(dir: &Path) -> Result<String> {
    fs::canonicalize(dir)
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|error| SkillsError::Io(error.to_string()))
}
