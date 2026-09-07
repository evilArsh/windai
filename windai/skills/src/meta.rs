//! SKILL.md 解析与元数据。

use std::{collections::HashMap, path::PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use yaml_rust2::{Yaml, YamlLoader, yaml::Hash};

use crate::{Error, Result, SKILL_NAME};

const NAME_MAX: usize = 64;
const DESCRIPTION_MAX: usize = 1024;
const COMPATIBILITY_MAX: usize = 500;

/// Skills 元数据
///
/// 定义见 [https://agentskills.io/specification]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SkillsMeta {
    /// SKILL.md 的父目录绝对路径（代码设置，非 frontmatter 字段）
    #[serde(skip_deserializing)]
    pub skill_dir: String,
    /// 技能名称
    ///
    /// 最大 64 字符，仅限小写字母、数字和连字符，不能以连字符开头或结尾
    pub name: String,
    /// 技能描述
    ///
    /// 最大 1024 字符，非空，描述技能功能和适用场景
    pub description: String,
    /// 许可证
    ///
    /// 可以是许可证名称或对捆绑许可证文件的引用
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// 兼容性
    ///
    /// 最大 500 字符，指示环境要求（目标产品、系统包、网络访问等）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    /// 附加元数据
    ///
    /// 任意键值对映射，用于存储额外的元数据
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    /// 预批准工具列表
    ///
    /// 空格分隔的字符串，列出技能可以使用的预批准工具
    #[serde(rename = "allowed-tools", skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<String>,
}

/// 从 skill 根目录读取并解析 `SKILL.md` 的 frontmatter。
///
/// `dir` 是 skill 根目录路径（`SKILL.md` 的父目录）；函数读取 `dir/SKILL.md`，
/// 解析其 YAML frontmatter，并将 `skill_dir` 设为该目录的绝对路径。
///
/// 宽松模式：超长字段截断、非字符串字段值忽略；但 `name`/`description` 必须存在且非空。
pub fn from_path(dir: PathBuf) -> Result<SkillsMeta> {
    let skill_file = dir.join(SKILL_NAME);
    if !skill_file.is_file() {
        return Err(Error::SKILLmdNotFound);
    }

    let content = std::fs::read_to_string(&skill_file).map_err(|e| Error::Io(e.to_string()))?;

    let frontmatter = extract_frontmatter(&content)?;

    let mut meta = parse_frontmatter(&frontmatter)?;
    meta.skill_dir = absolute_dir(&dir)?;
    Ok(meta)
}

/// 解析 frontmatter YAML 为 [`SkillsMeta`]（`skill_dir` 留空，由调用方填充）。
fn parse_frontmatter(frontmatter: &str) -> Result<SkillsMeta> {
    let docs = YamlLoader::load_from_str(frontmatter).map_err(|e| Error::Yaml(e.to_string()))?;
    let doc = docs
        .first()
        .ok_or_else(|| Error::Yaml("empty YAML document".to_string()))?;

    let hash = doc
        .as_hash()
        .ok_or_else(|| Error::Yaml("frontmatter is not a mapping".to_string()))?;

    let name = get_str(hash, "name").unwrap_or_default();
    let description = get_str(hash, "description").unwrap_or_default();

    if name.is_empty() {
        return Err(Error::MissingField("name".to_string()));
    }
    if description.is_empty() {
        return Err(Error::MissingField("description".to_string()));
    }

    Ok(SkillsMeta {
        skill_dir: String::new(),
        name: truncate(name, NAME_MAX),
        description: truncate(description, DESCRIPTION_MAX),
        license: get_str(hash, "license").map(str::to_string),
        compatibility: get_str(hash, "compatibility").map(|c| truncate(c, COMPATIBILITY_MAX)),
        metadata: get_metadata(hash),
        allowed_tools: get_str(hash, "allowed-tools").map(str::to_string),
    })
}

/// 从 mapping 取字符串字段值。
fn get_str<'a>(hash: &'a Hash, key: &str) -> Option<&'a str> {
    hash.get(&Yaml::String(key.to_string()))
        .and_then(Yaml::as_str)
}

/// 提取 `metadata` 映射；仅保留字符串值，非字符串值忽略。
fn get_metadata(hash: &Hash) -> Option<HashMap<String, String>> {
    let md = hash.get(&Yaml::String("metadata".to_string()))?.as_hash()?;
    let map = md
        .iter()
        .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
        .collect();
    Some(map)
}

/// 提取 frontmatter：以顶格 `---` 起始、以顶格 `---` 结束之间的内容。
fn extract_frontmatter(content: &str) -> Result<String> {
    let content = content.strip_prefix('\u{feff}').unwrap_or(content);
    let mut lines = content.lines();

    if lines.next().map(str::trim_end) != Some("---") {
        return Err(Error::FrontmatterNotFound);
    }

    let mut fm = String::new();
    for line in lines {
        if line.trim_end() == "---" {
            return Ok(fm);
        }
        fm.push_str(line);
        fm.push('\n');
    }
    Err(Error::FrontmatterNotFound)
}

/// 按字符数截断，避免切断 UTF-8 多字节字符。
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        s.chars().take(max).collect()
    } else {
        s.to_string()
    }
}

/// 返回目录的规范化绝对路径。
fn absolute_dir(dir: &PathBuf) -> Result<String> {
    std::fs::canonicalize(dir)
        .map(|p| p.to_string_lossy().into_owned())
        .map_err(|e| Error::Io(e.to_string()))
}
