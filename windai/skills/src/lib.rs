mod meta;
mod scan;

pub use meta::{SkillsMeta, from_path};
pub use scan::scan;

/// 技能元数据文件名
pub const SKILL_NAME: &str = "SKILL.md";

/// 库错误
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Path \"{0}\" not found")]
    PathNotFound(String),

    #[error("SKILL.md not found")]
    SKILLmdNotFound,

    #[error("frontmatter block not found or malformed")]
    FrontmatterNotFound,

    #[error("missing required field `{0}`")]
    MissingField(String),

    #[error("invalid YAML frontmatter: {0}")]
    Yaml(String),

    #[error("IO error: {0}")]
    Io(String),
}

pub type Result<T> = std::result::Result<T, Error>;
