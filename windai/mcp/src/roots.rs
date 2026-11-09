use crate::error::{McpError, Result};
use std::collections::HashMap;
use std::fmt;
use std::path::{Component, Path, PathBuf, is_separator};
use std::sync::{Arc, RwLock};

/// 调用者标识，core 给出，如 topic:42
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScopeId(String);

impl ScopeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ScopeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    /// 技能来源的标签
    pub source: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeRoots {
    /// fs 的可访问范围，空时不启动 fs
    pub workspace: Vec<PathBuf>,
    /// 技能扫描范围
    pub skills: Vec<SkillRoot>,
}

#[derive(Clone)]
pub struct Roots {
    /// mod 版本目录的根
    pub mods: PathBuf,
    /// 按调用者隔离的根集合
    scopes: Arc<RwLock<HashMap<ScopeId, ScopeRoots>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Path(PathBuf),
    Name(String),
}

impl Roots {
    pub fn new(mods: PathBuf) -> Self {
        Self {
            mods: normalize(&mods).unwrap_or(mods),
            scopes: Arc::default(),
        }
    }

    pub fn set_scope(&self, scope: &ScopeId, roots: ScopeRoots) {
        self.scopes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(scope.clone(), roots);
    }

    pub fn drop_scope(&self, scope: &ScopeId) {
        self.scopes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(scope);
    }

    pub fn scope(&self, scope: &ScopeId) -> Option<ScopeRoots> {
        self.scopes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(scope)
            .cloned()
    }

    pub fn check_cwd(&self, cwd: &Path) -> Result<PathBuf> {
        let resolved = normalize(cwd).ok_or_else(|| invalid_path(cwd))?;
        if resolved.starts_with(&self.mods) {
            Ok(resolved)
        } else {
            Err(invalid_path(cwd))
        }
    }

    pub fn check_command(&self, command: &str) -> Result<Command> {
        if command.chars().any(is_separator) {
            self.check_cwd(Path::new(command)).map(Command::Path)
        } else {
            Ok(Command::Name(command.to_owned()))
        }
    }
}

fn invalid_path(path: &Path) -> McpError {
    McpError::InvalidPath(path.display().to_string())
}

fn normalize(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let components: Vec<Component<'_>> = path.components().collect();
    let floor = components
        .iter()
        .position(|component| matches!(component, Component::Normal(_)))
        .unwrap_or(components.len());
    for split in (floor..=components.len()).rev() {
        let prefix: PathBuf = components[..split].iter().collect();
        let Ok(mut resolved) = prefix.canonicalize() else {
            continue;
        };
        for component in &components[split..] {
            match component {
                Component::Normal(name) => resolved.push(name),
                Component::ParentDir => {
                    resolved.pop();
                }
                Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            }
        }
        return Some(resolved);
    }
    None
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{Command, Roots, ScopeId, ScopeRoots};
    use tempfile::TempDir;

    #[test]
    fn cwd_must_stay_inside_the_mods_root() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("mods"));
        assert!(
            roots
                .check_cwd(&tmp.path().join("mods").join("demo").join("1"))
                .is_ok()
        );
        assert!(roots.check_cwd(&tmp.path().join("etc")).is_err());
        assert!(
            roots
                .check_cwd(&tmp.path().join("mods").join("..").join("etc"))
                .is_err()
        );
    }

    #[test]
    fn scopes_are_independent_and_replaced_in_place() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("mods"));
        let a = ScopeId::new("topic:1");
        let b = ScopeId::new("topic:2");
        roots.set_scope(
            &a,
            ScopeRoots {
                workspace: vec![tmp.path().join("ws-a")],
                skills: Vec::new(),
            },
        );
        roots.set_scope(
            &b,
            ScopeRoots {
                workspace: vec![tmp.path().join("ws-b")],
                skills: Vec::new(),
            },
        );
        assert_eq!(
            roots.scope(&a).unwrap().workspace,
            vec![tmp.path().join("ws-a")]
        );
        assert_eq!(
            roots.scope(&b).unwrap().workspace,
            vec![tmp.path().join("ws-b")]
        );

        roots.set_scope(
            &a,
            ScopeRoots {
                workspace: vec![tmp.path().join("ws-c")],
                skills: Vec::new(),
            },
        );
        assert_eq!(
            roots.scope(&a).unwrap().workspace,
            vec![tmp.path().join("ws-c")]
        );
        assert_eq!(
            roots.scope(&b).unwrap().workspace,
            vec![tmp.path().join("ws-b")]
        );
    }

    #[test]
    fn dropped_scopes_are_forgotten() {
        let tmp = TempDir::new().expect("create a temp dir");
        let roots = Roots::new(tmp.path().join("mods"));
        let scope = ScopeId::new("topic:1");
        roots.set_scope(
            &scope,
            ScopeRoots {
                workspace: vec![tmp.path().join("ws")],
                skills: Vec::new(),
            },
        );
        roots.drop_scope(&scope);
        assert!(roots.scope(&scope).is_none());
    }

    #[test]
    fn scope_id_exposes_its_text() {
        let scope = ScopeId::new("topic:42");
        assert_eq!(scope.as_str(), "topic:42");
        assert_eq!(scope.to_string(), "topic:42");
    }

    #[test]
    fn commands_are_fenced_only_when_they_contain_separators() {
        let tmp = TempDir::new().expect("create a temp dir");
        let mods = tmp.path().join("mods");
        fs::create_dir_all(mods.join("demo")).expect("create the mods root");
        let roots = Roots::new(mods.clone());

        assert_eq!(
            roots.check_command("npx").unwrap(),
            Command::Name("npx".to_owned())
        );
        assert_eq!(
            roots.check_command("bun").unwrap(),
            Command::Name("bun".to_owned())
        );

        let inside = mods.join("demo").join("server");
        let expected = mods
            .canonicalize()
            .expect("canonicalize the mods root")
            .join("demo")
            .join("server");
        assert_eq!(
            roots
                .check_command(inside.to_str().expect("utf-8 path"))
                .unwrap(),
            Command::Path(expected)
        );

        let outside = tmp.path().join("etc").join("server");
        assert!(
            roots
                .check_command(outside.to_str().expect("utf-8 path"))
                .is_err()
        );
        let relative = PathBuf::from("demo").join("server");
        assert!(
            roots
                .check_command(relative.to_str().expect("utf-8 path"))
                .is_err()
        );
    }
}
