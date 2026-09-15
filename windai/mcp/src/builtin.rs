pub mod fs;
pub mod skills;

#[derive(Debug, Clone, Copy)]
pub struct BuiltinSpec {
    /// registry 注册名
    pub name: &'static str,
    /// 描述
    pub description: &'static str,
}
pub const BUILTIN_FS: BuiltinSpec = BuiltinSpec {
    name: "wind-mcp-fs",
    description: "file and process capability",
};
pub const BUILTIN_SKILLS: BuiltinSpec = BuiltinSpec {
    name: "wind-mcp-skills",
    description: "skill discovery",
};
pub const BUILTIN_SERVERS: &[BuiltinSpec] = &[BUILTIN_FS, BUILTIN_SKILLS];

/// 判断名字是否属于常驻内建服务
///
/// 供业务层在写入配置前做重名提示；存储层不依赖本模块，不做该校验
pub fn is_builtin_name(name: &str) -> bool {
    BUILTIN_SERVERS.iter().any(|s| s.name == name)
}
