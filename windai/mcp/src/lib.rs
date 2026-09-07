pub mod builtin;
pub mod client;

use self::client::registry::Registry;

pub trait BuiltinMcp {
    fn get_name(&self) -> &'static str;
    fn get_description(&self) -> &'static str;
}

pub async fn init_builtin(_registry: &mut Registry) {
    // 特殊mcp，需要指定执行根目录
    // FsServer
    // SkillsServer

    // 内建MCP server全局初始化
}
