// ! TODO: 某些事件是否可以删除

use serde::{Deserialize, Serialize};
use serde_json::Value;
use wind_event::{
    def::{Directory, EventDef, Shape},
    schema,
};

/// 请求安装一个 mod
///
/// 安装是首次引入一个名字，同一名字已经存在时应当走更新
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModInstall {
    /// mod 名
    pub name: String,
    /// 版本号
    pub version: u32,
    /// 清单的规范化形式，缺省字段被补齐
    pub manifest: Value,
}

/// 一个 mod 已被安装
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModInstalled {
    /// mod 名
    pub name: String,
    /// 版本号
    pub version: u32,
    /// 清单的规范化形式，缺省字段被补齐
    pub manifest: Value,
    /// 前端组件入口的地址，没有组件时为空
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_entry: Option<String>,
}

/// 请求发布一个新版本
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModUpdate {
    /// mod 名
    pub name: String,
    /// 发布前的版本
    pub from_version: u32,
    /// 发布后的版本
    pub to_version: u32,
}

/// 一个 mod 已发布新版本
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModUpdated {
    /// mod 名
    pub name: String,
    /// 发布前的版本
    pub from_version: u32,
    /// 发布后的版本
    pub to_version: u32,
    /// 前端组件入口的地址，没有组件时为空
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_entry: Option<String>,
}

/// 请求删除一个 mod
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModDelete {
    /// mod 名
    pub name: String,
    /// 被删除时的当前版本
    pub version: u32,
}

/// 一个 mod 已被删除
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModDeleted {
    /// mod 名
    pub name: String,
    /// 被删除时的当前版本
    pub version: u32,
}

/// 一次安装开始
pub const CORE_MOD_INSTALL: EventDef<ModInstall> = EventDef::new(
    "core/mod.install",
    Shape::Observe,
    true,
    schema::<ModInstall>,
);

/// 一次安装完成
pub const CORE_MOD_INSTALLED: EventDef<ModInstalled> = EventDef::new(
    "core/mod.installed",
    Shape::Observe,
    true,
    schema::<ModInstalled>,
);

/// 一次发布开始
pub const CORE_MOD_UPDATE: EventDef<ModUpdate> =
    EventDef::new("core/mod.update", Shape::Observe, true, schema::<ModUpdate>);

/// 一次发布完成
pub const CORE_MOD_UPDATED: EventDef<ModUpdated> = EventDef::new(
    "core/mod.updated",
    Shape::Observe,
    true,
    schema::<ModUpdated>,
);

/// 一次删除开始
pub const CORE_MOD_DELETE: EventDef<ModDelete> =
    EventDef::new("core/mod.delete", Shape::Observe, true, schema::<ModDelete>);

/// 一次删除完成
pub const CORE_MOD_DELETED: EventDef<ModDeleted> = EventDef::new(
    "core/mod.deleted",
    Shape::Observe,
    true,
    schema::<ModDeleted>,
);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_MOD_INSTALL.meta(),
        CORE_MOD_INSTALLED.meta(),
        CORE_MOD_UPDATE.meta(),
        CORE_MOD_UPDATED.meta(),
        CORE_MOD_DELETE.meta(),
        CORE_MOD_DELETED.meta(),
    ])
}
