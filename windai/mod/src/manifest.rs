use crate::error::{ModError, Result};
use crate::schema::validate_fragment;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use url::Url;
use wind_event::{Directory, Subscription};

/// 名字的字符集为 `^[a-z][a-z0-9_-]{1,62}$`
const NAME_HINT: &str = "must start with a lowercase letter and hold 2 to 63 characters from \
                         lowercase letters, digits, underscores and hyphens";
/// 标识符的字符集为 `^[a-zA-Z_][a-zA-Z0-9_-]{0,62}$`
const SYMBOL_HINT: &str = "must start with a letter or an underscore and hold at most 63 \
                           letters, digits, underscores or hyphens";
/// 处理函数名的字符集为 `^[a-zA-Z_$][a-zA-Z0-9_$]*$`，即脚本里可导出的名字
const HANDLER_HINT: &str = "must be a name a script can export";
/// 单次调用超时的上限，毫秒
const MAX_REQUEST_TIMEOUT_MS: u64 = 300_000;
/// 单条订阅时间预算的上限，毫秒
const MAX_SUB_TIMEOUT_MS: u64 = 60_000;
/// mod 调用默认超时时间，毫秒
pub const DEFAULT_SUB_TIMEOUT_MS: u64 = 20;
const MIN_URL_LEN: usize = 8;
const MAX_DESCRIPTION_LEN: usize = 500;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    strum::AsRefStr,
    schemars::JsonSchema,
    Hash,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Capability {
    /// 对外状态，写入会广播给订阅方
    State,
    /// 私有键值存储，不广播
    Storage,
    Http,
    Agent,
    Fs,
}

/// `ctx` 上的一个属性
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Prop {
    State,
    Storage,
    Http,
    Agent,
    Fs,
    Emit,
    Notify,
    Log,
}

impl From<Capability> for Prop {
    fn from(capability: Capability) -> Self {
        match capability {
            Capability::State => Prop::State,
            Capability::Storage => Prop::Storage,
            Capability::Http => Prop::Http,
            Capability::Agent => Prop::Agent,
            Capability::Fs => Prop::Fs,
        }
    }
}

impl Prop {
    fn needs_io(self) -> bool {
        matches!(self, Prop::Http | Prop::Agent | Prop::Fs)
    }
}

// /// 一次调用里存在的属性集合
// #[derive(Debug, Clone)]
// pub struct CtxSet {
//     present: BTreeSet<Prop>,
// }

// impl CtxSet {
//     pub fn has(&self, prop: Prop) -> bool {
//         self.present.contains(&prop)
//     }

//     pub fn iter(&self) -> impl Iterator<Item = Prop> + '_ {
//         self.present.iter().copied()
//     }
// }

// pub fn available(declared: &[Capability], mode: CallMode) -> CtxSet {
//     let mut present = BTreeSet::new();
//     present.insert(Prop::Emit);
//     present.insert(Prop::Notify);
//     present.insert(Prop::Log);
//     for capability in declared {
//         let prop = Prop::from(*capability);
//         if !mode.allows_io() && prop.needs_io() {
//             continue;
//         }
//         present.insert(prop);
//     }
//     CtxSet { present }
// }

/// mod 能力清单
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Manifest {
    /// mod 名，系统唯一
    #[schemars(regex(pattern = "^[a-z][a-z0-9_-]{1,62}$"), length(min = 2, max = 63))]
    pub name: String,

    /// 版本号，单调递增
    #[schemars(range(min = 1))]
    pub version: u32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// 声明需要的宿主能力
    ///
    /// - 声明后才会出现在处理函数的 ctx 上。
    /// - intercept 形态的处理函数不注入任何 I/O 能力，
    /// 比如：声明了 "http" 的 Mod，其 intercept 处理函数中依然没有 ctx.http
    #[serde(default)]
    #[schemars(extend("uniqueItems" = true))]
    pub capabilities: Vec<Capability>,

    /// mcp 服务声明
    /// - Stdio 服务
    /// - Streamable 服务
    /// - `mcp/<name>` 中的服务
    #[serde(default)]
    pub mcp_servers: HashMap<String, McpServer>,

    /// mod 暴露的 js 函数调用入口
    #[serde(default)]
    pub requests: HashMap<String, Request>,

    /// 事件处理
    ///
    /// # on
    /// 订阅宿主的事件，订阅后宿主会调用 mod 的 `handler` 中声明的 js 函数
    /// # emit
    /// js 向宿主发送事件的通道
    #[serde(default)]
    pub events: Events,

    /// 共享 schema 字典
    #[serde(default)]
    pub defs: HashMap<String, Value>,
}

impl Manifest {
    pub fn parse(raw: &str) -> Result<Self> {
        let manifest: Manifest = serde_json::from_str(raw)
            .map_err(|error| ModError::ManifestShape(error.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        check(name_ok(&self.name), "name", NAME_HINT)?;
        check(
            self.version >= 1,
            "version",
            "must be an integer greater than zero",
        )?;
        if let Some(description) = &self.description {
            check(
                description.chars().count() <= MAX_DESCRIPTION_LEN,
                "description",
                format!("must be at most {MAX_DESCRIPTION_LEN} characters"),
            )?;
        }
        let mut seen = HashSet::new();
        for capability in &self.capabilities {
            check(
                seen.insert(*capability),
                "capabilities",
                "the same capability must not be declared twice",
            )?;
        }
        for (key, server) in &self.mcp_servers {
            let field = format!("mcpServers.{key}");
            check(symbol_ok(key), &field, SYMBOL_HINT)?;
            match (&server.command, &server.url) {
                (Some(command), None) => {
                    check(!command.is_empty(), &field, "the command must not be empty")?;
                }
                (None, Some(url)) => {
                    check(url.len() >= MIN_URL_LEN, &field, "the url is too short")?;
                    check(
                        Url::parse(url).is_ok(),
                        &field,
                        "the url is not a valid absolute uri",
                    )?;
                    check(
                        server.args.is_empty() && server.env.is_empty(),
                        &field,
                        "a remote server must not declare args or env",
                    )?;
                }
                _ => {
                    return Err(ModError::ManifestField {
                        field,
                        reason: "exactly one of command or url must be given".to_string(),
                    });
                }
            }
        }
        for (key, request) in &self.requests {
            let field = format!("requests.{key}");
            check(symbol_ok(key), &field, SYMBOL_HINT)?;
            check(
                handler_ok(&request.handler),
                format!("{field}.handler"),
                HANDLER_HINT,
            )?;
            if let Some(timeout) = request.timeout_ms {
                check(
                    (1..=MAX_REQUEST_TIMEOUT_MS).contains(&timeout),
                    format!("{field}.timeoutMs"),
                    format!("must be between 1 and {MAX_REQUEST_TIMEOUT_MS}"),
                )?;
            }
        }
        let mut state_keys = HashSet::new();
        for (key, decl) in &self.events.emit {
            let field = format!("events.emit.{key}");
            check(symbol_ok(key), &field, SYMBOL_HINT)?;
            check(
                decl.payload.is_some(),
                format!("{field}.payload"),
                "a payload schema is required",
            )?;
            match decl.retention {
                Retention::Journal => {
                    let event_key = decl.event_key.as_deref().unwrap_or_default();
                    check(
                        decl.event_key.is_some(),
                        format!("{field}.eventKey"),
                        "a journaled delivery requires an event key",
                    )?;
                    check(
                        symbol_ok(event_key),
                        format!("{field}.eventKey"),
                        SYMBOL_HINT,
                    )?;
                    check(
                        state_keys.insert(event_key.to_string()),
                        format!("{field}.eventKey"),
                        "two journaled channels must not declare the same event key",
                    )?;
                }
                Retention::Live => {}
            }
        }
        for (name, sub) in &self.events.on {
            let field = format!("events.on.{name}");
            check(
                name.starts_with("core/"),
                &field,
                "only host events can be subscribed",
            )?;

            check(
                handler_ok(&sub.handler),
                format!("{field}.handler"),
                HANDLER_HINT,
            )?;
            if let Some(timeout) = sub.timeout_ms {
                check(
                    (1..=MAX_SUB_TIMEOUT_MS).contains(&timeout),
                    format!("{field}.timeoutMs"),
                    format!("must be between 1 and {MAX_SUB_TIMEOUT_MS}"),
                )?;
            }
        }
        for key in self.defs.keys() {
            check(symbol_ok(key), format!("defs.{key}"), SYMBOL_HINT)?;
        }
        let defs: Map<String, Value> = self
            .defs
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        for (key, request) in &self.requests {
            let field = format!("requests.{key}");
            if let Some(schema) = &request.input_schema {
                validate_fragment(&format!("{field}.inputSchema"), schema, &defs)?;
            }
            if let Some(schema) = &request.output_schema {
                validate_fragment(&format!("{field}.outputSchema"), schema, &defs)?;
            }
        }
        for (key, decl) in &self.events.emit {
            if let Some(schema) = &decl.payload {
                validate_fragment(&format!("events.emit.{key}.payload"), schema, &defs)?;
            }
        }
        for (key, schema) in &self.defs {
            validate_fragment(&format!("defs.{key}"), schema, &defs)?;
        }
        Ok(())
    }

    pub fn event_name(&self, local: &str) -> String {
        format!("m_{}/{}", self.name, local)
    }

    pub fn requires_scripts(&self) -> bool {
        !self.requests.is_empty() || !self.events.on.is_empty()
    }

    pub fn subscriptions(
        &self,
        directory: &Directory,
        default_timeout_ms: u64,
    ) -> Result<Vec<Subscription>> {
        self.events
            .on
            .iter()
            .map(|(event, sub)| {
                let meta = directory
                    .get(event)
                    .ok_or_else(|| ModError::ManifestUnknownEvent {
                        name: event.clone(),
                    })?;
                Ok(Subscription {
                    mod_name: self.name.clone(),
                    mod_version: self.version,
                    event: meta.name,
                    handler: sub.handler.clone(),
                    timeout_ms: sub.timeout_ms.unwrap_or(default_timeout_ms),
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct McpServer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "snake_case")]
pub enum CallableBy {
    Agent,
    Ui,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Request {
    /// js 导出的函数名
    pub handler: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// 输入结构。宿主在调用边界校验
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,

    /// 输出结构。宿主在调用边界校验。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub callable_by: Vec<CallableBy>,

    /// 本次调用的超时，毫秒
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Events {
    #[serde(default)]
    pub on: BTreeMap<String, EventSub>,
    #[serde(default)]
    pub emit: BTreeMap<String, EmitDecl>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventSub {
    /// 宿主调用的 js 函数名
    pub handler: String,

    /// 本订阅的单次时间预算，毫秒
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EmitDecl {
    pub retention: Retention,

    /// 持久化数据的事件名是完整名字（m_<mod_name>/<event_key>）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_key: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "snake_case")]
pub enum Retention {
    /// 需要持久化的事件和 payload
    Journal,
    /// 只推给当前在线的订阅方，允许丢失
    Live,
}

fn check(ok: bool, field: impl ToString, reason: impl ToString) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(ModError::ManifestField {
            field: field.to_string(),
            reason: reason.to_string(),
        })
    }
}

fn name_ok(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    (2..=63).contains(&value.len())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn symbol_ok(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    value.len() <= 63
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn handler_ok(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' || first == '$' => {}
        _ => return false,
    }
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events;
    use serde_json::json;

    #[test]
    fn default_budget_is_twenty_milliseconds_and_explicit_budgets_are_preserved() {
        for (declaration, expected) in [
            (json!({"handler":"run"}), 20),
            (json!({"handler":"run", "timeoutMs":60000}), 60000),
        ] {
            let manifest = Manifest::parse(
                &json!({
                    "name":"demo", "version":1,
                    "events":{"on":{"core/mod.installed":declaration}}
                })
                .to_string(),
            )
            .expect("manifest");
            let subs = manifest
                .subscriptions(&events::directory(), DEFAULT_SUB_TIMEOUT_MS)
                .expect("subscriptions");
            assert_eq!(subs[0].timeout_ms, expected);
        }
    }
}
