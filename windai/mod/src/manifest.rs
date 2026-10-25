use super::capability::Capability;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use url::Url;
use wind_event::def::{Directory, Shape};
use wind_event::subscription::{Phase as SubPhase, Subscription};

/// mod 能力清单
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Manifest {
    /// mod 名，系统唯一
    pub name: String,
    /// 版本号，单调递增
    pub version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 声明需要的宿主能力
    ///
    /// - 声明后才会出现在处理函数的 ctx 上。
    /// - intercept 形态的处理函数不注入任何 I/O 能力，
    /// 因此声明了 "http" 的 Mod，其 intercept 处理函数中依然没有 ctx.http
    #[serde(default)]
    pub capabilities: Vec<Capability>,
    /// mod 提供的可注入提示词
    #[serde(default)]
    pub prompts: BTreeMap<String, Prompt>,
    /// mcp 服务声明
    /// - Stdio 服务
    /// - Streamable 服务
    /// - `mcp/<name>` 中的服务。TODO: 目前只支持可执行文件形式的服务
    #[serde(default)]
    pub mcp_servers: BTreeMap<String, McpServer>,
    /// mod 暴露的 js 函数调用入口
    #[serde(default)]
    pub requests: BTreeMap<String, Request>,
    /// 事件处理
    ///
    /// # on
    /// 订阅宿主的事件，订阅后宿主会唤醒 mod 并且执行 `handler` 中声明的 js 函数
    /// # emit
    /// js 向宿主发送事件的通道
    #[serde(default)]
    pub events: Events,
    /// 共享 schema 字典
    #[serde(default)]
    pub defs: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Prompt {
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Request {
    pub handler: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub callable_by: Vec<CallableBy>,
    /// 本次调用的超时，毫秒
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "snake_case")]
pub enum CallableBy {
    Agent,
    Ui,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Events {
    #[serde(default)]
    pub on: BTreeMap<String, EventSub>,
    #[serde(default)]
    pub emit: BTreeMap<String, EmitDecl>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventSub {
    /// 宿主调用的 js 函数名
    pub handler: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,
    /// 本订阅的单次时间预算，毫秒
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "snake_case")]
pub enum Phase {
    /// 安全与权限，最先执行
    Guard,
    /// 业务策略，在安全之后执行
    Policy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EmitDecl {
    pub delivery: Delivery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "snake_case")]
pub enum Delivery {
    /// 只推给当前在线的订阅方，允许丢失
    Notice,
    /// 先写回当前值再推出，新订阅方先收到基线
    Stateful,
}

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
pub const DEFAULT_SUB_TIMEOUT_MS: u64 = 1_000;
const MIN_URL_LEN: usize = 8;
const MAX_DESCRIPTION_LEN: usize = 500;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest does not match its declared shape: {0}")]
    Shape(String),
    #[error("field {field} is invalid: {reason}")]
    Field { field: String, reason: String },
    #[error("event {name} is not in the directory")]
    UnknownEvent { name: String },
}

pub fn parse(raw: &str, directory: &Directory) -> Result<Manifest, ManifestError> {
    let manifest: Manifest =
        serde_json::from_str(raw).map_err(|error| ManifestError::Shape(error.to_string()))?;
    manifest.validate(directory)?;
    Ok(manifest)
}

impl Manifest {
    pub fn validate(&self, directory: &Directory) -> Result<(), ManifestError> {
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
                "must be at most 500 characters",
            )?;
        }
        let mut seen = BTreeSet::new();
        for capability in &self.capabilities {
            check(
                seen.insert(*capability),
                "capabilities",
                "the same capability must not be declared twice",
            )?;
        }
        for key in self.prompts.keys() {
            check(symbol_ok(key), &format!("prompts.{key}"), SYMBOL_HINT)?;
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
                    return Err(ManifestError::Field {
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
                &format!("{field}.handler"),
                HANDLER_HINT,
            )?;
            if let Some(timeout) = request.timeout_ms {
                check(
                    (1..=MAX_REQUEST_TIMEOUT_MS).contains(&timeout),
                    &format!("{field}.timeoutMs"),
                    "must be between 1 and 300000",
                )?;
            }
        }
        let mut state_keys = BTreeSet::new();
        for (key, decl) in &self.events.emit {
            let field = format!("events.emit.{key}");
            check(symbol_ok(key), &field, SYMBOL_HINT)?;
            check(
                decl.payload.is_some(),
                &format!("{field}.payload"),
                "a payload schema is required",
            )?;
            match decl.delivery {
                Delivery::Stateful => {
                    let state_key = decl.state_key.as_deref().unwrap_or_default();
                    check(
                        decl.state_key.is_some(),
                        &format!("{field}.stateKey"),
                        "a stateful delivery requires a key",
                    )?;
                    check(
                        state_keys.insert(state_key.to_string()),
                        &format!("{field}.stateKey"),
                        "two stateful channels must not declare the same state key",
                    )?;
                }
                Delivery::Notice => {}
            }
        }
        for (name, sub) in &self.events.on {
            let field = format!("events.on.{name}");
            check(
                name.starts_with("core/"),
                &field,
                "only host events can be subscribed",
            )?;
            let meta = directory
                .get(name)
                .ok_or_else(|| ManifestError::UnknownEvent { name: name.clone() })?;
            check(
                handler_ok(&sub.handler),
                &format!("{field}.handler"),
                HANDLER_HINT,
            )?;
            if let Some(timeout) = sub.timeout_ms {
                check(
                    (1..=MAX_SUB_TIMEOUT_MS).contains(&timeout),
                    &format!("{field}.timeoutMs"),
                    "must be between 1 and 60000",
                )?;
            }
            match meta.shape {
                Shape::Intercept => {
                    check(
                        sub.phase.is_some(),
                        &field,
                        "an intercept subscription requires a phase",
                    )?;
                }
                Shape::Observe => {
                    check(sub.phase.is_none(), &field, "an observe event has no phase")?;
                }
            }
        }
        for key in self.defs.keys() {
            check(symbol_ok(key), &format!("defs.{key}"), SYMBOL_HINT)?;
        }
        Ok(())
    }

    pub fn stateful_channels(&self) -> impl Iterator<Item = (&str, String)> {
        self.events
            .emit
            .iter()
            .filter_map(|(name, decl)| match decl.delivery {
                Delivery::Stateful => Some((name.as_str(), self.event_name(name))),
                Delivery::Notice => None,
            })
    }

    pub fn event_name(&self, local: &str) -> String {
        format!("m_{}/{}", self.name, local)
    }

    pub fn subscriptions(
        &self,
        directory: &Directory,
        default_timeout_ms: u64,
    ) -> Vec<Subscription> {
        let version = u32::try_from(self.version).unwrap_or(u32::MAX);
        self.events
            .on
            .iter()
            .filter_map(|(event, sub)| {
                let meta = directory.get(event)?;
                Some(Subscription {
                    mod_name: self.name.clone(),
                    mod_version: version,
                    event: meta.name,
                    phase: sub.phase.map(|phase| match phase {
                        Phase::Guard => SubPhase::Guard,
                        Phase::Policy => SubPhase::Policy,
                    }),
                    handler: sub.handler.clone(),
                    timeout_ms: sub.timeout_ms.unwrap_or(default_timeout_ms),
                    registration_seq: 1,
                })
            })
            .collect()
    }
}

fn check(ok: bool, field: &str, reason: &str) -> Result<(), ManifestError> {
    if ok {
        Ok(())
    } else {
        Err(ManifestError::Field {
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
    use wind_event::def::{EventDef, Mount};

    /// 目录里一条拦截事件与一条观测事件
    fn directory() -> Directory {
        fn schema() -> serde_json::Value {
            serde_json::json!({ "type": "object" })
        }
        Directory::from_metas([
            EventDef::<serde_json::Value>::new(
                "core/mcp.tool.call",
                Shape::Intercept,
                false,
                Mount::Subsystem,
                schema,
            )
            .meta(),
            EventDef::<serde_json::Value>::new(
                "core/mod.notify",
                Shape::Observe,
                true,
                Mount::Subsystem,
                schema,
            )
            .meta(),
        ])
    }

    #[test]
    fn subscriptions_carry_the_static_names_the_directory_holds() {
        // 清单里的键是运行期数据，而派发器的索引按静态事件名建立，这一步把两者
        // 对应起来
        let manifest = parse(
            r#"{"name":"guard","version":3,
                "events":{"on":{"core/mcp.tool.call":{"handler":"onCall","phase":"policy",
                                                      "timeoutMs":250},
                                "core/mod.notify":{"handler":"onNotify"}}}}"#,
            &directory(),
        )
        .expect("the manifest is valid");
        let subs = manifest.subscriptions(&directory(), DEFAULT_SUB_TIMEOUT_MS);
        assert_eq!(subs.len(), 2);

        let call = subs
            .iter()
            .find(|sub| sub.handler == "onCall")
            .expect("find the intercept subscription");
        assert_eq!(call.event, "core/mcp.tool.call");
        assert_eq!(call.mod_name, "guard");
        assert_eq!(call.mod_version, 3);
        assert_eq!(call.phase, Some(SubPhase::Policy));
        assert_eq!(call.timeout_ms, 250);

        let notify = subs
            .iter()
            .find(|sub| sub.handler == "onNotify")
            .expect("find the observe subscription");
        assert_eq!(notify.phase, None, "an observe event has no phase");
        assert_eq!(
            notify.timeout_ms, DEFAULT_SUB_TIMEOUT_MS,
            "without a declared time budget, the host-provided default applies"
        );
    }
}
