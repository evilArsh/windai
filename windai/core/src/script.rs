use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wind_event::{
    CallCx, CallError, Directory, DispatchCx, EventDef, EventOutlet, Guards, Limits, ModCaller,
    ModEmitter, ModIdentity, Origin, PayloadValidator, Pending, Retention, Scope, Shape, Source,
    Subscription,
};
use wind_mod::manifest::{Manifest, Retention as ManifestRetention};
use wind_script::{ScriptCall, ScriptHost, ScriptScope, ScriptService, ScriptsError};
use wind_storage::Storage;
use wind_wire::AppDirs;
use wind_wire::event::{Envelope, Target};

/// 脚本发来的通知
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CoreModNotify {
    pub level: String,
    pub text: String,
}

/// 一条 mod 通知
pub const CORE_MOD_NOTIFY: EventDef<CoreModNotify> = EventDef::new(
    "core/mod.notify",
    Shape::Observe,
    true,
    wind_event::schema::<CoreModNotify>,
);

pub fn directory() -> Directory {
    Directory::from_metas([CORE_MOD_NOTIFY.meta()])
}

/// 脚本与事件总线的接线
pub struct ScriptRuntime {
    storage: Storage,
    dirs: AppDirs,
    scripts: Option<Arc<dyn ScriptService>>,
    sink: Arc<EventSink>,
    guards: Arc<Guards>,
    emitters: Mutex<HashMap<(String, u32), Arc<ModEmitter>>>,
}

impl ScriptRuntime {
    pub fn new(
        storage: Storage,
        dirs: AppDirs,
        outlet: Arc<dyn EventOutlet>,
        limits: Limits,
        scripts: Option<Arc<dyn ScriptService>>,
    ) -> Self {
        let guards = Arc::new(Guards::new(limits));
        let sink = Arc::new(EventSink::new(outlet, Arc::clone(&guards)));
        Self {
            storage,
            dirs,
            scripts,
            sink,
            guards,
            emitters: Mutex::new(HashMap::new()),
        }
    }

    fn emitter(&self, manifest: &Manifest) -> Arc<ModEmitter> {
        let key = (manifest.name.clone(), manifest.version);
        let mut emitters = self
            .emitters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(emitter) = emitters.get(&key) {
            return Arc::clone(emitter);
        }
        let channels = manifest
            .events
            .emit
            .iter()
            .map(|(name, decl)| {
                let (event, retention) = match decl.retention {
                    ManifestRetention::Journal => (
                        format!(
                            "m_{}/{}",
                            manifest.name,
                            decl.event_key.as_deref().unwrap_or(name)
                        ),
                        Retention::Journal,
                    ),
                    ManifestRetention::Live => {
                        (format!("m_{}/{}", manifest.name, name), Retention::Live)
                    }
                };
                wind_event::EmitChannel::new(
                    name.clone(),
                    event,
                    retention,
                    validator(decl.payload.as_ref()),
                )
            })
            .collect();
        let emitter = Arc::new(ModEmitter::new(
            ModIdentity {
                name: manifest.name.clone(),
                version: manifest.version,
            },
            channels,
            self.sink.outlet(),
            Arc::clone(&self.guards),
        ));
        emitters.insert(key, Arc::clone(&emitter));
        emitter
    }
}

#[async_trait]
impl ModCaller for ScriptRuntime {
    async fn call(
        &self,
        sub: &Subscription,
        payload: Value,
        call_cx: CallCx,
    ) -> Result<Option<Value>, CallError> {
        let row = self
            .storage
            .mod_by_name(&sub.mod_name)
            .await
            .map_err(|error| CallError::Internal(error.to_string()))?
            .ok_or_else(|| CallError::Internal(format!("mod {} is not installed", sub.mod_name)))?;
        if row.version != sub.mod_version {
            return Err(CallError::Internal(format!(
                "stale subscription: {} names version {} but {} is installed",
                sub.mod_name, sub.mod_version, row.version
            )));
        }
        let manifest = Manifest::parse(&row.manifest)
            .map_err(|error| CallError::Internal(error.to_string()))?;
        let emitter = self.emitter(&manifest);
        let source = Source::module(row.name.clone(), row.version);
        let cx = DispatchCx::root(
            Origin::Internal,
            call_cx.scope.clone(),
            source.clone(),
            call_cx.remaining(),
        );
        let host = Arc::new(Host {
            emitter,
            sink: Arc::clone(&self.sink),
            cx,
            source,
        });
        let Some(scripts) = &self.scripts else {
            return Err(CallError::Internal(
                "no script service is available".to_string(),
            ));
        };
        let scope = match &call_cx.scope {
            Scope::Global => ScriptScope::Global,
            Scope::Topic {
                topic_id,
                instance_id,
            } => ScriptScope::Topic {
                topic_id: *topic_id,
                instance_id: *instance_id,
            },
        };
        let call = ScriptCall {
            mod_name: row.name.clone(),
            mod_version: row.version,
            scope,
            root: self.dirs.mod_version(&row.name, row.version),
            handler: sub.handler.clone(),
            deadline: call_cx.deadline.into_std(),
            cancel: call_cx.cancel.clone(),
            input: payload,
            host,
        };
        scripts.call(call).await.map_err(map_error)
    }
}

fn map_error(error: ScriptsError) -> CallError {
    match error {
        ScriptsError::Handler(_)
        | ScriptsError::Compile(_)
        | ScriptsError::PayloadTooLarge { .. }
        | ScriptsError::Suspended => CallError::Handler(error.to_string()),
        ScriptsError::Timeout
        | ScriptsError::Busy
        | ScriptsError::Internal(_)
        | ScriptsError::Config(_) => CallError::Internal(error.to_string()),
    }
}

/// 宿主通知与 mod 事件的出口
struct EventSink {
    outlet: Arc<dyn EventOutlet>,
    guards: Arc<Guards>,
}

impl EventSink {
    fn new(outlet: Arc<dyn EventOutlet>, guards: Arc<Guards>) -> Self {
        Self { outlet, guards }
    }

    fn notify(
        &self,
        cx: &DispatchCx,
        source: Source,
        level: &str,
        text: &str,
    ) -> Result<(), ScriptsError> {
        self.guards
            .admit(cx)
            .map_err(|error| ScriptsError::Handler(error.to_string()))?;
        let payload = serde_json::to_value(CoreModNotify {
            level: level.to_string(),
            text: text.to_string(),
        })
        .map_err(|error| ScriptsError::Internal(error.to_string()))?;
        let envelope = Envelope::new(
            CORE_MOD_NOTIFY.name,
            cx.scope.clone(),
            cx.origin,
            source,
            Some(payload),
            Shape::Observe,
        );
        self.outlet
            .submit(Pending {
                envelope,
                retention: Retention::Journal,
                target: Target::Core,
            })
            .map_err(|error| ScriptsError::Internal(error.to_string()))
    }

    fn outlet(&self) -> Arc<dyn EventOutlet> {
        Arc::clone(&self.outlet)
    }
}

/// 一次脚本调用可用的宿主入口
struct Host {
    emitter: Arc<ModEmitter>,
    sink: Arc<EventSink>,
    cx: DispatchCx,
    source: Source,
}

impl ScriptHost for Host {
    fn emit(&self, channel: &str, payload: Value) -> Result<(), ScriptsError> {
        self.emitter
            .emit(&self.cx, channel, payload)
            .map_err(|error| ScriptsError::Handler(error.to_string()))
    }

    fn notify(&self, level: &str, text: &str) -> Result<(), ScriptsError> {
        self.sink.notify(&self.cx, self.source.clone(), level, text)
    }
}

/// 按 schema 的顶层 type 检查载荷
fn validator(schema: Option<&Value>) -> PayloadValidator {
    let kinds = schema.map(kinds_of).unwrap_or_default();
    Arc::new(move |payload| {
        if kinds.is_empty() || kinds.iter().any(|kind| accepts(kind, payload)) {
            Ok(())
        } else {
            Err(format!("the payload must be {}", kinds.join(" or ")))
        }
    })
}

fn kinds_of(schema: &Value) -> Vec<String> {
    match schema.get("type") {
        Some(Value::String(kind)) => vec![kind.clone()],
        Some(Value::Array(kinds)) => kinds
            .iter()
            .filter_map(|kind| kind.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

fn accepts(kind: &str, payload: &Value) -> bool {
    match kind {
        "integer" => payload.as_i64().is_some() || payload.as_u64().is_some(),
        "number" => payload.is_number(),
        "object" => payload.is_object(),
        "array" => payload.is_array(),
        "string" => payload.is_string(),
        "boolean" => payload.is_boolean(),
        "null" => payload.is_null(),
        _ => true,
    }
}
