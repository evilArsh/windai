//! 注册表测试共用的事件与 mod 替身

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use serde_json::Value;
use wind_event::{
    CallCx, CallError, Dispatcher, EnabledSet, EventOutlet, Limits, ModCaller, ModRoster, Pending,
    Scope, SubmitError, Subscription,
};

/// 记录事件名与载荷的出口
#[derive(Default)]
pub struct RecordingOutlet {
    events: Mutex<Vec<(String, Option<Value>)>>,
}

impl RecordingOutlet {
    pub fn names(&self) -> Vec<String> {
        self.events
            .lock()
            .expect("the event log must lock")
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }
}

impl EventOutlet for RecordingOutlet {
    fn submit(&self, pending: Pending) -> Result<(), SubmitError> {
        self.events
            .lock()
            .expect("the event log must lock")
            .push((pending.envelope.name, pending.envelope.payload));
        Ok(())
    }
}

/// 没有任何启用的 mod
pub struct EmptyRoster;

impl ModRoster for EmptyRoster {
    fn enabled(&self, _scope: &Scope) -> EnabledSet {
        EnabledSet::none()
    }
}

/// 不支持 mod 调用
pub struct NoCaller;

#[async_trait::async_trait]
impl ModCaller for NoCaller {
    async fn call(
        &self,
        _sub: &Subscription,
        _payload: Value,
        _cx: CallCx,
    ) -> Result<Option<Value>, CallError> {
        Err(CallError::Internal(
            "mod calls are not supported".to_string(),
        ))
    }
}

/// 构造只用于 MCP 事件的分发器
pub fn dispatcher(outlet: Arc<RecordingOutlet>) -> Arc<Dispatcher> {
    Arc::new(Dispatcher::new(
        wind_mcp::events::directory(),
        Arc::new(EmptyRoster),
        Arc::new(NoCaller),
        outlet,
        Limits::default(),
    ))
}
