use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use wind_event::host::CallMode;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, strum::AsRefStr,
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

/// 一次调用里存在的属性集合
#[derive(Debug, Clone)]
pub struct CtxSet {
    present: BTreeSet<Prop>,
}

impl CtxSet {
    pub fn has(&self, prop: Prop) -> bool {
        self.present.contains(&prop)
    }

    pub fn iter(&self) -> impl Iterator<Item = Prop> + '_ {
        self.present.iter().copied()
    }
}

pub fn available(declared: &[Capability], mode: CallMode) -> CtxSet {
    let mut present = BTreeSet::new();
    present.insert(Prop::Emit);
    present.insert(Prop::Notify);
    present.insert(Prop::Log);
    for capability in declared {
        let prop = Prop::from(*capability);
        if !mode.allows_io() && prop.needs_io() {
            continue;
        }
        present.insert(prop);
    }
    CtxSet { present }
}
