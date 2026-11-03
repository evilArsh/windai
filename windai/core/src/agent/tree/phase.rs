use super::domain::Domain;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum CancelOrigin {
    External,
    Ancestor(i64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "phase",
    content = "phase_payload",
    rename_all = "snake_case",
    bound(
        serialize = "D::Suspend: Serialize, D::Output: Serialize, D::Error: Serialize",
        deserialize = "D::Suspend: Deserialize<'de>, D::Output: Deserialize<'de>, D::Error: Deserialize<'de>"
    )
)]
pub enum Phase<D: Domain> {
    Running,
    Suspended(D::Suspend),
    WaitingChildren,
    Done(D::Output),
    Failed(D::Error),
    Cancelled(CancelOrigin),
    Aborted,
}

impl<D: Domain> Phase<D> {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Phase::Done(_) | Phase::Failed(_) | Phase::Cancelled(_) | Phase::Aborted
        )
    }

    pub fn has_live_job(&self) -> bool {
        matches!(self, Phase::Running)
    }

    pub fn parts(&self) -> Result<(String, Option<Value>), serde_json::Error> {
        let value = serde_json::to_value(self)?;
        let Parts {
            phase,
            phase_payload,
        } = serde_json::from_value(value)?;
        Ok((phase, phase_payload))
    }

    pub fn columns(&self) -> Result<(String, Option<String>), serde_json::Error> {
        let (phase, payload) = self.parts()?;
        Ok((phase, payload.map(|payload| payload.to_string())))
    }

    pub fn from_columns(name: &str, payload: Option<&str>) -> Result<Self, serde_json::Error> {
        let mut fields = Map::new();
        fields.insert("phase".to_string(), Value::String(name.to_string()));
        if let Some(payload) = payload {
            fields.insert("phase_payload".to_string(), serde_json::from_str(payload)?);
        }
        serde_json::from_value(Value::Object(fields))
    }
}

#[derive(Deserialize)]
struct Parts {
    phase: String,
    phase_payload: Option<Value>,
}
