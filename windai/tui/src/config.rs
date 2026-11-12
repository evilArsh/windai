use wind_storage::{Model, Provider};

#[derive(Debug, Clone, PartialEq)]
pub struct Selection {
    pub provider: Option<Provider>,
    pub model: Option<Model>,
    pub reasoning_effort: Option<String>,
}

impl Selection {
    pub fn new(provider: Option<Provider>, model: Option<Model>) -> Self {
        let reasoning_effort = model.as_ref().and_then(reasoning_effort);
        Self {
            provider,
            model,
            reasoning_effort,
        }
    }
}

fn reasoning_effort(model: &Model) -> Option<String> {
    model
        .config
        .as_ref()?
        .get("reasoning_effort")?
        .as_str()
        .map(str::to_owned)
}
