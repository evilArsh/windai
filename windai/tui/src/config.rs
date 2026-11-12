use wind_storage::{Model, Provider};

const REASONING_VALUES: [&str; 4] = ["low", "medium", "high", "max"];

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

    pub fn select_provider(
        &mut self,
        providers: &[Provider],
        models: &[Model],
        provider_id: i64,
    ) -> Option<String> {
        let provider = providers
            .iter()
            .find(|provider| provider.id == provider_id)?
            .clone();
        let model = models
            .iter()
            .filter(|model| model.provider_id == provider_id)
            .min_by_key(|model| model.id)
            .cloned();
        self.provider = Some(provider);
        self.model = model.clone();
        self.reasoning_effort = model.as_ref().and_then(reasoning_effort);
        match model {
            Some(_) => None,
            None => Some("this provider has no models".to_owned()),
        }
    }

    pub fn select_model(&mut self, models: &[Model], model_id: i64) -> Option<String> {
        let Some(provider) = self.provider.as_ref() else {
            return Some("no provider selected".to_owned());
        };
        let Some(model) = models
            .iter()
            .find(|model| model.id == model_id && model.provider_id == provider.id)
            .cloned()
        else {
            return Some("unknown model".to_owned());
        };
        self.reasoning_effort = reasoning_effort(&model);
        self.model = Some(model);
        None
    }

    pub fn reasoning_values(&self) -> Vec<String> {
        let mut values: Vec<String> = REASONING_VALUES
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
        if let Some(current) = &self.reasoning_effort
            && !values.iter().any(|value| value == current)
        {
            values.push(current.clone());
        }
        values
    }

    pub fn cycle_reasoning(&mut self) {
        self.cycle_reasoning_by(1);
    }

    pub fn cycle_reasoning_back(&mut self) {
        self.cycle_reasoning_by(-1);
    }

    fn cycle_reasoning_by(&mut self, step: isize) {
        let values = self.reasoning_values();
        if values.is_empty() {
            return;
        }
        let current = self
            .reasoning_effort
            .as_ref()
            .and_then(|current| values.iter().position(|value| value == current));
        let index = match current {
            Some(index) => (index as isize + step).rem_euclid(values.len() as isize) as usize,
            None => 0,
        };
        self.reasoning_effort = Some(values[index].clone());
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
