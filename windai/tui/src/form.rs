use serde_json::Value;
use wind_storage::{Model, Provider};
use wind_wire::AdapterType;

pub type ValidationError = (usize, String);
pub const ADAPTERS: [AdapterType; 2] = [AdapterType::OpenAICompletion, AdapterType::OpenAIResponse];

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderFields {
    pub name: String,
    pub base_url: String,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelFields {
    pub provider_id: i64,
    pub name: String,
    pub adapter: AdapterType,
    pub endpoint: Option<String>,
    pub config: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProviderForm {
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub focus: usize,
    pub error: Option<String>,
    pub cursor: usize,
    pub editing: Option<i64>,
}

impl ProviderForm {
    pub fn edit(provider: &Provider) -> Self {
        Self {
            name: provider.name.clone(),
            base_url: provider.base_url.clone(),
            editing: Some(provider.id),
            ..Self::default()
        }
    }

    pub fn validate(&self, providers: &[Provider]) -> Result<ProviderFields, ValidationError> {
        let name = self.name.trim();
        if name.is_empty() {
            return Err((0, "name is required".to_owned()));
        }
        if providers
            .iter()
            .any(|provider| provider.name == name && Some(provider.id) != self.editing)
        {
            return Err((0, "provider name already exists".to_owned()));
        }
        let base_url = self.base_url.trim();
        if base_url.is_empty() {
            return Err((1, "base_url is required".to_owned()));
        }
        if !is_http_url(base_url) {
            return Err((1, "base_url must start with http:// or https://".to_owned()));
        }
        let api_key = self.api_key.trim();
        Ok(ProviderFields {
            name: name.to_owned(),
            base_url: base_url.to_owned(),
            api_key: (!api_key.is_empty()).then(|| api_key.to_owned()),
        })
    }

    pub fn insert_char(&mut self, ch: char) {
        if ch.is_control() {
            return;
        }
        let cursor = self.cursor;
        let Some(text) = self.text_mut() else {
            return;
        };
        let byte = char_to_byte(text, cursor);
        text.insert(byte, ch);
        self.cursor = cursor + 1;
    }

    pub fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let cursor = self.cursor;
        let Some(text) = self.text_mut() else {
            return;
        };
        let start = char_to_byte(text, cursor - 1);
        let end = char_to_byte(text, cursor);
        text.replace_range(start..end, "");
        self.cursor = cursor - 1;
    }

    pub fn delete(&mut self) {
        let cursor = self.cursor;
        let Some(text) = self.text_mut() else {
            return;
        };
        if cursor >= text.chars().count() {
            return;
        }
        let start = char_to_byte(text, cursor);
        let end = char_to_byte(text, cursor + 1);
        text.replace_range(start..end, "");
    }

    pub fn move_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub fn move_right(&mut self) {
        let length = self.text().map(|text| text.chars().count()).unwrap_or(0);
        self.cursor = (self.cursor + 1).min(length);
    }

    pub fn home(&mut self) {
        self.cursor = 0;
    }

    pub fn end(&mut self) {
        self.cursor = self.text().map(|text| text.chars().count()).unwrap_or(0);
    }

    pub fn next_field(&mut self) {
        self.focus = (self.focus + 1).min(2);
        self.end();
    }

    pub fn previous_field(&mut self) {
        self.focus = self.focus.saturating_sub(1);
        self.end();
    }

    pub fn insert_str(&mut self, text: &str) {
        for ch in text.chars() {
            self.insert_char(ch);
        }
    }

    fn text(&self) -> Option<&String> {
        match self.focus {
            0 => Some(&self.name),
            1 => Some(&self.base_url),
            2 => Some(&self.api_key),
            _ => None,
        }
    }

    fn text_mut(&mut self) -> Option<&mut String> {
        match self.focus {
            0 => Some(&mut self.name),
            1 => Some(&mut self.base_url),
            2 => Some(&mut self.api_key),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ModelForm {
    pub provider_index: usize,
    pub name: String,
    pub adapter_index: usize,
    pub endpoint: String,
    pub config: String,
    pub focus: usize,
    pub error: Option<String>,
    pub cursor: usize,
    pub editing: Option<i64>,
}

impl ModelForm {
    pub fn edit(model: &Model, providers: &[Provider]) -> Self {
        Self {
            provider_index: providers
                .iter()
                .position(|provider| provider.id == model.provider_id)
                .unwrap_or(0),
            name: model.name.clone(),
            adapter_index: ADAPTERS
                .iter()
                .position(|adapter| *adapter == model.adapter)
                .unwrap_or(0),
            endpoint: model.endpoint.clone().unwrap_or_default(),
            config: model
                .config
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            // 字段顺序：提供商固定，焦点从名称开始
            focus: 1,
            editing: Some(model.id),
            ..Self::default()
        }
    }

    pub fn validate(
        &self,
        providers: &[Provider],
        models: &[Model],
    ) -> Result<ModelFields, ValidationError> {
        let Some(provider) = providers.get(self.provider_index) else {
            return Err((0, "provider is required".to_owned()));
        };
        let name = self.name.trim();
        if name.is_empty() {
            return Err((1, "name is required".to_owned()));
        }
        if models
            .iter()
            .any(|model| model.name == name && Some(model.id) != self.editing)
        {
            return Err((1, "model name already exists".to_owned()));
        }
        let Some(adapter) = ADAPTERS.get(self.adapter_index).copied() else {
            return Err((2, "adapter is required".to_owned()));
        };
        let endpoint = self.endpoint.trim();
        if !endpoint.is_empty() && !is_http_url(endpoint) {
            return Err((3, "endpoint must start with http:// or https://".to_owned()));
        }
        let config = self.config.trim();
        let config = if config.is_empty() {
            None
        } else {
            let value: Value = serde_json::from_str(config)
                .map_err(|_| (4, "config must be valid JSON".to_owned()))?;
            if !value.is_object() {
                return Err((4, "config must be a JSON object".to_owned()));
            }
            Some(value)
        };
        Ok(ModelFields {
            provider_id: provider.id,
            name: name.to_owned(),
            adapter,
            endpoint: (!endpoint.is_empty()).then(|| endpoint.to_owned()),
            config,
        })
    }

    pub fn insert_char(&mut self, ch: char) {
        if ch.is_control() {
            return;
        }
        let cursor = self.cursor;
        let Some(text) = self.text_mut() else {
            return;
        };
        let byte = char_to_byte(text, cursor);
        text.insert(byte, ch);
        self.cursor = cursor + 1;
    }

    pub fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let cursor = self.cursor;
        let Some(text) = self.text_mut() else {
            return;
        };
        let start = char_to_byte(text, cursor - 1);
        let end = char_to_byte(text, cursor);
        text.replace_range(start..end, "");
        self.cursor = cursor - 1;
    }

    pub fn delete(&mut self) {
        let cursor = self.cursor;
        let Some(text) = self.text_mut() else {
            return;
        };
        if cursor >= text.chars().count() {
            return;
        }
        let start = char_to_byte(text, cursor);
        let end = char_to_byte(text, cursor + 1);
        text.replace_range(start..end, "");
    }

    pub fn move_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub fn move_right(&mut self) {
        let length = self.text().map(|text| text.chars().count()).unwrap_or(0);
        self.cursor = (self.cursor + 1).min(length);
    }

    pub fn home(&mut self) {
        self.cursor = 0;
    }

    pub fn end(&mut self) {
        self.cursor = self.text().map(|text| text.chars().count()).unwrap_or(0);
    }

    pub fn next_field(&mut self) {
        self.focus = (self.focus + 1).min(4);
        self.end();
    }

    pub fn previous_field(&mut self) {
        self.focus = self.focus.saturating_sub(1);
        self.end();
    }

    pub fn insert_str(&mut self, text: &str) {
        for ch in text.chars() {
            self.insert_char(ch);
        }
    }

    pub fn cycle_provider(&mut self, providers: &[Provider], step: isize) {
        if self.editing.is_some() || providers.is_empty() {
            return;
        }
        let length = providers.len() as isize;
        self.provider_index = (self.provider_index as isize + step).rem_euclid(length) as usize;
    }

    pub fn cycle_adapter(&mut self, step: isize) {
        let length = ADAPTERS.len() as isize;
        self.adapter_index = (self.adapter_index as isize + step).rem_euclid(length) as usize;
    }

    fn text(&self) -> Option<&String> {
        match self.focus {
            1 => Some(&self.name),
            3 => Some(&self.endpoint),
            4 => Some(&self.config),
            _ => None,
        }
    }

    fn text_mut(&mut self) -> Option<&mut String> {
        match self.focus {
            1 => Some(&mut self.name),
            3 => Some(&mut self.endpoint),
            4 => Some(&mut self.config),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Form {
    Provider(ProviderForm),
    Model(ModelForm),
}

impl Form {
    pub fn title(&self) -> &'static str {
        match self {
            Form::Provider(form) if form.editing.is_some() => "edit provider",
            Form::Provider(_) => "new provider",
            Form::Model(form) if form.editing.is_some() => "edit model",
            Form::Model(_) => "new model",
        }
    }

    pub fn editing(&self) -> Option<i64> {
        match self {
            Form::Provider(form) => form.editing,
            Form::Model(form) => form.editing,
        }
    }

    pub fn insert_char(&mut self, ch: char) {
        match self {
            Form::Provider(form) => form.insert_char(ch),
            Form::Model(form) => form.insert_char(ch),
        }
    }

    pub fn insert_str(&mut self, text: &str) {
        match self {
            Form::Provider(form) => form.insert_str(text),
            Form::Model(form) => form.insert_str(text),
        }
    }

    pub fn backspace(&mut self) {
        match self {
            Form::Provider(form) => form.backspace(),
            Form::Model(form) => form.backspace(),
        }
    }

    pub fn delete(&mut self) {
        match self {
            Form::Provider(form) => form.delete(),
            Form::Model(form) => form.delete(),
        }
    }

    pub fn move_left(&mut self, providers: &[Provider]) {
        match self {
            Form::Provider(form) => form.move_left(),
            Form::Model(form) => match form.focus {
                0 => form.cycle_provider(providers, -1),
                2 => form.cycle_adapter(-1),
                _ => form.move_left(),
            },
        }
    }

    pub fn move_right(&mut self, providers: &[Provider]) {
        match self {
            Form::Provider(form) => form.move_right(),
            Form::Model(form) => match form.focus {
                0 => form.cycle_provider(providers, 1),
                2 => form.cycle_adapter(1),
                _ => form.move_right(),
            },
        }
    }

    pub fn home(&mut self) {
        match self {
            Form::Provider(form) => form.home(),
            Form::Model(form) => form.home(),
        }
    }

    pub fn end(&mut self) {
        match self {
            Form::Provider(form) => form.end(),
            Form::Model(form) => form.end(),
        }
    }

    pub fn next_field(&mut self) {
        match self {
            Form::Provider(form) => form.next_field(),
            Form::Model(form) => form.next_field(),
        }
    }

    pub fn previous_field(&mut self) {
        match self {
            Form::Provider(form) => form.previous_field(),
            Form::Model(form) => form.previous_field(),
        }
    }

    pub fn set_error(&mut self, field: usize, message: String) {
        match self {
            Form::Provider(form) => {
                form.focus = field.min(2);
                form.error = Some(message);
                form.end();
            }
            Form::Model(form) => {
                form.focus = field.min(4);
                form.error = Some(message);
                form.end();
            }
        }
    }

    pub fn clear_error(&mut self) {
        match self {
            Form::Provider(form) => form.error = None,
            Form::Model(form) => form.error = None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Form::Provider(form) => form.error.as_deref(),
            Form::Model(form) => form.error.as_deref(),
        }
    }
}

fn is_http_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn char_to_byte(text: &str, cursor: usize) -> usize {
    text.char_indices()
        .nth(cursor)
        .map(|(index, _)| index)
        .unwrap_or(text.len())
}
