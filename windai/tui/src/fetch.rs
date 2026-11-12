use crate::app::{BlockView, MessageView, ModView, NodeView, phase_detail};
use crate::config::Selection;
use std::collections::HashSet;
use wind_core::agent::Phase;
use wind_core::llm::domain::LlmDomain;
use wind_storage::error::StorageError;
use wind_storage::{AgentInstance, ContextRow, Model, Provider, Storage, Topic};
use wind_wire::{AgentRole, Usage};

pub struct Startup {
    pub selection: Selection,
    pub providers: Vec<Provider>,
    pub models: Vec<Model>,
    pub notice: Option<String>,
}

pub struct LoadedSession {
    pub topic: Topic,
    pub main: Option<i64>,
    pub messages: Vec<MessageView>,
    pub nodes: Vec<NodeView>,
    pub mods: Vec<ModView>,
    pub usage: Usage,
}

pub async fn load_startup(storage: &Storage) -> Result<Startup, StorageError> {
    let providers = storage.list_providers().await?;
    let Some(first_provider) = providers.first() else {
        return Ok(Startup {
            selection: Selection::new(None, None),
            providers,
            models: Vec::new(),
            notice: Some("no providers configured".to_owned()),
        });
    };
    let all_models = storage.list_models(None).await?;
    let topics = storage.list_topics().await?;
    let latest = topics
        .first()
        .and_then(|topic| topic.model_id)
        .and_then(|model_id| all_models.iter().find(|model| model.id == model_id));
    let selection = match latest.and_then(|model| {
        providers
            .iter()
            .find(|provider| provider.id == model.provider_id)
            .map(|provider| Selection::new(Some(provider.clone()), Some(model.clone())))
    }) {
        Some(selection) => selection,
        None => {
            let model = all_models
                .iter()
                .find(|model| model.provider_id == first_provider.id)
                .cloned();
            Selection::new(Some(first_provider.clone()), model)
        }
    };
    let models = match &selection.provider {
        Some(provider) => all_models
            .into_iter()
            .filter(|model| model.provider_id == provider.id)
            .collect(),
        None => Vec::new(),
    };
    Ok(Startup {
        selection,
        providers,
        models,
        notice: None,
    })
}

pub async fn load_session(storage: &Storage, topic_id: i64) -> Result<LoadedSession, StorageError> {
    let topic = storage
        .get_topic_by_id(topic_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "topic".to_owned(),
            key: topic_id.to_string(),
        })?;
    let instances = storage.list_instances_by_topic(topic_id).await?;
    let main = instances
        .iter()
        .find(|instance| instance.role == AgentRole::Main)
        .map(|instance| instance.id);
    let nodes = instances.into_iter().map(node_view).collect();
    let mods = load_mods(storage, topic_id).await?;
    let (messages, usage) = match main {
        Some(main_id) => {
            let rows = storage.list_instance_context(main_id).await?;
            group_context(rows)
        }
        None => (
            Vec::new(),
            Usage {
                input_tokens: 0,
                output_tokens: 0,
            },
        ),
    };
    Ok(LoadedSession {
        topic,
        main,
        messages,
        nodes,
        mods,
        usage,
    })
}

pub(crate) async fn load_mods(
    storage: &Storage,
    topic_id: i64,
) -> Result<Vec<ModView>, StorageError> {
    let mods = storage
        .enabled_mods(topic_id)
        .await?
        .into_iter()
        .map(|module| ModView {
            name: module.name,
            version: module.version,
        })
        .collect();
    Ok(mods)
}

fn node_view(instance: AgentInstance) -> NodeView {
    let (suspended_wait, reason) = match Phase::<LlmDomain>::from_columns(
        &instance.phase,
        instance.phase_payload.as_deref(),
    ) {
        Ok(phase) => phase_detail(&phase),
        Err(_) => (None, None),
    };
    NodeView {
        id: instance.id,
        parent_id: instance.parent_id,
        role: instance.role,
        phase: instance.phase,
        suspended_wait,
        reason,
        sealed: false,
    }
}

fn group_context(rows: Vec<ContextRow>) -> (Vec<MessageView>, Usage) {
    let mut messages: Vec<MessageView> = Vec::new();
    let mut counted: HashSet<i64> = HashSet::new();
    let mut input_tokens: i64 = 0;
    let mut output_tokens: i64 = 0;
    for row in rows {
        if counted.insert(row.message_id) {
            input_tokens += i64::from(row.input_tokens);
            output_tokens += i64::from(row.output_tokens);
        }
        if messages
            .last()
            .is_none_or(|message| message.id != row.message_id)
        {
            messages.push(MessageView::new(row.message_id, row.block.role));
        }
        if let Some(message) = messages.last_mut() {
            let mut block = BlockView::new(row.block.seq, row.block.revision, row.block.body);
            block.committed = true;
            message.blocks.push(block);
        }
    }
    for message in &mut messages {
        message.blocks.sort_by_key(|block| block.seq);
    }
    let usage = Usage {
        input_tokens: saturate(input_tokens),
        output_tokens: saturate(output_tokens),
    };
    (messages, usage)
}

fn saturate(value: i64) -> i32 {
    value.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32
}
