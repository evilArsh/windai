use wind_storage::error::StorageError;
use wind_storage::{ContextRow, Storage, Topic};
use wind_wire::{AgentRole, Usage};

use crate::app::{BlockView, MessageView};
use crate::config::Selection;

pub struct Startup {
    pub selection: Selection,
    pub notice: Option<String>,
}

pub struct LoadedSession {
    pub topic: Topic,
    pub main: Option<i64>,
    pub messages: Vec<MessageView>,
    pub usage: Usage,
}

pub async fn load_startup(storage: &Storage) -> Result<Startup, StorageError> {
    let providers = storage.list_providers().await?;
    let Some(first_provider) = providers.first() else {
        return Ok(Startup {
            selection: Selection::new(None, None),
            notice: Some("no providers configured".to_owned()),
        });
    };
    let models = storage.list_models(None).await?;
    let topics = storage.list_topics().await?;
    let latest = topics
        .first()
        .and_then(|topic| topic.model_id)
        .and_then(|model_id| models.iter().find(|model| model.id == model_id));
    if let Some(model) = latest
        && let Some(provider) = providers
            .iter()
            .find(|provider| provider.id == model.provider_id)
    {
        return Ok(Startup {
            selection: Selection::new(Some(provider.clone()), Some(model.clone())),
            notice: None,
        });
    }
    let model = models
        .iter()
        .find(|model| model.provider_id == first_provider.id)
        .cloned();
    Ok(Startup {
        selection: Selection::new(Some(first_provider.clone()), model),
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
        usage,
    })
}

fn group_context(rows: Vec<ContextRow>) -> (Vec<MessageView>, Usage) {
    let mut messages: Vec<MessageView> = Vec::new();
    let mut input_tokens: i64 = 0;
    let mut output_tokens: i64 = 0;
    for row in rows {
        input_tokens += i64::from(row.input_tokens);
        output_tokens += i64::from(row.output_tokens);
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
