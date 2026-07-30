use super::{executor::StorageExecutor, utils::ensure_affected};
use crate::{
    db::DbDriver,
    error::Result,
    insert,
    models::{
        AgentDefinitionData, ChatConfig, CreateTopic, PromptModuleBinding, ToolApprovalPolicy,
        Topic, UpdateTopic,
    },
    select_fields,
    storage::{TableName, next_id, now_ts},
    update, update_fields,
};
use sqlx::QueryBuilder;
use wind_ai::message::ReqConfig;

const DEFAULT_AGENT_KEY: &str = "default-helpful-assistant";
const DEFAULT_AGENT_NAME: &str = "Helpful Assistant";
const DEFAULT_AGENT_PROMPT: &str = "you are a helpful assistant";
const DEFAULT_DESCRIPTION: &str = "Default helpful assistant";
const DEFAULT_PROMPT_DESCRIPTION: &str = "Default helpful assistant prompt";

#[derive(Clone)]
pub struct TopicStorage {
    executor: StorageExecutor,
}

impl TopicStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    async fn batch_delete_by_ids(
        executor: &StorageExecutor,
        table: &str,
        column: &str,
        ids: &[i64],
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new(format!("DELETE FROM {} WHERE {} IN (", table, column));
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(") ");
        executor.execute(builder.build()).await?;

        Ok(())
    }
    pub async fn create(&self, data: CreateTopic) -> Result<Topic> {
        self.executor
            .transaction_required(
                |executor| async move { Self::new(executor).create_inner(data).await },
            )
            .await
    }

    async fn create_inner(&self, data: CreateTopic) -> Result<Topic> {
        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            TableName::TOPICS,
            ("id", id),
            ("parent_id", data.parent_id),
            ("label", data.label.clone()),
            ("icon", data.icon.clone()),
            ("created_at", now),
        );
        self.executor.execute(qb.build()).await?;

        let prompt_id = match self
            .executor
            .fetch_optional(
                select_fields!(TableName::PROMPT_MODULES, ("id"))
                    .push(" WHERE key = ")
                    .push_bind(DEFAULT_AGENT_KEY)
                    .build_query_as::<(i64,)>(),
            )
            .await?
        {
            Some((id,)) => id,
            None => {
                let prompt_id = next_id();
                let mut qb = insert!(
                    TableName::PROMPT_MODULES,
                    ("id", prompt_id),
                    ("key", DEFAULT_AGENT_KEY),
                    ("name", DEFAULT_AGENT_NAME),
                    ("description", DEFAULT_PROMPT_DESCRIPTION),
                    ("content", DEFAULT_AGENT_PROMPT),
                    ("active", true),
                    ("created_at", now_ts())
                );
                self.executor.execute(qb.build()).await?;
                prompt_id
            }
        };

        let agent_id = match self
            .executor
            .fetch_optional(
                select_fields!(TableName::AGENT_DEFINITION, ("id"))
                    .push(" WHERE key = ")
                    .push_bind(DEFAULT_AGENT_KEY)
                    .build_query_as::<(i64,)>(),
            )
            .await?
        {
            Some((id,)) => id,
            None => {
                let agent_id = next_id();
                let mut agent_data = AgentDefinitionData::default();
                agent_data.prompt_modules.push(PromptModuleBinding {
                    prompt_module_id: prompt_id,
                    required: true,
                    enabled: true,
                });
                let mut qb = insert!(
                    TableName::AGENT_DEFINITION,
                    ("id", agent_id),
                    ("key", DEFAULT_AGENT_KEY),
                    ("name", DEFAULT_AGENT_NAME),
                    ("description", DEFAULT_DESCRIPTION),
                    ("scope", "global"),
                    ("owner_topic_id", Option::<i64>::None),
                    ("cloned_from_agent_id", Option::<i64>::None),
                    ("active", true),
                    ("data", serde_json::to_string(&agent_data)?),
                    ("created_at", now_ts())
                );
                self.executor.execute(qb.build()).await?;
                agent_id
            }
        };

        let mut qb = insert!(
            TableName::TOPIC_AGENT_BINDINGS,
            ("id", next_id()),
            ("parent_topic_id", id),
            ("topic_id", id),
            ("agent_id", agent_id),
            ("role", "main"),
            ("model_id", Option::<i64>::None),
            ("chat_config_id", Option::<i64>::None),
            ("status", "created"),
            (
                "tool_approval_policy",
                serde_json::to_string(&ToolApprovalPolicy::default())?
            ),
            ("enabled", true),
            ("created_at", now_ts())
        );
        self.executor.execute(qb.build()).await?;

        Ok(Topic {
            id,
            parent_id: data.parent_id,
            label: data.label,
            icon: data.icon,
            created_at: now,
        })
    }

    pub async fn update(&self, id: i64, data: UpdateTopic) -> Result<()> {
        let mut qb = update!(
            TableName::TOPICS,
            id,
            ("parent_id", data.parent_id),
            ("label", data.label),
            ("icon", data.icon),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    /// 获取所有 topic
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        let mut qb = Self::select_topic();
        qb.push(" ORDER BY id ASC ");
        let rows = self
            .executor
            .fetch_all(qb.build_query_as::<Topic>())
            .await?;

        Ok(rows)
    }

    /// 获取 topic
    pub async fn get_topic(&self, id: i64) -> Result<Option<Topic>> {
        let mut qb = Self::select_topic();
        qb.push(" WHERE id = ").push_bind(id);
        let row = self
            .executor
            .fetch_optional(qb.build_query_as::<Topic>())
            .await?;

        Ok(row)
    }

    pub async fn delete_topics(&self, ids: &[i64]) -> Result<()> {
        self.executor
            .transaction_required(|executor| async move {
                Self::batch_delete_by_ids(&executor, TableName::CHAT_CONFIGS, "topic_id", ids)
                    .await?;
                Self::batch_delete_by_ids(&executor, TableName::MESSAGES, "topic_id", ids).await?;
                Self::batch_delete_by_ids(
                    &executor,
                    TableName::TOPIC_AGENT_BINDINGS,
                    "topic_id",
                    ids,
                )
                .await?;
                Self::batch_delete_by_ids(&executor, TableName::TOPICS, "id", ids).await?;
                Ok(())
            })
            .await
    }

    pub async fn create_chat_config(&self, topic_id: i64, config: ReqConfig) -> Result<ChatConfig> {
        let id = next_id();
        let now = now_ts();
        let mut qb = insert!(
            TableName::CHAT_CONFIGS,
            ("id", id),
            ("topic_id", topic_id),
            ("temperature", config.temperature),
            ("top_p", config.top_p),
            ("max_tokens", config.max_tokens),
            ("stream", config.stream),
            ("presence_penalty", config.presence_penalty),
            ("frequency_penalty", config.frequency_penalty),
            ("parallel_tool_calls", config.parallel_tool_calls),
            ("reasoning", config.reasoning),
            ("created_at", now)
        );
        self.executor.execute(qb.build()).await?;

        Ok(ChatConfig {
            id,
            topic_id,
            data: config,
            created_at: now,
        })
    }

    pub async fn update_chat_config(&self, topic_id: i64, config: ReqConfig) -> Result<()> {
        ensure_affected(
            self.executor
                .execute(
                    update_fields!(
                        TableName::CHAT_CONFIGS,
                        ("temperature", config.temperature),
                        ("top_p", config.top_p),
                        ("max_tokens", config.max_tokens),
                        ("stream", config.stream),
                        ("presence_penalty", config.presence_penalty),
                        ("frequency_penalty", config.frequency_penalty),
                        ("parallel_tool_calls", config.parallel_tool_calls),
                        ("reasoning", config.reasoning),
                        ("updated_at", Some(now_ts()))
                    )
                    .push(" WHERE topic_id =  ")
                    .push_bind(topic_id)
                    .build(),
                )
                .await?,
        )
    }

    pub async fn get_chat_config(&self, id: i64) -> Result<Option<ChatConfig>> {
        let row = self
            .executor
            .fetch_optional(
                select_fields!(
                    TableName::CHAT_CONFIGS,
                    (
                        "id",
                        "topic_id",
                        "temperature",
                        "top_p",
                        "max_tokens",
                        "stream",
                        "presence_penalty",
                        "frequency_penalty",
                        "parallel_tool_calls",
                        "reasoning",
                        "created_at"
                    )
                )
                .push(" WHERE id = ")
                .push_bind(id)
                .build_query_as::<ChatConfig>(),
            )
            .await?;

        Ok(row)
    }

    fn select_topic<'a>() -> sqlx::QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::TOPICS,
            ("id", "parent_id", "label", "icon", "created_at")
        )
    }
}
