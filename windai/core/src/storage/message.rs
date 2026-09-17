use super::{
    executor::StorageExecutor,
    utils::{self, ensure_affected, now_ts},
};
use crate::{
    db::DbDriver,
    delete_by_id,
    error::Result,
    insert,
    models::{CreateMessage, Message, UpdateMessage},
    select_fields,
    storage::TableName,
    update,
};
use sqlx::QueryBuilder;

#[derive(Clone)]
pub struct MessageStorage {
    executor: StorageExecutor,
}
impl MessageStorage {
    pub(crate) fn new(executor: StorageExecutor) -> Self {
        Self { executor }
    }

    fn select_common<'a>() -> QueryBuilder<'a, DbDriver> {
        select_fields!(
            TableName::MESSAGES,
            (
                "id",
                "from_id",
                "content",
                "model_id",
                "instance_id",
                "is_boundary",
                "is_excluded",
                "input_tokens",
                "output_tokens",
                "created_at"
            )
        )
    }

    /// 保存一条消息
    pub async fn create(&self, data: CreateMessage) -> Result<Message> {
        let now = now_ts();
        let mut qb = insert!(
            TableName::MESSAGES,
            ("from_id", data.from_id),
            ("content", utils::vec_to_str_default(Some(&data.content))?),
            ("model_id", data.model_id),
            ("instance_id", data.instance_id),
            ("is_boundary", data.is_boundary),
            ("is_excluded", data.is_excluded),
            ("input_tokens", data.input_tokens),
            ("output_tokens", data.output_tokens),
            ("created_at", now),
        );
        qb.push(" RETURNING id");
        let id: i64 = self
            .executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await?;

        Ok(Message {
            id,
            from_id: data.from_id,
            content: data.content,
            model_id: data.model_id,
            instance_id: data.instance_id,
            is_boundary: data.is_boundary,
            is_excluded: data.is_excluded,
            input_tokens: data.input_tokens,
            output_tokens: data.output_tokens,
            created_at: now,
        })
    }

    /// 更新消息
    pub async fn update(&self, id: i64, data: UpdateMessage) -> Result<()> {
        let mut qb = update!(
            TableName::MESSAGES,
            id,
            (
                "content",
                utils::vec_to_str_optional(data.content.as_deref())?
            ),
            ("model_id", data.model_id),
            ("input_tokens", data.input_tokens),
            ("output_tokens", data.output_tokens),
        );
        ensure_affected(self.executor.execute(qb.build()).await?)
    }

    pub async fn get(&self, id: i64) -> Result<Option<Message>> {
        let row = self
            .executor
            .fetch_optional(
                Self::select_common()
                    .push(" WHERE id = ")
                    .push_bind(id)
                    .build_query_as::<Message>(),
            )
            .await?;
        Ok(row)
    }

    /// 查询 from_id 为该 id 的助手消息
    pub async fn get_from_msg(&self, id: i64) -> Result<Option<Message>> {
        let row = self
            .executor
            .fetch_optional(
                Self::select_common()
                    .push(" WHERE from_id = ")
                    .push_bind(id)
                    .build_query_as::<Message>(),
            )
            .await?;
        Ok(row)
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        self.executor
            .with_tx(|executor| async move {
                let current = Self::new(executor.clone());
                let mut exclude_qb = None;
                match current.get(id).await? {
                    Some(m) => {
                        match m.from_id {
                            Some(from_id) => {
                                // 设置用户消息
                                exclude_qb = Some(update!(
                                    TableName::MESSAGES,
                                    from_id,
                                    ("is_excluded", Some(true))
                                ))
                            }
                            _ => match current.get_from_msg(id).await? {
                                Some(user) => {
                                    // 设置助手消息
                                    exclude_qb = Some(update!(
                                        TableName::MESSAGES,
                                        user.id,
                                        ("is_excluded", Some(true))
                                    ));
                                }
                                None => {}
                            },
                        }
                        if let Some(mut qb) = exclude_qb {
                            executor.execute(qb.build()).await?;
                        }
                        executor
                            .execute(delete_by_id!(TableName::MESSAGES, id).build())
                            .await?;
                    }
                    _ => {}
                }

                Ok(())
            })
            .await
    }
    /// 查询 instance_id 下所有的消息
    pub async fn list_by_instance(&self, instance_id: i64) -> Result<Vec<Message>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_common()
                    .push(" WHERE instance_id = ")
                    .push_bind(instance_id)
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<Message>(),
            )
            .await?;

        Ok(rows)
    }

    /// 查询实例下所有消息
    ///
    /// 获取从最新一条含有 is_boundary = true 的消息开始往后所有的消息
    pub(crate) async fn list_contexts(&self, instance_id: i64) -> Result<Vec<Message>> {
        let rows = self
            .executor
            .fetch_all(
                Self::select_common()
                    .push(" WHERE instance_id = ")
                    .push_bind(instance_id)
                    .push(" AND is_excluded = ")
                    .push_bind(false)
                    .push(" AND id > COALESCE((SELECT MAX(id) FROM ")
                    .push(TableName::MESSAGES)
                    .push(" WHERE is_boundary = TRUE AND instance_id = ")
                    .push_bind(instance_id)
                    .push("), 0)")
                    .push(" ORDER BY id ASC ")
                    .build_query_as::<Message>(),
            )
            .await?;

        Ok(rows)
    }
}

/// 这些用例直接构造 SQLite 连接池，因此只在 sqlite feature 下运行
#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::{
        models::{CreateInstance, CreateTopic},
        schema::init_schema,
        storage::Storage,
    };
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::str::FromStr;
    use wind_ai::message::{Content, Message as AiMessage, Role};

    const MODEL_ID: i64 = 1;

    /// 单连接内存池 + 初始化 schema
    async fn setup() -> Storage {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .expect("parse sqlite url")
            .shared_cache(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("connect sqlite");
        init_schema(&pool).await.expect("init schema");
        Storage::new(pool)
    }

    /// 建一个根 topic 及其主实例，返回实例 id（消息的归属作用域）
    async fn create_instance(storage: &Storage, label: &str) -> i64 {
        let topic = storage
            .topic()
            .create(CreateTopic {
                parent_id: None,
                label: label.into(),
                icon: None,
                model_id: None,
                tool_approval_policy: None,
            })
            .await
            .expect("create topic");
        storage
            .agent()
            .create_instance(CreateInstance::new_main(topic.id, None))
            .await
            .expect("create message instance")
            .id
    }

    fn user_msg(
        instance_id: i64,
        text: &str,
        is_boundary: bool,
        is_excluded: bool,
    ) -> CreateMessage {
        CreateMessage {
            from_id: None,
            content: vec![AiMessage::new_simple(
                Role::User,
                vec![Content::new_text(text.into())],
                None,
            )],
            model_id: MODEL_ID,
            instance_id,
            is_boundary,
            is_excluded,
            input_tokens: 5,
            output_tokens: 0,
        }
    }

    fn asst_msg(
        instance_id: i64,
        from_id: i64,
        text: &str,
        is_boundary: bool,
        is_excluded: bool,
    ) -> CreateMessage {
        CreateMessage {
            from_id: Some(from_id),
            content: vec![AiMessage::new_simple(
                Role::Assistant,
                vec![Content::new_text(text.into())],
                None,
            )],
            model_id: MODEL_ID,
            instance_id,
            is_boundary,
            is_excluded,
            input_tokens: 0,
            output_tokens: 10,
        }
    }

    /// 在 `instance_id` 下创建一对 user-assistant 消息，返回 (user_id, assistant_id)
    async fn create_pair(
        msg: &MessageStorage,
        instance_id: i64,
        user_exclude: bool,
        assistant_exclude: bool,
    ) -> (i64, i64) {
        let user = msg
            .create(user_msg(instance_id, "q", false, user_exclude))
            .await
            .expect("create user message");
        let assistant = msg
            .create(asst_msg(
                instance_id,
                user.id,
                "a",
                false,
                assistant_exclude,
            ))
            .await
            .expect("create assistant message");
        (user.id, assistant.id)
    }

    /// 两对 user-assistant 消息 + 一条 boundary 消息，字段保存各自 id
    #[derive(Default)]
    struct BoundarySequence {
        u1: i64,
        a1: i64,
        u2: i64,
        a2: i64,
        b: i64,
    }

    async fn create_boundary(msg: &MessageStorage, instance_id: i64) -> i64 {
        msg.create(user_msg(instance_id, "boundary", true, false))
            .await
            .expect("create boundary")
            .id
    }

    async fn create_user(msg: &MessageStorage, instance_id: i64) -> i64 {
        msg.create(user_msg(instance_id, "q", false, false))
            .await
            .expect("create user message")
            .id
    }

    async fn create_assistant(msg: &MessageStorage, instance_id: i64, from_id: i64) -> i64 {
        msg.create(asst_msg(instance_id, from_id, "a", false, false))
            .await
            .expect("create assistant message")
            .id
    }

    /// 按 u1 → a1 → u2 → a2 的顺序创建两对消息，boundary 插在 `position` 指示的空隙处
    ///
    /// `position` 即四个创建位次之间的空隙，4 表示全部消息之后
    async fn create_boundary_sequence(
        msg: &MessageStorage,
        instance_id: i64,
        position: usize,
    ) -> BoundarySequence {
        let mut s = BoundarySequence::default();
        for slot in 0..4 {
            if slot == position {
                s.b = create_boundary(msg, instance_id).await;
            }
            match slot {
                0 => s.u1 = create_user(msg, instance_id).await,
                1 => s.a1 = create_assistant(msg, instance_id, s.u1).await,
                2 => s.u2 = create_user(msg, instance_id).await,
                _ => s.a2 = create_assistant(msg, instance_id, s.u2).await,
            }
        }
        if position == 4 {
            s.b = create_boundary(msg, instance_id).await;
        }
        s
    }

    fn ids(messages: &[Message]) -> Vec<i64> {
        messages.iter().map(|m| m.id).collect()
    }

    /// is_excluded 标志：被排除的消息不进入上下文，但全量列表不受影响
    #[tokio::test]
    async fn list_contexts_ignores_excluded_messages() {
        let storage = setup().await;
        let msg = storage.message();

        // 全部未排除：两对消息都是上下文
        let instance = create_instance(&storage, "excluded-none").await;
        let (u1, a1) = create_pair(msg, instance, false, false).await;
        let (u2, a2) = create_pair(msg, instance, false, false).await;
        assert_eq!(
            ids(&msg.list_by_instance(instance).await.unwrap()),
            vec![u1, a1, u2, a2]
        );
        assert_eq!(
            ids(&msg.list_contexts(instance).await.unwrap()),
            vec![u1, a1, u2, a2]
        );

        // 逐个排除四条消息，等价于覆盖所有位置
        for excluded in ["u1", "a1", "u2", "a2"] {
            let instance = create_instance(&storage, &format!("excluded-{excluded}")).await;
            let (u1, a1) = create_pair(msg, instance, excluded == "u1", excluded == "a1").await;
            let (u2, a2) = create_pair(msg, instance, excluded == "u2", excluded == "a2").await;
            let excluded_id = match excluded {
                "u1" => u1,
                "a1" => a1,
                "u2" => u2,
                _ => a2,
            };

            let ctx = msg.list_contexts(instance).await.unwrap();
            let ctx_ids = ids(&ctx);
            assert_eq!(ctx_ids.len(), 3, "{excluded} 被排除后上下文应剩 3 条");
            assert!(
                !ctx_ids.contains(&excluded_id),
                "{excluded} 不应出现在上下文中"
            );
            assert!(
                ctx.iter().all(|m| !m.is_excluded),
                "上下文不应含 is_excluded 的消息"
            );
            assert_eq!(ids(&msg.list_by_instance(instance).await.unwrap()).len(), 4);
        }

        // 全部排除：上下文为空，全量列表仍完整
        let instance = create_instance(&storage, "excluded-all").await;
        create_pair(msg, instance, true, true).await;
        create_pair(msg, instance, true, true).await;
        assert!(msg.list_contexts(instance).await.unwrap().is_empty());
        assert_eq!(ids(&msg.list_by_instance(instance).await.unwrap()).len(), 4);
    }

    /// 删除一条消息会把配对消息标记为 is_excluded，两者都离开上下文
    #[tokio::test]
    async fn list_contexts_drops_deleted_pair() {
        let storage = setup().await;
        let msg = storage.message();

        // 删除助手消息：配对的用户消息被排除，自身不存在
        let instance = create_instance(&storage, "del-assistant").await;
        let (u1, a1) = create_pair(msg, instance, false, false).await;
        let (u2, a2) = create_pair(msg, instance, false, false).await;
        msg.delete(a1).await.expect("delete assistant");

        assert!(
            msg.get(u1).await.unwrap().expect("u1 exists").is_excluded,
            "删除助手消息后配对的用户消息应被排除"
        );
        assert!(msg.get(a1).await.unwrap().is_none());
        assert_eq!(
            ids(&msg.list_contexts(instance).await.unwrap()),
            vec![u2, a2]
        );
        let list_ids = ids(&msg.list_by_instance(instance).await.unwrap());
        assert_eq!(list_ids, vec![u1, u2, a2], "已删除的消息不再出现在全量列表");

        // 删除用户消息：配对的助手消息被排除
        let instance = create_instance(&storage, "del-user").await;
        let (u1, a1) = create_pair(msg, instance, false, false).await;
        let (u2, a2) = create_pair(msg, instance, false, false).await;
        msg.delete(u1).await.expect("delete user");

        assert!(
            msg.get(a1).await.unwrap().expect("a1 exists").is_excluded,
            "删除用户消息后配对的助手消息应被排除"
        );
        assert!(msg.get(u1).await.unwrap().is_none());
        assert_eq!(
            ids(&msg.list_contexts(instance).await.unwrap()),
            vec![u2, a2]
        );
        assert_eq!(
            ids(&msg.list_by_instance(instance).await.unwrap()),
            vec![a1, u2, a2]
        );
    }

    /// boundary 消息之前的消息不进入上下文，boundary 自身也不进入
    #[tokio::test]
    async fn list_contexts_starts_after_last_boundary() {
        let storage = setup().await;
        let msg = storage.message();

        for position in 0..5usize {
            let instance = create_instance(&storage, &format!("boundary-{position}")).await;
            let s = create_boundary_sequence(msg, instance, position).await;

            // id 随创建单调递增，故插入位置直接决定 id 顺序
            let expected_order: Vec<i64> = match position {
                0 => vec![s.b, s.u1, s.a1, s.u2, s.a2],
                1 => vec![s.u1, s.b, s.a1, s.u2, s.a2],
                2 => vec![s.u1, s.a1, s.b, s.u2, s.a2],
                3 => vec![s.u1, s.a1, s.u2, s.b, s.a2],
                _ => vec![s.u1, s.a1, s.u2, s.a2, s.b],
            };
            assert_eq!(
                ids(&msg.list_by_instance(instance).await.unwrap()),
                expected_order,
                "position {position} 下全量列表顺序错误"
            );

            let expected_ctx: Vec<i64> = expected_order
                .iter()
                .copied()
                .filter(|id| *id > s.b)
                .collect();
            let ctx = msg.list_contexts(instance).await.unwrap();
            assert_eq!(
                ids(&ctx),
                expected_ctx,
                "position {position} 下上下文应只含 boundary 之后的消息"
            );

            // 删除 boundary 之后的助手消息，其消息对一并离开上下文
            msg.delete(s.a1).await.expect("delete assistant");
            let after_delete: Vec<i64> = expected_ctx
                .iter()
                .copied()
                .filter(|id| *id != s.u1 && *id != s.a1)
                .collect();
            let ctx = msg.list_contexts(instance).await.unwrap();
            assert_eq!(
                ids(&ctx),
                after_delete,
                "position {position} 删除后上下文错误"
            );
            assert!(
                ctx.iter().all(|m| !m.is_excluded),
                "position {position} 上下文不应含 is_excluded 的消息"
            );
        }
    }
}
