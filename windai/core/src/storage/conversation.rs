use super::Storage;
use super::TableName;
use super::utils::{decode_enum, ensure_lte_one, execute_expecting_rows};
use crate::db::{DbDriver, DbRow};
use crate::error::{CoreError, Result};
use crate::insert;
use crate::insert_fields;
use crate::models::MessageCreate;
use crate::models::TurnCreate;
use crate::models::{BlockCreate, BlockRow, Message, Turn, UnpairedToolCall};
use crate::select_fields;
use crate::update_fields;
use sqlx::{FromRow, QueryBuilder, Row};
use wind_stream::block::BlockKind;

fn select_turns<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::TURN,
        (
            "id",
            "instance_id",
            "topic_id",
            "from_id",
            "is_boundary",
            "created_at"
        )
    )
}

fn select_messages<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::MESSAGE,
        (
            "id",
            "turn_id",
            "seq",
            "role",
            "model_id",
            "finish_reason",
            "input_tokens",
            "output_tokens",
            "created_at"
        )
    )
}

fn select_blocks<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::BLOCK,
        (
            "id",
            "message_id",
            "seq",
            "kind",
            "call_id",
            "revision",
            "content",
            "data",
            "finished"
        )
    )
}

impl<'s> FromRow<'s, DbRow> for Turn {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            instance_id: row.try_get("instance_id")?,
            topic_id: row.try_get("topic_id")?,
            from_id: row.try_get("from_id")?,
            is_boundary: row.try_get("is_boundary")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'s> FromRow<'s, DbRow> for Message {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        const ROLE: &str = "message.role";
        let role: String = row.try_get("role")?;
        Ok(Self {
            id: row.try_get("id")?,
            turn_id: row.try_get("turn_id")?,
            seq: row.try_get("seq")?,
            role: decode_enum(ROLE, &role).map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            model_id: row.try_get("model_id")?,
            finish_reason: row.try_get("finish_reason")?,
            input_tokens: row.try_get("input_tokens")?,
            output_tokens: row.try_get("output_tokens")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'s> FromRow<'s, DbRow> for BlockRow {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        const KIND: &str = "block.kind";
        let kind: String = row.try_get("kind")?;
        let data: Option<String> = row.try_get("data")?;
        Ok(Self {
            id: row.try_get("id")?,
            message_id: row.try_get("message_id")?,
            seq: row.try_get("seq")?,
            kind: decode_enum(KIND, &kind).map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            call_id: row.try_get("call_id")?,
            revision: row.try_get("revision")?,
            content: row.try_get("content")?,
            data: Self::decode_data(data.as_deref())
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            finished: row.try_get("finished")?,
        })
    }
}

impl<'s> FromRow<'s, DbRow> for UnpairedToolCall {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        let data: Option<String> = row.try_get("data")?;
        Ok(Self {
            turn_id: row.try_get("turn_id")?,
            message_id: row.try_get("message_id")?,
            call_id: row.try_get("call_id")?,
            data: BlockRow::decode_data(data.as_deref())
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
        })
    }
}

impl Storage {
    /// 插入 turn 并返回新 id
    pub async fn turn_create(&self, turn: TurnCreate) -> Result<i64> {
        let mut qb = insert!(
            TableName::TURN,
            ("instance_id", turn.instance_id),
            ("topic_id", turn.topic_id),
            ("from_id", turn.from_id),
            ("is_boundary", turn.is_boundary),
            ("created_at", turn.created_at),
        );
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 写入 message 并返回新 id，seq 取该 turn 内的下一个
    pub async fn message_create(&self, message: MessageCreate) -> Result<i64> {
        let mut qb = insert_fields!(
            TableName::MESSAGE,
            (
                "turn_id",
                "seq",
                "role",
                "model_id",
                "finish_reason",
                "input_tokens",
                "output_tokens",
                "created_at"
            )
        );
        qb.push("SELECT ")
            .push_bind(message.turn_id)
            .push(", (SELECT COALESCE(MAX(seq), -1) + 1 FROM ")
            .push(TableName::MESSAGE)
            .push(" WHERE turn_id = ")
            .push_bind(message.turn_id)
            .push("), ")
            .push_bind(message.role.to_string())
            .push(", ")
            .push_bind(message.model_id)
            .push(", ")
            .push_bind(message.finish_reason)
            .push(", ")
            .push_bind(message.input_tokens)
            .push(", ")
            .push_bind(message.output_tokens)
            .push(", ")
            .push_bind(message.created_at)
            .push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 写入结束原因与 token 用量
    pub async fn message_seal(
        &self,
        message_id: i64,
        finish_reason: &str,
        input_tokens: i64,
        output_tokens: i64,
    ) -> Result<()> {
        let mut qb = update_fields!(
            TableName::MESSAGE,
            ("finish_reason", Some(finish_reason)),
            ("input_tokens", Some(input_tokens)),
            ("output_tokens", Some(output_tokens)),
        );
        qb.push(" WHERE id = ").push_bind(message_id);
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 插入 block，seq 取该 message 内的下一个
    pub async fn block_append(&self, block: BlockCreate) -> Result<()> {
        let data = block.encode_data()?;
        let mut qb = insert_fields!(
            TableName::BLOCK,
            (
                "id",
                "message_id",
                "seq",
                "kind",
                "call_id",
                "revision",
                "content",
                "data",
                "finished"
            )
        );
        qb.push("SELECT ")
            .push_bind(block.id)
            .push(", ")
            .push_bind(block.message_id)
            .push(", (SELECT COALESCE(MAX(seq), -1) + 1 FROM ")
            .push(TableName::BLOCK)
            .push(" WHERE message_id = ")
            .push_bind(block.message_id)
            .push("), ")
            .push_bind(block.kind.to_string())
            .push(", ")
            .push_bind(block.call_id)
            .push(", ")
            .push_bind(block.revision)
            .push(", ")
            .push_bind(block.content)
            .push(", ")
            .push_bind(data)
            .push(", ")
            .push_bind(block.finished);
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 按指定的 seq 插入 block
    pub async fn block_insert(&self, block: BlockRow) -> Result<()> {
        let data = block.encode_data()?;
        execute_expecting_rows(
            &self.executor,
            insert!(
                TableName::BLOCK,
                ("id", block.id),
                ("message_id", block.message_id),
                ("seq", block.seq),
                ("kind", block.kind.to_string()),
                ("call_id", block.call_id),
                ("revision", block.revision),
                ("content", block.content),
                ("data", data),
                ("finished", block.finished),
            ),
        )
        .await
    }

    /// 返回当前最大 block id，空表为 0
    pub async fn block_id_seed(&self) -> Result<u64> {
        let seed: i64 = self
            .executor
            .fetch_one_scalar(
                QueryBuilder::<DbDriver>::new("SELECT COALESCE(MAX(id), 0) AS seed FROM ")
                    .push(TableName::BLOCK)
                    .build_query_scalar::<i64>(),
            )
            .await?;
        u64::try_from(seed).map_err(|_| CoreError::MalformedColumn {
            column: "block.id".into(),
            detail: "block id is negative".into(),
        })
    }

    /// 列出 message 内已完成的 block，按序号升序
    pub async fn block_list_by_message(&self, message_id: i64) -> Result<Vec<BlockRow>> {
        self.executor
            .fetch_all(
                select_blocks()
                    .push(" WHERE message_id = ")
                    .push_bind(message_id)
                    .push(" AND finished = TRUE ORDER BY seq ASC")
                    .build_query_as::<BlockRow>(),
            )
            .await
    }

    /// 按调用标识取出已完成的工具结果，不存在时为 None
    pub async fn block_find_tool_result(&self, call_id: &str) -> Result<Option<BlockRow>> {
        let rows = self
            .executor
            .fetch_all(
                select_blocks()
                    .push(" WHERE kind = ")
                    .push_bind(BlockKind::ToolResult.as_ref())
                    .push(" AND call_id = ")
                    .push_bind(call_id)
                    .push(" AND finished = TRUE")
                    .build_query_as::<BlockRow>(),
            )
            .await?;
        ensure_lte_one(rows)
    }

    /// 列出实例的 turn，按 id 升序
    pub async fn turn_list_by_instance(&self, instance_id: i64) -> Result<Vec<Turn>> {
        self.executor
            .fetch_all(
                select_turns()
                    .push(" WHERE instance_id = ")
                    .push_bind(instance_id)
                    .push(" ORDER BY id ASC")
                    .build_query_as::<Turn>(),
            )
            .await
    }

    /// 记录 turn 的起始 message
    pub async fn turn_set_from(&self, turn_id: i64, message_id: i64) -> Result<()> {
        let mut qb = update_fields!(TableName::TURN, ("from_id", Some(message_id)));
        qb.push(" WHERE id = ").push_bind(turn_id);
        execute_expecting_rows(&self.executor, qb).await
    }

    /// 返回 message 所属 turn，不存在时为 None
    pub async fn message_turn(&self, message_id: i64) -> Result<Option<i64>> {
        let row: Option<(i64,)> = self
            .executor
            .fetch_optional(
                QueryBuilder::<DbDriver>::new("SELECT turn_id FROM ")
                    .push(TableName::MESSAGE)
                    .push(" WHERE id = ")
                    .push_bind(message_id)
                    .build_query_as::<(i64,)>(),
            )
            .await?;
        Ok(row.map(|(turn_id,)| turn_id))
    }

    /// 按 turn 批量取出 message，先按 turn 再按序号升序
    pub async fn message_list_by_turns(&self, turn_ids: &[i64]) -> Result<Vec<Message>> {
        if turn_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = select_messages();
        qb.push(" WHERE turn_id IN (");
        {
            let mut separated = qb.separated(", ");
            for id in turn_ids {
                separated.push_bind(*id);
            }
        }
        qb.push(") ORDER BY turn_id ASC, seq ASC");
        self.executor
            .fetch_all(qb.build_query_as::<Message>())
            .await
    }

    /// 按 message 批量取出已完成的 block，先按 message 再按序号升序
    pub async fn block_list_by_messages(&self, message_ids: &[i64]) -> Result<Vec<BlockRow>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = select_blocks();
        qb.push(" WHERE message_id IN (");
        {
            let mut separated = qb.separated(", ");
            for id in message_ids {
                separated.push_bind(*id);
            }
        }
        qb.push(") AND finished = TRUE ORDER BY message_id ASC, seq ASC");
        self.executor
            .fetch_all(qb.build_query_as::<BlockRow>())
            .await
    }

    /// 列出实例内还没有对应 tool_result 的工具调用
    pub(crate) async fn tool_calls_without_results(
        &self,
        instance_id: i64,
    ) -> Result<Vec<UnpairedToolCall>> {
        /*
            SELECT t.id AS turn_id, b.message_id, b.call_id, b.data
            FROM turn t
            JOIN message m ON m.turn_id = t.id
            JOIN block b ON b.message_id = m.id
            WHERE t.instance_id = ?
            AND b.kind = 'tool_call'
            AND b.finished = TRUE
            AND b.call_id IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM block r
                JOIN message rm ON rm.id = r.message_id
                JOIN turn rt ON rt.id = rm.turn_id
                WHERE r.call_id = b.call_id
                AND r.kind = 'tool_result'
                AND r.finished = TRUE
                AND rt.instance_id = t.instance_id
            )
            ORDER BY t.id ASC, m.seq ASC, b.seq ASC
        */
        let mut qb = QueryBuilder::<DbDriver>::new(
            "SELECT t.id AS turn_id, b.message_id, b.call_id, b.data FROM ",
        );
        qb.push(TableName::TURN)
            .push(" t JOIN ")
            .push(TableName::MESSAGE)
            .push(" m ON m.turn_id = t.id JOIN ")
            .push(TableName::BLOCK)
            .push(" b ON b.message_id = m.id WHERE t.instance_id = ")
            .push_bind(instance_id)
            .push(" AND b.kind = ")
            .push_bind(BlockKind::ToolCall.as_ref())
            .push(" AND b.finished = TRUE AND b.call_id IS NOT NULL")
            .push(" AND NOT EXISTS (SELECT 1 FROM ")
            .push(TableName::BLOCK)
            .push(" r JOIN ")
            .push(TableName::MESSAGE)
            .push(" rm ON rm.id = r.message_id JOIN ")
            .push(TableName::TURN)
            .push(" rt ON rt.id = rm.turn_id WHERE r.call_id = b.call_id AND r.kind = ")
            .push_bind(BlockKind::ToolResult.as_ref())
            .push(" AND r.finished = TRUE AND rt.instance_id = t.instance_id)")
            .push(" ORDER BY t.id ASC, m.seq ASC, b.seq ASC");
        self.executor
            .fetch_all(qb.build_query_as::<UnpairedToolCall>())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::models::Role;
    use tempfile::TempDir;

    async fn storage() -> (TempDir, Storage) {
        let dir = TempDir::new().expect("create temp dir");
        let db = Db::open(&dir.path().join("t.db"))
            .await
            .expect("open database");
        db.init_schema().await.expect("create table");
        (dir, Storage::new(db))
    }

    async fn turn(storage: &Storage, instance_id: i64) -> i64 {
        storage
            .turn_create(TurnCreate {
                instance_id,
                topic_id: 1,
                from_id: None,
                is_boundary: false,
                created_at: None,
            })
            .await
            .expect("create turn")
    }

    async fn message(storage: &Storage, turn_id: i64) -> i64 {
        storage
            .message_create(MessageCreate {
                turn_id,
                role: Role::Assistant,
                model_id: None,
                finish_reason: None,
                input_tokens: 0,
                output_tokens: 0,
                created_at: None,
            })
            .await
            .expect("create message")
    }

    fn text_block(id: i64, message_id: i64) -> BlockCreate {
        BlockCreate {
            id,
            message_id,
            kind: BlockKind::Text,
            call_id: None,
            revision: 1,
            content: "text".into(),
            data: None,
            finished: true,
        }
    }

    fn tool_block(
        id: i64,
        message_id: i64,
        kind: BlockKind,
        call_id: Option<&str>,
        finished: bool,
    ) -> BlockRow {
        BlockRow {
            id,
            message_id,
            seq: id,
            kind,
            call_id: call_id.map(str::to_string),
            revision: 1,
            content: "{}".into(),
            data: Some(serde_json::json!({ "name": "windai_spawn_agent" })),
            finished,
        }
    }

    #[tokio::test]
    async fn an_unanswered_tool_call_is_listed_with_its_turn() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        storage
            .block_insert(tool_block(
                1,
                message_id,
                BlockKind::ToolCall,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert call");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].turn_id, turn_id);
        assert_eq!(calls[0].message_id, message_id);
        assert_eq!(calls[0].call_id, "c-1");
        assert_eq!(
            calls[0].data,
            Some(serde_json::json!({ "name": "windai_spawn_agent" }))
        );
    }

    #[tokio::test]
    async fn appended_blocks_take_consecutive_seqs() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        for id in 1..=3 {
            storage
                .block_append(text_block(id, message_id))
                .await
                .expect("append block");
        }

        let rows = storage
            .block_list_by_message(message_id)
            .await
            .expect("list blocks");
        let seqs: Vec<i64> = rows.iter().map(|row| row.seq).collect();
        assert_eq!(seqs, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn concurrent_appends_take_distinct_seqs() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        let mut tasks = Vec::new();
        for id in 1..=8 {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                storage.block_append(text_block(id, message_id)).await
            }));
        }
        for task in tasks {
            task.await.expect("join").expect("append block");
        }

        let rows = storage
            .block_list_by_message(message_id)
            .await
            .expect("list blocks");
        let seqs: Vec<i64> = rows.iter().map(|row| row.seq).collect();
        assert_eq!(seqs, (0..8).collect::<Vec<_>>());
    }

    fn assistant_message(turn_id: i64) -> MessageCreate {
        MessageCreate {
            turn_id,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: None,
        }
    }

    async fn message_at(storage: &Storage, turn_id: i64, seq: i64) -> Result<()> {
        sqlx::query("INSERT INTO message (turn_id, seq, role) VALUES (?, ?, 'assistant')")
            .bind(turn_id)
            .bind(seq)
            .execute(storage.pool())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn appended_messages_take_consecutive_seqs() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        for _ in 0..3 {
            storage
                .message_create(assistant_message(turn_id))
                .await
                .expect("append message");
        }

        let rows = storage
            .message_list_by_turns(&[turn_id])
            .await
            .expect("list messages");
        let seqs: Vec<i64> = rows.iter().map(|row| row.seq).collect();
        assert_eq!(seqs, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn concurrent_message_appends_take_distinct_seqs() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                storage.message_create(assistant_message(turn_id)).await
            }));
        }
        for task in tasks {
            task.await.expect("join").expect("append message");
        }

        let rows = storage
            .message_list_by_turns(&[turn_id])
            .await
            .expect("list messages");
        let seqs: Vec<i64> = rows.iter().map(|row| row.seq).collect();
        assert_eq!(seqs, (0..8).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_duplicate_message_seq_is_rejected() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        message_at(&storage, turn_id, 0)
            .await
            .expect("insert message");
        let duplicate = message_at(&storage, turn_id, 0).await;
        assert!(
            duplicate.is_err(),
            "a turn cannot hold two messages with the same seq"
        );
    }

    #[tokio::test]
    async fn a_matching_tool_result_answers_the_call() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        storage
            .block_insert(tool_block(
                1,
                message_id,
                BlockKind::ToolCall,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert call");
        storage
            .block_insert(tool_block(
                2,
                message_id,
                BlockKind::ToolResult,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert result");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert!(calls.is_empty());
    }

    #[tokio::test]
    async fn a_result_for_another_call_does_not_answer_the_call() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        storage
            .block_insert(tool_block(
                1,
                message_id,
                BlockKind::ToolCall,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert call");
        storage
            .block_insert(tool_block(
                2,
                message_id,
                BlockKind::ToolResult,
                Some("c-2"),
                true,
            ))
            .await
            .expect("insert another call's result");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert_eq!(
            calls.len(),
            1,
            "a result answers only the call with the same id"
        );
    }

    #[tokio::test]
    async fn a_result_from_another_instance_does_not_answer_the_call() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        storage
            .block_insert(tool_block(
                1,
                message_id,
                BlockKind::ToolCall,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert call");

        let other_turn = turn(&storage, 2).await;
        let other_message = message(&storage, other_turn).await;
        storage
            .block_insert(tool_block(
                2,
                other_message,
                BlockKind::ToolResult,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert other result");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert_eq!(
            calls.len(),
            1,
            "a result must belong to the same instance to answer the call"
        );
    }

    #[tokio::test]
    async fn a_result_in_another_message_of_the_instance_answers_the_call() {
        let (_dir, storage) = storage().await;
        let first_turn = turn(&storage, 1).await;
        let first_message = message(&storage, first_turn).await;
        storage
            .block_insert(tool_block(
                1,
                first_message,
                BlockKind::ToolCall,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert call");

        let second_turn = turn(&storage, 1).await;
        let second_message = message(&storage, second_turn).await;
        storage
            .block_insert(tool_block(
                2,
                second_message,
                BlockKind::ToolResult,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert result");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert!(
            calls.is_empty(),
            "a result in another message of the same instance answers the call"
        );
    }

    #[tokio::test]
    async fn an_unfinished_call_is_not_listed() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        storage
            .block_insert(tool_block(
                1,
                message_id,
                BlockKind::ToolCall,
                Some("c-1"),
                false,
            ))
            .await
            .expect("insert open call");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert!(calls.is_empty());
    }

    #[tokio::test]
    async fn an_unfinished_result_does_not_answer_the_call() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        storage
            .block_insert(tool_block(
                1,
                message_id,
                BlockKind::ToolCall,
                Some("c-1"),
                true,
            ))
            .await
            .expect("insert call");
        storage
            .block_insert(tool_block(
                2,
                message_id,
                BlockKind::ToolResult,
                Some("c-1"),
                false,
            ))
            .await
            .expect("insert open result");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert_eq!(calls.len(), 1);
    }

    #[tokio::test]
    async fn a_call_without_an_id_is_not_listed() {
        let (_dir, storage) = storage().await;
        let turn_id = turn(&storage, 1).await;
        let message_id = message(&storage, turn_id).await;
        storage
            .block_insert(tool_block(1, message_id, BlockKind::ToolCall, None, true))
            .await
            .expect("insert call");

        let calls = storage
            .tool_calls_without_results(1)
            .await
            .expect("query unanswered calls");
        assert!(calls.is_empty());
    }
}
