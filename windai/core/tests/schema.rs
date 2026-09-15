//! schema 与模型的契约回归测试
//!
//! 每张表断言一次它对应的模型实际读写到的列，覆盖 `schema.rs` 的全部 12 张表：
//! 列名被改名或删除而模型未同步时，这里会先于运行期报错
//!
//! 只探测列是否存在，不依赖 SQLite 专有语法，可在 SQLite 与 PostgreSQL 上共用

#[path = "./common/lib.rs"]
mod common;

use wind_core::db::DbPool;

/// 初始化 schema 并断言 `table` 上存在全部 `cols` 指定的列
///
/// 通过 `SELECT ... WHERE 1 = 0` 让数据库自行校验列名，缺列时 prepare 失败
async fn assert_table_columns(table: &str, cols: &[&str]) {
    let pool = common::init_test_pool().await;
    wind_core::schema::init_schema(&pool)
        .await
        .expect("init schema");
    let sql = format!("SELECT {} FROM {} WHERE 1 = 0", cols.join(", "), table);
    sqlx::query(&sql)
        .execute(&pool)
        .await
        .unwrap_or_else(|err| panic!("table `{table}` is missing a column: {err}"));
}

/// 断言 `table.col` 已不存在
async fn assert_column_dropped(pool: &DbPool, table: &str, col: &str) {
    let sql = format!("SELECT {col} FROM {table} WHERE 1 = 0");
    assert!(
        sqlx::query(&sql).execute(pool).await.is_err(),
        "column `{table}.{col}` should have been dropped"
    );
}

#[tokio::test]
async fn providers_columns_match_model() {
    assert_table_columns(
        "providers",
        &[
            "id",
            "name",
            "alias",
            "description",
            "base_url",
            "doc",
            "active",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn models_columns_match_model() {
    assert_table_columns(
        "models",
        &[
            "id",
            "name",
            "provider_id",
            "alias",
            "adapter",
            "modalities",
            "active",
            "icon",
            "endpoint",
            "config",
            "frequency",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn credentials_columns_match_model() {
    assert_table_columns(
        "credentials",
        &["id", "provider_id", "key", "active", "created_at"],
    )
    .await;
}

#[tokio::test]
async fn topics_columns_match_model() {
    assert_table_columns(
        "topics",
        &[
            "id",
            "parent_id",
            "label",
            "icon",
            "model_id",
            "tool_approval_policy",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn messages_columns_match_model() {
    assert_table_columns(
        "messages",
        &[
            "id",
            "from_id",
            "content",
            "model_id",
            "instance_id",
            "is_boundary",
            "is_excluded",
            "input_tokens",
            "output_tokens",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn mcp_servers_columns_match_model() {
    assert_table_columns(
        "mcp_servers",
        &[
            "id",
            "type",
            "name",
            "url",
            "description",
            "command",
            "args",
            "env",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn json_rule_columns_match_model() {
    assert_table_columns(
        "json_rule",
        &[
            "id",
            "provider_id",
            "adapter",
            "json_rule",
            "active",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn prompt_modules_columns_match_model() {
    assert_table_columns(
        "prompt_modules",
        &[
            "id",
            "alias",
            "description",
            "content",
            "active",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn agent_definitions_columns_match_model() {
    assert_table_columns(
        "agent_definitions",
        &[
            "id",
            "key",
            "name",
            "description",
            "owner_topic_id",
            "cloned_from_id",
            "active",
            "data",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn agent_instances_columns_match_model() {
    assert_table_columns(
        "agent_instances",
        &[
            "id",
            "parent_id",
            "topic_id",
            "agent_id",
            "role",
            "status",
            "mode",
            "created_at",
        ],
    )
    .await;
}

#[tokio::test]
async fn topic_agent_maps_columns_match_model() {
    assert_table_columns(
        "topic_agent_maps",
        &["id", "topic_id", "agent_id", "created_at"],
    )
    .await;
}

#[tokio::test]
async fn tool_approval_requests_columns_match_model() {
    assert_table_columns(
        "tool_approval_requests",
        &[
            "id",
            "topic_id",
            "message_id",
            "instance_id",
            "tool_call_id",
            "tool_name",
            "arguments",
            "status",
            "created_at",
        ],
    )
    .await;
}

/// 已被移除的历史概念不得随 schema 回流
///
/// `chat_configs` / `topic_agent_bindings` 两张表整体删除；其余为落在保留表上的废弃列
#[tokio::test]
async fn dropped_columns_are_absent() {
    let pool = common::init_test_pool().await;
    wind_core::schema::init_schema(&pool)
        .await
        .expect("init schema");

    for (table, col) in [
        ("messages", "stream"),
        ("agent_instances", "chat_config_id"),
        ("agent_instances", "tool_approval_policy"),
        ("agent_instances", "enabled"),
        ("agent_instances", "model_id"),
        // 主 Agent 偏好已废弃：映射只表达「topic 拥有该能力」
        ("topic_agent_maps", "role"),
    ] {
        assert_column_dropped(&pool, table, col).await;
    }
}

/// 已删除的表整体不可查
#[tokio::test]
async fn dropped_tables_are_absent() {
    let pool = common::init_test_pool().await;
    wind_core::schema::init_schema(&pool)
        .await
        .expect("init schema");

    for table in ["chat_configs", "topic_agent_bindings"] {
        let sql = format!("SELECT id FROM {table} WHERE 1 = 0");
        assert!(
            sqlx::query(&sql).execute(&pool).await.is_err(),
            "table `{table}` should have been dropped"
        );
    }
}
