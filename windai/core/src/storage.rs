pub mod activity;
pub mod agent;
pub mod approval;
pub mod artifact;
pub mod mcp;
pub mod message;
pub mod model;
pub mod prompt;
pub mod provider;
pub mod topic;
pub mod utils;

use self::{
    activity::TopicActivityStorage, agent::AgentStorage, approval::ToolApprovalStorage,
    artifact::AgentArtifactStorage, mcp::McpStorage, message::MessageStorage, model::ModelStorage,
    prompt::PromptStorage, provider::ProviderStorage, topic::TopicStorage,
};
use super::db::DbPool;
use chrono::Utc;
use ferroid::{
    generator::AtomicSnowflakeGenerator,
    id::SnowflakeTwitterId,
    time::{MonotonicClock, TWITTER_EPOCH},
};
use std::sync::OnceLock;

pub struct Storage {
    db: DbPool,
    provider: provider::ProviderStorage,
    topic: topic::TopicStorage,
    model: model::ModelStorage,
    message: message::MessageStorage,
    mcp: mcp::McpStorage,
    agent: agent::AgentStorage,
    prompt: prompt::PromptStorage,
    approval: approval::ToolApprovalStorage,
    activity: activity::TopicActivityStorage,
    artifact: artifact::AgentArtifactStorage,
}

impl Storage {
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: ProviderStorage::new(db.clone()),
            topic: TopicStorage::new(db.clone()),
            model: ModelStorage::new(db.clone()),
            message: MessageStorage::new(db.clone()),
            mcp: McpStorage::new(db.clone()),
            agent: AgentStorage::new(db.clone()),
            prompt: PromptStorage::new(db.clone()),
            approval: ToolApprovalStorage::new(db.clone()),
            activity: TopicActivityStorage::new(db.clone()),
            artifact: AgentArtifactStorage::new(db.clone()),
            db,
        }
    }
    pub fn provider(&self) -> &ProviderStorage {
        &self.provider
    }
    pub fn topic(&self) -> &TopicStorage {
        &self.topic
    }
    pub fn model(&self) -> &ModelStorage {
        &self.model
    }
    pub fn message(&self) -> &MessageStorage {
        &self.message
    }

    pub fn mcp(&self) -> &McpStorage {
        &self.mcp
    }

    pub fn agent(&self) -> &AgentStorage {
        &self.agent
    }

    pub fn prompt(&self) -> &PromptStorage {
        &self.prompt
    }

    pub fn approval(&self) -> &ToolApprovalStorage {
        &self.approval
    }

    pub fn activity(&self) -> &TopicActivityStorage {
        &self.activity
    }

    pub fn artifact(&self) -> &AgentArtifactStorage {
        &self.artifact
    }

    pub async fn close(&self) {
        if !self.db.is_closed() {
            self.db.close().await;
        }
    }
}

pub fn now_ts() -> i64 {
    Utc::now().timestamp()
}

type SnowflakeGen = AtomicSnowflakeGenerator<SnowflakeTwitterId, MonotonicClock<1>>;
static ID_GENERATOR: OnceLock<SnowflakeGen> = OnceLock::new();

/// 初始化 ID 生成器
pub fn init_id_generator(machine_id: u16) {
    let clock = MonotonicClock::<1>::with_epoch(TWITTER_EPOCH);
    let generator = AtomicSnowflakeGenerator::new(machine_id as u64, clock);
    let _ = ID_GENERATOR.set(generator).map_err(|_| {
        log::warn!("ID generator has already been initialized");
    });
}

pub fn next_id() -> i64 {
    let generator = ID_GENERATOR
        .get()
        .expect("ID generator not initialized. Call init_id_generator() first.");
    let id: SnowflakeTwitterId = generator.next_id(|_| std::thread::yield_now());
    id.to_raw() as i64
}

/// 单行 UPDATE，自动追加 `updated_at`。
/// 所有值必须使用 Option 包装。
/// 值为 `None` 时，将忽略该字段。
/// 全部字段为 None 时不生成任何 SQL。
///
/// 用法:
/// ```ignore
/// let mut qb = update!("table_name", id,
///     ("name", data.name),                             // Option<String>
///     ("type", data.r#type.map(|v| v.to_string())),    // Option + transform
///     ("status", Some("active")),                      // 非 Option，始终拼入
/// );
/// assert_eq!(qb.sql(), "UPDATE table_name SET name = ?, type = ?, status = ? WHERE id = ?")
/// ```
#[macro_export]
macro_rules! update {
    ($table:literal, $id:expr, $(($col:literal, $val:expr)),+ $(,)?) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        let mut __count = 0usize;
        let mut __need_prefix = true;
        $(
            if let Some(__v) = $val {
                if __need_prefix {
                    __qb.push("UPDATE ").push($table).push(" SET ");
                    __need_prefix = false;
                } else {
                    __qb.push(", ");
                }
                __qb.push($col).push(" = ").push_bind(__v);
                __count += 1;
            }
        )+
        if __count > 0 {
            __qb.push(", updated_at = ");
            __qb.push_bind($crate::storage::now_ts());
            __qb.push(" WHERE id = ").push_bind($id);
        }
        __qb
    }};
}

/// UPDATE 语句拼接。
/// 所有值必须使用 Option 包装。
/// 值为 `None` 时，将忽略该字段。
/// 全部字段为 None 时不生成任何 SQL。
///
/// 用法:
/// ```ignore
/// let mut qb = update_fields!("table_name",
///     ("name", data.name),                             // Option<String>
///     ("type", data.r#type.map(|v| v.to_string())),    // Option + transform
///     ("status", Some("active")),                      // 非 Option，始终拼入
/// );
/// assert_eq!(qb.sql(), "UPDATE table_name SET name = ?, type = ?, status = ? ")
/// ```
#[macro_export]
macro_rules! update_fields {
    ($table:literal, $(($col:literal, $val:expr)),+ $(,)?) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        let mut __need_prefix = true;
        $(
            if let Some(__v) = $val {
                if __need_prefix {
                    __qb.push("UPDATE ").push($table).push(" SET ");
                    __need_prefix = false;
                } else {
                    __qb.push(", ");
                }
                __qb.push($col).push(" = ").push_bind(__v);
            }
        )+
        __qb
    }};
}

/// INSERT 语句拼接。
///
/// 用法:
/// ```ignore
/// let qb = insert_fields!("table_name", ("users", "id", "name"));
/// assert_eq!(qb.sql(), "INSERT INTO table_name (users, id, name) ")
/// ```
#[macro_export]
macro_rules! insert_fields {
    ($table:literal, ($($field:literal),+ $(,)?)) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("INSERT INTO ").push($table).push(" (");
        {
            let mut __sep = __qb.separated(", ");
            $(
                __sep.push($field);
            )+
        }
        __qb.push(") ");
        __qb
    }};
}

/// 单行 INSERT，自动追加 `created_at`。
///
/// 用法:
/// ```ignore
/// let mut qb = insert!("table_name",
///     ("name", "hello"),
///     ("type", "stdio"),
///     ("url", "http://example.com"),
/// );
/// assert_eq!(qb.sql(), "INSERT INTO table_name (name, type, url, created_at) VALUES (?, ?, ?, ?)");
/// ```
#[macro_export]
macro_rules! insert {
    ($table:literal, $(($col:literal, $val:expr)),+ $(,)?) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        let __ts = $crate::storage::now_ts();
        __qb.push("INSERT INTO ").push($table).push(" (");
        {
            let mut __sep = __qb.separated(", ");
            $(
                __sep.push($col);
            )+
            __sep.push("created_at");
        }
        __qb.push(") VALUES (");
        {
            let mut __sep = __qb.separated(", ");
            $(
                __sep.push_bind($val);
            )+
            __sep.push_bind(__ts);
        }
        __qb.push(")");
        __qb
    }};
}

/// 单行主键 id 删除
///
/// 用法：
/// ```ignore
/// let mut qb = delete_by_id!("table_name", 0);
/// assert_eq!(qb.sql(), "DELETE FROM table_name WHERE id = ?");
/// ```
#[macro_export]
macro_rules! delete_by_id {
    ($table:expr, $id:expr) => {{
        let mut __qb: sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("DELETE FROM ");
        __qb.push($table);
        __qb.push(" WHERE id = ");
        __qb.push_bind($id);
        __qb
    }};
}

/// SELECT 查询字段
///
/// ```ignore
/// let qb = select_fields!("table_name", ("users", "id", "name"));
/// assert_eq!(qb.sql(), "SELECT users, id, name FROM table_name");
/// ```
#[macro_export]
macro_rules! select_fields {
    ($table:expr, ($($field:literal),+ $(,)?)) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("SELECT ");
        {
            let mut __first = true;
            $(
                if !__first { __qb.push(", "); }
                __first = false;
                __qb.push($field);
            )+
        }
        __qb.push(" FROM ");
        __qb.push($table);
        __qb.push(" ");
        __qb
    }};
}

/// 单行主键 id 查询
///
/// ```ignore
/// let qb = get_by_id!("table_name", 1, ("users", "id"));
/// assert_eq!(qb.sql(), "SELECT users, id FROM table_name WHERE id = ?");
/// ```
#[macro_export]
macro_rules! get_by_id {
    // 不指定字段时，查询所有字段
    ($table:expr, $id:expr) => {{
        let mut __qb:sqlx::QueryBuilder<'_, $crate::db::DbDriver> = sqlx::QueryBuilder::new("");
        __qb.push("SELECT * FROM ");
        __qb.push($table);
        __qb.push(" WHERE id = ");
        __qb.push_bind($id);
        __qb
    }};

    // 指定查询字段
    ($table:expr, $id:expr, ($($field:literal),+ $(,)?)) => {{
        let mut __qb = $crate::select_fields!($table, ($($field),+));
        __qb.push("WHERE id = ");
        __qb.push_bind($id);
        __qb
    }};
}

#[cfg(test)]
mod tests {
    // ==================== insert! ====================

    #[test]
    fn insert_generates_correct_sql() {
        let qb = insert!("users", ("name", "alice"), ("age", 30),);
        assert_eq!(
            qb.sql(),
            "INSERT INTO users (name, age, created_at) VALUES (?, ?, ?)"
        );
    }

    #[test]
    fn insert_single_column() {
        let qb = insert!("logs", ("msg", "started"),);
        assert_eq!(qb.sql(), "INSERT INTO logs (msg, created_at) VALUES (?, ?)");
    }

    // ==================== update! ====================

    #[test]
    fn update_all_fields() {
        let qb = update!("users", 42, ("name", Some("bob")), ("age", Some(25)),);
        assert_eq!(
            qb.sql(),
            "UPDATE users SET name = ?, age = ?, updated_at = ? WHERE id = ?"
        );
    }

    #[test]
    fn update_mixed_none_and_some() {
        let qb = update!(
            "users",
            1,
            ("name", Option::<&str>::None),
            ("age", Some(99)),
            ("email", Some("a@b.com")),
        );
        assert_eq!(
            qb.sql(),
            "UPDATE users SET age = ?, email = ?, updated_at = ? WHERE id = ?"
        );
    }

    #[test]
    fn update_all_none_is_empty() {
        let qb = update!(
            "users",
            1,
            ("name", Option::<&str>::None),
            ("age", Option::<i64>::None),
        );
        assert_eq!(qb.sql(), "");
    }

    #[test]
    fn update_non_option_value() {
        let qb = update!("users", 7, ("status", Some("active")),);
        assert_eq!(
            qb.sql(),
            "UPDATE users SET status = ?, updated_at = ? WHERE id = ?"
        );
    }

    #[test]
    fn update_with_transform() {
        let opt = Some(42);
        let qb = update!("users", 5, ("score", opt.map(|v| v * 10)),);
        assert_eq!(
            qb.sql(),
            "UPDATE users SET score = ?, updated_at = ? WHERE id = ?"
        );
    }

    #[test]
    fn update_only_one_field() {
        let qb = update!("items", 99, ("title", Some("new title")),);
        assert_eq!(
            qb.sql(),
            "UPDATE items SET title = ?, updated_at = ? WHERE id = ?"
        );
    }

    // ==================== update_fields! ====================

    #[test]
    fn update_fields_basic() {
        let qb = update_fields!("users", ("name", Some("alice")), ("age", Some(30)),);
        assert_eq!(qb.sql(), "UPDATE users SET name = ?, age = ?");
    }

    #[test]
    fn update_fields_mixed_none() {
        let qb = update_fields!(
            "configs",
            ("key", Some("theme")),
            ("value", Option::<&str>::None),
        );
        assert_eq!(qb.sql(), "UPDATE configs SET key = ?");
    }

    #[test]
    fn update_fields_all_none_is_empty() {
        let qb = update_fields!(
            "configs",
            ("a", Option::<&str>::None),
            ("b", Option::<i64>::None),
        );
        assert_eq!(qb.sql(), "");
    }

    // ==================== insert_fields! ====================

    #[test]
    fn insert_fields_generates_prefix() {
        let qb = insert_fields!("users", ("id", "name", "email"));
        assert_eq!(qb.sql(), "INSERT INTO users (id, name, email) ");
    }

    #[test]
    fn insert_fields_single_column() {
        let qb = insert_fields!("logs", ("message"));
        assert_eq!(qb.sql(), "INSERT INTO logs (message) ");
    }

    // ==================== delete_by_id! ====================

    #[test]
    fn delete_by_id_generates_sql() {
        let qb = delete_by_id!("users", 42);
        assert_eq!(qb.sql(), "DELETE FROM users WHERE id = ?");
    }

    #[test]
    fn delete_by_id_expression_table() {
        let qb = delete_by_id!("some_table", 0);
        assert_eq!(qb.sql(), "DELETE FROM some_table WHERE id = ?");
    }

    // ==================== select_fields! ====================

    #[test]
    fn select_fields_multiple() {
        let qb = select_fields!("users", ("id", "name", "email"));
        assert_eq!(qb.sql(), "SELECT id, name, email FROM users ");
    }

    #[test]
    fn select_fields_single() {
        let qb = select_fields!("items", ("count"));
        assert_eq!(qb.sql(), "SELECT count FROM items ");
    }

    // ==================== get_by_id! ====================

    #[test]
    fn get_by_id_all_columns() {
        let qb = get_by_id!("users", 1);
        assert_eq!(qb.sql(), "SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn get_by_id_specific_columns() {
        let qb = get_by_id!("users", 1, ("id", "name"));
        assert_eq!(qb.sql(), "SELECT id, name FROM users WHERE id = ?");
    }

    #[test]
    fn get_by_id_single_column() {
        let qb = get_by_id!("items", 99, ("status"));
        assert_eq!(qb.sql(), "SELECT status FROM items WHERE id = ?");
    }
}
