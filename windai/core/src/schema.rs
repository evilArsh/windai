use crate::{db::DbPool, error::Result};

const SCHEMA_SQLITE: &str = r#"
CREATE TABLE IF NOT EXISTS providers (
    id              INTEGER PRIMARY KEY,
    name            TEXT    NOT NULL UNIQUE,
    alias           TEXT,
    description     TEXT,
    base_url        TEXT    NOT NULL,
    doc             TEXT,
    active          BOOLEAN NOT NULL,
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS models (
    id              INTEGER PRIMARY KEY,
    name            TEXT    NOT NULL,
    provider_id     INTEGER NOT NULL,
    alias           TEXT,
    adaptor         TEXT    NOT NULL,
    modalities      TEXT    DEFAULT '[]',
    active          BOOLEAN NOT NULL,
    icon            TEXT,
    endpoint        TEXT,
    frequency       INTEGER DEFAULT 0,
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS credentials (
    id              INTEGER PRIMARY KEY,
    provider_id     INTEGER NOT NULL,
    key             TEXT    NOT NULL,
    active          BOOLEAN NOT NULL,
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS topics (
    id              INTEGER PRIMARY KEY,
    parent_id       INTEGER,
    chat_config_id  INTEGER NOT NULL,
    label           TEXT    NOT NULL,
    icon            TEXT,
    max_context     INTEGER,
    topic_index     INTEGER NOT NULL,
    tool_approval_policy TEXT NOT NULL DEFAULT '{"type":"allow_all"}',
    mcp_server_ids  TEXT    DEFAULT '[]',
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY,
    from_id         INTEGER,
    stream          BOOLEAN NOT NULL DEFAULT 0,
    content         TEXT    NOT NULL DEFAULT '[]',
    model_id        INTEGER NOT NULL,
    topic_id        INTEGER NOT NULL,
    message_index   INTEGER NOT NULL,
    is_boundary     BOOLEAN NOT NULL,
    is_excluded     BOOLEAN NOT NULL,
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    tools_allowed   TEXT    DEFAULT '[]',
    tools_denied    TEXT    DEFAULT '[]',
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS chat_configs (
    id              INTEGER PRIMARY KEY,
    topic_id        INTEGER NOT NULL UNIQUE,
    temperature     REAL,
    top_p           REAL,
    max_tokens      INTEGER,
    stream          BOOLEAN DEFAULT 0,
    presence_penalty REAL,
    frequency_penalty REAL,
    parallel_tool_calls BOOLEAN,
    reasoning       BOOLEAN DEFAULT 0,
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS mcp_servers ( 
    id              INTEGER PRIMARY KEY,
    type            TEXT NOT NULL,
    name            TEXT NOT NULL UNIQUE,
    url             TEXT,
    description     TEXT,
    command         TEXT,
    args            TEXT DEFAULT '[]',
    env             TEXT DEFAULT '{}',
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS json_rule (
    id          INTEGER PRIMARY KEY,
    provider_id INTEGER NOT NULL,
    adaptor     TEXT    NOT NULL,
    json_rule   TEXT    NOT NULL,
    active      BOOLEAN NOT NULL,
    created_at  INTEGER,
    updated_at  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_name_provider ON providers(name);

CREATE INDEX IF NOT EXISTS idx_models_provider ON models(provider_id);

CREATE INDEX IF NOT EXISTS idx_credentials_provider ON credentials(provider_id);

CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic_id, message_index);

CREATE INDEX IF NOT EXISTS idx_topics_parent_id_topic_index ON topics(parent_id, topic_index);
CREATE INDEX IF NOT EXISTS idx_topics_chat_config_id ON topics(chat_config_id);

CREATE INDEX IF NOT EXISTS idx_chat_configs_topic_id ON chat_configs(topic_id);

CREATE INDEX IF NOT EXISTS idx_mcp_servers_name ON mcp_servers(name);

CREATE INDEX IF NOT EXISTS idx_provider_adaptor ON json_rule(provider_id,adaptor);
"#;

/// 初始化数据库表结构
pub async fn init_schema(pool: &DbPool) -> Result<()> {
    sqlx::raw_sql(SCHEMA_SQLITE).execute(pool).await?;
    Ok(())
}
