use crate::{db::DbPool, error::Result};

const SCHEMA_SQLITE: &str = r#"
CREATE TABLE IF NOT EXISTS providers (
    id              BIGINT  PRIMARY KEY,
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
    id              BIGINT  PRIMARY KEY,
    name            TEXT    NOT NULL,
    provider_id     INTEGER NOT NULL,
    alias           TEXT,
    adapter         TEXT    NOT NULL,
    modalities      TEXT    DEFAULT '[]',
    active          BOOLEAN NOT NULL,
    icon            TEXT,
    endpoint        TEXT,
    frequency       INTEGER DEFAULT 0,
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS credentials (
    id              BIGINT  PRIMARY KEY,
    provider_id     INTEGER NOT NULL,
    key             TEXT    NOT NULL,
    active          BOOLEAN NOT NULL,
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS topics (
    id              BIGINT  PRIMARY KEY,
    parent_id       INTEGER,
    chat_config_id  INTEGER NOT NULL,
    label           TEXT    NOT NULL,
    icon            TEXT,
    topic_index     INTEGER NOT NULL,
    tool_approval_policy TEXT NOT NULL DEFAULT '{"type":"allow_all"}',
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS messages (
    id              BIGINT  PRIMARY KEY,
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
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS chat_configs (
    id              BIGINT  PRIMARY KEY,
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
    id              BIGINT  PRIMARY KEY,
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
    id          BIGINT  PRIMARY KEY,
    provider_id INTEGER NOT NULL,
    adapter     TEXT    NOT NULL,
    json_rule   TEXT    NOT NULL,
    active      BOOLEAN NOT NULL,
    created_at  INTEGER,
    updated_at  INTEGER
);
CREATE TABLE IF NOT EXISTS prompt_modules (
    id              BIGINT  PRIMARY KEY,
    key             TEXT    NOT NULL UNIQUE,
    name            TEXT    NOT NULL,
    description     TEXT    NOT NULL,
    module_type     TEXT    NOT NULL,
    content         TEXT    NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT 1,
    data            TEXT    NOT NULL DEFAULT '{}',
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS agent_definitions (
    id                      BIGINT  PRIMARY KEY,
    key                     TEXT    NOT NULL UNIQUE,
    name                    TEXT    NOT NULL,
    description             TEXT    NOT NULL,
    scope                   TEXT    NOT NULL DEFAULT 'global',
    owner_topic_id          BIGINT,
    cloned_from_agent_id    BIGINT,
    role                    TEXT    NOT NULL,
    active                  BOOLEAN NOT NULL DEFAULT 1,
    data                    TEXT    NOT NULL DEFAULT '{}',
    created_at              INTEGER,
    updated_at              INTEGER
);
CREATE TABLE IF NOT EXISTS topic_agent_bindings (
    id              BIGINT  PRIMARY KEY,
    topic_id        BIGINT  NOT NULL,
    agent_id        BIGINT  NOT NULL,
    binding_role    TEXT    NOT NULL,
    alias           TEXT,
    model_id        BIGINT,
    chat_config_id  BIGINT,
    enabled         BOOLEAN NOT NULL DEFAULT 1,
    config          TEXT    NOT NULL DEFAULT '{}',
    created_at      INTEGER,
    updated_at      INTEGER
);
CREATE TABLE IF NOT EXISTS tool_approval_requests (
    id                  BIGINT  PRIMARY KEY,
    root_topic_id       BIGINT  NOT NULL,
    topic_id            BIGINT  NOT NULL,
    message_id          BIGINT  NOT NULL,
    agent_instance_id   BIGINT,
    tool_call_id        TEXT    NOT NULL,
    tool_name           TEXT    NOT NULL,
    arguments           TEXT    NOT NULL,
    status              TEXT    NOT NULL,
    decision            TEXT,
    created_at          INTEGER,
    updated_at          INTEGER
);

CREATE INDEX IF NOT EXISTS idx_name_provider ON providers(name);

CREATE INDEX IF NOT EXISTS idx_models_provider ON models(provider_id);

CREATE INDEX IF NOT EXISTS idx_credentials_provider ON credentials(provider_id);

CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic_id, message_index);

CREATE INDEX IF NOT EXISTS idx_topics_parent_id_topic_index ON topics(parent_id, topic_index);
CREATE INDEX IF NOT EXISTS idx_topics_chat_config_id ON topics(chat_config_id);

CREATE INDEX IF NOT EXISTS idx_chat_configs_topic_id ON chat_configs(topic_id);

CREATE INDEX IF NOT EXISTS idx_mcp_servers_name ON mcp_servers(name);

CREATE INDEX IF NOT EXISTS idx_provider_adapter ON json_rule(provider_id,adapter);

CREATE INDEX IF NOT EXISTS idx_prompt_modules_key ON prompt_modules(key);

CREATE INDEX IF NOT EXISTS idx_agent_definitions_key ON agent_definitions(key);
CREATE INDEX IF NOT EXISTS idx_agent_definitions_scope ON agent_definitions(scope);
CREATE INDEX IF NOT EXISTS idx_agent_definitions_owner_topic ON agent_definitions(owner_topic_id);

CREATE INDEX IF NOT EXISTS idx_topic_agent_bindings_topic ON topic_agent_bindings(topic_id);
CREATE INDEX IF NOT EXISTS idx_topic_agent_bindings_agent ON topic_agent_bindings(agent_id);
CREATE INDEX IF NOT EXISTS idx_topic_agent_bindings_role ON topic_agent_bindings(topic_id, binding_role);
CREATE UNIQUE INDEX IF NOT EXISTS idx_topic_one_main_agent
ON topic_agent_bindings(topic_id)
WHERE binding_role = 'main' AND enabled = 1;

CREATE INDEX IF NOT EXISTS idx_tool_approvals_root_topic ON tool_approval_requests(root_topic_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_topic ON tool_approval_requests(topic_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_message ON tool_approval_requests(message_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_instance ON tool_approval_requests(agent_instance_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_status ON tool_approval_requests(status);
"#;

/// 初始化数据库表结构
pub async fn init_schema(pool: &DbPool) -> Result<()> {
    sqlx::raw_sql(SCHEMA_SQLITE).execute(pool).await?;
    Ok(())
}
