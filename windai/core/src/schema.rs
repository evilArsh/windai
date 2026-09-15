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
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS models (
    id              BIGINT  PRIMARY KEY,
    name            TEXT    NOT NULL,
    provider_id     BIGINT  NOT NULL,
    alias           TEXT,
    adapter         TEXT    NOT NULL,
    modalities      TEXT    DEFAULT '[]',
    active          BOOLEAN NOT NULL,
    icon            TEXT,
    endpoint        TEXT,
    config          TEXT,
    frequency       BIGINT DEFAULT 0,
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS credentials (
    id              BIGINT  PRIMARY KEY,
    provider_id     BIGINT  NOT NULL,
    key             TEXT    NOT NULL,
    active          BOOLEAN NOT NULL,
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS topics (
    id              BIGINT  PRIMARY KEY,
    parent_id       BIGINT,
    label           TEXT    NOT NULL,
    model_id        BIGINT,
    tool_approval_policy TEXT,
    icon            TEXT,
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS messages (
    id              BIGINT  PRIMARY KEY,
    from_id         BIGINT,
    content         TEXT    NOT NULL DEFAULT '[]',
    model_id        BIGINT NOT NULL,
    instance_id     BIGINT NOT NULL,
    is_boundary     BOOLEAN NOT NULL,
    is_excluded     BOOLEAN NOT NULL,
    input_tokens    BIGINT NOT NULL DEFAULT 0,
    output_tokens   BIGINT NOT NULL DEFAULT 0,
    created_at      BIGINT,
    updated_at      BIGINT
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
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS json_rule (
    id          BIGINT  PRIMARY KEY,
    provider_id BIGINT  NOT NULL,
    adapter     TEXT    NOT NULL,
    json_rule   TEXT    NOT NULL,
    active      BOOLEAN NOT NULL,
    created_at  BIGINT,
    updated_at  BIGINT
);
CREATE TABLE IF NOT EXISTS prompt_modules (
    id              BIGINT  PRIMARY KEY,
    alias           TEXT    NOT NULL,
    description     TEXT    NOT NULL,
    content         TEXT    NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT 1,
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS agent_definitions (
    id                      BIGINT  PRIMARY KEY,
    key                     TEXT    NOT NULL UNIQUE,
    name                    TEXT    NOT NULL,
    description             TEXT    NOT NULL,
    owner_topic_id          BIGINT,
    cloned_from_id          BIGINT,
    active                  BOOLEAN NOT NULL DEFAULT 1,
    data                    TEXT    NOT NULL DEFAULT '{}',
    created_at              BIGINT,
    updated_at              BIGINT
);
CREATE TABLE IF NOT EXISTS agent_instances (
    id              BIGINT  PRIMARY KEY,
    parent_id       BIGINT,
    topic_id        BIGINT  NOT NULL,
    agent_id        BIGINT,
    role            TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'idle',
    mode            TEXT,
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS topic_agent_maps (
    id              BIGINT  PRIMARY KEY,
    topic_id        BIGINT  NOT NULL,
    agent_id        BIGINT  NOT NULL,
    created_at      BIGINT,
    updated_at      BIGINT
);
CREATE TABLE IF NOT EXISTS tool_approval_requests (
    id                  BIGINT  PRIMARY KEY,
    topic_id            BIGINT  NOT NULL,
    message_id          BIGINT  NOT NULL,
    instance_id         BIGINT  NOT NULL,
    tool_call_id        TEXT    NOT NULL,
    tool_name           TEXT    NOT NULL,
    arguments           TEXT    NOT NULL,
    status              TEXT    NOT NULL,
    created_at          BIGINT,
    updated_at          BIGINT
);

CREATE INDEX IF NOT EXISTS idx_topics_parent_id ON topics(parent_id);

CREATE INDEX IF NOT EXISTS idx_name_provider ON providers(name);

CREATE INDEX IF NOT EXISTS idx_models_provider ON models(provider_id);

CREATE INDEX IF NOT EXISTS idx_credentials_provider ON credentials(provider_id);

CREATE INDEX IF NOT EXISTS idx_messages_instance ON messages(instance_id, id);

CREATE INDEX IF NOT EXISTS idx_mcp_servers_name ON mcp_servers(name);

CREATE INDEX IF NOT EXISTS idx_provider_adapter ON json_rule(provider_id,adapter);

CREATE INDEX IF NOT EXISTS idx_agent_definitions_owner_topic ON agent_definitions(owner_topic_id);

CREATE INDEX IF NOT EXISTS idx_agent_instances_topic ON agent_instances(topic_id);
CREATE INDEX IF NOT EXISTS idx_agent_instances_agent ON agent_instances(agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_instances_parent ON agent_instances(parent_id);
CREATE INDEX IF NOT EXISTS idx_agent_instances_role ON agent_instances(topic_id, role);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_instances_unique_main ON agent_instances(topic_id) WHERE role = 'main';

CREATE INDEX IF NOT EXISTS idx_topic_agent_maps_topic ON topic_agent_maps(topic_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_topic_agent_maps_unique ON topic_agent_maps(topic_id, agent_id);

CREATE INDEX IF NOT EXISTS idx_tool_approvals_topic ON tool_approval_requests(topic_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_message ON tool_approval_requests(message_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_instance ON tool_approval_requests(instance_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_status ON tool_approval_requests(status);
"#;

/// 初始化数据库表结构
pub async fn init_schema(pool: &DbPool) -> Result<()> {
    sqlx::raw_sql(SCHEMA_SQLITE).execute(pool).await?;
    Ok(())
}
