use super::db::{BEGIN_WRITE, DbPool};
use super::error::Result;

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS providers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    alias       TEXT,
    description TEXT,
    base_url    TEXT    NOT NULL,
    doc         TEXT,
    active      BOOLEAN NOT NULL,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS models (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id BIGINT  NOT NULL,
    name        TEXT    NOT NULL,
    alias       TEXT,
    adapter     TEXT    NOT NULL,
    modalities  TEXT,
    active      BOOLEAN NOT NULL,
    icon        TEXT,
    endpoint    TEXT,
    config      TEXT,
    frequency   BIGINT,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS credentials (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id BIGINT  NOT NULL,
    key         TEXT    NOT NULL,
    active      BOOLEAN NOT NULL,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS json_rule (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id BIGINT  NOT NULL,
    adapter     TEXT    NOT NULL,
    json_rule   TEXT    NOT NULL,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS topics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id   BIGINT,
    label       TEXT    NOT NULL,
    model_id    BIGINT,
    icon        TEXT,
    workspace   TEXT,
    created_at  BIGINT,
    updated_at  BIGINT
);
CREATE TABLE IF NOT EXISTS turn (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id BIGINT  NOT NULL,
    topic_id    BIGINT  NOT NULL,
    from_id     BIGINT,
    created_at  BIGINT
);
CREATE TABLE IF NOT EXISTS message (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    turn_id         BIGINT  NOT NULL,
    role            TEXT    NOT NULL,
    model_id        BIGINT,
    finish_reason   TEXT,
    input_tokens    BIGINT  NOT NULL DEFAULT 0,
    output_tokens   BIGINT  NOT NULL DEFAULT 0,
    created_at      BIGINT
);
CREATE TABLE IF NOT EXISTS block (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      BIGINT  NOT NULL,
    seq             BIGINT  NOT NULL,
    kind            TEXT    NOT NULL,
    role            TEXT    NOT NULL,
    revision        BIGINT  NOT NULL DEFAULT 0,
    content         TEXT,
    format          TEXT,
    item_id         TEXT,
    call_id         TEXT,
    call_name       TEXT,
    call_arguments  TEXT,
    call_res        TEXT
);
CREATE TABLE IF NOT EXISTS agent_instances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id       BIGINT,
    topic_id        BIGINT  NOT NULL,
    role            TEXT    NOT NULL,
    mode            TEXT,
    phase           TEXT    NOT NULL,
    phase_payload   TEXT,
    created_at      BIGINT
);
CREATE TABLE IF NOT EXISTS agent_pending (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id  BIGINT  NOT NULL,
    kind         TEXT    NOT NULL,
    payload_json TEXT    NOT NULL,
    source_id    BIGINT,
    created_at   BIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS spawn_calls (
    parent_id           BIGINT NOT NULL,
    call_id             TEXT   NOT NULL,
    child_id            BIGINT NOT NULL,
    created_at          BIGINT NOT NULL,
    result_delivered_at BIGINT,
    PRIMARY KEY (parent_id, call_id),
    UNIQUE (child_id)
);
CREATE TABLE IF NOT EXISTS external_waits (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id   BIGINT  NOT NULL,
    message_id    BIGINT  NOT NULL,
    payload_json  TEXT    NOT NULL,
    created_at    BIGINT  NOT NULL,
    answered_at   BIGINT,
    decision_json TEXT
);
CREATE TABLE IF NOT EXISTS event_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch           BIGINT  NOT NULL,
    seq             BIGINT  NOT NULL,
    create_time     BIGINT  NOT NULL,
    name            TEXT    NOT NULL,
    shape           TEXT    NOT NULL,
    route_kind      TEXT    NOT NULL,
    route_name      TEXT,
    route_channel   TEXT,
    topic_id        BIGINT,
    instance_id     BIGINT,
    origin          TEXT    NOT NULL,
    source_kind     TEXT    NOT NULL,
    source_name     TEXT    NOT NULL,
    source_version  BIGINT  NOT NULL DEFAULT 0,
    payload         TEXT,
    UNIQUE (epoch, seq)
);
CREATE TABLE IF NOT EXISTS feed_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    epoch     BIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS mods (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT    NOT NULL UNIQUE,
    version             INTEGER NOT NULL,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    manifest            TEXT    NOT NULL,
    compiled            TEXT,
    engine_fingerprint  TEXT
);
CREATE TABLE IF NOT EXISTS topic_mods (
    topic_id INTEGER NOT NULL,
    mod_id   INTEGER NOT NULL,
    PRIMARY KEY (topic_id, mod_id),
    FOREIGN KEY (topic_id) REFERENCES topics (id) ON DELETE CASCADE,
    FOREIGN KEY (mod_id) REFERENCES mods (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_turn_instance ON turn(instance_id, id);
CREATE INDEX IF NOT EXISTS idx_turn_topic ON turn(topic_id, id);

CREATE INDEX IF NOT EXISTS idx_message_turn ON message(turn_id, id);

CREATE INDEX IF NOT EXISTS idx_credentials_active
    ON credentials(provider_id) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_json_rule_provider_adapter
    ON json_rule(provider_id, adapter);

CREATE UNIQUE INDEX IF NOT EXISTS idx_block_message ON block(message_id, seq);
CREATE INDEX IF NOT EXISTS idx_block_call_id ON block(call_id);
CREATE INDEX IF NOT EXISTS idx_block_tool_result ON block(call_id) WHERE kind = 'tool_result';

CREATE INDEX IF NOT EXISTS idx_agent_instances_topic ON agent_instances(topic_id);
CREATE INDEX IF NOT EXISTS idx_agent_instances_parent ON agent_instances(parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_instances_main
    ON agent_instances(topic_id) WHERE role = 'main';

CREATE INDEX IF NOT EXISTS idx_agent_pending_instance ON agent_pending(instance_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_pending_source
    ON agent_pending(instance_id, kind, source_id) WHERE source_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_external_waits_instance ON external_waits(instance_id);
CREATE INDEX IF NOT EXISTS idx_external_waits_open
    ON external_waits(instance_id) WHERE answered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_event_log_topic_key
    ON event_log(topic_id, instance_id, epoch, seq);
CREATE INDEX IF NOT EXISTS idx_event_log_route_key
    ON event_log(route_name, route_channel, epoch, seq);
CREATE INDEX IF NOT EXISTS idx_event_log_global_key
    ON event_log(epoch, seq) WHERE topic_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_topic_mods_mod ON topic_mods(mod_id, topic_id);

INSERT INTO feed_meta (singleton, epoch) VALUES (1, 0)
    ON CONFLICT (singleton) DO NOTHING;
"#;

pub async fn create_if_missing(pool: &DbPool) -> Result<()> {
    let mut tx = pool.begin_with(BEGIN_WRITE).await?;
    sqlx::raw_sql(SCHEMA).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

/// 给已有数据库补充后加的列
pub async fn ensure_columns(pool: &DbPool) -> Result<()> {
    match sqlx::query("ALTER TABLE topics ADD COLUMN workspace TEXT")
        .execute(pool)
        .await
    {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(error)) if error.message().contains("duplicate column") => Ok(()),
        Err(error) => Err(error.into()),
    }
}
