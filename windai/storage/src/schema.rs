use super::db::DbPool;
use super::error::Result;

// TODO: 旧的，仅供参考
// pub const SCHEMA: &str = r#"
// CREATE TABLE IF NOT EXISTS providers (
//     id              INTEGER PRIMARY KEY AUTOINCREMENT,
//     name            TEXT    NOT NULL UNIQUE,
//     alias           TEXT,
//     description     TEXT,
//     base_url        TEXT    NOT NULL,
//     doc             TEXT,
//     active          BOOLEAN NOT NULL,
//     created_at      BIGINT,
//     updated_at      BIGINT
// );
// CREATE TABLE IF NOT EXISTS models (
//     id              INTEGER PRIMARY KEY AUTOINCREMENT,
//     name            TEXT    NOT NULL,
//     provider_id     BIGINT  NOT NULL,
//     alias           TEXT,
//     adapter         TEXT    NOT NULL,
//     modalities      TEXT    DEFAULT '[]',
//     active          BOOLEAN NOT NULL,
//     icon            TEXT,
//     endpoint        TEXT,
//     config          TEXT,
//     frequency       BIGINT DEFAULT 0,
//     created_at      BIGINT,
//     updated_at      BIGINT
// );
// CREATE TABLE IF NOT EXISTS credentials (
//     id              INTEGER PRIMARY KEY AUTOINCREMENT,
//     provider_id     BIGINT  NOT NULL,
//     key             TEXT    NOT NULL,
//     active          BOOLEAN NOT NULL,
//     created_at      BIGINT,
//     updated_at      BIGINT
// );
// CREATE TABLE IF NOT EXISTS topics (
//     id                  INTEGER PRIMARY KEY AUTOINCREMENT,
//     parent_id           BIGINT,
//     label               TEXT    NOT NULL,
//     model_id            BIGINT,
//     tool_approval_policy TEXT    NOT NULL,
//     icon                TEXT,
//     created_at          BIGINT,
//     updated_at          BIGINT
// );
// CREATE TABLE IF NOT EXISTS json_rule (
//     id          INTEGER PRIMARY KEY AUTOINCREMENT,
//     provider_id BIGINT  NOT NULL,
//     adapter     TEXT    NOT NULL,
//     json_rule   TEXT    NOT NULL,
//     active      BOOLEAN NOT NULL,
//     created_at  BIGINT,
//     updated_at  BIGINT
// );
// CREATE TABLE IF NOT EXISTS mcp_servers (
//     id          INTEGER PRIMARY KEY AUTOINCREMENT,
//     type        TEXT    NOT NULL,
//     name        TEXT    NOT NULL UNIQUE,
//     url         TEXT,
//     description TEXT,
//     command     TEXT,
//     args        TEXT    DEFAULT '[]',
//     env         TEXT    DEFAULT '{}',
//     created_at  BIGINT,
//     updated_at  BIGINT
// );
// CREATE TABLE IF NOT EXISTS turn (
//     id              INTEGER PRIMARY KEY AUTOINCREMENT,
//     instance_id     BIGINT  NOT NULL,
//     topic_id        BIGINT  NOT NULL,
//     from_id         BIGINT,
//     is_boundary     BOOLEAN NOT NULL DEFAULT FALSE,
//     created_at      BIGINT
// );
// CREATE TABLE IF NOT EXISTS message (
//     id              INTEGER PRIMARY KEY AUTOINCREMENT,
//     turn_id         BIGINT  NOT NULL,
//     seq             BIGINT  NOT NULL,
//     role            TEXT    NOT NULL,
//     model_id        BIGINT,
//     finish_reason   TEXT,
//     input_tokens    BIGINT  NOT NULL DEFAULT 0,
//     output_tokens   BIGINT  NOT NULL DEFAULT 0,
//     created_at      BIGINT
// );
// CREATE TABLE IF NOT EXISTS block (
//     id              INTEGER PRIMARY KEY,
//     message_id      BIGINT  NOT NULL,
//     seq             BIGINT  NOT NULL,
//     kind            TEXT    NOT NULL,
//     call_id         TEXT,
//     revision        BIGINT  NOT NULL DEFAULT 0,
//     content         TEXT    NOT NULL DEFAULT '',
//     data            TEXT,
//     finished        BOOLEAN NOT NULL DEFAULT FALSE
// );
// CREATE INDEX IF NOT EXISTS idx_turn_instance ON turn(instance_id, id);
// CREATE INDEX IF NOT EXISTS idx_turn_topic ON turn(topic_id, id);
// CREATE UNIQUE INDEX IF NOT EXISTS idx_message_turn ON message(turn_id, seq);
// CREATE UNIQUE INDEX IF NOT EXISTS idx_block_message ON block(message_id, seq);
// CREATE INDEX IF NOT EXISTS idx_block_call_id ON block(call_id);
// CREATE TABLE IF NOT EXISTS agent_instances (
//     id              INTEGER PRIMARY KEY AUTOINCREMENT,
//     parent_id       BIGINT,
//     topic_id        BIGINT  NOT NULL,
//     role            TEXT    NOT NULL,
//     mode            TEXT,
//     phase           TEXT    NOT NULL,
//     phase_payload   TEXT,
//     created_at      BIGINT,
//     updated_at      BIGINT
// );
// CREATE TABLE IF NOT EXISTS external_waits (
//     handle          TEXT PRIMARY KEY,
//     instance_id     BIGINT  NOT NULL,
//     kind            TEXT    NOT NULL,
//     message_id      BIGINT  NOT NULL,
//     payload_json    TEXT    NOT NULL,
//     created_at      BIGINT  NOT NULL,
//     answered_at     BIGINT,
//     decision_json   TEXT
// );
// CREATE TABLE IF NOT EXISTS agent_pending (
//     id           INTEGER PRIMARY KEY AUTOINCREMENT,
//     instance_id  BIGINT NOT NULL,
//     kind         TEXT   NOT NULL,
//     payload_json TEXT   NOT NULL,
//     source_id    BIGINT,
//     created_at   BIGINT NOT NULL
// );
// CREATE TABLE IF NOT EXISTS spawn_calls (
//     parent_id           BIGINT NOT NULL,
//     call_id             TEXT   NOT NULL,
//     child_id            BIGINT NOT NULL,
//     created_at          BIGINT NOT NULL,
//     result_delivered_at BIGINT,
//     PRIMARY KEY (parent_id, call_id),
//     UNIQUE (child_id)
// );
// CREATE TABLE IF NOT EXISTS mods (
//     id              INTEGER PRIMARY KEY AUTOINCREMENT,
//     name            TEXT    NOT NULL UNIQUE,
//     current_version BIGINT  NOT NULL,
//     created_at      BIGINT  NOT NULL,
//     updated_at      BIGINT
// );
// CREATE TABLE IF NOT EXISTS mod_versions (
//     mod_id              BIGINT  NOT NULL,
//     version             BIGINT  NOT NULL,
//     manifest            TEXT    NOT NULL,
//     compiled            TEXT,
//     engine_fingerprint  TEXT,
//     created_at          BIGINT  NOT NULL,
//     PRIMARY KEY (mod_id, version)
// );
// CREATE TABLE IF NOT EXISTS topic_mods (
//     topic_id    BIGINT  NOT NULL,
//     mod_id      BIGINT  NOT NULL,
//     PRIMARY KEY (topic_id, mod_id)
// );
// CREATE TABLE IF NOT EXISTS mod_state (
//     mod_id      BIGINT  NOT NULL,
//     scope_kind  TEXT    NOT NULL,
//     scope_id    BIGINT  NOT NULL DEFAULT 0,
//     kind        TEXT    NOT NULL,
//     key         TEXT    NOT NULL,
//     value_json  TEXT    NOT NULL,
//     updated_at  BIGINT  NOT NULL,
//     PRIMARY KEY (mod_id, scope_kind, scope_id, kind, key)
// );
// CREATE TABLE IF NOT EXISTS event_log (
//     seq         BIGINT  PRIMARY KEY,
//     time        BIGINT  NOT NULL,
//     name        TEXT    NOT NULL,
//     shape       TEXT    NOT NULL,
//     scope_kind  TEXT    NOT NULL,
//     scope_id    BIGINT  NOT NULL DEFAULT 0,
//     origin      TEXT    NOT NULL,
//     source      TEXT    NOT NULL,
//     cause       BIGINT,
//     payload     TEXT    NOT NULL
// );
// CREATE INDEX IF NOT EXISTS idx_event_log_scope ON event_log(scope_kind, scope_id, seq);
// CREATE INDEX IF NOT EXISTS idx_agent_instances_topic ON agent_instances(topic_id);
// CREATE INDEX IF NOT EXISTS idx_agent_instances_parent ON agent_instances(parent_id);
// CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_instances_main ON agent_instances(topic_id) WHERE role = 'main';
// CREATE INDEX IF NOT EXISTS idx_external_waits_instance ON external_waits(instance_id);
// CREATE INDEX IF NOT EXISTS idx_external_waits_open ON external_waits(instance_id) WHERE answered_at IS NULL;
// CREATE INDEX IF NOT EXISTS idx_agent_pending_instance ON agent_pending(instance_id, id);
// CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_pending_source
//     ON agent_pending(instance_id, kind, source_id) WHERE source_id IS NOT NULL;
// CREATE INDEX IF NOT EXISTS idx_topic_mods_mod ON topic_mods(mod_id);
// "#;

pub const SCHEMA: &str = r#""#;
pub async fn create_if_missing(pool: &DbPool) -> Result<()> {
    sqlx::raw_sql(SCHEMA).execute(pool).await?;
    Ok(())
}
