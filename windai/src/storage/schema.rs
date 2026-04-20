use super::{Storage, StorageError};
use crate::storage::lock_db;
use rusqlite::Connection;

impl Storage {
    pub(crate) fn create_tables(&self) -> Result<(), StorageError> {
        let conn = lock_db!(&self);
        Self::create_tables_by_conn(&conn)
    }
    pub(crate) fn create_tables_by_conn(conn: &Connection) -> Result<(), StorageError> {
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS providers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    alias           TEXT,
    description     TEXT,
    base_url        TEXT,
    doc             TEXT,
    active          BOOLEAN NOT NULL DEFAULT 1,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);
CREATE TABLE IF NOT EXISTS models (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    alias           TEXT,
    provider_id     INTEGER NOT NULL DEFAULT,
    adaptor         TEXT    NOT NULL,
    modalities      TEXT    NOT NULL DEFAULT '[]',
    active          BOOLEAN NOT NULL DEFAULT 1,
    icon            TEXT,
    endpoint        TEXT,
    frequency       INTEGER DEFAULT 0,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);
CREATE TABLE IF NOT EXISTS credentials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id     INTEGER,
    api_key         TEXT    NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT 1,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);
CREATE TABLE IF NOT EXISTS topics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id       INTEGER,
    label           TEXT    NOT NULL,
    icon            TEXT,
    max_context     INTEGER NOT NULL DEFAULT 0,
    index           INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);
CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id         INTEGER,
    role            TEXT    NOT NULL,
    raw_content     TEXT    NOT NULL DEFAULT '',
    content         TEXT    NOT NULL DEFAULT '',
    reasoning_content TEXT,
    transcript      TEXT,
    content_type    TEXT    NOT NULL,
    model_id        INTEGER NOT NULL,
    topic_id        INTEGER NOT NULL,
    index           INTEGER NOT NULL DEFAULT 10,
    stream          BOOLEAN NOT NULL DEFAULT 0,
    is_boundary     BOOLEAN NOT NULL DEFAULT 0,
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_models_provider ON models(provider_id);
CREATE INDEX IF NOT EXISTS idx_credentials_provider ON credentials(provider_id);
CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic_id, index);
            "#,
        )?;

        Ok(())
    }
}
