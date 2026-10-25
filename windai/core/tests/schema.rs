//! 验证建库可重复执行，以及表约束生效。

use sqlx::Row;
use tempfile::TempDir;
use wind_core::db::Db;

async fn fresh() -> (TempDir, Db) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Db::open(&dir.path().join("test.db"))
        .await
        .expect("open database");
    db.init_schema().await.expect("create table");
    (dir, db)
}

#[tokio::test]
async fn building_the_schema_twice_succeeds() {
    let (_dir, db) = fresh().await;
    db.init_schema().await.expect("build schema twice");
}

#[tokio::test]
async fn a_topic_cannot_hold_two_main_instances() {
    let (_dir, db) = fresh().await;
    let insert =
        "INSERT INTO agent_instances (topic_id, role, phase) VALUES (1, 'main', 'running')";
    sqlx::query(insert)
        .execute(db.pool())
        .await
        .expect("insert first main instance");
    assert!(
        sqlx::query(insert).execute(db.pool()).await.is_err(),
        "a second main instance must be rejected by the unique index"
    );

    let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM agent_instances WHERE topic_id = 1")
        .fetch_one(db.pool())
        .await
        .expect("query")
        .get("n");
    assert_eq!(
        count, 1,
        "a second main instance must be rejected by the unique index"
    );
}

#[tokio::test]
async fn the_block_identifier_is_supplied_by_the_caller() {
    let (_dir, db) = fresh().await;
    sqlx::query("INSERT INTO message (id, turn_id, seq, role) VALUES (1, 1, 0, 'assistant')")
        .execute(db.pool())
        .await
        .expect("insert message");
    sqlx::query(
        "INSERT INTO block (id, message_id, seq, kind) VALUES (9007199254740993, 1, 0, 'text')",
    )
    .execute(db.pool())
    .await
    .expect("insert block with explicit id");
}

#[tokio::test]
async fn two_blocks_cannot_share_a_position_in_a_message() {
    let (_dir, db) = fresh().await;
    sqlx::query("INSERT INTO message (id, turn_id, seq, role) VALUES (1, 1, 0, 'assistant')")
        .execute(db.pool())
        .await
        .expect("insert message");
    let insert = "INSERT INTO block (id, message_id, seq, kind) VALUES (?, 1, 0, 'text')";
    sqlx::query(insert)
        .bind(1_i64)
        .execute(db.pool())
        .await
        .expect("insert first block");
    assert!(
        sqlx::query(insert)
            .bind(2_i64)
            .execute(db.pool())
            .await
            .is_err(),
        "a second block in the same message must not occupy the same position: a duplicate seq from the assembler must be reported at the write step"
    );
}
