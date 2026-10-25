//! 验证事务的提交、回滚与嵌套。

#[path = "common/lib.rs"]
mod common;

use common::store;
use wind_core::error::{CoreError, Result};
use wind_core::models::TurnCreate;
use wind_core::storage::Storage;

fn turn(instance_id: i64) -> TurnCreate {
    TurnCreate {
        instance_id,
        topic_id: 1,
        from_id: None,
        is_boundary: false,
        created_at: Some(0),
    }
}

async fn count_turns(store: &Storage) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM turn")
        .fetch_one(store.pool())
        .await
        .expect("query")
}

#[tokio::test]
async fn a_transaction_that_returns_ok_keeps_its_writes() {
    let (_dir, store) = store().await;
    store
        .with_tx(|tx| async move {
            tx.turn_create(turn(1)).await?;
            Ok(())
        })
        .await
        .expect("commit");
    assert_eq!(count_turns(&store).await, 1);
}

#[tokio::test]
async fn a_transaction_that_returns_err_rolls_its_writes_back() {
    let (_dir, store) = store().await;
    let failed: Result<()> = store
        .with_tx(|tx| async move {
            tx.turn_create(turn(1)).await?;
            Err(CoreError::Internal("deliberate failure".into()))
        })
        .await;
    assert!(failed.is_err());
    assert_eq!(
        count_turns(&store).await,
        0,
        "when the closure fails, every row written inside it must be rolled back"
    );
}

#[tokio::test]
async fn a_read_through_the_outer_handle_cannot_see_the_uncommitted_row() {
    let (_dir, store) = store().await;
    let outer = store.clone();
    store
        .with_tx(move |tx| async move {
            tx.turn_create(turn(1)).await?;
            assert_eq!(
                count_turns(&outer).await,
                0,
                "the outer handle acquires another connection, so it stays outside the transaction; \
                 everything that must join the transaction goes through the closure parameter, which is why it exists"
            );
            Ok(())
        })
        .await
        .expect("finish transaction");
    assert_eq!(count_turns(&store).await, 1);
}

#[tokio::test]
async fn a_nested_call_does_not_commit_or_roll_back_on_its_own() {
    let (_dir, store) = store().await;
    store
        .with_tx(|tx| async move {
            tx.turn_create(turn(1)).await?;
            // 内层复用外层事务，只把结果返回外层。提交与回滚仍由外层决定
            let inner: Result<()> = tx
                .with_tx(|_| async move { Err(CoreError::Internal("inner failure".into())) })
                .await;
            assert!(
                inner.is_err(),
                "an inner failure must hand the error back to the outer scope"
            );
            Ok(())
        })
        .await
        .expect("finish outer transaction");
    assert_eq!(
        count_turns(&store).await,
        1,
        "an inner failure does not roll back the whole transaction by itself; the outer scope must decide to propagate the error"
    );
}

#[tokio::test]
async fn an_explicit_transaction_commits_when_asked() {
    let (_dir, store) = store().await;
    let tx = store.begin().await.expect("begin transaction");
    tx.storage().turn_create(turn(1)).await.expect("write");
    tx.commit().await.expect("commit");
    assert_eq!(count_turns(&store).await, 1);
}

#[tokio::test]
async fn an_explicit_transaction_rolls_back_when_asked() {
    let (_dir, store) = store().await;
    let tx = store.begin().await.expect("begin transaction");
    tx.storage().turn_create(turn(1)).await.expect("write");
    tx.rollback().await.expect("roll back");
    assert_eq!(count_turns(&store).await, 0);
}
