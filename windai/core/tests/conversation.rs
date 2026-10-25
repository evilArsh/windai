//! 验证会话、消息与块三层的读写。

#[path = "common/lib.rs"]
mod common;

use common::store;
use wind_core::models::{BlockKind, BlockRow, MessageCreate, Role, TurnCreate};

fn block(id: i64, message_id: i64, seq: i64, call_id: Option<&str>) -> BlockRow {
    BlockRow {
        id,
        message_id,
        seq,
        kind: if call_id.is_some() {
            BlockKind::ToolResult
        } else {
            BlockKind::Text
        },
        call_id: call_id.map(str::to_string),
        revision: 1,
        content: "22 degrees".into(),
        data: None,
        finished: true,
    }
}

#[tokio::test]
async fn a_new_database_starts_the_block_counter_at_zero() {
    let (_dir, store) = store().await;
    let seed = store.block_id_seed().await.expect("query");
    assert_eq!(seed, 0, "an empty database starts the block id seed at 0");
}

#[tokio::test]
async fn the_seed_follows_the_largest_existing_block() {
    let (_dir, store) = store().await;
    store
        .turn_create(TurnCreate {
            instance_id: 1,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: Some(0),
        })
        .await
        .expect("build turn");
    store
        .message_create(MessageCreate {
            turn_id: 1,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: Some(0),
        })
        .await
        .expect("build message");
    store
        .block_insert(block(41, 1, 0, None))
        .await
        .expect("insert block");
    assert_eq!(
        store.block_id_seed().await.expect("query"),
        41,
        "the id of a new block after a restart must continue after the existing blocks, otherwise history would be overwritten"
    );
}

#[tokio::test]
async fn blocks_come_back_in_message_and_sequence_order() {
    let (_dir, store) = store().await;
    store
        .turn_create(TurnCreate {
            instance_id: 1,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: Some(0),
        })
        .await
        .expect("build turn");
    store
        .message_create(MessageCreate {
            turn_id: 1,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: Some(0),
        })
        .await
        .expect("build message");
    store
        .block_insert(block(2, 1, 1, None))
        .await
        .expect("insert");
    store
        .block_insert(block(1, 1, 0, None))
        .await
        .expect("insert");

    let rows = store.block_list_by_message(1).await.expect("query");
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test]
async fn a_tool_result_is_found_by_its_pairing_key() {
    let (_dir, store) = store().await;
    store
        .turn_create(TurnCreate {
            instance_id: 1,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: Some(0),
        })
        .await
        .expect("build turn");
    store
        .message_create(MessageCreate {
            turn_id: 1,
            role: Role::Tool,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: Some(0),
        })
        .await
        .expect("build message");
    store
        .block_insert(block(1, 1, 0, Some("abc")))
        .await
        .expect("insert");

    let found = store
        .block_find_tool_result("abc")
        .await
        .expect("query")
        .expect("must be found");
    assert_eq!(found.call_id.as_deref(), Some("abc"));
    assert!(
        store
            .block_find_tool_result("missing")
            .await
            .expect("query")
            .is_none(),
        "a call with no pairing returns none instead of an error"
    );
}

#[tokio::test]
async fn an_uncommitted_block_is_never_returned_as_history() {
    let (_dir, store) = store().await;
    store
        .turn_create(TurnCreate {
            instance_id: 1,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: Some(0),
        })
        .await
        .expect("build turn");
    store
        .message_create(MessageCreate {
            turn_id: 1,
            role: Role::Assistant,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: Some(0),
        })
        .await
        .expect("build message");
    store
        .block_insert(block(1, 1, 0, None))
        .await
        .expect("insert");
    let mut pending = block(2, 1, 1, None);
    pending.finished = false;
    store.block_insert(pending).await.expect("insert");

    let rows = store.block_list_by_message(1).await.expect("query");
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    assert_eq!(
        ids,
        vec![1],
        "an uncommitted block must not be read by history queries: reading it would show a half-written message to the user as if it were committed"
    );
}

#[tokio::test]
async fn an_uncommitted_tool_result_is_not_paired_to_a_call() {
    let (_dir, store) = store().await;
    store
        .turn_create(TurnCreate {
            instance_id: 1,
            topic_id: 1,
            from_id: None,
            is_boundary: false,
            created_at: Some(0),
        })
        .await
        .expect("build turn");
    store
        .message_create(MessageCreate {
            turn_id: 1,
            role: Role::Tool,
            model_id: None,
            finish_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            created_at: Some(0),
        })
        .await
        .expect("build message");
    let mut pending = block(1, 1, 0, Some("abc"));
    pending.finished = false;
    store.block_insert(pending).await.expect("insert");

    assert!(
        store
            .block_find_tool_result("abc")
            .await
            .expect("query")
            .is_none(),
        "an unfinished tool result must not be paired with a call: pairing it would show the model a result for a call that never finished"
    );
}
