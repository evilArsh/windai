//! 验证配置实体的读写。

#[path = "common/lib.rs"]
mod common;

use common::store;
use wind_core::models::{AllowPolicy, TopicCreate, TopicUpdate};

#[tokio::test]
async fn a_new_topic_pre_approves_everything() {
    let (_dir, store) = store().await;
    let id = store
        .topic_create(TopicCreate {
            label: "work".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    let topic = store
        .topic_get(id)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(
        topic.tool_approval_policy.allow,
        vec!["*".to_string()],
        "a new topic pre-approves everything: a fresh install should not stop the first tool call waiting for confirmation"
    );
}

#[tokio::test]
async fn the_three_policy_shapes_stay_distinct() {
    let (_dir, store) = store().await;
    let all = store
        .topic_create(TopicCreate {
            label: "allow all".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    let some = store
        .topic_create(TopicCreate {
            label: "allowlist".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    let none = store
        .topic_create(TopicCreate {
            label: "ask all".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    store
        .topic_update(
            some,
            TopicUpdate {
                tool_approval_policy: Some(AllowPolicy {
                    allow: vec!["fetch".into()],
                }),
                ..Default::default()
            },
        )
        .await
        .expect("set");
    store
        .topic_update(
            none,
            TopicUpdate {
                tool_approval_policy: Some(AllowPolicy { allow: vec![] }),
                ..Default::default()
            },
        )
        .await
        .expect("set");

    let a = store
        .topic_get(all)
        .await
        .expect("query")
        .expect("must exist");
    let b = store
        .topic_get(some)
        .await
        .expect("query")
        .expect("must exist");
    let c = store
        .topic_get(none)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(a.tool_approval_policy.allow, vec!["*".to_string()]);
    assert_eq!(b.tool_approval_policy.allow, vec!["fetch".to_string()]);
    assert_eq!(c.tool_approval_policy.allow, Vec::<String>::new());
}

#[tokio::test]
async fn an_empty_list_is_stored_as_an_empty_list_not_as_a_wildcard() {
    let (_dir, store) = store().await;
    let id = store
        .topic_create(TopicCreate {
            label: "ask all".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    store
        .topic_update(
            id,
            TopicUpdate {
                tool_approval_policy: Some(AllowPolicy { allow: vec![] }),
                ..Default::default()
            },
        )
        .await
        .expect("set");

    let raw: String = sqlx::query_scalar("SELECT tool_approval_policy FROM topics WHERE id = ?")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .expect("query");
    assert_eq!(
        raw, "{\"allow\":[]}",
        "an empty list must be stored as-is: it means \"everything needs review\", and writing a wildcard silently allows everything"
    );
}

#[tokio::test]
async fn setting_a_label_keeps_the_policy_untouched() {
    let (_dir, store) = store().await;
    let id = store
        .topic_create(TopicCreate {
            label: "work".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    store
        .topic_update(
            id,
            TopicUpdate {
                tool_approval_policy: Some(AllowPolicy {
                    allow: vec!["fetch".into()],
                }),
                ..Default::default()
            },
        )
        .await
        .expect("set");
    store
        .topic_update(
            id,
            TopicUpdate {
                label: Some("worksheet".into()),
                ..Default::default()
            },
        )
        .await
        .expect("rename topic");

    let topic = store
        .topic_get(id)
        .await
        .expect("query")
        .expect("must exist");
    assert_eq!(topic.label, "worksheet");
    assert_eq!(
        topic.tool_approval_policy.allow,
        vec!["fetch".to_string()],
        "renaming must not wipe out the policy along with the name"
    );
}

#[tokio::test]
async fn deleting_a_topic_removes_it() {
    let (_dir, store) = store().await;
    let id = store
        .topic_create(TopicCreate {
            label: "temp".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    store.topic_delete(id).await.expect("delete");
    assert!(store.topic_get(id).await.expect("query").is_none());
}

#[tokio::test]
async fn a_malformed_allow_list_is_reported_rather_than_silently_emptied() {
    let (_dir, store) = store().await;
    let id = store
        .topic_create(TopicCreate {
            label: "work".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    // 绕过存储层直接写一个形状错误的值。`tool_approval_policy` 存成字符串而不是数组
    sqlx::query("UPDATE topics SET tool_approval_policy = '{\"allow\":\"fetch\"}' WHERE id = ?")
        .bind(id)
        .execute(store.pool())
        .await
        .expect("write");

    assert!(
        store.topic_get(id).await.is_err(),
        "a non-array allow list must raise an error rather than fall back to an empty list -- an empty list means \"nothing is pre-approved\", \
         and silent degradation would divert every tool call to a question while the caller cannot tell the config is broken"
    );
}

#[tokio::test]
async fn an_allow_entry_that_is_not_a_string_is_rejected() {
    let (_dir, store) = store().await;
    let id = store
        .topic_create(TopicCreate {
            label: "work".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    sqlx::query(
        "UPDATE topics SET tool_approval_policy = '{\"allow\":[\"fetch\",7]}' WHERE id = ?",
    )
    .bind(id)
    .execute(store.pool())
    .await
    .expect("write");

    assert!(
        store.topic_get(id).await.is_err(),
        "a non-string entry mixed into the allow list must be reported rather than silently dropped"
    );
}

#[tokio::test]
async fn topics_are_listed_by_parent() {
    let (_dir, store) = store().await;
    let root = store
        .topic_create(TopicCreate {
            label: "root".into(),
            ..Default::default()
        })
        .await
        .expect("build");
    store
        .topic_create(TopicCreate {
            label: "first child".into(),
            parent_id: Some(root),
            ..Default::default()
        })
        .await
        .expect("build");
    store
        .topic_create(TopicCreate {
            label: "second child".into(),
            parent_id: Some(root),
            ..Default::default()
        })
        .await
        .expect("build");

    assert_eq!(store.topic_list().await.expect("query").len(), 3);
    assert_eq!(store.topic_children_of(root).await.expect("query").len(), 2);
}
