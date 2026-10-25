use crate::error::Result;
use crate::models::{BlockRow, Message as MessageRow};
use crate::storage::Storage;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use wind_stream::block::{BlockKind, MEDIA_FILE, MEDIA_IMAGE};
use wind_stream::id::TurnId;
use wind_wire::message::{Content, Message, Role};
use wind_wire::tool::{FunctionCall, FunctionCallOutput};

/// 持有所有投影缓存；某一轮写入后，让每个缓存丢掉这一轮
#[derive(Clone, Default)]
pub struct ProjectionInvalidations {
    caches: Arc<Mutex<Vec<Weak<TurnCache>>>>,
}

impl ProjectionInvalidations {
    fn track(&self, cache: &Arc<TurnCache>) {
        self.lock().push(Arc::downgrade(cache));
    }

    pub fn watched(&self) -> bool {
        let mut caches = self.lock();
        caches.retain(|cache| cache.strong_count() > 0);
        !caches.is_empty()
    }

    pub fn invalidate(&self, turn: TurnId) {
        let mut caches = self.lock();
        caches.retain(|cache| match cache.upgrade() {
            Some(cache) => {
                cache.invalidate_turn(turn);
                true
            }
            None => false,
        });
    }

    pub fn invalidate_all(&self) {
        let mut caches = self.lock();
        caches.retain(|cache| match cache.upgrade() {
            Some(cache) => {
                cache.clear();
                true
            }
            None => false,
        });
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Weak<TurnCache>>> {
        self.caches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct TurnCache {
    turns: Mutex<BTreeMap<TurnId, Vec<Message>>>,
    weight: AtomicU64,
}

impl TurnCache {
    fn contains(&self, turn: TurnId) -> bool {
        self.lock().contains_key(&turn)
    }

    fn get(&self, turn: TurnId) -> Option<Vec<Message>> {
        self.lock().get(&turn).cloned()
    }

    fn put(&self, turn: TurnId, messages: Vec<Message>) {
        let added = messages.iter().map(weight_of).sum::<u64>();
        let mut turns = self.lock();
        let replaced = turns.insert(turn, messages);
        self.weight.fetch_add(added, Ordering::Relaxed);
        if let Some(replaced) = replaced {
            let removed = replaced.iter().map(weight_of).sum::<u64>();
            self.weight.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    fn invalidate_turn(&self, turn: TurnId) {
        let mut turns = self.lock();
        if let Some(removed) = turns.remove(&turn) {
            let removed = removed.iter().map(weight_of).sum::<u64>();
            self.weight.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    fn clear(&self) {
        let mut turns = self.lock();
        turns.clear();
        self.weight.store(0, Ordering::Relaxed);
    }

    /// 缓存里所有消息大约占多少字节
    fn weight(&self) -> u64 {
        self.weight.load(Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<TurnId, Vec<Message>>> {
        self.turns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// 按轮保存投影出的上下文；某一轮有块提交或消息定稿时，丢掉这一轮
pub struct ContextProjection {
    storage: Storage,
    instance_id: i64,
    cache: Arc<TurnCache>,
    rebuilt: AtomicU64,
}

impl ContextProjection {
    pub fn new(storage: Storage, instance_id: i64, invalidations: ProjectionInvalidations) -> Self {
        let cache = Arc::new(TurnCache {
            turns: Mutex::new(BTreeMap::new()),
            weight: AtomicU64::new(0),
        });
        invalidations.track(&cache);
        Self {
            storage,
            instance_id,
            cache,
            rebuilt: AtomicU64::new(0),
        }
    }

    pub async fn context(&self, through: Option<TurnId>) -> Result<Vec<Message>> {
        let turns: Vec<TurnId> = self
            .storage
            .turn_list_by_instance(self.instance_id)
            .await?
            .into_iter()
            .map(|turn| TurnId(turn.id))
            .filter(|turn| through.is_none_or(|bound| turn.0 <= bound.0))
            .collect();
        let missing: Vec<TurnId> = turns
            .iter()
            .copied()
            .filter(|turn| !self.cache.contains(*turn))
            .collect();
        let rebuilt = self.rebuild(&missing).await?;
        self.rebuilt.fetch_add(rebuilt, Ordering::Relaxed);

        let mut context = Vec::new();
        for turn in turns {
            if let Some(messages) = self.cache.get(turn) {
                context.extend(messages);
            }
        }
        Ok(normalize(context))
    }

    pub fn invalidate(&self, turn: TurnId) {
        self.cache.invalidate_turn(turn);
    }

    pub fn rebuilt_turns(&self) -> u64 {
        self.rebuilt.load(Ordering::Relaxed)
    }

    pub(crate) fn weight(&self) -> u64 {
        self.cache.weight()
    }

    async fn rebuild(&self, missing: &[TurnId]) -> Result<u64> {
        if missing.is_empty() {
            return Ok(0);
        }
        let turn_ids: Vec<i64> = missing.iter().map(|turn| turn.0).collect();
        let messages = self.storage.message_list_by_turns(&turn_ids).await?;
        let message_ids: Vec<i64> = messages.iter().map(|row| row.id).collect();
        let blocks = self.storage.block_list_by_messages(&message_ids).await?;

        let grouped = group_blocks(&blocks);
        let mut by_turn: HashMap<i64, Vec<&MessageRow>> = HashMap::new();
        for row in &messages {
            by_turn.entry(row.turn_id).or_default().push(row);
        }
        let mut rebuilt = 0;
        for turn in missing {
            let Some(rows) = by_turn.get(&turn.0) else {
                continue;
            };
            self.cache.put(*turn, project_rows(rows, &grouped));
            rebuilt += 1;
        }
        Ok(rebuilt)
    }
}

fn group_blocks(blocks: &[BlockRow]) -> HashMap<i64, Vec<&BlockRow>> {
    let mut grouped: HashMap<i64, Vec<&BlockRow>> = HashMap::new();
    for block in blocks {
        grouped.entry(block.message_id).or_default().push(block);
    }
    grouped
}

fn project_rows(messages: &[&MessageRow], grouped: &HashMap<i64, Vec<&BlockRow>>) -> Vec<Message> {
    messages
        .iter()
        .map(|row| match grouped.get(&row.id) {
            Some(own) => project_message(row, own),
            None => project_message(row, &[]),
        })
        .collect()
}

fn project_message(row: &MessageRow, blocks: &[&BlockRow]) -> Message {
    let mut message = Message {
        role: row.role,
        created_at: row.created_at.unwrap_or(0),
        input_tokens: tokens(row.input_tokens),
        output_tokens: tokens(row.output_tokens),
        ..Message::default()
    };
    let mut reasoning = String::new();
    let mut calls = Vec::new();
    for block in blocks {
        match block.kind {
            BlockKind::Text => message
                .content
                .push(Content::new_text(block.content.clone())),
            BlockKind::Reasoning => reasoning.push_str(&block.content),
            BlockKind::ToolCall => calls.push(function_call(block)),
            BlockKind::ToolResult => message.content.push(function_call_output(block)),
            BlockKind::Media => message.content.push(media_content(block)),
        }
    }
    message.reasoning_content = (!reasoning.is_empty()).then_some(reasoning);
    message.tool_calls = (!calls.is_empty()).then_some(calls);
    message
}

fn function_call(block: &BlockRow) -> FunctionCall {
    let name = block
        .data
        .as_ref()
        .and_then(|data| data.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arguments = match block.data.as_ref().and_then(|data| data.get("arguments")) {
        Some(arguments) => serde_json::to_string(arguments).unwrap_or_default(),
        None => block.content.clone(),
    };
    FunctionCall {
        id: block.call_id.clone().unwrap_or_default(),
        name,
        arguments,
    }
}

fn function_call_output(block: &BlockRow) -> Content {
    Content::FunctionCall {
        data: FunctionCallOutput {
            id: block.call_id.clone().unwrap_or_default(),
            content: block.data.clone().unwrap_or(Value::Null),
        },
    }
}

fn media_content(block: &BlockRow) -> Content {
    let tag = block
        .data
        .as_ref()
        .and_then(|data| data.get("mime"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    match tag {
        MEDIA_IMAGE => Content::new_image(block.content.clone()),
        MEDIA_FILE => Content::new_file(block.content.clone()),
        format => Content::new_audio(block.content.clone(), format.to_string()),
    }
}

fn tokens(count: i64) -> i32 {
    i32::try_from(count).unwrap_or(i32::MAX)
}

/// 一条消息占用的近似字节
fn weight_of(message: &Message) -> u64 {
    let reasoning = message.reasoning_content.as_deref().map_or(0, str::len);
    let content: usize = message.content.iter().map(content_weight).sum();
    let calls: usize = message
        .tool_calls
        .iter()
        .flatten()
        .map(|call| call.id.len() + call.name.len() + call.arguments.len())
        .sum();
    64 + (reasoning + content + calls) as u64
}

fn content_weight(content: &Content) -> usize {
    match content {
        Content::Text { data } | Content::Image { data } | Content::File { data } => data.len(),
        Content::Audio { data } => data.content.len() + data.format.len(),
        Content::FunctionCall { data } => data.id.len() + value_weight(&data.content),
    }
}

fn value_weight(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 8,
        Value::String(text) => text.len(),
        Value::Array(items) => items.iter().map(value_weight).sum(),
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| key.len() + value_weight(value))
            .sum(),
    }
}

/// 把投影结果整理成模型接口接受的消息序列
fn normalize(messages: Vec<Message>) -> Vec<Message> {
    let present: Vec<Message> = messages
        .into_iter()
        .filter(|message| !is_empty(message))
        .collect();
    let present = drop_leading_assistant(present);
    let repaired = repair_tools(present);
    merge_neighbours(repaired)
}

fn is_empty(message: &Message) -> bool {
    let has_content = message.content.iter().any(|content| match content {
        Content::Text { data } => !data.is_empty(),
        _ => true,
    });
    !has_content
        && message
            .reasoning_content
            .as_deref()
            .is_none_or(str::is_empty)
        && message.tool_calls.as_ref().is_none_or(Vec::is_empty)
}

fn has_calls(message: &Message) -> bool {
    message
        .tool_calls
        .as_ref()
        .is_some_and(|calls| !calls.is_empty())
}

fn repair_tools(messages: Vec<Message>) -> Vec<Message> {
    let mut out = Vec::with_capacity(messages.len());
    let mut pending = messages.into_iter().peekable();
    while let Some(message) = pending.next() {
        if message.role == Role::Tool {
            continue;
        }
        let calls = message.tool_calls.clone().unwrap_or_default();
        out.push(message);
        if calls.is_empty() {
            continue;
        }
        let wanted: BTreeSet<String> = calls.iter().map(|call| call.id.clone()).collect();
        let mut answered = BTreeSet::new();
        while let Some(tool) = pending.next_if(|next| next.role == Role::Tool) {
            let kept: Vec<Content> = tool
                .content
                .into_iter()
                .filter(|content| match content {
                    Content::FunctionCall { data } => {
                        if wanted.contains(&data.id) {
                            answered.insert(data.id.clone());
                            true
                        } else {
                            false
                        }
                    }
                    _ => false,
                })
                .collect();
            if !kept.is_empty() {
                out.push(Message {
                    role: Role::Tool,
                    content: kept,
                    ..Message::default()
                });
            }
        }
        let missing: Vec<String> = calls
            .iter()
            .filter(|call| !answered.contains(&call.id))
            .map(|call| call.id.clone())
            .collect();
        if !missing.is_empty() {
            out.push(Message {
                role: Role::Tool,
                content: missing
                    .into_iter()
                    .map(|id| {
                        Content::new_function_call(
                            id,
                            serde_json::json!({ "error": "the call did not complete" }),
                        )
                    })
                    .collect(),
                ..Message::default()
            });
        }
    }
    out
}

/// 合并临近的 User 或者 Assistant消息
fn merge_neighbours(messages: Vec<Message>) -> Vec<Message> {
    let mut out: Vec<Message> = Vec::with_capacity(messages.len());
    for message in messages {
        let mergeable =
            message.role == Role::User || (message.role == Role::Assistant && !has_calls(&message));
        if let Some(last) = out.last_mut()
            && last.role == message.role
            && mergeable
            && !has_calls(last)
        {
            last.content.extend(message.content);
            if let Some(reasoning) = message.reasoning_content {
                match &mut last.reasoning_content {
                    Some(existing) => {
                        existing.push('\n');
                        existing.push_str(&reasoning);
                    }
                    None => last.reasoning_content = Some(reasoning),
                }
            }
            continue;
        }
        out.push(message);
    }
    out
}

fn drop_leading_assistant(mut messages: Vec<Message>) -> Vec<Message> {
    let leading = messages
        .iter()
        .take_while(|message| message.role == Role::Assistant)
        .count();
    messages.drain(..leading);
    messages
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_weight_follows_the_turns_the_cache_holds() {
        let cache = TurnCache {
            turns: Mutex::new(BTreeMap::new()),
            weight: AtomicU64::new(0),
        };
        let first = vec![text("hello")];
        let second = vec![text("world")];
        cache.put(TurnId(1), first.clone());
        cache.put(TurnId(2), second.clone());
        assert_eq!(
            cache.weight(),
            first.iter().chain(&second).map(weight_of).sum::<u64>()
        );

        cache.invalidate_turn(TurnId(1));
        assert_eq!(
            cache.weight(),
            second.iter().map(weight_of).sum::<u64>(),
            "removing a turn removes its weight"
        );

        cache.clear();
        assert_eq!(cache.weight(), 0, "clearing the cache clears its weight");
    }

    #[test]
    fn replacing_a_turn_counts_its_new_messages_once() {
        let cache = TurnCache {
            turns: Mutex::new(BTreeMap::new()),
            weight: AtomicU64::new(0),
        };
        cache.put(TurnId(1), vec![text("old text")]);
        cache.put(TurnId(1), vec![text("new")]);
        assert_eq!(cache.weight(), weight_of(&text("new")));
    }

    #[test]
    fn tool_results_and_calls_count_toward_the_weight() {
        let message = Message {
            content: vec![Content::new_function_call(
                "c-1".to_string(),
                serde_json::json!({ "output": "some output text" }),
            )],
            tool_calls: Some(vec![FunctionCall {
                id: "c-1".to_string(),
                name: "fs__read".to_string(),
                arguments: "{\"path\":\"/tmp\"}".to_string(),
            }]),
            ..Message::default()
        };
        let expected = 64
            + ("c-1".len() + "output".len() + "some output text".len()) as u64
            + ("c-1".len() + "fs__read".len() + "{\"path\":\"/tmp\"}".len()) as u64;
        assert_eq!(weight_of(&message), expected);
    }

    fn text(data: &str) -> Message {
        Message {
            content: vec![Content::new_text(data.to_string())],
            ..Message::default()
        }
    }

    fn user(data: &str) -> Message {
        Message {
            role: Role::User,
            ..text(data)
        }
    }

    fn assistant(data: &str) -> Message {
        Message {
            role: Role::Assistant,
            ..text(data)
        }
    }

    fn asking(id: &str) -> Message {
        Message {
            role: Role::Assistant,
            tool_calls: Some(vec![FunctionCall {
                id: id.to_string(),
                name: "fs__read".to_string(),
                arguments: "{}".to_string(),
            }]),
            ..Message::default()
        }
    }

    fn tool_result(id: &str) -> Message {
        Message {
            role: Role::Tool,
            content: vec![Content::new_function_call(
                id.to_string(),
                serde_json::json!({ "ok": true }),
            )],
            ..Message::default()
        }
    }

    #[test]
    fn a_message_without_content_is_dropped() {
        let blank = Message {
            role: Role::User,
            ..Message::default()
        };
        assert_eq!(normalize(vec![blank, user("hello")]), vec![user("hello")]);
    }

    #[test]
    fn a_message_holding_only_empty_text_is_dropped() {
        let blank = Message {
            role: Role::User,
            content: vec![Content::new_text(String::new())],
            ..Message::default()
        };
        assert!(normalize(vec![blank]).is_empty());
    }

    #[test]
    fn adjacent_user_messages_are_merged() {
        let normalized = normalize(vec![user("a"), user("b")]);
        assert_eq!(normalized.len(), 1);
        assert_eq!(
            normalized[0].content,
            vec![
                Content::new_text("a".to_string()),
                Content::new_text("b".to_string())
            ]
        );
    }

    #[test]
    fn an_assistant_tool_call_without_a_result_gets_one() {
        let normalized = normalize(vec![user("go"), asking("c-1")]);
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized[2].role, Role::Tool);
        assert_eq!(
            normalized[2].content,
            vec![Content::new_function_call(
                "c-1".to_string(),
                serde_json::json!({ "error": "the call did not complete" })
            )]
        );
    }

    #[test]
    fn a_tool_result_without_a_call_is_dropped() {
        assert_eq!(
            normalize(vec![user("go"), tool_result("c-1")]),
            vec![user("go")]
        );
    }

    #[test]
    fn a_leading_assistant_message_is_dropped() {
        assert_eq!(
            normalize(vec![assistant("hi"), user("go")]),
            vec![user("go")]
        );
    }

    #[test]
    fn a_leading_assistant_tool_call_is_dropped_with_its_result() {
        assert_eq!(
            normalize(vec![asking("c-1"), tool_result("c-1"), user("go")]),
            vec![user("go")]
        );
    }

    #[test]
    fn a_well_formed_history_is_unchanged() {
        let history = vec![user("q1"), assistant("a1"), user("q2"), assistant("a2")];
        assert_eq!(normalize(history.clone()), history);
    }

    #[test]
    fn a_complete_tool_exchange_is_unchanged() {
        let history = vec![user("go"), asking("c-1"), tool_result("c-1")];
        assert_eq!(normalize(history.clone()), history);
    }
}
