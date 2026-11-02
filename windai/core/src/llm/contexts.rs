use crate::error::Result;
use serde_json::{Value, json};
use wind_storage::{ContextRow, Storage};
use wind_wire::{
    AudioContent, BlockKind, Content, FunctionCall, FunctionCallOutput, Message, Role,
};

/// 装配实例的全部历史上下文
pub async fn assemble(storage: &Storage, instance_id: i64) -> Result<Vec<Message>> {
    let rows = storage.list_instance_context(instance_id).await?;
    Ok(project(rows))
}

fn project(rows: Vec<ContextRow>) -> Vec<Message> {
    let mut messages: Vec<Message> = Vec::new();
    let mut last_message_id: Option<i64> = None;
    for row in rows {
        if last_message_id != Some(row.message_id) {
            messages.push(new_message(&row));
            last_message_id = Some(row.message_id);
        }
        if let Some(message) = messages.last_mut() {
            apply_block(message, &row);
        }
    }
    normalize(messages)
}

fn new_message(row: &ContextRow) -> Message {
    Message {
        role: row.role,
        content: Vec::new(),
        reasoning_content: None,
        created_at: row.message_created_at.unwrap_or(0),
        input_tokens: token_count(row.input_tokens),
        output_tokens: token_count(row.output_tokens),
        tool_calls: None,
    }
}

fn apply_block(message: &mut Message, row: &ContextRow) {
    match row.kind {
        BlockKind::Text => message.content.push(Content::Text {
            data: row.content.clone().unwrap_or_default(),
        }),
        BlockKind::Image => message.content.push(Content::Image {
            data: row.content.clone().unwrap_or_default(),
        }),
        BlockKind::File => message.content.push(Content::File {
            data: row.content.clone().unwrap_or_default(),
        }),
        BlockKind::Audio => message.content.push(Content::Audio {
            data: AudioContent {
                format: row.format.clone().unwrap_or_default(),
                content: row.content.clone().unwrap_or_default(),
            },
        }),
        BlockKind::Reasoning => {
            let part = row.content.as_deref().unwrap_or_default();
            if !part.is_empty() {
                message
                    .reasoning_content
                    .get_or_insert_with(String::new)
                    .push_str(part);
            }
        }
        BlockKind::ToolCall => {
            message
                .tool_calls
                .get_or_insert_with(Vec::new)
                .push(FunctionCall {
                    id: row.call_id.clone().unwrap_or_default(),
                    name: row.call_name.clone().unwrap_or_default(),
                    arguments: row.call_arguments.clone().unwrap_or_default(),
                });
        }
        BlockKind::ToolResult => message.content.push(Content::FunctionCall {
            data: FunctionCallOutput {
                id: row.call_id.clone().unwrap_or_default(),
                content: row.call_res.clone().unwrap_or(Value::Null),
            },
        }),
    }
}

fn normalize(messages: Vec<Message>) -> Vec<Message> {
    let mut messages = messages;
    messages.retain(has_content);
    let leading = messages
        .iter()
        .take_while(|message| message.role == Role::Assistant)
        .count();
    messages.drain(..leading);
    let messages = repair_tools(messages);
    merge_adjacent(messages)
}

fn has_content(message: &Message) -> bool {
    if message
        .tool_calls
        .as_ref()
        .is_some_and(|calls| !calls.is_empty())
    {
        return true;
    }
    if message
        .reasoning_content
        .as_deref()
        .is_some_and(|text| !text.trim().is_empty())
    {
        return true;
    }
    message.content.iter().any(|item| match item {
        Content::Text { data } => !data.trim().is_empty(),
        Content::Image { .. } | Content::File { .. } | Content::Audio { .. } => true,
        Content::FunctionCall { .. } => true,
    })
}

fn repair_tools(messages: Vec<Message>) -> Vec<Message> {
    let mut out: Vec<Message> = Vec::with_capacity(messages.len());
    let mut expected: Vec<String> = Vec::new();
    let mut insert_at: Option<usize> = None;
    for message in messages {
        if message.role == Role::Tool {
            if expected.is_empty() {
                continue;
            }
            let mut kept = Message {
                role: Role::Tool,
                created_at: message.created_at,
                input_tokens: message.input_tokens,
                output_tokens: message.output_tokens,
                ..Message::default()
            };
            for item in message.content {
                match item {
                    Content::FunctionCall { data } => {
                        if expected.iter().any(|id| id == &data.id) {
                            expected.retain(|id| id != &data.id);
                            kept.content.push(Content::FunctionCall { data });
                        }
                    }
                    other => kept.content.push(other),
                }
            }
            if !kept.content.is_empty() {
                out.push(kept);
                insert_at = Some(out.len());
            }
            continue;
        }
        if !expected.is_empty() {
            append_missing_results(&mut out, insert_at, &mut expected);
        }
        let has_calls = message.role == Role::Assistant
            && message
                .tool_calls
                .as_ref()
                .is_some_and(|calls| !calls.is_empty());
        expected = if has_calls {
            message
                .tool_calls
                .as_ref()
                .map(|calls| calls.iter().map(|call| call.id.clone()).collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        out.push(message);
        insert_at = has_calls.then_some(out.len());
    }
    if !expected.is_empty() {
        append_missing_results(&mut out, insert_at, &mut expected);
    }
    out
}

fn append_missing_results(
    out: &mut Vec<Message>,
    insert_at: Option<usize>,
    expected: &mut Vec<String>,
) {
    let content: Vec<Content> = expected
        .drain(..)
        .map(|id| Content::new_function_call(id, json!({ "error": "the call did not complete" })))
        .collect();
    if content.is_empty() {
        return;
    }
    let message = Message {
        role: Role::Tool,
        content,
        ..Message::default()
    };
    match insert_at {
        Some(at) if at <= out.len() => out.insert(at, message),
        _ => out.push(message),
    }
}

fn merge_adjacent(messages: Vec<Message>) -> Vec<Message> {
    let mut out: Vec<Message> = Vec::with_capacity(messages.len());
    for message in messages {
        let mergeable = out.last().is_some_and(|prev| {
            (prev.role == Role::User && message.role == Role::User)
                || (prev.role == Role::Assistant
                    && message.role == Role::Assistant
                    && prev
                        .tool_calls
                        .as_ref()
                        .is_none_or(|calls| calls.is_empty())
                    && message
                        .tool_calls
                        .as_ref()
                        .is_none_or(|calls| calls.is_empty()))
        });
        if mergeable && let Some(prev) = out.last_mut() {
            prev.content.extend(message.content);
            if let Some(reasoning) = message.reasoning_content {
                prev.reasoning_content
                    .get_or_insert_with(String::new)
                    .push_str(&reasoning);
            }
            prev.input_tokens = prev.input_tokens.max(message.input_tokens);
            prev.output_tokens = prev.output_tokens.max(message.output_tokens);
        } else {
            out.push(message);
        }
    }
    out
}

fn token_count(value: i64) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}
