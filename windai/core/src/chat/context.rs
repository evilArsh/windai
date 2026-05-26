use crate::error::{CoreError, Result};
use crate::models::Message as CoreMessage;
use wind_ai::message::{Message as AiMessage, Role};

/// 构建历史消息上下文
///
/// 1. topic 中必须存在 id 为 `user_message_id` 的消息，
/// 并且从此消息所在位置开始向后的消息 id 必须是 `message_id`；
/// 因为一个User消息可能对应多个Assistant消息
///
/// 2. 以 `message_id` 为中心找出最小边界范围的上下文
///
/// 函数不负责对消息上下文做消息合理性校验，考虑以下情况：
/// - (User, Assistant) 消息对缺失。比如User 消息被删除后应该标记 Assistant 消息为 `is_excluded`
/// - 忽略MCP调用中间结果。历史消息上下文不会包含实时 MCP 调用产生的中间结果，只包含最终的结果，中间结果只在实时请求中包含
pub fn build_chat_context(
    raw_messages: Vec<CoreMessage>,
    topic_id: i64,
    user_message_id: i64,
    message_id: i64,
    max_context: Option<i32>,
) -> Result<(CoreMessage, Vec<AiMessage>)> {
    let max_context = max_context
        .map(|c| if c < 1 { 1 } else { c as usize })
        .unwrap_or(1);
    let user_index = raw_messages
        .iter()
        .position(|m| m.id == user_message_id)
        .ok_or_else(|| CoreError::Chat("User message not found".to_string()))?;

    let mut raw = raw_messages;
    let mut assistants = raw.split_off(user_index + 1);

    let asst_pos = assistants
        .iter()
        .position(|a| a.id == message_id && a.from_id == Some(user_message_id))
        .ok_or_else(|| {
            CoreError::Chat(format!(
                "Cannot find assistant message in current topic. (topic_id={}, message_id={})",
                topic_id, message_id
            ))
        })?;

    let last = raw.last().ok_or_else(|| {
        CoreError::Chat(format!(
            "Insufficient chat context for topic {}: need at least 1 non-excluded messages, but got 0.",
            topic_id,
        ))
    })?;
    if last.id != user_message_id {
        return Err(CoreError::Chat(format!(
            "The last message of current topic does not match user message. Expected {}, got {}",
            user_message_id, last.id
        )));
    }

    let (system_idx, boundary_idx) = find_system_and_boundary(&raw);
    let sys_offset = system_idx.map(|i| i + 1).unwrap_or(0);
    let boundary_start = boundary_idx.map(|i| i + 1).unwrap_or(0);

    let start_index = std::cmp::min(
        boundary_start,
        raw.len()
            .saturating_sub(sys_offset)
            .saturating_sub(max_context),
    );

    // 确保第一条记录是 Role::User
    let start_pos = raw[start_index..]
        .iter()
        .position(|slice| {
            slice
                .content
                .iter()
                .any(|c| c.is_simple() && c.role == Role::User)
        })
        .unwrap_or(0);
    let start = start_index + start_pos;

    let system_content = system_idx
        .map(|si| std::mem::take(&mut raw[si].content).into_iter().next())
        .flatten();

    let est_len = raw.len().saturating_sub(start) + system_content.is_some() as usize;
    let mut contexts = Vec::with_capacity(est_len);

    if let Some(sys) = system_content {
        contexts.push(sys);
    }

    for (i, m) in raw.into_iter().enumerate().skip(start) {
        if Some(i) == system_idx {
            continue;
        }
        if let Some(c) = m.content.into_iter().find(|c| c.is_simple()) {
            contexts.push(c);
        }
    }

    Ok((assistants.swap_remove(asst_pos), contexts))
}

fn find_system_and_boundary(messages: &[CoreMessage]) -> (Option<usize>, Option<usize>) {
    let mut sys = None;
    let mut boundary = None;
    for (i, m) in messages.iter().enumerate().rev() {
        if sys.is_none() && is_system_message(m) {
            sys = Some(i);
        }
        if boundary.is_none() && m.is_boundary {
            boundary = Some(i);
        }
        if sys.is_some() && boundary.is_some() {
            break;
        }
    }
    (sys, boundary)
}

fn is_system_message(message: &CoreMessage) -> bool {
    message.content.len() == 1 && message.content[0].role == Role::System
        || message.content[0].role == Role::Developer
}
