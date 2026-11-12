use serde_json::Value;
use wind_core::llm::events::{BlockCommitted, BlockDelta, MessageInterrupted, MessageSeal};
use wind_wire::event::{FeedEvent, Scope};
use wind_wire::{Role, Usage};

use crate::app::{BlockView, MessageView, Session};
use crate::text::{decode, interrupted_reason};

pub fn apply(session: &mut Session, event: &FeedEvent) -> bool {
    match &event.envelope.scope {
        Scope::Global => {
            session.log.record(session.topic.id, event);
            false
        }
        Scope::Topic { topic_id, .. } if *topic_id == session.topic.id => {
            session.log.record(session.topic.id, event);
            match event.envelope.payload.as_ref() {
                Some(payload) => reduce_payload(session, &event.envelope.name, payload),
                None => false,
            }
        }
        _ => false,
    }
}

fn reduce_payload(session: &mut Session, name: &str, payload: &Value) -> bool {
    match name {
        "core/block.delta" => match decode::<BlockDelta>(payload) {
            Some(event) => apply_delta(session, &event),
            None => decode_failed(session, name),
        },
        "core/block.committed" => match decode::<BlockCommitted>(payload) {
            Some(event) => apply_committed(session, &event),
            None => decode_failed(session, name),
        },
        "core/message.seal" => match decode::<MessageSeal>(payload) {
            Some(event) => apply_seal(session, &event),
            None => decode_failed(session, name),
        },
        "core/message.interrupted" => match decode::<MessageInterrupted>(payload) {
            Some(event) => apply_interrupted(session, &event),
            None => decode_failed(session, name),
        },
        _ => false,
    }
}

fn decode_failed(session: &mut Session, name: &str) -> bool {
    session
        .log
        .push_local(&format!("cannot decode {name} payload"));
    false
}

fn apply_delta(session: &mut Session, event: &BlockDelta) -> bool {
    let message = message_mut(session, event.message_id, event.delta.role);
    let block = &event.delta;
    match message
        .blocks
        .binary_search_by_key(&block.seq, |view| view.seq)
    {
        Ok(index) => {
            if message.blocks[index].revision >= block.revision {
                return false;
            }
            message.blocks[index] = BlockView::new(block.seq, block.revision, block.body.clone());
            true
        }
        Err(index) => {
            message.blocks.insert(
                index,
                BlockView::new(block.seq, block.revision, block.body.clone()),
            );
            true
        }
    }
}

fn apply_committed(session: &mut Session, event: &BlockCommitted) -> bool {
    match &event.block {
        Some(block) => {
            let message = message_mut(session, event.message_id, block.role);
            let mut view = BlockView::new(block.seq, block.revision, block.body.clone());
            view.committed = true;
            match message
                .blocks
                .binary_search_by_key(&block.seq, |view| view.seq)
            {
                Ok(index) => {
                    let current = &mut message.blocks[index];
                    if current.revision < block.revision {
                        *current = view;
                        true
                    } else if current.committed {
                        false
                    } else {
                        current.committed = true;
                        true
                    }
                }
                Err(index) => {
                    message.blocks.insert(index, view);
                    true
                }
            }
        }
        None => {
            let Some(message) = session
                .messages
                .iter_mut()
                .find(|message| message.id == event.message_id)
            else {
                return false;
            };
            match message
                .blocks
                .binary_search_by_key(&event.seq, |view| view.seq)
            {
                Ok(index) if !message.blocks[index].committed => {
                    message.blocks[index].committed = true;
                    true
                }
                _ => false,
            }
        }
    }
}

fn apply_seal(session: &mut Session, event: &MessageSeal) -> bool {
    let mut changed = add_usage(&mut session.usage, event.usage);
    if let Some(message) = session
        .messages
        .iter_mut()
        .find(|message| message.id == event.message_id)
        && message.sealed.as_deref() != Some(event.finish_reason.as_str())
    {
        message.sealed = Some(event.finish_reason.clone());
        changed = true;
    }
    changed
}

fn apply_interrupted(session: &mut Session, event: &MessageInterrupted) -> bool {
    let Some(message) = session
        .messages
        .iter_mut()
        .find(|message| message.id == event.message_id)
    else {
        return false;
    };
    let reason = interrupted_reason(event.reason);
    if message.interrupted.as_deref() == Some(reason) {
        return false;
    }
    message.interrupted = Some(reason.to_owned());
    true
}

fn message_mut(session: &mut Session, id: i64, role: Role) -> &mut MessageView {
    match session.messages.iter().position(|message| message.id == id) {
        Some(index) => &mut session.messages[index],
        None => {
            session.messages.push(MessageView::new(id, role));
            let index = session.messages.len() - 1;
            &mut session.messages[index]
        }
    }
}

fn add_usage(usage: &mut Usage, delta: Usage) -> bool {
    let input_tokens = usage.input_tokens.saturating_add(delta.input_tokens);
    let output_tokens = usage.output_tokens.saturating_add(delta.output_tokens);
    let changed = input_tokens != usage.input_tokens || output_tokens != usage.output_tokens;
    usage.input_tokens = input_tokens;
    usage.output_tokens = output_tokens;
    changed
}
