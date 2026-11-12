use crate::app::{BlockView, MessageView, NodeView, Session, phase_detail, phase_is_terminal};
use crate::text::{decode, interrupted_reason};
use serde_json::Value;
use wind_core::agent::Phase;
use wind_core::agent::events::{AgentSealed, AgentSpawn, AgentStatus};
use wind_core::llm::domain::LlmDomain;
use wind_core::llm::events::{BlockCommitted, BlockDelta, MessageInterrupted, MessageSeal};
use wind_wire::event::{FeedEvent, Scope};
use wind_wire::{AgentRole, Role, Usage};

pub fn apply(session: &mut Session, event: &FeedEvent) -> bool {
    match &event.envelope.scope {
        Scope::Global => {
            session.log.record(session.topic.id, event);
            if is_mod_event(&event.envelope.name) {
                session.reload_mods = true;
                return true;
            }
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
        "core/agent.spawn" => match decode::<AgentSpawn>(payload) {
            Some(event) => apply_spawn(session, &event),
            None => decode_failed(session, name),
        },
        "core/agent.status" => match decode::<AgentStatus>(payload) {
            Some(event) => apply_status(session, &event),
            None => decode_failed(session, name),
        },
        "core/agent.sealed" => match decode::<AgentSealed>(payload) {
            Some(event) => apply_sealed(session, &event),
            None => decode_failed(session, name),
        },
        name if is_mod_event(name) => {
            session.reload_mods = true;
            true
        }
        _ => false,
    }
}

fn is_mod_event(name: &str) -> bool {
    matches!(
        name,
        "core/mod.installed" | "core/mod.updated" | "core/mod.deleted"
    )
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
            let current = &mut message.blocks[index];
            if current.revision >= block.revision {
                return false;
            }
            let committed = current.committed;
            *current = BlockView::new(block.seq, block.revision, block.body.clone());
            current.committed = committed;
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
    let Some(message) = session
        .messages
        .iter_mut()
        .find(|message| message.id == event.message_id)
    else {
        return add_usage(&mut session.usage, event.usage);
    };
    let mut changed = if message.from_history {
        false
    } else {
        add_usage(&mut session.usage, event.usage)
    };
    if message.sealed.as_deref() != Some(event.finish_reason.as_str()) {
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

fn apply_spawn(session: &mut Session, event: &AgentSpawn) -> bool {
    let role = if session.main == Some(event.instance_id) {
        AgentRole::Main
    } else {
        AgentRole::Child
    };
    if let Some(node) = session
        .nodes
        .iter_mut()
        .find(|node| node.id == event.instance_id)
    {
        let mut changed = false;
        if node.role != role {
            node.role = role;
            changed = true;
        }
        if node.parent_id != event.parent_id {
            node.parent_id = event.parent_id;
            changed = true;
        }
        return changed;
    }
    session.nodes.push(NodeView {
        id: event.instance_id,
        parent_id: event.parent_id,
        role,
        phase: "running".to_owned(),
        suspended_wait: None,
        reason: None,
        sealed: false,
    });
    true
}

fn apply_status(session: &mut Session, event: &AgentStatus) -> bool {
    let payload = event.phase_payload.as_ref().map(ToString::to_string);
    let Ok(phase) = Phase::<LlmDomain>::from_columns(&event.phase, payload.as_deref()) else {
        session.log.push_local(&format!(
            "cannot decode agent {} phase {}",
            event.instance_id, event.phase
        ));
        return false;
    };
    let (suspended_wait, reason) = phase_detail(&phase);
    let is_main = session.main == Some(event.instance_id);
    let was_terminal = session
        .nodes
        .iter()
        .find(|node| node.id == event.instance_id)
        .is_some_and(|node| phase_is_terminal(&node.phase));
    let mut changed = false;
    if let Some(node) = session
        .nodes
        .iter_mut()
        .find(|node| node.id == event.instance_id)
    {
        if node.phase != event.phase
            || node.suspended_wait != suspended_wait
            || node.reason != reason
        {
            node.phase = event.phase.clone();
            node.suspended_wait = suspended_wait;
            node.reason = reason;
            changed = true;
        }
    } else {
        session.nodes.push(NodeView {
            id: event.instance_id,
            parent_id: None,
            role: AgentRole::Child,
            phase: event.phase.clone(),
            suspended_wait,
            reason,
            sealed: false,
        });
        changed = true;
    }
    if is_main
        && !was_terminal
        && phase.is_terminal()
        && let Some(message) = session.messages.iter_mut().rev().find(|message| {
            !message.from_history && message.sealed.is_none() && message.interrupted.is_none()
        })
    {
        message.interrupted = Some(event.phase.clone());
        changed = true;
    }
    changed
}

fn apply_sealed(session: &mut Session, event: &AgentSealed) -> bool {
    if session.main != Some(event.instance_id) {
        return false;
    }
    let Some(node) = session
        .nodes
        .iter_mut()
        .find(|node| node.id == event.instance_id)
    else {
        return false;
    };
    if node.sealed {
        return false;
    }
    node.sealed = true;
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
