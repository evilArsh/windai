use crate::agent::domain::{AgentDomain, AgentFailure, AgentOutcome, AgentProgress};
use crate::agent::event::TopicEvent;
use crate::agent::tree::TreeEvent;
use crate::agent::tree::phase::{Phase, Terminal};
use crate::error::Result;
use crate::models::{AgentMode, AgentStatus};
use crate::storage::Storage;

/// 树相位到对外状态的投影
///
/// `Cancelling` 是短暂中间相，不对外发
fn visible_status(phase: &Phase<AgentDomain>) -> Option<AgentStatus> {
    match phase {
        Phase::Cancelling(_) => None,
        other => Some(crate::agent::store::status_of(other)),
    }
}

/// 把一个树事件翻译成对外事件
///
/// 返回 `None` 表示这个事件不对外；
/// `Sealed` 由调用方处理（关流），不在这里翻译
pub async fn to_topic_events(
    storage: &Storage,
    event: &TreeEvent<AgentDomain>,
) -> Result<Vec<TopicEvent>> {
    let mut out = Vec::new();
    match event {
        // NodeCreated 不翻译：`InstanceCreated` 由领域的 `AgentProgress` 发，
        // 两边都发会让每个新节点重复宣告一次
        TreeEvent::NodeCreated { .. } => {}
        TreeEvent::PhaseChanged { node, to } => {
            let Some(instance) = storage.agent().get_instance(*node).await? else {
                return Ok(out);
            };
            if let Some(status) = visible_status(to) {
                out.push(TopicEvent::TaskStatusChanged {
                    instance_id: *node,
                    status,
                    mode: instance.mode.unwrap_or(AgentMode::Sync),
                });
            }
            if let Phase::Suspended(payload) = to.as_ref() {
                let requests = storage
                    .approval()
                    .list_pending_by_instance(payload.instance_id)
                    .await?;
                out.push(TopicEvent::ApprovalRequired {
                    instance_id: payload.instance_id,
                    message_id: payload.message_id,
                    requests,
                });
            }
        }
        TreeEvent::Progress { node, chunk } => match chunk {
            AgentProgress::InstanceCreated(instance) => {
                out.push(TopicEvent::InstanceCreated {
                    data: instance.clone(),
                });
            }
            AgentProgress::MessageCreated(message) => out.push(TopicEvent::MessageCreated {
                instance_id: message.instance_id,
                data: message.clone(),
            }),
            AgentProgress::Block {
                message_id,
                index,
                data,
            } => out.push(TopicEvent::Message {
                instance_id: *node,
                message_id: *message_id,
                index: *index,
                data: data.clone(),
            }),
        },
        TreeEvent::Terminated { node, terminal } => {
            let Some(instance) = storage.agent().get_instance(*node).await? else {
                return Ok(out);
            };
            let mode = instance.mode.unwrap_or(AgentMode::Sync);
            let status = match terminal {
                Terminal::Done(_) => AgentStatus::Finished,
                Terminal::Failed(_) => AgentStatus::Failed,
                Terminal::Cancelled(_) => AgentStatus::Cancelled,
                Terminal::Aborted => AgentStatus::Failed,
            };
            out.push(TopicEvent::TaskStatusChanged {
                instance_id: *node,
                status,
                mode,
            });
            match terminal {
                Terminal::Done(AgentOutcome { message_id }) => {
                    out.push(TopicEvent::MessageFinished {
                        instance_id: *node,
                        message_id: *message_id,
                    })
                }
                Terminal::Failed(AgentFailure { message_id, error }) => {
                    out.push(TopicEvent::Error {
                        instance_id: Some(*node),
                        message_id: *message_id,
                        error: error.clone(),
                    })
                }
                Terminal::Aborted => out.push(TopicEvent::Error {
                    instance_id: Some(*node),
                    message_id: None,
                    error: "instance aborted".into(),
                }),
                Terminal::Cancelled(_) => {}
            }
        }
        TreeEvent::Sealed => {}
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tree::phase::CancelOrigin;

    #[test]
    fn cancelling_has_no_visible_status() {
        assert!(
            visible_status(&Phase::Cancelling(CancelOrigin::External)).is_none(),
            "Cancelling 是短暂中间相，不得对外发 TaskStatusChanged"
        );
        assert_eq!(
            visible_status(&Phase::Running),
            Some(AgentStatus::Running),
            "在途相照常对外"
        );
        assert_eq!(
            visible_status(&Phase::Cancelled(CancelOrigin::External)),
            Some(AgentStatus::Cancelled)
        );
        assert_eq!(
            visible_status(&Phase::Aborted),
            Some(AgentStatus::Failed),
            "Aborted 呈现为失败"
        );
    }
}
