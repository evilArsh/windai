use super::{
    JobId, NodeId, Tree,
    domain::Domain,
    event::{Effect, Event},
    phase::Phase,
    view::TreeEvent,
};
use crate::agent::Input;

impl<D: Domain> Tree<D> {
    pub fn reduce(&mut self, event: Event<D>) -> Vec<Effect<D>> {
        let before = self.is_quiescent();
        let mut effects = self.reduce_inner(event);
        if !before
            && self.is_quiescent()
            && let Some(root) = self.root()
        {
            effects.push(Effect::Publish(TreeEvent::Sealed { root }));
        }
        effects
    }

    pub(super) fn reduce_inner(&mut self, event: Event<D>) -> Vec<Effect<D>> {
        if !event.bypasses_entry_absorption()
            && let Some(node) = event.node()
            && self.phase(node).is_some_and(Phase::is_terminal)
            && !event.reopens()
        {
            return Vec::new();
        }
        match event {
            Event::Spawn {
                id,
                parent,
                spec,
                detached,
            } => self.reduce_spawn(id, parent, spec, detached),
            Event::Enqueue { node, spec } => self.reduce_enqueue(node, spec),
            Event::Deliver { node } => self.reduce_deliver(node),
            Event::Completed { node, job, output } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.finish_terminal(node, Phase::Done(output))
            }
            Event::Failed { node, job, error } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.finish_terminal(node, Phase::Failed(error))
            }
            Event::Spawned { node, job, spawn } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.set_job(node, None);
                let mut effects = Vec::new();
                for request in spawn {
                    effects.extend(self.reduce_inner(Event::Spawn {
                        id: request.id,
                        parent: Some(node),
                        spec: request.spec,
                        detached: request.detached,
                    }));
                }
                if self.barrier_children(node).is_empty() {
                    let job = self.next_job_id();
                    self.set_job(node, Some(job));
                    self.set_phase(node, Phase::Running);
                    effects.push(Effect::Persist { node });
                    effects.push(Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Phase::Running,
                    }));
                    effects.push(Effect::Launch {
                        node,
                        job,
                        input: Input::Children,
                    });
                } else {
                    self.set_phase(node, Phase::WaitingChildren);
                    effects.push(Effect::Persist { node });
                    effects.push(Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Phase::WaitingChildren,
                    }));
                }
                effects
            }
            Event::Suspended { node, job, payload } => {
                if !self.accepts_job_report(node, job) {
                    return Vec::new();
                }
                self.set_job(node, None);
                self.set_phase(node, Phase::Suspended(payload.clone()));
                vec![
                    Effect::Persist { node },
                    Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Phase::Suspended(payload),
                    }),
                ]
            }
            Event::Resume {
                node,
                expect,
                input,
            } => {
                let matched = matches!(self.phase(node), Some(Phase::Suspended(current)) if *current == expect);
                if !matched {
                    return Vec::new();
                }
                let job = self.next_job_id();
                self.set_job(node, Some(job));
                self.set_phase(node, Phase::Running);
                vec![
                    Effect::Persist { node },
                    Effect::Publish(TreeEvent::PhaseChanged {
                        node,
                        to: Phase::Running,
                    }),
                    Effect::Launch {
                        node,
                        job,
                        input: Input::External(input),
                    },
                ]
            }
            Event::CancelRequest { root, origin } => self.reduce_cancel_request(root, origin),
            Event::Aborted { node, job } => {
                if self.job_of(node) != Some(job) {
                    return Vec::new();
                }
                self.finish_terminal(node, Phase::Aborted)
            }
            Event::Recovered { node } => self.reduce_recovered(node),
        }
    }

    fn reduce_spawn(
        &mut self,
        id: NodeId,
        parent: Option<NodeId>,
        spec: D::Spec,
        detached: bool,
    ) -> Vec<Effect<D>> {
        if !self.insert_node(id, parent, Phase::Running, detached) {
            return Vec::new();
        }
        let job = self.next_job_id();
        self.set_job(id, Some(job));
        vec![
            Effect::Persist { node: id },
            Effect::Publish(TreeEvent::NodeCreated { node: id, parent }),
            Effect::Launch {
                node: id,
                job,
                input: Input::Fresh(spec),
            },
        ]
    }

    pub(crate) fn reduce_enqueue(&mut self, node: NodeId, spec: D::Spec) -> Vec<Effect<D>> {
        let Some(phase) = self.phase(node) else {
            log::warn!("[tree] enqueue for unknown node ignored: {node}");
            return Vec::new();
        };
        let terminal = phase.is_terminal();
        self.push_pending(node, spec);
        if terminal {
            return self.drain_pending(node);
        }
        Vec::new()
    }

    pub(super) fn drain_pending(&mut self, node: NodeId) -> Vec<Effect<D>> {
        let specs = self.take_pending(node);
        if specs.is_empty() {
            return Vec::new();
        }
        let job = self.next_job_id();
        self.set_job(node, Some(job));
        self.set_phase(node, Phase::Running);
        vec![
            Effect::Persist { node },
            Effect::Publish(TreeEvent::PhaseChanged {
                node,
                to: Phase::Running,
            }),
            Effect::Launch {
                node,
                job,
                input: Input::Idle(specs),
            },
        ]
    }

    pub(crate) fn reduce_deliver(&mut self, node: NodeId) -> Vec<Effect<D>> {
        match self.node(node) {
            None => log::warn!("[tree] deliver for unknown node ignored: {node}"),
            Some(current) if current.delivered => {}
            Some(_) => self.set_delivered(node, true),
        }
        Vec::new()
    }

    pub(super) fn owe_delivery(&mut self, node: NodeId) -> Option<Effect<D>> {
        if !self.is_detached(node) {
            return None;
        }
        self.set_delivered(node, false);
        Some(Effect::Deliver { node })
    }

    pub(super) fn accepts_job_report(&self, node: NodeId, job: JobId) -> bool {
        self.job_of(node) == Some(job) && matches!(self.phase(node), Some(Phase::Running))
    }

    pub(super) fn finish_terminal(&mut self, node: NodeId, phase: Phase<D>) -> Vec<Effect<D>> {
        self.set_job(node, None);
        self.set_phase(node, phase.clone());
        let delivery = self.owe_delivery(node);
        let mut effects = vec![
            Effect::Persist { node },
            Effect::Publish(TreeEvent::PhaseChanged { node, to: phase }),
        ];
        effects.extend(delivery);
        effects.extend(self.reenter_parent_if_ready(node));
        effects.extend(self.drain_pending(node));
        effects
    }

    fn reduce_recovered(&mut self, node: NodeId) -> Vec<Effect<D>> {
        let disposition = match self.phase(node) {
            Some(Phase::Running) if self.job_of(node).is_none() => Some(Phase::Aborted),
            _ => None,
        };
        match disposition {
            Some(phase) => self.finish_terminal(node, phase),
            None => {
                let mut effects = self.reenter_parent_if_ready(node);
                if self.phase(node).is_some_and(Phase::is_terminal) {
                    effects.extend(self.drain_pending(node));
                    if self.node(node).is_some_and(|current| !current.delivered) {
                        effects.push(Effect::Deliver { node });
                    }
                }
                effects
            }
        }
    }

    pub(super) fn reenter_parent_if_ready(&mut self, child: NodeId) -> Vec<Effect<D>> {
        let Some(parent) = self.node(child).and_then(|node| node.parent) else {
            return Vec::new();
        };
        if !matches!(self.phase(parent), Some(Phase::WaitingChildren)) {
            return Vec::new();
        }
        if !self.all_children_terminal(parent) {
            return Vec::new();
        }
        let job = self.next_job_id();
        self.set_job(parent, Some(job));
        self.set_phase(parent, Phase::Running);
        vec![
            Effect::Persist { node: parent },
            Effect::Publish(TreeEvent::PhaseChanged {
                node: parent,
                to: Phase::Running,
            }),
            Effect::Launch {
                node: parent,
                job,
                input: Input::Children,
            },
        ]
    }
}
