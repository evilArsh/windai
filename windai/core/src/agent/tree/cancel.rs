use super::{
    JobId, NodeId, Tree,
    domain::Domain,
    event::Effect,
    phase::{CancelOrigin, Phase, Terminal},
    view::TreeEvent,
};

impl<D: Domain> Tree<D> {
    pub(super) fn reduce_cancel_request(
        &mut self,
        root: NodeId,
        origin: CancelOrigin,
    ) -> Vec<Effect<D>> {
        let mut effects = Vec::new();
        for id in self.subtree_post_order(root) {
            let node_origin = if id == root {
                origin
            } else {
                CancelOrigin::Ancestor(root)
            };
            self.clear_pending(id);
            match self.phase(id) {
                Some(Phase::Suspended(_) | Phase::WaitingChildren) => {
                    self.set_job(id, None);
                    self.set_phase(id, Phase::Cancelled(node_origin));
                    self.owe_delivery(id);
                    effects.push(Effect::Persist { node: id });
                    effects.push(Effect::Publish(TreeEvent::Terminated {
                        node: id,
                        terminal: Terminal::Cancelled(node_origin),
                    }));
                    if id == root {
                        effects.extend(self.reenter_parent_if_ready(id));
                    }
                }
                Some(Phase::Running | Phase::Cancelling(_)) => {
                    self.set_phase(id, Phase::Cancelling(node_origin));
                    effects.push(Effect::Persist { node: id });
                    effects.push(Effect::Publish(TreeEvent::PhaseChanged {
                        node: id,
                        to: Box::new(Phase::Cancelling(node_origin)),
                    }));
                    if let Some(job) = self.job_of(id) {
                        effects.push(Effect::Stop { node: id, job });
                    }
                }
                _ => {}
            }
        }
        effects
    }

    pub(super) fn reduce_stopped(&mut self, node: NodeId, job: JobId) -> Vec<Effect<D>> {
        if self.job_of(node) != Some(job) {
            return Vec::new();
        }
        let Some(Phase::Cancelling(origin)) = self.phase(node) else {
            return Vec::new();
        };
        let origin = *origin;
        self.set_job(node, None);
        self.set_phase(node, Phase::Cancelled(origin));
        self.owe_delivery(node);
        let mut effects = vec![
            Effect::Persist { node },
            Effect::Publish(TreeEvent::Terminated {
                node,
                terminal: Terminal::Cancelled(origin),
            }),
        ];
        effects.extend(self.reenter_parent_if_ready(node));
        effects
    }
}
