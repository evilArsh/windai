use super::{
    NodeId, Tree,
    domain::Domain,
    event::Effect,
    phase::{CancelOrigin, Phase},
    view::TreeEvent,
};

impl<D: Domain> Tree<D> {
    pub(super) fn reduce_cancel_request(
        &mut self,
        root: NodeId,
        origin: CancelOrigin,
    ) -> Vec<Effect<D>> {
        if self.phase(root).is_none_or(Phase::is_terminal) {
            return Vec::new();
        }
        let mut effects = Vec::new();
        for id in self.subtree_post_order(root) {
            let node_origin = if id == root {
                origin
            } else {
                CancelOrigin::Ancestor(root)
            };
            self.clear_pending(id);
            if self.phase(id).is_some_and(Phase::is_terminal) {
                continue;
            }
            let job = self.job_of(id);
            self.set_job(id, None);
            self.set_phase(id, Phase::Cancelled(node_origin));
            self.owe_delivery(id);
            effects.push(Effect::Persist { node: id });
            effects.push(Effect::Publish(TreeEvent::PhaseChanged {
                node: id,
                to: Phase::Cancelled(node_origin),
            }));
            if let Some(job) = job {
                effects.push(Effect::Stop { node: id, job });
            }
        }
        effects.extend(self.reenter_parent_if_ready(root));
        effects
    }
}
