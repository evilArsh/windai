use super::behavior::{Behavior, Ctx, Input, Outcome};
use super::domain::{
    AgentDomain, AgentFailure, AgentOutcome, AgentSpec, CallDecision, SuspendedWait, WaitAnswer,
    WaitKind, pending_call_ids,
};
use crate::agent::tool::SPAWN_AGENT_NAME;
use crate::conversation::assembler::ContextProjection;
use crate::conversation::writer::BlockWriter;
use crate::error::CoreError;
use crate::llm::execute::denied_result;
use crate::llm::policy::CallPlan;
use crate::llm::ports::{FunctionCallOutput, McpTools, ModRequests, TopicMods};
use crate::llm::tools::ToolCatalog;
use crate::llm::turn;
use crate::storage::Storage;
use crate::usecase::UseCases;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use wind_event::dispatch::Dispatcher;
use wind_wire::message::Message;
use wind_wire::tool::FunctionCall;

/// 一次请求最多跑几轮工具调用
pub const DEFAULT_MAX_ROUNDS: u32 = 30;

/// 投影缓存的总重量上限，单位为近似字节
const PROJECTION_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// 投影缓存的条目上限
const PROJECTION_CACHE_ENTRIES: usize = 512;

pub struct AgentRunner {
    pub(crate) writer: BlockWriter,
    pub(crate) usecases: UseCases,
    pub(crate) bus: Arc<Dispatcher>,
    pub(crate) mods: Arc<dyn TopicMods>,
    pub(crate) mcp: Option<Arc<dyn McpTools>>,
    pub(crate) requests: Arc<dyn ModRequests>,
    pub(crate) storage: Storage,
    pub(crate) max_rounds: u32,
    pub(crate) catalog: Mutex<Option<CachedCatalog>>,
    /// 每个实例的投影缓存；总字节数或条目数超限时，丢掉最久没用的实例
    projections: Mutex<HashMap<i64, CachedProjection>>,
    /// 访问序号，每次取投影时递增,用来比较谁最久没用
    projection_clock: AtomicU64,
}

/// 一个实例的缓存条目：投影本身、上次使用的序号和占用的字节数
struct CachedProjection {
    projection: Arc<ContextProjection>,
    /// 上次使用时的序号，越小表示越久没用
    last_used: u64,
    /// 缓存的消息大约占多少字节
    weight: u64,
}

pub(crate) struct CachedCatalog {
    pub(crate) topic_id: i64,
    pub(crate) at: Instant,
    pub(crate) catalog: Arc<ToolCatalog>,
}

impl AgentRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        writer: BlockWriter,
        usecases: UseCases,
        bus: Arc<Dispatcher>,
        mods: Arc<dyn TopicMods>,
        mcp: Option<Arc<dyn McpTools>>,
        requests: Arc<dyn ModRequests>,
        storage: Storage,
        max_rounds: u32,
    ) -> Self {
        Self {
            writer,
            usecases,
            bus,
            mods,
            mcp,
            requests,
            storage,
            max_rounds,
            catalog: Mutex::new(None),
            projections: Mutex::new(HashMap::new()),
            projection_clock: AtomicU64::new(0),
        }
    }

    pub(crate) async fn context(&self, instance_id: i64) -> Result<Vec<Message>, CoreError> {
        let projection = self.projection_of(instance_id);
        let context = projection.context(None).await?;
        self.settle(instance_id, &projection);
        Ok(context)
    }

    /// 取出实例的投影；没有就新建，并记为刚用过
    fn projection_of(&self, instance_id: i64) -> Arc<ContextProjection> {
        let now = self.projection_clock.fetch_add(1, Ordering::Relaxed);
        let mut projections = self
            .projections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = projections
            .entry(instance_id)
            .or_insert_with(|| CachedProjection {
                projection: Arc::new(ContextProjection::new(
                    self.storage.clone(),
                    instance_id,
                    self.writer.invalidations(),
                )),
                last_used: now,
                weight: 0,
            });
        entry.last_used = now;
        Arc::clone(&entry.projection)
    }

    /// 重新统计投影占的字节数；超过上限时从最久没用(LRU)的实例开始丢弃
    fn settle(&self, instance_id: i64, used: &Arc<ContextProjection>) {
        let mut projections = self
            .projections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(entry) = projections.get_mut(&instance_id) else {
            return;
        };
        if !Arc::ptr_eq(&entry.projection, used) {
            return;
        }
        entry.weight = used.weight();
        let mut total: u64 = projections.values().map(|entry| entry.weight).sum();
        while total > PROJECTION_CACHE_BYTES || projections.len() > PROJECTION_CACHE_ENTRIES {
            let victim = least_recent(
                projections
                    .iter()
                    .map(|(instance, entry)| (*instance, entry.last_used)),
                instance_id,
            );
            let Some(victim) = victim else {
                break;
            };
            if let Some(removed) = projections.remove(&victim) {
                total = total.saturating_sub(removed.weight);
            }
        }
    }

    pub async fn report_spawns(&self, instance_id: i64) -> Result<Option<i64>, CoreError> {
        turn::report_spawns(self, instance_id).await
    }

    async fn serve(
        &self,
        instance_id: i64,
        spec: AgentSpec,
        batch: &[AgentSpec],
        reentry: bool,
    ) -> Result<Outcome<AgentDomain>, AgentFailure> {
        let failure = |message_id: Option<i64>, error: CoreError| AgentFailure {
            message_id,
            error: error.to_string(),
        };
        let topic_id = turn::topic_of(&self.storage, instance_id)
            .await
            .map_err(|error| failure(spec.message_id, error))?;

        let cx = turn::round_cx(topic_id);

        let composed = turn::compose_pending(self, &cx, topic_id, instance_id, batch)
            .await
            .map_err(|error| failure(spec.message_id, error))?;

        let spawn_turn = turn::report_spawns(self, instance_id)
            .await
            .map_err(|error| failure(spec.message_id, error))?;

        let contexts = turn::assemble(self, instance_id)
            .await
            .map_err(|error| failure(spec.message_id, error))?;

        let turn_id = match spec.turn_id {
            Some(turn_id) => turn_id,
            None => match spec.message_id {
                Some(message_id) => self
                    .storage
                    .message_turn(message_id)
                    .await
                    .map_err(|error| failure(Some(message_id), error))?
                    .ok_or_else(|| AgentFailure {
                        message_id: Some(message_id),
                        error: format!("message {message_id} belongs to no turn"),
                    })?,
                None => match composed {
                    Some(input) => input.turn_id,
                    None if reentry => match spawn_turn {
                        Some(turn_id) => turn_id,
                        None => turn::open_turn(&self.storage, instance_id, topic_id, None)
                            .await
                            .map_err(|error| failure(None, error))?,
                    },
                    None => turn::open_turn(&self.storage, instance_id, topic_id, None)
                        .await
                        .map_err(|error| failure(None, error))?,
                },
            },
        };
        self.rounds(instance_id, topic_id, turn_id, contexts, spec.message_id)
            .await
    }

    async fn rounds(
        &self,
        instance_id: i64,
        topic_id: i64,
        turn_id: i64,
        mut contexts: Vec<Message>,
        mut last: Option<i64>,
    ) -> Result<Outcome<AgentDomain>, AgentFailure> {
        let failure = |message_id: Option<i64>, error: CoreError| AgentFailure {
            message_id,
            error: error.to_string(),
        };
        let cx = turn::round_cx(topic_id);
        let mut rounds = 0;
        loop {
            rounds += 1;
            if rounds > self.max_rounds {
                return Err(AgentFailure {
                    message_id: last,
                    error: format!("tool calls did not converge"),
                });
            }
            let prepared = turn::prepare(self, &cx, topic_id, instance_id)
                .await
                .map_err(|error| failure(last, error))?;
            let here = turn::open_round(&self.storage, turn_id, prepared.model_id)
                .await
                .map_err(|error| failure(last, error))?;
            last = Some(here.message_id);
            let message = turn::exchange(self, &cx, topic_id, &here, &prepared, &contexts)
                .await
                .map_err(|error| failure(last, error))?;
            contexts.push(message.clone());
            let calls = message.tool_calls.clone().unwrap_or_default();
            if calls.is_empty() {
                return Ok(Outcome::Completed(AgentOutcome {
                    message_id: here.message_id,
                }));
            }
            let plan = turn::adopt(self, &cx, topic_id, calls, instance_id)
                .await
                .map_err(|error| failure(last, error))?;
            if !plan.pending.is_empty() {
                let pending_call_ids: Vec<String> =
                    plan.pending.iter().map(|call| call.id.clone()).collect();
                let mut payload = serde_json::json!({
                    "pending_call_ids": pending_call_ids,
                    "pending_calls": plan.pending,
                });
                let has_spawn = plan
                    .admitted
                    .iter()
                    .any(|call| call.name == SPAWN_AGENT_NAME);
                // 挂起期间还没执行的放行调用；答复之后它们也要执行
                let admitted_calls = if has_spawn {
                    plan.admitted.clone()
                } else {
                    Vec::new()
                };
                payload["admitted_calls"] = serde_json::json!(admitted_calls);
                if !has_spawn {
                    let executed_ids: Vec<String> =
                        plan.admitted.iter().map(|call| call.id.clone()).collect();
                    let executed = turn::execute(
                        self,
                        &cx,
                        CallPlan {
                            admitted: plan.admitted.clone(),
                            pending: Vec::new(),
                        },
                        &prepared.catalog,
                        instance_id,
                    )
                    .await
                    .map_err(|error| failure(last, error))?;
                    let mut results: Vec<FunctionCallOutput> = Vec::new();
                    results.extend(executed.results);
                    if !results.is_empty() {
                        let folded = turn::fold(self, &cx, topic_id, &here, &results)
                            .await
                            .map_err(|error| failure(last, error))?;
                        contexts.push(folded);
                    }
                    payload["executed_call_ids"] = serde_json::json!(executed_ids);
                }
                return Ok(Outcome::Suspend(SuspendedWait {
                    instance_id,
                    kind: WaitKind::Approval.to_string(),
                    message_id: here.message_id,
                    handle: nanoid::nanoid!(),
                    payload: Some(payload),
                }));
            }
            let executed = turn::execute(self, &cx, plan.clone(), &prepared.catalog, instance_id)
                .await
                .map_err(|error| failure(last, error))?;
            let mut results: Vec<FunctionCallOutput> = Vec::new();
            results.extend(executed.results);
            if !executed.spawns.is_empty() {
                if !results.is_empty() {
                    turn::fold(self, &cx, topic_id, &here, &results)
                        .await
                        .map_err(|error| failure(last, error))?;
                }
                let spawn = turn::spawn_requests(self, &cx, topic_id, instance_id, executed.spawns)
                    .await
                    .map_err(|error| failure(last, error))?;
                return Ok(Outcome::Spawn { spawn });
            }
            if results.is_empty() {
                return Ok(Outcome::Completed(AgentOutcome {
                    message_id: here.message_id,
                }));
            }
            let folded = turn::fold(self, &cx, topic_id, &here, &results)
                .await
                .map_err(|error| failure(last, error))?;
            contexts.push(folded);
        }
    }

    async fn resume(
        &self,
        instance_id: i64,
        answer: WaitAnswer,
    ) -> Result<Outcome<AgentDomain>, AgentFailure> {
        let failure = |message_id: Option<i64>, error: CoreError| AgentFailure {
            message_id,
            error: error.to_string(),
        };
        let topic_id = turn::topic_of(&self.storage, instance_id)
            .await
            .map_err(|error| failure(None, error))?;
        let cx = turn::round_cx(topic_id);
        let Some(wait) = self
            .storage
            .wait_get(&answer.handle)
            .await
            .map_err(|error| failure(None, error))?
        else {
            log::warn!(
                "[llm] resuming unknown wait {} as a fresh round",
                answer.handle
            );
            let turn_id = turn::open_turn(&self.storage, instance_id, topic_id, None)
                .await
                .map_err(|error| failure(None, error))?;
            let contexts = turn::assemble(self, instance_id)
                .await
                .map_err(|error| failure(None, error))?;
            return self
                .rounds(instance_id, topic_id, turn_id, contexts, None)
                .await;
        };
        let message_id = wait.message_id;
        let calls = turn::calls_on_message(self, message_id)
            .await
            .map_err(|error| failure(Some(message_id), error))?;
        let decisions: Vec<CallDecision> = match wait.decision.clone() {
            Some(value) => serde_json::from_value(value).map_err(|error| {
                failure(
                    Some(message_id),
                    CoreError::MalformedColumn {
                        column: "external_waits.decision_json".into(),
                        detail: error.to_string(),
                    },
                )
            })?,
            None => answer.decisions.clone(),
        };
        let turn_id = match self
            .storage
            .message_turn(message_id)
            .await
            .map_err(|error| failure(Some(message_id), error))?
        {
            Some(turn_id) => turn_id,
            None if calls.is_empty() => turn::open_turn(&self.storage, instance_id, topic_id, None)
                .await
                .map_err(|error| failure(Some(message_id), error))?,
            None => {
                return Err(failure(
                    Some(message_id),
                    CoreError::MalformedColumn {
                        column: "external_waits.message_id".into(),
                        detail: format!("message {message_id} belongs to no turn"),
                    },
                ));
            }
        };
        if calls.is_empty() {
            let contexts = turn::assemble(self, instance_id)
                .await
                .map_err(|error| failure(Some(message_id), error))?;
            return self
                .rounds(instance_id, topic_id, turn_id, contexts, Some(message_id))
                .await;
        }
        let pending = pending_call_ids(&wait.payload);
        let admitted_calls: Vec<FunctionCall> = wait
            .payload
            .get("admitted_calls")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        let pending_calls: Vec<FunctionCall> = wait
            .payload
            .get("pending_calls")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        // 只按挂起载荷里的改写结果恢复，不读库里的原始调用：
        // mod 删掉或改名的调用不能在这里复活
        let calls: Vec<FunctionCall> = admitted_calls.into_iter().chain(pending_calls).collect();
        let executed_ids: Vec<String> = wait
            .payload
            .get("executed_call_ids")
            .and_then(serde_json::Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let catalog = turn::catalog_of(self, &cx, topic_id)
            .await
            .map_err(|error| failure(Some(message_id), error))?;
        let mut approved = Vec::new();
        let mut refusals: HashMap<String, FunctionCallOutput> = HashMap::new();
        for call in &calls {
            if executed_ids.iter().any(|id| id == &call.id) {
                continue;
            }
            if !pending.iter().any(|id| id == &call.id) {
                approved.push(call.clone());
                continue;
            }
            let allowed = decisions
                .iter()
                .find(|decision| decision.call_id == call.id)
                .is_some_and(|decision| decision.allow);
            match allowed {
                true => approved.push(call.clone()),
                false => {
                    refusals.insert(
                        call.id.clone(),
                        denied_result(&call.id, "the user did not allow this call"),
                    );
                }
            }
        }
        let executed = turn::execute(
            self,
            &cx,
            CallPlan {
                admitted: approved,
                pending: Vec::new(),
            },
            &catalog,
            instance_id,
        )
        .await
        .map_err(|error| failure(Some(message_id), error))?;
        let spawn_ids: HashSet<&str> = executed
            .spawns
            .iter()
            .map(|spawn| spawn.call_id.as_str())
            .collect();
        let mut results: HashMap<String, FunctionCallOutput> = executed
            .results
            .into_iter()
            .map(|result| (result.id.clone(), result))
            .collect();
        let ordered: Vec<FunctionCallOutput> = calls
            .iter()
            .filter(|call| !spawn_ids.contains(call.id.as_str()))
            .filter_map(|call| {
                refusals
                    .remove(&call.id)
                    .or_else(|| results.remove(&call.id))
            })
            .collect();
        if !ordered.is_empty() {
            let locus = turn::Locus {
                message_id,
                turn_id,
            };
            turn::fold(self, &cx, topic_id, &locus, &ordered)
                .await
                .map_err(|error| failure(Some(message_id), error))?;
        }
        if !executed.spawns.is_empty() {
            let spawn = turn::spawn_requests(self, &cx, topic_id, instance_id, executed.spawns)
                .await
                .map_err(|error| failure(Some(message_id), error))?;
            return Ok(Outcome::Spawn { spawn });
        }
        let contexts = turn::assemble(self, instance_id)
            .await
            .map_err(|error| failure(Some(message_id), error))?;
        self.rounds(instance_id, topic_id, turn_id, contexts, Some(message_id))
            .await
    }
}

#[async_trait::async_trait]
impl Behavior<AgentDomain> for AgentRunner {
    async fn run(&self, ctx: Ctx, input: Input<AgentDomain>) -> Outcome<AgentDomain> {
        let instance_id = ctx.node;
        let (spec, batch, reentry) = match input {
            Input::Fresh(spec) => {
                let batch = match spec.pending_id {
                    Some(_) => vec![spec.clone()],
                    None => Vec::new(),
                };
                (spec, batch, false)
            }
            Input::Children(_) => (
                AgentSpec {
                    instance_id,
                    message_id: None,
                    pending_id: None,
                    turn_id: None,
                },
                Vec::new(),
                true,
            ),
            Input::External(answer) => {
                return match self.resume(instance_id, answer).await {
                    Ok(outcome) => outcome,
                    Err(failure) => Outcome::Failed(failure),
                };
            }
            Input::Idle(specs) => {
                // TODO: multiple inputs?
                let spec = specs.first().cloned().unwrap_or(AgentSpec {
                    instance_id,
                    message_id: None,
                    pending_id: None,
                    turn_id: None,
                });
                (spec, specs, false)
            }
        };
        match self.serve(instance_id, spec, &batch, reentry).await {
            Ok(outcome) => outcome,
            Err(failure) => Outcome::Failed(failure),
        }
    }
}

fn least_recent(entries: impl IntoIterator<Item = (i64, u64)>, keep: i64) -> Option<i64> {
    entries
        .into_iter()
        .filter(|(instance, _)| *instance != keep)
        .min_by_key(|(_, last_used)| *last_used)
        .map(|(instance, _)| instance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_least_recent_entry_is_evicted_first() {
        assert_eq!(least_recent([(1, 10), (2, 30), (3, 20)], 0), Some(1));
    }

    #[test]
    fn the_entry_in_use_is_never_the_victim() {
        assert_eq!(least_recent([(1, 10), (2, 30)], 1), Some(2));
    }

    #[test]
    fn a_lone_entry_has_no_victim() {
        assert_eq!(least_recent([(1, 10)], 1), None);
    }
}
