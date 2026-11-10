# Agent 树

`wind_core::agent` 是一棵与业务无关的 agent 实例树。树的每个节点代表一个运行实例，
节点的相位（节点当前状态）、待消费输入与派生关系由一组事件归约得到；归约指用事件更新
树状态并产生副作用，由 `Agent` 执行：持久化相位、发布事件、启动或停止一次运行。

本文描述树的状态机、事件与副作用、取消与恢复。具体业务类型 `LlmDomain` 的回合流程见
「LLM 回合」；树节点的数据库行见「存储」。

## Domain

树不决定一次运行做什么，也不决定运行结果长什么样。`Domain` 只声明五组关联类型：

```rust
pub trait Domain: 'static + Clone {
    type Spec: Serialize + DeserializeOwned + Clone + Debug + Send + Sync;
    type Suspend: Serialize + DeserializeOwned + Clone + PartialEq + Debug + Send + Sync;
    type Resume: Debug + Send + Sync;
    type Output: Serialize + DeserializeOwned + Clone + Debug + Send + Sync;
    type Error: Serialize + DeserializeOwned + Clone + Debug + Send + Sync;
}
```

| 关联类型 | 含义 |
| --- | --- |
| `Spec` | 启动或排队一次运行所需的输入 |
| `Suspend` | 运行挂起时向外保存的载荷 |
| `Resume` | 恢复挂起运行所需的输入 |
| `Output` | 正常完成的结果 |
| `Error` | 失败的结果 |

本仓库唯一的实现是 `LlmDomain`：

| 类型 | 取值 |
| --- | --- |
| `Spec` | `LlmSpec { instance_id, message_id?, pending_id?, turn_id? }` |
| `Suspend` | `Suspended { external_wait_id }` |
| `Resume` | `WaitAnswer { external_wait_id, decisions }` |
| `Output` | `LlmOutcome { message_id }` |
| `Error` | `LlmFailure { message_id?, error }` |

## 节点与树

```rust
pub type NodeId = i64;
pub type JobId = u64;

pub struct Node<D: Domain> {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub phase: Phase<D>,
    pub job: Option<JobId>,
    pub detached: bool,
    pub pending: VecDeque<D::Spec>,
    pub delivered: bool,
}

pub struct Tree<D: Domain> {
    nodes: BTreeMap<NodeId, Node<D>>,
    root: Option<NodeId>,
    job_seq: JobId,
}
```

`NodeId` 就是 `agent_instances.id`。`JobId` 是树内单调递增的序号，只用于识别“当前这一次
运行”；它不是持久化身份。

树的派生关系：

- `insert_node` 拒绝重复 id 与未知父节点；父节点存在时把新节点追加到 `children`。
- `parent` 为空且 `root` 为空时，该节点成为根。
- `restore_node` 直接写入 id、父、子、相位、`detached` 与待消费输入；恢复的节点
  `delivered = true`，`job = None`。
- `children` 的顺序就是插入或恢复时的顺序；`subtree_post_order` 先子后父。
- `barrier_children(id)` 返回不处于 `detached` 的子节点。等待屏障只看这些子节点。
- `is_quiescent` 要求全部节点相位终态、待消费输入为空、`delivered = true`。
- `in_flight_nodes` 返回相位为 `Running` 的节点；`undelivered_nodes` 返回
  `delivered = false` 的节点。

`detached` 节点不进入父节点的等待屏障，但它的终态需要一次 `Deliver` 才能让整棵树静止。
`Deliver` 由宿主在消费完后台结果后发送；核心库没有发送它的地方。

## 相位

```rust
pub enum Phase<D: Domain> {
    Running,
    Suspended(D::Suspend),
    WaitingChildren,
    Done(D::Output),
    Failed(D::Error),
    Cancelled(CancelOrigin),
    Aborted,
}

pub enum CancelOrigin {
    External,
    Ancestor(i64),
}
```

| 相位 | 含义 | 终态 |
| --- | --- | --- |
| `Running` | 有一次运行在飞 | 否 |
| `Suspended` | 运行已挂起，等待外部输入 | 否 |
| `WaitingChildren` | 有未终态的非 detached 子节点 | 否 |
| `Done` | 正常完成 | 是 |
| `Failed` | 运行返回错误 | 是 |
| `Cancelled` | 被取消；`External` 为直接取消，`Ancestor(root)` 为随祖先取消 | 是 |
| `Aborted` | 运行以不可解释的方式中止，如 panic 或恢复时发现没有任务 | 是 |

`has_live_job` 只对 `Running` 为真。相位序列化为 `phase` 判别式与 `phase_payload` 载荷两列：

- `serde` tag 为 `phase`，content 为 `phase_payload`，全部 `snake_case`。
- `Phase::columns` 返回 `(String, Option<String>)`，载荷是 JSON 文本。
- `Phase::from_columns` 用同一形状反序列化。

## 事件与副作用

```rust
pub enum Event<D: Domain> {
    Spawn { id, parent, spec, detached },
    Enqueue { node, spec },
    Deliver { node },
    Resume { node, expect: D::Suspend, input: D::Resume },
    CancelRequest { root, origin },
    Suspended { node, job, payload: D::Suspend },
    Spawned { node, job, spawn: Vec<SpawnRequest<D>> },
    Completed { node, job, output: D::Output },
    Failed { node, job, error: D::Error },
    Aborted { node, job },
    Recovered { node },
}
```

外部输入是 `Spawn`、`Enqueue`、`Resume`、`CancelRequest`、`Deliver`；运行报告是
`Suspended`、`Spawned`、`Completed`、`Failed`、`Aborted`；`Recovered` 只在启动恢复时
逐个节点发送。

```rust
pub enum Effect<D: Domain> {
    Launch { node, job, input: Input<D> },
    Stop { node, job },
    Persist { node },
    Publish(TreeEvent<D>),
}

pub enum TreeEvent<D: Domain> {
    NodeCreated { node, parent },
    PhaseChanged { node, to: Phase<D> },
    Sealed { root },
}
```

`Agent` 执行副作用：

| 副作用 | 执行 |
| --- | --- |
| `Persist` | 把当前相位写入 `agent_instances`；失败时记录日志并发出 `core/agent.persist_failed` |
| `Publish` | 把 `TreeEvent` 转成 `core/agent.spawn`、`core/agent.status` 或 `core/agent.sealed` |
| `Launch` | 派生一个 tokio 任务执行 `Runner::run` |
| `Stop` | 取消对应任务的令牌 |

## 归约

`Tree::reduce(event)` 先记录归约前是否静止；归约后若从“非静止”变为“静止”，在副作用末尾
追加 `Publish(TreeEvent::Sealed { root })`。

入口吸收：若事件不以 `CancelRequest`、`Recovered`、`Deliver` 绕过，且事件指向的节点已经
处于终态，且事件不是 `Spawn`/`Enqueue`，则直接返回空副作用。终态节点只接受重新打开它的
`Spawn`/`Enqueue`。

| 事件 | 条件 | 副作用 |
| --- | --- | --- |
| `Spawn` | id 未重复、父节点存在 | 插入 `Running` 节点；分配 job；`Persist`、`NodeCreated`、`Launch(Fresh)` |
| `Enqueue` | 节点存在 | 入 `pending`；节点终态时立即 `drain_pending` |
| `Deliver` | 节点存在 | 置 `delivered = true`；无副作用 |
| `Completed` / `Failed` | job 是当前 job 且相位为 `Running` | `finish_terminal` |
| `Spawned` | 同上 | 清空 job；逐个归约 `Spawn`；无屏障子节点则启动 `Children` 运行，否则转 `WaitingChildren` |
| `Suspended` | 同上 | 清空 job；转 `Suspended(payload)`；`Persist`、`PhaseChanged` |
| `Resume` | 相位是 `Suspended` 且载荷等于 `expect` | 新 job；转 `Running`；`Persist`、`PhaseChanged`、`Launch(External)` |
| `CancelRequest` | 根节点存在且非终态 | 后序取消整棵子树，见「取消」 |
| `Aborted` | job 是当前 job | `finish_terminal(Aborted)` |
| `Recovered` | — | 见「恢复」 |

`Input` 有四种形态，由 `Agent` 原样交给 `Runner`：

```rust
pub enum Input<D: Domain> {
    Fresh(D::Spec),
    Children,
    External(D::Resume),
    Idle(Vec<D::Spec>),
}
```

### `finish_terminal`

```text
job = None
phase = 终态
owe_delivery(node)
Persist
PhaseChanged
reenter_parent_if_ready(node)
drain_pending(node)
```

顺序不可交换：先持久化并发布本节点的终态，再尝试唤醒父节点，最后处理本节点在运行期间
排队的输入。若 `drain_pending` 又启动了新 job，本节点会立即离开终态；`reduce` 在全部
副作用生成后再判断静止。

### `drain_pending`

取出节点全部待消费输入；为空则无副作用。否则：

```text
新 job
phase = Running
Persist
PhaseChanged(Running)
Launch(Idle(specs))
```

### `reenter_parent_if_ready`

父节点必须处于 `WaitingChildren`，且全部非 detached 子节点都已是终态。满足时给父节点
分配新 job、转 `Running`，发出 `Persist`、`PhaseChanged(Running)` 与 `Launch(Children)`。

`Children` 输入让 `LlmRunner` 以 `reentry = true` 重新进入：它会先回补子实例的结果，再决定
是否继续模型回合。

## 取消

`CancelRequest { root, origin }`：

```text
根节点不存在或已终态 → 无副作用
按 subtree_post_order(root) 遍历（先子后父）：
    清空该节点的待消费输入
    已终态 → 跳过
    记下当前 job
    job = None
    phase = Cancelled(origin 或 Ancestor(root))
    owe_delivery(node)
    Persist、PhaseChanged、Stop(job)
最后 reenter_parent_if_ready(root)
```

根节点使用请求给出的 `origin`；子孙使用 `CancelOrigin::Ancestor(root)`。子节点先于父节点
处理，所以父节点唤醒时看到的子节点都已是终态。

`owe_delivery` 只对 `detached` 节点置 `delivered = false`。取消一个后台节点后，父节点
不会等待它，但整棵树要静止仍需宿主发送 `Deliver`。

## 恢复

`Agent::run` 在进入事件循环前，对恢复进来的每个节点发送 `Recovered { node }`：

```text
phase == Running 且 job == None  → finish_terminal(Aborted)，然后 delivered = true
其余                              → reenter_parent_if_ready(node)
                                    若节点已终态 → drain_pending(node)
```

恢复的节点没有在飞任务（`job = None`），因此持久化为 `Running` 的节点必然是上一个进程
留下的未完成任务，统一转为 `Aborted`。`Suspended` 节点保持挂起，等待宿主用
`AgentHandle::resume` 恢复。

核心库没有从数据库装载树的函数。宿主需要自己读取 `agent_instances`（并组合
`spawn_calls` 与 `agent_pending`），构造 `AgentOptions.restored` 的 `NodeSnapshot`。

## 运行任务

`Jobs` 管理每个节点的在飞任务：

```text
launch(request)
  新建取消令牌，登记 (node, job)
  tokio::spawn:
      select! {
          取消 → 不发报告
          catch_unwind(Runner::run(ctx, input)) → 报告
      }
      从登记表移除
      Ok(outcome) → Completed / Failed / Suspend / Spawn
      Err(panic)  → Aborted
```

`Ctx` 传给 `Runner`：

```rust
pub struct Ctx {
    pub node: NodeId,
    pub job: JobId,
    pub cancel: CancellationToken,
}
```

`Jobs::stop(node, job)` 取消对应令牌；`Jobs::shutdown()` 取消全部并清空登记表。任务是
`tokio::spawn` 独立任务，`Agent` 的事件循环不等待它完成，只等待报告事件。

## Agent 循环

```rust
pub struct AgentOptions<D: Domain> {
    pub topic_id: i64,
    pub storage: Storage,
    pub dispatcher: Arc<Dispatcher>,
    pub runner: Arc<dyn Runner<D>>,
    pub restored: Vec<NodeSnapshot<D>>,
    pub cancel: CancellationToken,
}
```

`Agent::new` 建立事件通道（容量 1024）与 `Jobs`，恢复节点。`Agent::start` 派生任务并返回
`AgentHandle`。

```text
run():
    对每个恢复节点 apply(Recovered)
    loop {
        select! {
            biased;
            取消 → break
            事件 → apply(event)
            通道关闭 → break
        }
    }
    jobs.shutdown()
```

`apply(event)` 调 `tree.reduce(event)`，逐个执行副作用。整个归约与副作用执行在同一个
任务里串行，因此同一话题的树状态不会被并发修改。`Runner` 的实际运行在独立任务中。

`AgentHandle`：

| 方法 | 发送的事件 |
| --- | --- |
| `spawn(id, parent, spec, detached)` | `Spawn` |
| `enqueue(node, spec)` | `Enqueue` |
| `resume(node, expect, input)` | `Resume` |
| `cancel(root, origin)` | `CancelRequest` |
| `deliver(node)` | `Deliver` |
| `shutdown()` | 取消整个 `Agent` 的令牌 |
| `is_stopped()` / `wait_stopped()` | 读/等待同一个令牌 |

发送是异步的，通道满时等待。通道关闭时返回 `CoreError::Closed`。

## 持久化与事件

`Agent` 只持久化相位：

```sql
UPDATE agent_instances SET phase = ?, phase_payload = ? WHERE id = ?
```

受影响行数为 0 时记录日志，并发出 `core/agent.persist_failed`。该事件使用
`Origin::Internal`、`Source::core("agent")`、作用域 `Scope::Topic { topic_id, instance_id }`、
50ms 预算。`core/agent.persist_failed` 的 `log = false`，不写数据库。

树发布的三条观察事件：

| 树事件 | 事件 | 载荷 |
| --- | --- | --- |
| `NodeCreated { node, parent }` | `core/agent.spawn` | `AgentSpawn { instance_id, parent_id, detached }` |
| `PhaseChanged { node, to }` | `core/agent.status` | `AgentStatus { instance_id, phase, phase_payload }` |
| `Sealed { root }` | `core/agent.sealed` | `AgentSealed { instance_id }` |

三者都使用 `log = true`，进入 `event_log` 与前端流。载荷中的 `phase` 是判别式字符串，
`phase_payload` 是同一份相位载荷的 JSON 值。

## `LlmDomain` 的接线

| 树动作 | `LlmRunner` 的行为 |
| --- | --- |
| `Launch(Fresh(spec))` | 读取 `spec.pending_id`，把待消费输入合成用户消息；确定或创建 turn；进入模型回合 |
| `Launch(Children)` | 以 `reentry = true` 调用；先回补子实例的工具结果，再决定是否继续 |
| `Launch(External(answer))` | 先校验并答复 `external_waits`，执行批准的工具，再组装上下文进入模型回合 |
| `Launch(Idle(specs))` | 把多条待消费输入合成一条用户消息，进入模型回合 |
| 返回 `Outcome::Spawn { spawn }` | 树为每个子实例插入 `Spawn`，父节点转 `WaitingChildren` 或立即运行 |
| 返回 `Outcome::Suspend { payload }` | 树转 `Suspended` 并持久化；等待 `Resume` |
| 返回 `Outcome::Completed` / `Failed` | 树转 `Done` / `Failed`；终态节点若仍有排队输入，立即 drain |

## 当前边界

| 项 | 现状 |
| --- | --- |
| 主实例行 | `Agent` 不创建；宿主必须先写 `agent_instances` |
| 后台结果交付 | `PendingKind::BackgroundResult`、`AgentHandle::deliver` 没有生产者；`detached` 节点终态后整棵树可能一直不静止 |
| `core/agent.create` / `terminate` / `cancel` / `tree` | 目录中已声明，没有产生者 |
| 树装载 | 核心库没有从数据库构造 `restored` 的函数 |
| 相位持久化失败 | 只记录与发事件；内存中的树继续运行 |
| 取消传播 | `CancelRequest` 只取消树中已有节点；正在运行的任务通过 `Jobs` 的令牌取消 |
