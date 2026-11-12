# windai

An embeddable AI agent engine that you extend with **mods**, observe and steer through
**events**, and script in **QuickJS**.

windai is not a chat application and not a server. It is a Rust library that runs the whole
agent harness in one process: conversation state, model calls, tool execution, mod lifecycle,
script execution and event delivery. A host application assembles the crates, decides how
users sign in, and forwards events to its own front end.

That split is the point of the project: the parts that change often — persona, prompts,
skills, tools, UI panels, approval rules — live in mods, not in the engine. A mod can be
installed, updated or disabled without rebuilding the host.

> Status: `0.2.0`, library-only, no CLI and no composition root in this repository. The
> public API is still moving. Names such as `wind-core` and `mod.json` are stable; trait
> signatures are not.

---

## What you can do with it

| If you are… | windai gives you |
| --- | --- |
| Building an AI product | A complete agent tree, streaming model rounds, MCP tools, SQLite persistence and an in-process event stream — as a library you embed. |
| Writing a mod | A folder with a manifest, optional prompt/skill/MCP/UI resources, and JavaScript handlers that can rewrite what the model sees and react to what happens. |
| Building a front end | One event stream per process, ordered by `(epoch, seq)`, carrying every visible fact, with a short replay window; plus a per-version URL for mod UI modules. |
| Integrating another system | Interception points before prompts, requests and tool calls, and a SQLite journal of every logged event. |

Three ideas carry the design:

1. **Mods are the extension surface.** Prompts, skills, MCP servers, UI entries and scripts
   all belong to a mod. The engine knows nothing about your product's behaviour.
2. **Events are the only cross-layer notification.** Every state change that matters is
   published once with an identity, then routed to the destinations it is meant for: the front
   end, the event log, and mod handlers.
3. **Scripts are sandboxed, isolated and short-lived.** Each handler call gets a fresh
   QuickJS runtime, a deadline and a memory cap. Nothing survives between calls except what
   the host stores.

---

## In one picture

```text
 host application
 ├─ feed runtime            assigns (epoch, seq), keeps a replay window, writes event_log
 ├─ dispatcher              matches events to enabled mods, runs intercept chains
 ├─ resource service        publishes mod prompts, skills, MCP servers and UI entries
 ├─ MCP registry            talks to declared MCP servers and built-in fs/skills services
 ├─ script runtime          runs mod JavaScript on a fixed pool of QuickJS threads
 ├─ agent tree              one node per instance, one task per running node
 └─ SQLite                  domain rows + event_log, WAL, up to 5 connections
```

A single run looks like this:

```text
user input ──► agent node ──► LLM round
                                 ├─ core/prompt.collect    intercept  mods append/remove prompt blocks
                                 ├─ core/rule.apply        intercept  mods patch the provider request body
                                 ├─ core/llm.request       intercept  mods patch messages and tools
                                 ├─ core/block.delta       observe    streamed text reaches the front end
                                 ├─ core/block.committed   observe    a block is final and has a row id
                                 ├─ core/message.seal      observe    the model turn is finished
                                 └─ core/llm.toolcall      intercept  mods inspect, rewrite or suspend tools
                                          │
                                          └─ tools run, another round starts, or the node finishes
node finishes ──► core/agent.status  observe
tree is idle  ──► core/agent.sealed  observe
```

---

## Vocabulary

| Term | Meaning |
| --- | --- |
| **Topic** | A conversation workspace. It owns a directory at `<root>/topics/<topic_id>` and is the unit for enabling mods. |
| **Instance** | One node in the agent tree, stored in `agent_instances`. A topic has a main instance and can spawn children. |
| **Event** | An immutable record: a name, a scope, an origin, a source, a payload and a shape. Identified by `(epoch, seq)`. |
| **Shape** | `observe` (a fact, cannot be changed) or `intercept` (a decision point, mods may rewrite it). |
| **Mod** | An installed extension package: a manifest plus optional prompts, skills, MCP servers, scripts and a UI entry. |
| **Handler** | A JavaScript function exported from `scripts/index.js` and named in the manifest. |
| **UI entry** | `ui/index.js` inside a mod version. The backend publishes its URL; the front-end shell loads it. |
| **Feed** | The in-process event bus. It owns event identity, the replay window and the `event_log` writer. |

A **scope** is either `global` or `topic { topic_id, instance_id }`. This matters more than it
looks: mod subscriptions are resolved per topic, so global events (the `core/mcp.*` family)
are currently never delivered to mods.

---

## The mod system

A mod is a directory tree on disk plus a row in the database. It is not code linked into the
engine, and installing one never recompiles anything.

```text
<root>/mods/<name>/<version>/      version directory, immutable in practice
    mod.json                       manifest, required
    scripts/index.js               script entry, required only if handlers are declared
    scripts/lib/*.js               imports, resolved relative to the version directory
    prompts/*.md|*.txt|*.html      prompt fragments, sorted by file name
    skills/<skill>/SKILL.md        skills, exposed through the built-in skills service
    ui/index.js                    front-end entry, optional
    mcp/                           reserved; there is no loader for it yet
<root>/mods/<name>/data/           private, shared by all versions, kept on delete
```

`<root>` is `WIND_ROOT_DIR`, or `~/.windai` when unset. The database is
`<root>/windai.db`, compiled bytecode lives under `<root>/bytecode`.

### Manifest

```json
{
  "name": "house-style",
  "version": 1,
  "description": "Append a house style block to the system prompt",
  "capabilities": ["storage"],
  "mcpServers": {
    "time": { "command": "uvx", "args": ["mcp-server-time"] }
  },
  "requests": {},
  "events": {
    "on": {
      "core/prompt.collect": { "handler": "onPrompt", "timeoutMs": 200 }
    },
    "emit": {
      "usage": {
        "retention": "journal",
        "eventKey": "usage",
        "payload": { "type": "object" }
      }
    }
  },
  "defs": {}
}
```

| Field | Required | Rules |
| --- | --- | --- |
| `name` | yes | `^[a-z][a-z0-9_-]{1,62}$`, globally unique, must equal the directory name |
| `version` | yes | integer `>= 1`, must equal the directory name and increase on update |
| `description` | no | at most 500 characters |
| `capabilities` | no | any of `state`, `storage`, `http`, `agent`, `fs`; duplicates rejected |
| `mcpServers` | no | named stdio or Streamable HTTP servers; exactly one of `command` / `url` |
| `requests` | no | declared JS entry points; validated, not yet reachable |
| `events.on` | no | keys must start with `core/` and exist in the host event directory |
| `events.emit` | no | outbound channels used by `ctx.emit` |
| `defs` | no | shared local JSON Schema fragments |

`events.on.<event>` takes a `handler` and an optional `timeoutMs` (1–60000, default **20 ms**).
`events.emit.<channel>` requires `retention` and `payload`; `journal` additionally requires a
unique `eventKey` within the mod.

The generated schema is checked into [`docs/manifest.schema.json`](docs/manifest.schema.json).

### Lifecycle

```text
install   first install requires version 1; directory must equal mods/<name>/<version>; manifest must match
update    new version must be greater than the stored one
delete    drops the database rows and cascade-deletes the topic links; files stay on disk
enable    links a mod to one topic; unrelated topics keep their own sets
disable   removes that link
restore   rebuilds the in-memory subscription index and enabled sets from the database
```

Every lifecycle change runs in one SQLite write transaction. Publication happens afterwards, outside
that transaction: the rebuilt subscription index and the replaced enabled sets. Install, update
and delete additionally emit `core/mod.installed` / `core/mod.updated` / `core/mod.deleted`. If
publication panics, the installer reloads from the database rather than serving a half-applied
state.

Two consequences worth knowing:

- **Subscription publishing is not enablement.** The index always contains every installed
  mod's subscriptions; the enabled set decides which of them actually run. An installed but
  disabled mod costs a lookup, nothing more.
- **Resources are pulled, not pushed.** Prompts and MCP declarations are read on each turn,
  skills are scanned when the skills service needs them. Installing a mod does not inject
  anything into a running turn; the next turn sees it.

### What a mod contributes

| Contribution | Reaches the model or user today? |
| --- | --- |
| `prompts/*` | Yes. Concatenated into the system prompt, ordered by mod name then file name. |
| `skills/` | Yes, as an extra root of the built-in `skills` service. |
| `mcpServers` | Yes, as MCP tools named `<server>0m0<tool>` after the service is renamed `mod_<mod>_<local>`. |
| `events.on` | Yes, for topic-scoped events on topics where the mod is enabled. |
| `events.emit` | Yes, as `m_<mod>/…` events routed to the front end and optionally to `event_log`. |
| `ui/index.js` | Only announced: the version's URL appears in the install/update events. |
| `requests` | No. Parsed and validated, but there is no tool source and no call entry point. |
| `capabilities` | No. Declarations are validated; nothing is injected into `ctx` beyond `emit`, `notify` and `log`. |

The last two rows are deliberate gaps, listed again in [Current limitations](#current-limitations).

---

## Events

### Envelope and identity

```rust
pub struct Envelope<P> {
    pub time: i64,          // epoch milliseconds
    pub name: String,
    pub scope: Scope,
    pub origin: Origin,     // agent | ui | internal
    pub source: Source,     // core { subsystem } | mod { name, version }
    pub payload: Option<P>,
    pub shape: Shape,
}
```

`(epoch, seq)` identifies an event for the lifetime of the database. `epoch` is leased from
`feed_meta` on every `feed::open`; `seq` is a counter inside that feed. The pair is unique in
`event_log`, and `feed` assigns it from a single task, so allocation order is event order.
`event_log.id` is just a row number and must not be used as an event id.

### Two shapes

| | `observe` | `intercept` |
| --- | --- | --- |
| Meaning | something happened | something is about to happen |
| Front end | delivered, replayed | never delivered |
| Mods | asynchronous, batched, return value discarded | synchronous, serial, return value replaces the payload |
| Logged | when `log = true` | when `log = true` |
| Budget | subscription timeout | `min(subscription timeout, caller's remaining budget)` |

An intercept chain calls one mod at a time, in index order, so the second mod sees the first
mod's rewrite. A handler that returns `undefined` or `null` leaves the payload alone. A
handler that returns something that cannot be parsed back into that event's payload is
reported as `core/dispatch.failed` and the previous payload is kept. The chain still commits
one event even when no mod is subscribed.

`core/llm.request` and `core/rule.apply` are intercepted with `decide_without_payload`: mods
receive the full payload, but `event_log` stores `NULL` for it. That keeps whole conversations
and provider bodies out of the journal.

### Delivery rules

- **Ordering.** Same event, several mods: mod name ascending. Within a mod, `events.on` is
  sorted by event name. There are no priorities.
- **Enablement.** `Scope::Topic` resolves to the mods enabled on that topic. `Scope::Global`
  resolves to the empty set, so `core/mcp.*` interceptors and observers never run.
- **Observe batching.** Jobs are collected in 5 ms windows and grouped by
  `(mod, event, scope)`. A group of one delivers the value itself; a group of many delivers
  an array. Handlers must accept both.
- **Rate limits.** At most 8 nested dispatch levels (not yet reachable), 1000 events per
  trigger, and 100 events per 3 seconds per mod for `ctx.emit`.
- **Failure isolation.** A handler error, a timeout or an unparsable rewrite becomes
  `core/dispatch.failed` or `core/dispatch.timeout`. The run continues; only that mod's
  contribution is lost.
- **Ordering versus durability.** `journal` events notify mods after the database commit;
  `live`, `group` and `close` events notify immediately. A failed write drops the batch from
  history but still notifies mods, so the order mods see for journaled events matches the log
  while the log may have holes.

### Catalog

Every event that a mod can subscribe to starts with `core/`. The table lists the payload's
main fields; full schemas are generated from the Rust types.

**Dispatch**

| Event | Shape | Logged | Payload | Emitted by |
| --- | --- | --- | --- | --- |
| `core/dispatch.failed` | observe | yes | `event`, `failure`, `target?`, `reason` | dispatcher, observe pool |
| `core/dispatch.timeout` | observe | yes | `event`, `target`, `duration_ms` | dispatcher, observe pool |

`failure` is one of `bad_rewrite`, `handler_error`, `call_error`, `too_deep`,
`too_many_events`, `rate_limited`, `bad_payload`, `wrong_shape`, `not_in_directory`.

**Agent**

| Event | Shape | Logged | Payload | Emitted by |
| --- | --- | --- | --- | --- |
| `core/agent.create` | intercept | yes | `topic_id`, `parent_id?`, `mode`, `task?` | reserved, no producer |
| `core/agent.terminate` | intercept | yes | `instance_id`, `reason` | reserved, no producer |
| `core/agent.cancel` | intercept | yes | `instance_id`, `origin` | reserved, no producer |
| `core/agent.spawn` | observe | yes | `instance_id`, `parent_id?`, `detached` | agent tree |
| `core/agent.status` | observe | yes | `instance_id`, `phase`, `phase_payload?` | agent tree |
| `core/agent.sealed` | observe | yes | `instance_id` | agent tree |
| `core/agent.persist_failed` | observe | no | `topic_id`, `instance_id`, `phase`, `error` | agent tree |
| `core/agent.tree` | observe | yes | `nodes` | reserved, no producer |

Phases are `running`, `suspended`, `waiting_children`, `done`, `failed`, `cancelled`,
`aborted`.

**LLM**

| Event | Shape | Logged | Payload | Emitted by |
| --- | --- | --- | --- | --- |
| `core/prompt.collect` | intercept | yes | `topic_id`, `instance_id`, `fragments` | turn preparation |
| `core/rule.apply` | intercept | yes (no payload) | `adapter`, `body` | request preparation |
| `core/llm.request` | intercept | yes (no payload) | `contexts`, `tools` | request preparation |
| `core/llm.toolcall` | intercept | yes | `instance_id`, `message_id`, `calls`, `suspend?` | tool planning |
| `core/message.seal` | observe | no | `message_id`, `finish_reason`, `usage` | message stream |
| `core/message.interrupted` | observe | no | `message_id`, `reason` | message stream |
| `core/block.delta` | observe | no | `message_id`, `seq`, `revision`, block fields | message stream |
| `core/block.committed` | observe | no | `message_id`, `seq`, `block_id`, `block?` | message stream |
| `core/input.accepted` | observe | yes | `topic_id`, `instance_id`, `pending_id` | reserved, no producer |

**Mod**

| Event | Shape | Logged | Payload | Emitted by |
| --- | --- | --- | --- | --- |
| `core/mod.installed` | observe | yes | `name`, `version`, `manifest`, `ui_entry?` | installer |
| `core/mod.updated` | observe | yes | `name`, `from_version`, `to_version`, `ui_entry?` | installer |
| `core/mod.deleted` | observe | yes | `name`, `version` | installer |
| `core/mod.notify` | observe | yes | `level`, `text` | `ctx.notify` |

**MCP** (global scope; not delivered to mods today)

| Event | Shape | Logged | Payload |
| --- | --- | --- | --- |
| `core/mcp.launch` | intercept | yes | `name`, `refs`, `definition` |
| `core/mcp.replaced` | intercept | yes | `name`, `refs`, `old`, `new` |
| `core/mcp.starting` | observe | yes | `name`, `refs` |
| `core/mcp.started` | observe | yes | `name`, `refs`, `tools` |
| `core/mcp.failed` | observe | yes | `name`, `refs`, `error` |
| `core/mcp.stopped` | observe | yes | `name`, `refs`, `reason` |
| `core/mcp.log` | observe | no | `name`, `stream`, `line` |

Mod-emitted events are named `m_<mod>/<eventKey>` (journal) or `m_<mod>/<channel>` (live).
They are not in the directory, so they are never dispatched to other mods.

---

## QuickJS scripts

`wind-script` executes mod JavaScript. It is not a plugin ABI, not a persistent VM, and not a
way to call arbitrary Rust. One call in, one JSON value out.

### Execution model

```text
ScriptService::call
  circuit breaker    → suspended while cooling down
  deadline check     → timeout if already past
  thread pool slot   → busy if none free
  new Runtime        → memory cap, interrupt handler
  load entry module  → memory cache → bytecode file → compile source
  new Context        → a fresh realm every call
  eval module        → top-level code runs again
  find export        → handler must exist and be a function
  build ctx          → mod, scope, emit, notify, log
  call handler       → JSON input, JSON output
```

Defaults: 4 interpreter threads, 64 MiB per runtime, no queueing. If all threads are busy the
call returns `busy` immediately. Cancellation ends the caller's wait; the interpreter stops at
the deadline through QuickJS's interrupt handler.

A module-level variable is **not** state. The runtime and the context are new on every call,
so top-level assignments run again and nothing is retained. Persistent state needs a host
capability; today the only durable path is an MCP server you declare.

Three consecutive timeouts trip a breaker and pause that mod (by name, across versions) for 30
seconds. Cold calls — first load, waiting for initialization, compiling from source — do not
count.

### The `ctx` facade

```js
export function onSomething(input, ctx) {
  ctx.mod;                        // { name, version }, read-only
  ctx.scope;                      // { kind: "global" } | { kind: "topic", topic_id, instance_id }
  ctx.emit(channel, payload);     // a declared outbound channel
  ctx.notify({ text, level });    // level: "info" | "warn" | "error", becomes core/mod.notify
  ctx.log('debug', 'anything');   // host log, target "mod", no event
}
```

Constraints that matter in practice:

- `ctx.emit` accepts only channels declared in `events.emit`. Payloads are JSON and limited to
  64 KiB. The channel's `retention` cannot be overridden by the script.
- `ctx.notify` is the intended way to surface a message to the user. It always becomes a
  journaled `core/mod.notify` with `{ level, text }`.
- `ctx.log` writes to the host logger and produces no event.
- Reading `ctx.state`, `ctx.storage`, `ctx.http`, `ctx.agent` or `ctx.fs` yields `undefined`.
  The capability names are accepted in the manifest but not injected yet.
- Do not cache `ctx` in a module-level variable. The next call gets a different object.

Payload validation at emit time only checks the top-level `type` of the declared schema.
Full JSON Schema validation of payloads is not wired in yet.

### Modules and bytecode

- The entry is always `scripts/index.js`.
- Imports must be relative (`./`, `../`), must spell the `.js` extension, and must resolve
  inside `scripts/`. Bare specifiers, absolute paths, `:` specifiers, directory resolution and
  symlink escapes are rejected.
- Only static imports are supported.
- Compiled bytecode is cached by content under
  `<root>/bytecode/<mod>/<engine-id>/<sha256(module || 0x00 || source)>.bin`. The file carries a
  format tag and a digest. A corrupt file is deleted, its source is used instead, and that
  cache entry stays disabled for the life of the script service.
- The in-memory module cache is keyed by the version directory and the module path.
  **Editing scripts inside an existing version directory does not take effect until the script
  service is rebuilt.** Publish a new version instead — that is also what makes
  `core/mod.updated` fire.

### Example: append a prompt block (`intercept`)

`mod.json`

```json
{
  "name": "house-style",
  "version": 1,
  "events": {
    "on": {
      "core/prompt.collect": { "handler": "onPrompt", "timeoutMs": 200 }
    }
  }
}
```

`scripts/index.js`

```js
export function onPrompt(input, ctx) {
  const fragments = Array.isArray(input.fragments) ? input.fragments : [];
  ctx.log('debug', 'prompt fragments', String(fragments.length));
  return {
    ...input,
    fragments: [...fragments, 'Answer in the user\'s language, and keep code blocks intact.'],
  };
}
```

The returned object replaces the payload for the next mod in the chain and for the turn. The
fragments are joined into the system prompt, so this is the cheapest way to add a persona,
policy or output format.

### Example: gate destructive tools (`intercept` + suspension)

```js
const DESTRUCTIVE = /^(delete|remove|drop|purge)_/i;

export function onToolCall(input, ctx) {
  const calls = Array.isArray(input.calls) ? input.calls : [];
  const risky = calls.filter((call) => DESTRUCTIVE.test(call.name || ''));
  if (risky.length === 0) {
    return null;                       // no change
  }
  ctx.notify({
    text: `Approval required: ${risky.map((call) => call.name).join(', ')}`,
    level: 'warn',
  });
  return { ...input, suspend: true };   // suspend the whole batch
}
```

With `suspend: true` the host does not execute this batch. It stores the pre-rewrite calls in
`external_waits`, moves the node to `suspended`, and waits for the host to resume it with a
`WaitAnswer`. Calls left undecided are allowed by default, so a gate should decide explicitly
in the host UI.

### Example: batch-aware observation

```js
export function onSeal(input, ctx) {
  const seals = Array.isArray(input) ? input : [input];   // observe may batch
  const output = seals.reduce((sum, seal) => sum + (seal.usage?.output_tokens ?? 0), 0);
  ctx.emit('usage', { turns: seals.length, output_tokens: output });
}
```

Observe handlers receive either one payload or an array. The `each()` helper below is the
smallest correct way to normalise the two forms:

```js
const each = (input) => (Array.isArray(input) ? input : [input]);
```

### Failure behaviour

| Situation | Result |
| --- | --- |
| Missing export, not a function, syntax error | `handler` error, reported as `core/dispatch.failed` |
| Handler throws | `handler` error; intercept keeps the previous payload |
| Deadline reached | `core/dispatch.timeout`; the caller moves on |
| Payload over 64 KiB | `payload_too_large`, surfaced as a handler error |
| Unknown emit channel | JS exception |
| All interpreter threads busy | `busy`, no retry |
| 3 consecutive timeouts | 30 second suspension for that mod |

---

## The UI hook

The engine does not render anything. It answers one question for the front-end shell:
*which UI module belongs to this mod version?*

### Discovery

When a version directory contains `ui/index.js`, install and update announce its URL. The
resource loader additionally canonicalises that path and requires the result to stay inside the
version directory. The payload of `core/mod.installed` carries:

```json
{
  "name": "house-style",
  "version": 2,
  "manifest": { "...": "..." },
  "ui_entry": "windai://mods/house-style/2/ui/index.js"
}
```

`core/mod.installed` and `core/mod.updated` carry `ui_entry`; `core/mod.deleted` does not. The
field is omitted when the version has no UI file, which is also how the shell learns that an
update removed the UI.

The library also exposes the same information per topic:
`wind_mod::resources::Resources::topic(topic_id)` returns the enabled mods, each with
`ui: Option<PathBuf>`. That is the correct source for deciding what to mount on a given topic,
because `ui_entry` in an install event is process-wide while enablement is per topic.

### Division of responsibility

| This repository | The front-end shell |
| --- | --- |
| Detects `ui/index.js` | Resolves `windai://mods/…` to a file or URL |
| Validates it is inside the version directory | Defines the module format (ESM, IIFE, bundler output) |
| Publishes `ui_entry` on install and update | Loads, mounts, unmounts and re-mounts on version change |
| Publishes every visible event through the feed | Subscribes, replays, de-duplicates and renders |
| Stores finalized messages and blocks in SQLite | Reads them when it needs content older than the replay window |

There is no `mount` function in the backend contract, no `UiCtx` injection and no UI event
bridge implemented here. Those belong to the shell, so a UI module is exactly what the shell
says it is. A file that only exports nothing is enough to be discovered:

```js
// ui/index.js — the module format belongs to the front-end shell
export {};
```

### Lifecycle for a shell

```text
connect
  ├─ subscribe to the feed, remember Opening.cut
  ├─ read finalized content from message / block tables
  └─ ask the host for the enabled mods of the current topic

core/mod.installed  → if enabled on this topic and ui_entry present, load and mount
core/mod.updated    → unmount the old module, load ui_entry (or nothing) for the new version
core/mod.deleted    → unmount and drop any state the module held
```

Because the URL contains the version, updating a mod always produces a new URL. The shell
should treat the URL as the cache key: reload on change, keep the old instance until the new
one mounts, and never assume the module survives a reload.

### What a UI can know

The shell decides what to pass in, but the backend supplies three things it cannot get
elsewhere:

1. **The event stream.** Every `observe` event, including `core/block.delta`,
   `core/message.seal`, `core/agent.status`, `core/mod.notify` and each mod's own
   `m_<mod>/…` events. Intercept events never appear.
2. **History.** `event_log` holds journaled events with scope, source and payload. The replay
   window is memory only; the shell must page `event_log` itself for anything older.
3. **Finalized content.** `core/block.committed` and `core/message.seal` refer to rows in
   `block` and `message`. Late subscribers must read those tables, because closed message
   groups are not replayed.

A practical rule for UI modules: render from the event stream while it flows, and reconcile
against the domain tables when the stream is reconnected. `Opening.cut` tells you where replay
ends and your own paging begins.

---

## Embedding windai

### Crates

| Crate | Depends on | Responsibility |
| --- | --- | --- |
| `wind-wire` | — | Shared types: messages, blocks, tool declarations, MCP definitions, event envelope, `FeedEvent`, `AppDirs`, service traits |
| `wind-event` | wire | Event catalog, envelope, dispatch, guards, subscription index, observe pool, mod emitter |
| `wind-storage` | wire | SQLite schema, transactions, row models, per-domain statements |
| `wind-ai` | wire | Provider adapters, request building, SSE parsing, HTTP |
| `wind-rule` | — | JSON rule engine for request bodies |
| `wind-script` | wire | QuickJS engine, module loader, facade, bytecode cache, interpreter pool, deadlines, breaker |
| `wind-mod` | wire, event, storage | Manifest, package, installer, resources, roster, generated schema |
| `wind-mcp` | wire, event | MCP registry, connections, launch planning, built-in `fs` and `skills` |
| `wind-core` | all of the above | Agent tree, LLM rounds, feed, resource adapter, script wiring |

Dependency direction is acyclic and enforced by the crate graph: `wind-core` is the only crate
that depends on every other internal crate; `wind-script` depends on `wind-wire` only;
`wind-mod` does not depend on `wind-script`, so a mod with broken scripts still installs.

### Assembly

There is no composition root in the repository. The order below is the one used by the
integration tests and is the smallest working sequence.

```rust
use std::sync::Arc;
use wind_core::feed::{self, FeedOptions};
use wind_core::resources::ResourceService;
use wind_core::script::ScriptRuntime;
use wind_event::{Dispatcher, Limits, ModCaller, ModRoster, Observer};
use wind_mcp::McpOptions;
use wind_mod::installer::Installer;
use wind_mod::resources::Resources;
use wind_mod::roster::Roster;
use wind_storage::Storage;
use wind_wire::AppDirs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dirs = AppDirs::resolve();
    let storage = Storage::open(&dirs.db_file()).await?;
    let (outlet, runtime) = feed::open(storage.clone(), FeedOptions::default()).await?;

    let caller = Arc::new(ScriptRuntime::quickjs(
        storage.clone(),
        dirs.clone(),
        Arc::new(outlet.clone()),
        Limits::default(),
    )?);
    let roster = Arc::new(Roster::load(&storage).await?);

    let directory = wind_event::directory()
        .merge(wind_core::agent::events::directory())
        .merge(wind_core::llm::events::directory())
        .merge(wind_core::script::directory())
        .merge(wind_mod::events::directory())
        .merge(wind_mcp::events::directory());

    let dispatcher = Arc::new(Dispatcher::new(
        directory,
        roster.clone() as Arc<dyn ModRoster>,
        caller as Arc<dyn ModCaller>,
        Arc::new(outlet.clone()),
        Limits::default(),
    ));

    let registry = wind_mcp::init(McpOptions::new(dirs.mods_root()), dispatcher.clone());
    let services = ResourceService::new(
        Installer::new(storage.clone(), dispatcher.clone(), roster, dirs.clone()),
        Resources::new(storage.clone(), dirs.clone()),
        storage.clone(),
        registry,
        dirs.topics_root(),
    );
    services.restore().await?;

    let observer: Arc<dyn Observer> = dispatcher.clone();
    tokio::spawn(runtime.run(vec![Arc::downgrade(&observer)]));

    let opening = outlet.subscribe().await?;
    let _frames = opening.subscription;   // encode recv() into SSE / WebSocket frames
    Ok(())
}
```

Notes that save debugging time:

- `Dispatcher::new` starts the observe pool, so it must run inside a Tokio runtime.
- Observers must be attached **before** `runtime.run(...)`. `run` takes weak references, so
  dropping the dispatcher stops mod delivery while events keep flowing.
- `feed::open` only leases the epoch; nothing is delivered until `run` is polled.
- `ScriptRuntime::quickjs` is the default script stack. Passing a caller that always returns
  `CallError::Internal` gives you a script-free build; script mods still install and enable,
  but their handlers fail at call time.
- Shut down in this order: `ResourceService::shutdown`, `FeedHandle::shutdown`, then await the
  feed task.

### What the host must supply

| Area | Why it is not in the library |
| --- | --- |
| The main `agent_instances` row | Phase persistence assumes the row exists; `Agent` only logs `core/agent.persist_failed` when the update matches nothing |
| A user-input entry point | Writing an `agent_pending` row and handing `LlmSpec { pending_id }` to `AgentHandle` is an app decision |
| HTTP/SSE/WebSocket endpoints | `FeedHandle` is an in-process subscription; transport and auth are yours |
| Approval answers | `external_waits` rows are written by the runner; the UI flow and `AgentHandle::resume` call are yours |
| Event history queries | `wind-storage` writes `event_log` but has no read API for it |
| The front-end shell | UI loading, module format, mounting, `UiCtx` |
| `AppDirs` root | It decides database, topics, skills, mods and bytecode locations |

---

## Data model

SQLite, WAL, `BEGIN IMMEDIATE` for every write, at most five pooled connections, foreign keys
on, `busy_timeout` 8 seconds.

Domain tables: `providers`, `models`, `credentials`, `json_rule`, `topics`, `turn`,
`message`, `block`, `agent_instances`, `agent_pending`, `spawn_calls`, `external_waits`,
`mods`, `topic_mods`. Event tables: `event_log`, `feed_meta`.

Two write paths, no shared transaction:

```text
runner / installer / agent / mcp / script
   ├─ domain rows ─────────────────────► storage ──► SQLite
   └─ events ──► dispatcher ──► feed ──► subscribers
                                     └─► writer ──► event_log
```

Consequences:

- A block row and its `core/block.committed` event are separate writes. The event is emitted
  only after the block transaction commits.
- A failed `event_log` write drops that batch but still notifies mods. History can have gaps.
- `event_log` is not a change log for the domain tables, and the domain tables are not a
  projection of the event log. The only shared identity is the ids inside payloads.
- History must be reconstructed from both: payloads for what happened, tables for content.

---

## Building and testing

```bash
cargo build
cargo test
cargo fmt --all
```

Requirements: a Rust toolchain with edition 2024 support (1.85 or newer); the workspace is
developed against current stable. The workspace version is `0.2.0`.

Some integration tests drive a real model. They need a `.env` at the repository root:

```bash
cp .env.example .env    # then fill in TEST_BASE_URL, TEST_KEY, TEST_MODEL, TEST_ADAPTER
```

Those tests are `#[ignore]`d. Run them explicitly:

```bash
RUST_LOG=info cargo test -p wind-core --test mods -- --include-ignored --nocapture
```

The `mods` test installs the two fixture mods under `windai/core/tests/mods` (a prompt
rewriter and a roleplay mod), enables them on a topic, runs a real exchange and asserts that
each script observed and intercepted what it declared. It is the best end-to-end example in
the repository. Fixtures are copied to `~/.windai_test/mods`; the database is rebuilt each run.

To try the engine by hand, set `WIND_ROOT_DIR` to a scratch directory and install a mod whose
package sits at `<root>/mods/<name>/<version>`.

---

## Current limitations

These are code facts, not intentions. Each is a candidate for the next iteration.

| Area | Today |
| --- | --- |
| Script `ctx` | Only `emit`, `notify`, `log`. `state`, `storage`, `http`, `agent`, `fs` are declared but not injected. |
| Capabilities | Validated in the manifest, not enforced or scoped at runtime. |
| `requests` | No tool source, no call entry point; not visible to the model. |
| Global events | `core/mcp.*` never reaches mods, because the roster is topic-indexed. |
| Mod-to-mod events | `events.on` only accepts `core/`; `m_<mod>/…` is not dispatched. |
| Host intercept points | `core/agent.create`, `.terminate`, `.cancel` and `.tree` have no producer. |
| `core/input.accepted` | No producer; input arrives through `agent_pending`. |
| Background children | `PendingKind::BackgroundResult` and `AgentHandle::deliver` have no producer. |
| `wind_spawn_agent.fork` | Parsed, ignored; children never inherit the parent context. |
| Event history | `event_log` is write-only; `Opening.cut` is the only stitch point. |
| Retention window | No capacity limit; it shrinks only on `close` or writer acknowledgement. |
| UI | Discovery and `ui_entry` only; loading and mounting belong to the shell. |
| Bytecode edits | Same version directory keeps serving the previously loaded module. |

---

## Documentation

The design documents are in Chinese and describe the implementation in more depth than this
overview.

| Document | Content |
| --- | --- |
| [`docs/architecture.md`](docs/architecture.md) | Crate boundaries, in-process topology, the two write paths, host assembly |
| [`docs/events.md`](docs/events.md) | Event identity, shapes, retention, dispatch, feed internals, front-end rules |
| [`docs/mod.md`](docs/mod.md) | Package layout, manifest, lifecycle, resources, subscriptions |
| [`docs/script.md`](docs/script.md) | QuickJS execution, module loading, bytecode cache, deadlines, breaker |
| [`docs/agent.md`](docs/agent.md) | Agent tree state machine, phases, cancel and resume |
| [`docs/llm.md`](docs/llm.md) | Rounds, streaming, blocks, tool catalog, tool execution |
| [`docs/ai.md`](docs/ai.md) | Provider adapters, request building, SSE parsing |
| [`docs/mcp.md`](docs/mcp.md) | MCP registry, connection planning, built-in services |
| [`docs/storage.md`](docs/storage.md) | Schema, transactions, per-domain statements |
| [`docs/rule.md`](docs/rule.md) | Request-body rule engine |
| [`docs/facade.d.ts`](docs/facade.d.ts) | What a mod script sees, and what is still missing |
| [`docs/manifest.schema.json`](docs/manifest.schema.json) | Generated manifest schema |

## License

MIT OR Apache-2.0.
