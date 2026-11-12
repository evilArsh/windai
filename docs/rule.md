# 规则

`wind-rule` 是一个面向 JSON 请求体的规则引擎。它按顺序执行一组操作，用条件选择分支，
用表达式计算值。规则的存储与加载在 `wind-storage` 与 `wind-core`，本 crate 只负责编译与执行。

规则在 `wind-core` 的用途是修补提供商请求体：先由适配器构造请求，再套用该提供商 +
适配器的规则集，最后让 `core/rule.apply` 的 mod 改写链处理。请求构造见「AI」；mod 改写链
见「事件」。

## 规则集

```rust
pub struct RuleSet {
    ops: Vec<CompiledOp>,
}

impl RuleSet {
    pub fn new() -> Self;
    pub fn from_json(json: &str) -> Result<Self>;
    pub fn append_rule_str(&mut self, json: &str) -> Result<()>;
    pub fn apply(&self, body: &mut Value, ctx: &EvalContext) -> Result<()>;
    pub fn clear(&mut self);
}
```

输入 JSON 的形状固定为：

```json
{
  "rules": [
    { "type": "set", "path": "x", "value": 1 }
  ]
}
```

`from_json` 编译整个规则集；`append_rule_str` 把另一份规则集的操作追加到当前集合末尾，
保留原有顺序。`apply` 按顺序执行；规则集为空时直接返回。

## 操作

| `type` | 字段 | 语义 |
| --- | --- | --- |
| `set` | `path`、`value` | 把 `value` 写到 `path`；路径上缺失的对象会创建 |
| `remove` | `path` | 删除 `path`；叶子不存在时不报错 |
| `map_value` | `path`、`mappings`、`default?`、`remove_source?` | 读取 `path` 的值，在映射表中找目标对象，合并到请求体根部 |
| `compute` | `path`、`expr` | 用 `evalexpr` 计算表达式，结果写回 `path` |
| `for_each` | `path`、`then` | 遍历 `path` 指向的数组，对每个对象执行 `then` |
| `when` | `cond`、`then`、`else?` | 条件为真执行 `then`，否则执行 `else` |

### `set` 与 `remove`

```json
[
  { "type": "set", "path": "x", "value": 1 },
  { "type": "set", "path": "a.b", "value": "v" }
]
```

`set` 使用 `path::walk`：逐段进入对象，缺失段创建为空对象，最终段写入值。中间段不是对象时
返回 `Error::Path`。

`remove` 使用 `path::remove`：中间段必须存在且为对象；叶子不存在时静默成功。

### `map_value`

```json
{
  "type": "map_value",
  "path": "reasoning_effort",
  "mappings": {
    "medium": { "thinking": { "type": "enabled" } },
    "high": { "thinking": { "type": "enabled" } }
  },
  "default": { "thinking": { "type": "disabled" } },
  "remove_source": true
}
```

执行顺序：

1. 读取 `path`。中间段缺失或不是对象时报错；叶子缺失时值为 `None`。
2. 在 `mappings` 中按 JSON 值相等查找目标；没有命中时用 `default`。
3. `remove_source = true` 时删除 `path`。
4. 目标存在时，把目标对象合并到请求体根部；目标不存在时记录警告并成功返回。
5. 目标不是对象或请求体不是对象时返回 `Error::Path`。

`mappings` 的键按 JSON 字面量解析：`"null"` 转为 `null`，`"1"` 转为数字，`"true"` 转为
布尔；解析失败时按字符串处理。因此字符串键 `"medium"` 与值 `"medium"` 比较。

合并只发生在根部一层：

- 两边都是对象时，逐键覆盖请求体根对象的同名字段；内层对象整体替换，不再递归合并。
- 其余类型直接替换根对象的同名字段。

目标内容不会写回 `path`；`path` 只用于读取与可选的删除。

### `compute`

```json
{ "type": "compute", "path": "max_tokens", "expr": "min($value, 4096)" }
```

`expr` 由 `evalexpr` 编译为表达式树。执行时注入两个变量：

- `$value`：`path` 当前值。缺失或为对象/`null` 时是 `Empty`；数组转为 tuple；
  数字、字符串、布尔保持类型。
- `$ctx.<key>`：`EvalContext` 中的每个键。

结果写回 `path`：`Int`、`Float`、`String`、`Boolean`、`Tuple` 转成对应 JSON；`Empty` 转为
`null`。`$ctx` 只支持单层键，嵌套路径尚未支持。

### `for_each`

```json
{
  "type": "for_each",
  "path": "messages",
  "then": [
    {
      "type": "when",
      "cond": { "eq": ["$value.role", "assistant"] },
      "then": [{ "type": "remove", "path": "reasoning_content" }]
    }
  ]
}
```

`path` 必须是数组；最后一段缺失时静默返回；中间段缺失或不是对象时报错。路径存在但不是
数组时同样报错。只遍历数组中的对象元素，非对象元素跳过。
每个元素作为该次执行的请求体，因此 `$value` 指向元素本身。

### `when`

```json
{
  "type": "when",
  "cond": { "eq": ["$ctx.provider", "deepseek"] },
  "then": [
    { "type": "map_value", "path": "reasoning_effort", "mappings": {} }
  ],
  "else": [
    { "type": "remove", "path": "reasoning_effort" }
  ]
}
```

条件在当前的请求体上求值。`else` 可省略，省略时条件为假不执行任何操作。

## 条件

条件是一个只含一个键的对象。键是操作符，值是该操作符的参数：

| 操作符 | 参数 | 语义 |
| --- | --- | --- |
| `eq` | 两个值 | JSON 值相等 |
| `neq` | 两个值 | JSON 值不相等 |
| `exists` | 一个点分路径字符串 | 路径存在且不是中间段错误 |
| `and` | 条件数组 | 全部为真；空数组为真 |
| `or` | 条件数组 | 任一为真；空数组为假 |
| `not` | 一个条件 | 取反 |

值参数中的字符串以 `$` 开头时按变量解析：

| 写法 | 取值 |
| --- | --- |
| `$value` | 当前请求体 |
| `$value.a.b` | 当前请求体的 `a.b` 路径 |
| `$ctx.key` | `EvalContext` 中的 `key` |
| 其他字符串 | 字面量 |

变量解析失败时得到 `null`。因此 `eq` 与 `neq` 比较的是解析后的 JSON 值；`exists` 只接受
字符串路径。

条件对象必须恰好有一个键，否则返回 `Error::Condition`。`eq` / `neq` 必须恰好有两个参数，
`and` / `or` 必须是数组，`not` 的参数必须是条件对象。

## 求值上下文

```rust
pub struct EvalContext {
    vars: serde_json::Map<String, Value>,
}

impl EvalContext {
    pub fn new() -> Self;
    pub fn with(self, key: &str, value: impl Into<Value>) -> Self;
    pub fn get(&self, key: &str) -> Option<&Value>;
}
```

`with` 按字面量存入键；`get` 先按键的点分路径拆分，首段必须是已存的键，后续段在值内查找。
因此 `with("provider", ...)` 可以用 `$ctx.provider` 取到；键本身包含 `.` 时无法用同一个
`get` 取回。

`From<Value>` 把对象转为变量表；非对象转为 `{ "data": value }`。

`wind-core` 在应用规则时构造：

```rust
EvalContext::new()
    .with("provider", provider.name)
    .with("model", model.name)
```

## 在核心中的使用

`wind-core::llm::rule`：

```rust
pub(crate) fn compile_rules(rows: &[Rule]) -> Result<RuleSet>
pub(crate) fn context_of(provider: &str, model: &str) -> EvalContext
pub(crate) fn apply_rules(
    rules: &RuleSet,
    body: &mut serde_json::Value,
    context: &EvalContext,
) -> Result<()>
```

`compile_rules` 按 `list_rules(provider_id, adapter)` 的 `id` 升序追加每行的 `json_rule`。
`apply_rules` 在适配器构造请求体之后、`core/rule.apply` 的 mod 改写链之前执行。多个规则集
按行的插入顺序叠加，后一条可以覆盖前一条的结果。

规则错误在核心中映射为 `CoreError::Denied`，结束本轮运行。

## 当前边界

| 项 | 现状 |
| --- | --- |
| `$ctx` 嵌套路径 | 求值时只注入单层键；`EvalContext::get` 支持嵌套，但表达式侧不支持 |
| `map_value` 目标位置 | 只合并到请求体根部，不能写到 `path` 或指定目标路径 |
| 字面量 `$` | 以 `$` 开头的字符串一律按变量解析，没有转义写法 |
| `compute` 类型 | 对象与 `null` 注入为 `Empty`；数组注入为 tuple；没有对象构造 |
| 规则作用域 | 规则按提供商 + 适配器存储，不按话题或模型区分 |
