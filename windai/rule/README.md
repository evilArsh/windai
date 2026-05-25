# wind-rule

声明式 JSON 规则引擎，将 JSON 规则编译为变换指令后按序执行。

## Usage

### 快速示例

```json
{
  "rules": [
    {
      "type": "when",
      "cond": { "eq": ["$ctx.provider", "deepseek"] },
      "then": [
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
      ],
      "else": [{ "type": "remove", "path": "reasoning_effort" }]
    },
    { "type": "compute", "path": "max_tokens", "expr": "min($value, 4096)" },
    { "type": "set", "path": "stream", "value": true }
  ]
}
```

### 支持的操作

| 操作        | 说明                                              |
| ----------- | ------------------------------------------------- |
| `set`       | 在任意嵌套路径创建/覆盖字段                       |
| `remove`    | 删除指定路径的字段                                |
| `map_value` | 根据字段值映射为目标对象结构，可选删除源字段      |
| `compute`   | 对字段求表达式并写回（算术/逻辑/字符串/内置函数） |
| `when`      | 条件分支，支持 then/else 子规则                   |

### 条件表达式

`eq` / `neq` / `exists` / `and` / `or` / `not`，可嵌套组合。

### 变量

- `$value`
  - 在`when`条件的`cond`中，代表当前请求 body
  - 在其它规则中代表当前路径对应的值
- 可注入`$ctx.xxxx` — 上下文变量

### 代码集成

```rust
use wind_rule::{EvalContext, RuleSet};

let mut rule = RuleSet::from_json(&json_str)?;
let ctx = EvalContext::new()
    .with("provider", "deepseek")
    .with("model", "deepseek-chat");
rule.apply(&mut request_body, &ctx)?;
```

## TODO

- [x] `set` / `remove` / `map_value` / `compute` / `when` 基础操作
- [x] 条件表达式（eq/neq/exists/and/or/not）
- [x] evalexpr 表达式计算
- [x] 上下文变量注入
- [ ] 条件增强：`gt`/`gte`/`lt`/`lte`、`contains`/`starts_with`/`ends_with`、`regex`、`is_null`/`is_string`/`is_number`
- [ ] 数组操作：`push`、`remove_at`、`filter`、`insert_at`
- [ ] 中间变量：`let` 操作定义临时变量
- [ ] 嵌套上下文路径解析（`$ctx.a.b.c`）
- [ ] 错误恢复：`try`/`catch` 或 `on_error: "skip"`
- [ ] Dry-run 模式（输出 diff 不实际修改）
- [ ] 规则历史与回滚
- [ ] 允许用户在 compute 中注册 Rust 函数
- [ ] `before` / `after` 时间判断

## 待完善

- **仅请求变换**
- **无数组操作** — 不能增删改查 JSON 数组元素
- **无状态** — 每次 `apply()` 是纯函数，不能跨请求计数、缓存或自适应
- **表达式弱类型** — Object 和 Null 在 evalexpr 中退化为 Empty，无法参与运算
- **无错误恢复** — 单步失败即整体失败，不支持跳过或 fallback
- **合并目标固定** — `map_value` 的映射结果只能合并到 body 根级别
- **工具定义变换** — 在 tools 数组发出前修改；tool result 回传前清洗
- **有状态规则** — 计数器、速率限制、滑动窗口
- **规则模板** — `$include` 引用公共规则片段
- **A/B 测试** — 按百分比/哈希分流应用不同规则
- **自定义函数** — 允许用户在 compute 中注册 Rust 函数
- **时间条件** — `before` / `after` 时间判断
- **多阶段管道** — `pre_request → post_request → pre_response → post_response`
- **规则版本化** — 规则历史与回滚
