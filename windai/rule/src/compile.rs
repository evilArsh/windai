use super::{
    EvalContext,
    cond::CompiledCond,
    error::{Error, Result},
    path,
};
use evalexpr::{self, ContextWithMutableVariables, HashMapContext, Node};
use serde::Deserialize;
use serde_json::Value;

/// 规则指令集
#[derive(Debug, Clone)]
pub struct RuleSet {
    /// 编译后的指令
    ops: Vec<CompiledOp>,
}

impl RuleSet {
    fn parse(json_str: &str) -> Result<Vec<CompiledOp>> {
        let raw: RawRuleSet = serde_json::from_str(json_str)
            .map_err(|e| Error::InvalidRule(format!("failed to parse rule JSON: {e}")))?;
        let mut ops = Vec::new();
        for raw_op in raw.rules {
            compile_op(raw_op, &mut ops)?;
        }
        Ok(ops)
    }
    pub fn new() -> Self {
        Self { ops: vec![] }
    }
    pub fn from_json(json: &str) -> Result<Self> {
        Ok(Self {
            ops: Self::parse(json)?,
        })
    }
    pub fn append_rule_str(&mut self, json: &str) -> Result<()> {
        self.ops.append(&mut Self::parse(json)?);
        Ok(())
    }

    /// 基于当前规则指令集修改 `body` 中的字段和值
    ///
    /// `ctx` 为规则执行时注入的上下文
    pub fn apply(&self, body: &mut Value, ctx: &EvalContext) -> Result<()> {
        if self.ops.is_empty() {
            return Ok(());
        }
        for op in &self.ops {
            op.exec(body, ctx)?;
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.ops.clear();
    }
}

#[derive(Deserialize)]
struct RawRuleSet {
    rules: Vec<RawOp>,
}

/// 原始规则集
///
/// # examples
/// ```json
/// [
///   {"type": "set", "path": "x", "value": 1},
///   {"type": "remove", "path": "x"},
///   {
///       "type": "map_value",
///       "path": "reasoning_effort",
///       "mappings": {
///           "medium": {"thinking": {"type": "enabled"}},
///           "high": {"thinking": {"type": "enabled"}}
///       },
///       "default": {"thinking": {"type": "disabled"}},
///       "remove_source": true
///   },
///   {"type": "compute", "path": "max_tokens", "expr": "min($value, 4096)"},
///   {
///     "type": "when",
///     "cond": {
///         "eq": ["$ctx.provider", "deepseek"]
///     },
///     "then": [
///       {
///         "type": "map_value",
///         "path": "reasoning_effort",
///         "mappings": {
///           "medium": { "thinking": { "type": "enabled" } },
///           "high": { "thinking": { "type": "enabled" } }
///         },
///         "default": { "thinking": { "type": "disabled" } },
///         "remove_source": true
///       }
///     ],
///     "else": [{ "type": "remove", "path": "reasoning_effort" }]
///   }
/// ]
///  ```
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RawOp {
    Set {
        path: String,
        value: Value,
    },
    Remove {
        path: String,
    },
    MapValue {
        path: String,
        mappings: Value,
        #[serde(default)]
        default: Option<Value>,
        #[serde(default)]
        remove_source: bool,
    },
    Compute {
        path: String,
        expr: String,
    },
    When {
        cond: Value,
        then: Vec<RawOp>,
        #[serde(rename = "else", default)]
        else_: Option<Vec<RawOp>>,
    },
}

#[derive(Debug, Clone)]
enum CompiledOp {
    Set {
        segs: Vec<String>,
        value: Value,
    },
    Remove {
        segs: Vec<String>,
    },
    MapValue {
        segs: Vec<String>,
        mappings: Vec<(Value, Value)>,
        default: Option<Value>,
        remove_source: bool,
    },
    Compute {
        segs: Vec<String>,
        op_tree: evalexpr::Node,
    },
    When {
        cond: CompiledCond,
        then: Vec<CompiledOp>,
        else_: Vec<CompiledOp>,
    },
}

impl CompiledOp {
    /// 执行操作
    ///
    /// 1. [CompiledOp::Set]
    ///
    /// 如果路径不存在，将会在 body 上创建对象并设置值,
    /// 如果子路径存在非 Object 值，则此次操作将失败。
    /// 比如在以下 body 中设置路径 `foo.bar.zoo` 的值会失败：
    /// ```json
    /// {
    ///   "foo":: {
    ///     "bar": 1
    ///   }
    /// }
    ///
    /// ```
    ///
    /// 2. [CompiledOp::Remove]
    ///
    /// 如果路径不存在或子路径存在非 Object 值，则失败
    ///
    /// 3. [CompiledOp::MapValue]
    ///
    /// 根据指定字段值的条件，映射出新的字段值。
    ///
    /// 如果指定的字段值不存在，则跳过
    ///
    /// 4. [CompiledOp::Compute]
    ///
    /// 对一个指定的路径求值。
    ///
    /// 内建函数参考： [https://crates.io/crates/evalexpr]
    /// 5. [CompiledOp::When]
    ///
    /// 根据设定的条件设置字段值
    fn exec(&self, body: &mut Value, ctx: &super::EvalContext) -> Result<()> {
        match self {
            CompiledOp::Set { segs, value } => {
                let dst = path::walk(body, segs)?;
                *dst = value.clone();
                Ok(())
            }
            CompiledOp::Remove { segs } => path::remove(body, segs),
            CompiledOp::MapValue {
                segs,
                mappings,
                default,
                remove_source,
            } => {
                let source_val = path::get(body, segs)?;
                let target = find_mapping(mappings, source_val, default.as_ref());
                if *remove_source {
                    log::debug!("[MapValue] remove source");
                    path::remove(body, segs)?;
                }
                if let Some(target) = target {
                    if let Value::Object(map) = target
                        && let Value::Object(body) = body
                    {
                        merge_root(body, map);
                        Ok(())
                    } else {
                        Err(Error::Path(format!(
                            "target value or body is not an object"
                        )))
                    }
                } else {
                    log::warn!("[MapValue] no target value found");
                    Ok(())
                }
            }
            CompiledOp::Compute { segs, op_tree } => {
                let current = Some(path::get(body, segs)?);
                let result = eval_compute(op_tree, &current, ctx)?;
                let dst = path::walk(body, segs)?;
                *dst = result;
                Ok(())
            }
            CompiledOp::When { cond, then, else_ } => {
                if cond.eval(body, ctx)? {
                    for op in then {
                        op.exec(body, ctx)?;
                    }
                    Ok(())
                } else {
                    for op in else_ {
                        op.exec(body, ctx)?;
                    }
                    Ok(())
                }
            }
        }
    }
}

fn compile_op(raw: RawOp, out: &mut Vec<CompiledOp>) -> Result<()> {
    match raw {
        RawOp::Set { path, value } => {
            let segs = path::segments(&path);
            out.push(CompiledOp::Set { segs, value });
        }
        RawOp::Remove { path } => {
            let segs = path::segments(&path);
            out.push(CompiledOp::Remove { segs });
        }
        RawOp::MapValue {
            path,
            mappings,
            default,
            remove_source,
        } => {
            let segs = path::segments(&path);
            let mappings = compile_mappings(&mappings)?;
            out.push(CompiledOp::MapValue {
                segs,
                mappings,
                default,
                remove_source,
            });
        }
        RawOp::Compute { path, expr } => {
            let segs = path::segments(&path);
            // 构建运算树
            let op_tree = evalexpr::build_operator_tree(&expr)?;
            out.push(CompiledOp::Compute { segs, op_tree });
        }
        RawOp::When { cond, then, else_ } => {
            let cond = CompiledCond::compile(&cond)?;
            let mut then_ops = Vec::new();
            for t in then {
                compile_op(t, &mut then_ops)?;
            }
            let mut else_ops = Vec::new();
            if let Some(el) = else_ {
                for e in el {
                    compile_op(e, &mut else_ops)?;
                }
            }
            out.push(CompiledOp::When {
                cond,
                then: then_ops,
                else_: else_ops,
            });
        }
    }
    Ok(())
}

/// 解析映射对象集合
/// - key 为 `"null"` 时解析成 `Value::Null`
/// - key 为纯数字的字符串时解析成 `Value:Number`
/// - 当 key 解析失败时，回退回字符串 `Value:String`
fn compile_mappings(raw: &Value) -> Result<Vec<(Value, Value)>> {
    let obj = raw.as_object().ok_or_else(|| {
        Error::InvalidRule("mappings must be an object of source_value → target_value".into())
    })?;
    let mut result = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        let key = if k == "null" {
            Value::Null
        } else {
            serde_json::from_str(k).unwrap_or_else(|_| Value::String(k.clone()))
        };
        result.push((key, v.clone()));
    }
    Ok(result)
}

fn find_mapping<'a>(
    mappings: &'a [(Value, Value)],
    source: &Value,
    default: Option<&'a Value>,
) -> Option<&'a Value> {
    for (key, target) in mappings {
        if key == source {
            return Some(target);
        }
    }
    default
}

fn eval_compute(
    op_tree: &Node,
    current_value: &Option<&Value>,
    ctx: &EvalContext,
) -> Result<Value> {
    let mut eval_ctx = HashMapContext::new();
    // 注入当前字段值
    let _ = eval_ctx.set_value("$value".to_string(), json_to_evalexpr(current_value));
    // 注入上下文变量。TODO: 支持嵌套上下文路径
    for (k, v) in ctx.entries() {
        let _ = eval_ctx.set_value(format!("$ctx.{k}"), json_to_evalexpr(&Some(v)));
    }
    let result = op_tree.eval_with_context_mut(&mut eval_ctx)?;
    Ok(evalexpr_to_json(result))
}

fn json_to_evalexpr(v: &Option<&Value>) -> evalexpr::Value {
    if let Some(v) = v {
        match v {
            Value::Bool(b) => evalexpr::Value::Boolean(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    evalexpr::Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    evalexpr::Value::Float(f)
                } else {
                    evalexpr::Value::Empty
                }
            }
            Value::String(s) => evalexpr::Value::String(s.clone()),
            Value::Array(arr) => {
                let items: Vec<evalexpr::Value> =
                    arr.iter().map(|v| json_to_evalexpr(&Some(v))).collect();
                evalexpr::Value::Tuple(items)
            }
            Value::Object(_) | Value::Null => evalexpr::Value::Empty,
        }
    } else {
        evalexpr::Value::Empty
    }
}

fn evalexpr_to_json(v: evalexpr::Value) -> Value {
    match v {
        evalexpr::Value::Int(i) => Value::Number(i.into()),
        evalexpr::Value::Float(f) => {
            let n = serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into());
            Value::Number(n)
        }
        evalexpr::Value::String(s) => Value::String(s),
        evalexpr::Value::Boolean(b) => Value::Bool(b),
        evalexpr::Value::Empty => Value::Null,
        evalexpr::Value::Tuple(items) => {
            Value::Array(items.into_iter().map(evalexpr_to_json).collect())
        }
    }
}

/// 将 map_value 产生的目标对象合并到 body 根级别。
/// TODO: 考虑以下情况
///
/// ```json
/// body: {"thinking": {"type": "enabled"}},
/// map : {"thinking": {"type": "disabled", foo: "bar"}}
/// ```
///
/// 合并后结果为
///
/// ```json
/// {"thinking": {"type": "disabled", foo: "bar"}}
/// ```
///
/// 可以考虑是否 skip 已有字段，skip后
///
/// ```json
/// {"thinking": {"type": "enabled",foo: "bar"}}
/// ```
///
fn merge_root(body: &mut serde_json::Map<String, Value>, map: &serde_json::Map<String, Value>) {
    for (k, v) in map {
        match (body.get_mut(k), v) {
            (Some(Value::Object(existing)), Value::Object(incoming)) => {
                for (ik, iv) in incoming {
                    existing.insert(ik.clone(), iv.clone());
                }
            }
            (_, incoming) => {
                body.insert(k.clone(), incoming.clone());
            }
        }
    }
}
