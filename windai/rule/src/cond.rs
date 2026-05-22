use crate::error::{Error, Result};
use crate::path;
use serde_json::Value;

/// 条件参数：字面量或变量引用
#[derive(Debug, Clone)]
pub enum Arg {
    Literal(Value),
    Variable(String), // $xxx 去掉 $ 前缀
}

// OK
/// 编译后的条件
///
/// # examples
///
/// 1. [CompiledCond::Eq]
/// ```json
/// {
///  "eq": ["$ctx.provider", "deepseek"]
/// }
/// ```
///
/// 2. [CompiledCond::Neq]
/// ```json
/// {
///  "neq": ["$ctx.provider", "deepseek"]
/// }
/// ```
///
/// 3. [CompiledCond::Exists]
/// ```json
/// {
///  "exists": ["reasoning_effort"]
/// }
/// ```
///
/// 4. [CompiledCond::And]
/// ```json
/// {
///  "and": [
///    {"eq": ["$ctx.provider", "deepseek"]},
///    {"exists": ["reasoning_effort"]}
///  ]
/// }
/// ```
///
/// 5. [CompiledCond::Or]
/// ```json
/// {
///  "or": [
///    {"eq": ["$ctx.provider", "deepseek"]},
///    {"exists": ["reasoning_effort"]}
///  ]
/// }
/// ```
///
/// 6. [CompiledCond::Not]
/// ```json
/// {
///  "not": {"eq": ["$ctx.provider", "deepseek"]}
/// }
/// ```
#[derive(Debug, Clone)]
pub enum CompiledCond {
    Eq(Arg, Arg),
    Neq(Arg, Arg),
    // target 中的路径段
    Exists(Vec<String>),
    And(Vec<CompiledCond>),
    Or(Vec<CompiledCond>),
    Not(Box<CompiledCond>),
}

impl CompiledCond {
    // OK
    /// 从原始 JSON value 编译条件。
    ///
    /// JSON 格式: `{"op": args}`，如 `{"eq": ["$value", "deepseek"]}`
    /// 每个条件对象恰好有一个 key。
    pub fn compile(raw: &Value) -> Result<Self> {
        let obj = raw
            .as_object()
            .ok_or_else(|| Error::InvalidRule("condition must be an object".into()))?;
        if obj.len() != 1 {
            return Err(Error::InvalidRule(format!(
                "condition object must have exactly one key, got {}: {:?}",
                obj.len(),
                obj.keys().collect::<Vec<_>>()
            )));
        }
        let (op, args) = obj.iter().next().unwrap();
        match op.as_str() {
            "eq" => {
                let (a, b) = parse_two_args(args)?;
                Ok(CompiledCond::Eq(a, b))
            }
            "neq" => {
                let (a, b) = parse_two_args(args)?;
                Ok(CompiledCond::Neq(a, b))
            }
            "exists" => {
                let path_str = args
                    .as_str()
                    .ok_or_else(|| Error::InvalidRule("exists requires a string path".into()))?;
                let segs = path::segments(path_str);
                Ok(CompiledCond::Exists(segs))
            }
            "and" => {
                let arr = args.as_array().ok_or_else(|| {
                    Error::InvalidRule("and requires an array of conditions".into())
                })?;
                let subs = arr
                    .iter()
                    .map(CompiledCond::compile)
                    .collect::<Result<Vec<_>>>()?;
                Ok(CompiledCond::And(subs))
            }
            "or" => {
                let arr = args.as_array().ok_or_else(|| {
                    Error::InvalidRule("or requires an array of conditions".into())
                })?;
                let subs = arr
                    .iter()
                    .map(CompiledCond::compile)
                    .collect::<Result<Vec<_>>>()?;
                Ok(CompiledCond::Or(subs))
            }
            "not" => {
                let sub = CompiledCond::compile(args)?;
                Ok(CompiledCond::Not(Box::new(sub)))
            }
            _ => Err(Error::InvalidRule(format!(
                "unknown condition operator: {op}"
            ))),
        }
    }

    /// 对目标 body 和上下文求值
    pub fn eval(&self, body: &Value, ctx: &super::EvalContext) -> Result<bool> {
        match self {
            CompiledCond::Eq(a, b) => {
                let va = resolve_arg(a, body, ctx);
                let vb = resolve_arg(b, body, ctx);
                Ok(va == vb)
            }
            CompiledCond::Neq(a, b) => {
                let va = resolve_arg(a, body, ctx);
                let vb = resolve_arg(b, body, ctx);
                Ok(va != vb)
            }
            CompiledCond::Exists(segs) => Ok(path::get(body, segs).is_some()),
            CompiledCond::And(conds) => {
                for c in conds {
                    if !c.eval(body, ctx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            CompiledCond::Or(conds) => {
                for c in conds {
                    if c.eval(body, ctx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            CompiledCond::Not(cond) => cond.eval(body, ctx).map(|b| !b),
        }
    }
}

fn parse_two_args(args: &Value) -> Result<(Arg, Arg)> {
    let arr = args
        .as_array()
        .ok_or_else(|| Error::InvalidRule("eq/neq requires an array of two arguments".into()))?;
    if arr.len() != 2 {
        return Err(Error::InvalidRule(format!(
            "eq/neq requires exactly 2 arguments, got {}",
            arr.len()
        )));
    }
    Ok((parse_arg(&arr[0]), parse_arg(&arr[1])))
}

fn parse_arg(val: &Value) -> Arg {
    if let Some(s) = val.as_str() {
        if let Some(rest) = s.strip_prefix('$') {
            return Arg::Variable(rest.to_string());
        }
    }
    Arg::Literal(val.clone())
}

fn resolve_arg(arg: &Arg, body: &Value, ctx: &super::EvalContext) -> Value {
    match arg {
        Arg::Literal(v) => v.clone(),
        Arg::Variable(name) => {
            // 优先从 body 取值（用于 map_value 的 $value）
            if name == "value" {
                return body.clone();
            }
            // 其次从上下文取值
            if let Some(rest) = name.strip_prefix("ctx.") {
                if let Some(v) = ctx.get(rest) {
                    return v.clone();
                }
            }
            Value::Null
        }
    }
}
