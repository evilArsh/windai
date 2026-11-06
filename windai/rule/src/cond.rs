use super::EvalContext;
use super::error::{Error, Result};
use super::path;
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum Arg {
    Literal(Value),
    Variable(String),
}

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
    Exists(Vec<String>),
    And(Vec<CompiledCond>),
    Or(Vec<CompiledCond>),
    Not(Box<CompiledCond>),
}

impl CompiledCond {
    pub fn compile(raw: &Value) -> Result<Self> {
        let obj = raw
            .as_object()
            .ok_or_else(|| Error::Condition("condition must be an object".into()))?;
        if obj.len() != 1 {
            return Err(Error::Condition(format!(
                "condition object must have exactly one key, got {}: {:?}",
                obj.len(),
                obj.keys().collect::<Vec<_>>()
            )));
        }
        let (op, args) = obj
            .iter()
            .next()
            .ok_or_else(|| Error::Condition("condition object must have exactly one key".into()))?;
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
                    .ok_or_else(|| Error::Condition("exists requires a string path".into()))?;
                let segs = path::segments(path_str);
                Ok(CompiledCond::Exists(segs))
            }
            "and" => {
                let arr = args.as_array().ok_or_else(|| {
                    Error::Condition("and requires an array of conditions".into())
                })?;
                let subs = arr
                    .iter()
                    .map(CompiledCond::compile)
                    .collect::<Result<Vec<_>>>()?;
                Ok(CompiledCond::And(subs))
            }
            "or" => {
                let arr = args
                    .as_array()
                    .ok_or_else(|| Error::Condition("or requires an array of conditions".into()))?;
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
            _ => Err(Error::Condition(format!(
                "unknown condition operator: {op}"
            ))),
        }
    }

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
            CompiledCond::Exists(segs) => Ok(match path::get(body, segs) {
                Ok(v) => v.is_some(),
                Err(_) => false,
            }),
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
        return Err(Error::Condition(format!(
            "eq/neq requires exactly 2 arguments, got {}",
            arr.len()
        )));
    }
    Ok((parse_arg(&arr[0]), parse_arg(&arr[1])))
}

fn parse_arg(val: &Value) -> Arg {
    if let Some(s) = val.as_str()
        && s.starts_with('$')
    {
        return Arg::Variable(s.to_string());
    }
    Arg::Literal(val.clone())
}

fn resolve_arg(arg: &Arg, body: &Value, ctx: &EvalContext) -> Value {
    match arg {
        Arg::Literal(v) => v.clone(),
        Arg::Variable(name) => {
            if name == "$value" {
                return body.clone();
            }
            if let Some(rest) = name.strip_prefix("$value.")
                && let Ok(Some(value)) = path::get(body, &path::segments(rest))
            {
                return value.clone();
            }
            if let Some(rest) = name.strip_prefix("$ctx.")
                && let Some(v) = ctx.get(rest)
            {
                return v.clone();
            }
            Value::Null
        }
    }
}
