use crate::error::{CoreError, Result};
use wind_rule::{EvalContext, RuleSet};
use wind_storage::Rule;

/// 注入 `$ctx` 上下文
pub(crate) fn context_of(provider: &str, model: &str) -> EvalContext {
    EvalContext::new()
        .with("provider", provider)
        .with("model", model)
}

pub(crate) fn apply_rules(
    rules: &RuleSet,
    body: &mut serde_json::Value,
    context: &EvalContext,
) -> Result<()> {
    rules.apply(body, context).map_err(denied)
}

pub(crate) fn compile_rules(rows: &[Rule]) -> Result<RuleSet> {
    let mut rules = RuleSet::new();
    for row in rows {
        rules.append_rule_str(&row.json_rule).map_err(denied)?;
    }
    Ok(rules)
}

fn denied(error: wind_rule::Error) -> CoreError {
    CoreError::Denied(error.to_string())
}
