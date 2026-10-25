//! 提供商结构侧的声明式改写

use super::events::{CORE_RULE_APPLY, RuleApply};
use crate::error::{CoreError, Result};
use crate::models::AdapterType;
use crate::models::RuleRow;
use std::sync::Arc;
use wind_event::context::DispatchCx;
use wind_event::dispatch::Dispatcher;
use wind_rule::{EvalContext, RuleSet};

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

pub(crate) fn compile_rules(rows: &[RuleRow]) -> Result<RuleSet> {
    let mut rules = RuleSet::new();
    for row in rows {
        rules.append_rule_str(&row.json_rule).map_err(denied)?;
    }
    Ok(rules)
}

pub(crate) async fn intercept(
    bus: &Arc<Dispatcher>,
    cx: &DispatchCx,
    adapter: AdapterType,
    body: serde_json::Value,
) -> serde_json::Value {
    let payload = RuleApply { adapter, body };
    bus.decide(&CORE_RULE_APPLY, cx, payload).await.body
}

fn denied(error: wind_rule::Error) -> CoreError {
    CoreError::Denied(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::events::directory;
    use std::result::Result as StdResult;
    use std::time::Duration;
    use wind_event::dispatch::Limits;
    use wind_event::envelope::{Origin, Scope, Source};
    use wind_event::host::{CallCx, HostError, ModEnablement, ModHost};
    use wind_event::subscription::{EnabledSet, Phase as SubPhase, Subscription};

    #[test]
    fn the_context_carries_the_provider_and_the_model() {
        let context = context_of("deepseek", "deepseek-chat");
        // 规则里写 $ctx.provider 与 $ctx.model
        assert!(context.get("provider").is_some());
        assert!(context.get("model").is_some());
    }

    #[test]
    fn a_rule_rewrites_the_provider_side_body() {
        let rules = RuleSet::from_json(
            r#"{"rules":[{"type":"set","path":"thinking","value":{"type":"enabled"}}]}"#,
        )
        .expect("compile");
        let mut body = serde_json::json!({ "model": "deepseek-chat" });
        apply_rules(&rules, &mut body, &context_of("deepseek", "deepseek-chat")).expect("evaluate");
        assert_eq!(body["thinking"]["type"], serde_json::json!("enabled"));
    }

    #[test]
    fn rules_that_do_not_apply_leave_the_body_untouched() {
        let rules = RuleSet::from_json(
            r#"{"rules":[{"type":"when","cond":{"eq":["$ctx.provider","openai"]},
                 "then":[{"type":"set","path":"x","value":1}]}]}"#,
        )
        .expect("compile");
        let mut body = serde_json::json!({ "model": "m" });
        let before = body.clone();
        apply_rules(&rules, &mut body, &context_of("deepseek", "deepseek-chat")).expect("evaluate");
        assert_eq!(
            body, before,
            "not a single field changes when the condition does not hold"
        );
    }

    #[test]
    fn a_broken_rule_is_reported_rather_than_skipped() {
        // 跳过它会让一个改了但没生效的规则永远不被发现
        let error = RuleSet::from_json(r#"{"rules":[{"type":"nonsense"}]}"#)
            .expect_err("reject an unknown rule type");
        assert!(error.to_string().contains("nonsense"), "{error}");
    }

    #[test]
    fn rules_from_several_sources_are_merged_into_one_set() {
        // 同一个提供商与适配器上的规则分几行存放，合并后每一条都要生效
        let rows = vec![
            RuleRow {
                id: 1,
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule:
                    r#"{"rules":[{"type":"set","path":"thinking","value":{"type":"enabled"}}]}"#
                        .into(),
            },
            RuleRow {
                id: 2,
                provider_id: 1,
                adapter: AdapterType::OpenAICompletion,
                json_rule: r#"{"rules":[{"type":"set","path":"stream","value":true}]}"#.into(),
            },
        ];
        let rules = compile_rules(&rows).expect("compile");
        let mut body = serde_json::json!({ "model": "m" });
        apply_rules(&rules, &mut body, &context_of("deepseek", "deepseek-chat")).expect("evaluate");
        assert_eq!(body["thinking"]["type"], serde_json::json!("enabled"));
        assert_eq!(body["stream"], serde_json::json!(true));
    }

    /// 一个按固定结果作答的宿主替身
    struct StanceHost;

    #[async_trait::async_trait]
    impl ModHost for StanceHost {
        async fn call(
            &self,
            _sub: &Subscription,
            mut payload: serde_json::Value,
            _cx: CallCx,
        ) -> StdResult<Option<serde_json::Value>, HostError> {
            payload["body"]["seen_adapter"] = payload["adapter"].clone();
            payload["body"]["thinking"]["type"] = serde_json::json!("enabled");
            Ok(Some(payload))
        }
    }

    /// 全部订阅者都启用
    struct AllEnabled;

    impl ModEnablement for AllEnabled {
        fn enabled(&self, _scope: &Scope) -> EnabledSet {
            EnabledSet::all()
        }
    }

    /// 组装一条本事件的拦截订阅
    fn subscription() -> Subscription {
        Subscription {
            mod_name: "rule".into(),
            mod_version: 1,
            event: CORE_RULE_APPLY.name,
            phase: Some(SubPhase::Guard),
            handler: "onEvent".into(),
            timeout_ms: 1000,
            registration_seq: 1,
        }
    }

    /// 组装一个对本事件有订阅者的事件总线
    fn bus_with(host: Arc<dyn ModHost>) -> Arc<Dispatcher> {
        let dispatcher =
            Dispatcher::new(directory(), Arc::new(AllEnabled), host, Limits::default());
        dispatcher.rebuild_index(vec![subscription()]);
        Arc::new(dispatcher)
    }

    /// 顶层操作的上下文
    fn cx() -> DispatchCx {
        DispatchCx::root(
            Origin::Agent,
            Scope::Topic { topic_id: 1 },
            Source::core("llm"),
            Duration::from_secs(5),
        )
    }

    #[tokio::test]
    async fn a_subscribed_mod_can_rewrite_the_provider_side_body() {
        let bus = bus_with(Arc::new(StanceHost));
        let body = intercept(
            &bus,
            &cx(),
            AdapterType::OpenAICompletion,
            serde_json::json!({ "model": "m" }),
        )
        .await;
        assert_eq!(body["thinking"]["type"], serde_json::json!("enabled"));
        assert_eq!(body["seen_adapter"], serde_json::json!("openai_completion"));
    }

    #[tokio::test]
    async fn a_body_nobody_subscribes_to_comes_back_unchanged() {
        let bus = Arc::new(Dispatcher::new(
            directory(),
            Arc::new(AllEnabled),
            Arc::new(StanceHost),
            Limits::default(),
        ));
        let original = serde_json::json!({ "model": "m" });
        let body = intercept(&bus, &cx(), AdapterType::OpenAICompletion, original.clone()).await;
        assert_eq!(
            body, original,
            "not a single field changes when nobody subscribes"
        );
    }
}
