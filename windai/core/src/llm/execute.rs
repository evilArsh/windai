//! 一批调用的执行

use super::policy::CallPlan;
use super::ports::{FunctionCall, FunctionCallOutput, McpTools, ModRequests, TopicMods};
use super::tools::{ToolCatalog, ToolOrigin};
use crate::agent::tool::{SpawnAgentAction, parse_spawn_agents};
use crate::error::CoreError;
use wind_event::context::DispatchCx;

#[derive(Debug)]
pub struct ExecutedPlan {
    pub results: Vec<FunctionCallOutput>,
    pub spawns: Vec<SpawnAgentAction>,
}

pub async fn execute_plan(
    plan: CallPlan,
    catalog: &ToolCatalog,
    cx: &DispatchCx,
    mcp: Option<&dyn McpTools>,
    requests: &dyn ModRequests,
    mods: &dyn TopicMods,
) -> Result<ExecutedPlan, CoreError> {
    execute_each(plan.admitted, catalog, cx, mcp, requests, mods).await
}

async fn execute_each(
    calls: Vec<FunctionCall>,
    catalog: &ToolCatalog,
    cx: &DispatchCx,
    mcp: Option<&dyn McpTools>,
    requests: &dyn ModRequests,
    mods: &dyn TopicMods,
) -> Result<ExecutedPlan, CoreError> {
    let mut results = Vec::with_capacity(calls.len());
    let mut agent_calls = Vec::new();
    for call in calls {
        let Some(origin) = catalog.lookup(&call.name) else {
            results.push(unknown_result(&call.id, &call.name));
            continue;
        };
        match origin {
            ToolOrigin::Agent { .. } => agent_calls.push(call),
            ToolOrigin::Mcp { server, tool } => {
                results.push(call_mcp(cx, mcp, &call, server, tool).await);
            }
            ToolOrigin::ModRequest { mod_name, request } => {
                results.push(call_mod_request(cx, requests, mods, &call, mod_name, request).await?);
            }
        }
    }
    let spawns = parse_spawn_agents(&agent_calls)?;
    Ok(ExecutedPlan { results, spawns })
}

async fn call_mcp(
    cx: &DispatchCx,
    mcp: Option<&dyn McpTools>,
    call: &FunctionCall,
    server: &str,
    tool: &str,
) -> FunctionCallOutput {
    let Some(mcp) = mcp else {
        return unknown_result(&call.id, &call.name);
    };
    let arguments = parse_arguments(&call.arguments);
    match mcp.call(cx, &call.id, server, tool, arguments).await {
        Ok(value) => done_result(&call.id, value),
        Err(error) => unknown_result(&call.id, &error.to_string()),
    }
}

async fn call_mod_request(
    cx: &DispatchCx,
    requests: &dyn ModRequests,
    mods: &dyn TopicMods,
    call: &FunctionCall,
    mod_name: &str,
    request: &str,
) -> Result<FunctionCallOutput, CoreError> {
    let version = mods.version_of(mod_name).await?;
    let input = parse_arguments(&call.arguments);
    match requests
        .call(&cx.scope, mod_name, version, request, input)
        .await
    {
        Ok(value) => Ok(done_result(&call.id, value)),
        Err(error) => Ok(denied_result(&call.id, &error.to_string())),
    }
}

fn parse_arguments(arguments: &str) -> serde_json::Value {
    match serde_json::from_str(arguments) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("cannot parse tool arguments: {error}");
            serde_json::json!({})
        }
    }
}

pub(crate) fn denied_result(call_id: &str, reason: &str) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: serde_json::json!({ "refused": true, "reason": reason }),
    }
}

pub(crate) fn done_result(call_id: &str, value: serde_json::Value) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: value,
    }
}

pub(crate) fn unknown_result(call_id: &str, name: &str) -> FunctionCallOutput {
    FunctionCallOutput {
        id: call_id.to_string(),
        content: serde_json::json!({ "error": format!("unknown tool {name}") }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::ports::{DeclaredTool, FunctionTool, Tools};
    use std::time::Duration;
    use wind_event::envelope::{Origin, Scope, Source};

    #[test]
    fn a_denied_call_becomes_a_result_the_model_can_read() {
        let result = denied_result("c-1", "example.com is not allowed");
        assert_eq!(result.id, "c-1");
        assert_eq!(
            result.content,
            serde_json::json!({ "refused": true, "reason": "example.com is not allowed" })
        );
    }

    #[test]
    fn the_result_of_a_successful_call_is_the_tools_value() {
        let result = done_result("c-1", serde_json::json!({ "temp": 22 }));
        assert_eq!(result.content, serde_json::json!({ "temp": 22 }));
    }

    #[test]
    fn a_tool_that_is_not_in_the_catalog_produces_an_error_result() {
        // 模型可能给出一个没声明过的工具名。那是一次失败的调用，不是一次崩溃
        let result = unknown_result("c-1", "no_such_tool");
        assert_eq!(
            result.content["error"],
            serde_json::json!("unknown tool no_such_tool")
        );
    }

    #[tokio::test]
    async fn an_agent_tool_becomes_a_spawn_rather_than_a_result() {
        // agent 工具的结论要等子实例，本轮不能给它编一个 tool_result
        let plan = CallPlan {
            admitted: vec![FunctionCall {
                id: "c-1".into(),
                name: "windai_spawn_agent".into(),
                arguments: r#"{"mode":"sync","task":"weather"}"#.into(),
            }],
            pending: Vec::new(),
        };
        let executed = execute_plan(
            plan,
            &ToolCatalog::assemble(Vec::new(), Vec::new()),
            &cx(),
            None,
            &FakeRequests,
            &FakeMods,
        )
        .await
        .expect("execute");

        assert!(
            executed.results.is_empty(),
            "agent tools do not produce a tool_result"
        );
        assert_eq!(executed.spawns.len(), 1);
        assert_eq!(executed.spawns[0].call_id, "c-1");
        assert_eq!(executed.spawns[0].data.mode, "sync");
        assert_eq!(executed.spawns[0].data.task, "weather");
    }

    #[tokio::test]
    async fn the_results_keep_the_input_order() {
        // 模型按这个顺序读结果，打乱会让它把结果配错调用
        let plan = CallPlan {
            admitted: vec![call("c-1", "m_research__fetch"), call("c-2", "fs__read")],
            pending: Vec::new(),
        };
        let executed = execute_plan(
            plan,
            &catalog(),
            &cx(),
            Some(&FakeMcp),
            &FakeRequests,
            &FakeMods,
        )
        .await
        .expect("execute");
        let ids: Vec<&str> = executed.results.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["c-1", "c-2"]);
    }

    #[tokio::test]
    async fn a_mod_request_asks_the_mod_seam_for_the_version() {
        // 版本必须由 mod 接口给出：执行这段计划时不去查数据库
        let plan = CallPlan {
            admitted: vec![call("c-1", "m_research__fetch")],
            pending: Vec::new(),
        };
        let executed = execute_plan(plan, &catalog(), &cx(), None, &FakeRequests, &FakeMods)
            .await
            .expect("execute");
        assert_eq!(
            executed.results[0].content,
            serde_json::json!({ "version": 7 })
        );
    }

    #[tokio::test]
    async fn a_call_that_cannot_be_executed_becomes_an_error_result() {
        // 清单里查不到的名字与没有 MCP 接口可走的调用都是一次失败的调用
        let plan = CallPlan {
            admitted: vec![call("c-1", "no_such_tool"), call("c-2", "fs__read")],
            pending: Vec::new(),
        };
        let executed = execute_plan(plan, &catalog(), &cx(), None, &FakeRequests, &FakeMods)
            .await
            .expect("execute");
        assert_eq!(
            executed.results.len(),
            2,
            "each of the two calls gets its own result"
        );
        assert_eq!(
            executed.results[0].content["error"],
            serde_json::json!("unknown tool no_such_tool")
        );
        assert_eq!(
            executed.results[1].content["error"],
            serde_json::json!("unknown tool fs__read")
        );
    }

    #[tokio::test]
    async fn a_mixed_batch_runs_the_other_calls_before_folding_the_spawn() {
        // 同一轮里既有 agent 工具又有其他调用时，先执行其他调用，最后把 agent 调用
        // 折成派生请求
        let plan = CallPlan {
            admitted: vec![
                FunctionCall {
                    id: "c-1".into(),
                    name: "windai_spawn_agent".into(),
                    arguments: r#"{"mode":"background","task":"write a draft"}"#.into(),
                },
                call("c-2", "fs__read"),
                call("c-3", "m_research__fetch"),
            ],
            pending: Vec::new(),
        };
        let executed = execute_plan(
            plan,
            &catalog(),
            &cx(),
            Some(&FakeMcp),
            &FakeRequests,
            &FakeMods,
        )
        .await
        .expect("execute");
        let ids: Vec<&str> = executed.results.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["c-2", "c-3"],
            "the result of an agent call comes from the child instance"
        );
        assert_eq!(
            executed.results.len(),
            2,
            "none of the other calls is missing"
        );
        assert_eq!(executed.spawns.len(), 1);
        assert_eq!(executed.spawns[0].call_id, "c-1");
    }

    #[tokio::test]
    async fn truncated_arguments_become_an_empty_object_without_stalling_the_batch() {
        // 解不开的原文是一次失败的调用，不该让同一批里别的调用停摆
        let plan = CallPlan {
            admitted: vec![
                FunctionCall {
                    id: "c-1".into(),
                    name: "fs__echo".into(),
                    arguments: r#"{"path":"/tmp"#.into(),
                },
                call("c-2", "fs__read"),
            ],
            pending: Vec::new(),
        };
        let executed = execute_plan(
            plan,
            &catalog(),
            &cx(),
            Some(&FakeMcp),
            &FakeRequests,
            &FakeMods,
        )
        .await
        .expect("execute");
        assert_eq!(
            executed.results[0].content,
            serde_json::json!({}),
            "an unparseable source is returned as an empty object"
        );
        assert_eq!(executed.results.len(), 2, "the batch is not interrupted");
        assert_eq!(executed.results[1].id, "c-2");
    }

    /// 一次调用
    fn call(id: &str, name: &str) -> FunctionCall {
        FunctionCall {
            id: id.into(),
            name: name.into(),
            arguments: "{}".into(),
        }
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

    /// 构造一份工具声明
    fn declared(origin: ToolOrigin, exposed: &str) -> DeclaredTool {
        DeclaredTool {
            origin,
            declaration: Tools::Function(FunctionTool {
                name: exposed.to_string(),
                description: None,
                parameters: None,
                strict: None,
            }),
        }
    }

    /// 一份含一个 mod request 与四个 MCP 工具的清单
    fn catalog() -> ToolCatalog {
        let from_mods = vec![declared(
            ToolOrigin::ModRequest {
                mod_name: "research".into(),
                request: "fetch".into(),
            },
            "m_research__fetch",
        )];
        let from_mcp = vec![
            declared(
                ToolOrigin::Mcp {
                    server: "fs".into(),
                    tool: "read".into(),
                },
                "fs__read",
            ),
            declared(
                ToolOrigin::Mcp {
                    server: "fs".into(),
                    tool: "blocked".into(),
                },
                "fs__blocked",
            ),
            declared(
                ToolOrigin::Mcp {
                    server: "fs".into(),
                    tool: "secret".into(),
                },
                "fs__secret",
            ),
            declared(
                ToolOrigin::Mcp {
                    server: "fs".into(),
                    tool: "echo".into(),
                },
                "fs__echo",
            ),
        ];
        ToolCatalog::assemble(from_mods, from_mcp)
    }

    /// 一个按工具名给出不同去向的 MCP 替身
    struct FakeMcp;

    #[async_trait::async_trait]
    impl McpTools for FakeMcp {
        async fn declarations(
            &self,
            _cx: &DispatchCx,
            _topic_id: i64,
        ) -> Result<Vec<DeclaredTool>, CoreError> {
            Ok(Vec::new())
        }

        async fn call(
            &self,
            _cx: &DispatchCx,
            _call_id: &str,
            _server: &str,
            tool: &str,
            arguments: serde_json::Value,
        ) -> Result<serde_json::Value, CoreError> {
            Ok(match tool {
                // 回显收到的参数，供参数解不开的用例观察本层交出了什么
                "echo" => arguments,
                _ => serde_json::json!({ "read": "x" }),
            })
        }
    }

    /// 一个交出本轮版本的 mod 替身
    struct FakeMods;

    #[async_trait::async_trait]
    impl TopicMods for FakeMods {
        async fn prompt_fragments(&self, _topic_id: i64) -> Result<Vec<String>, CoreError> {
            Ok(Vec::new())
        }

        async fn agent_tools(&self, _topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError> {
            Ok(Vec::new())
        }

        async fn version_of(&self, _mod_name: &str) -> Result<i64, CoreError> {
            Ok(7)
        }
    }

    /// 一个把版本带回来的 request 执行替身
    struct FakeRequests;

    #[async_trait::async_trait]
    impl ModRequests for FakeRequests {
        async fn call(
            &self,
            _scope: &Scope,
            _mod_name: &str,
            version: i64,
            _request: &str,
            _input: serde_json::Value,
        ) -> Result<serde_json::Value, CoreError> {
            Ok(serde_json::json!({ "version": version }))
        }
    }
}
