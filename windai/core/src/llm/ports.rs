//! 请求链路的三个接口

pub use super::tools::ToolOrigin;
use crate::error::CoreError;
use wind_event::context::DispatchCx;
use wind_event::envelope::Scope;
pub use wind_wire::tool::{FunctionCall, FunctionCallOutput, FunctionTool, Tools};

#[async_trait::async_trait]
pub trait TopicMods: Send + Sync {
    async fn prompt_fragments(&self, topic_id: i64) -> Result<Vec<String>, CoreError>;

    async fn agent_tools(&self, topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError>;

    async fn version_of(&self, mod_name: &str) -> Result<i64, CoreError>;
}

#[async_trait::async_trait]
pub trait McpTools: Send + Sync {
    async fn declarations(
        &self,
        cx: &DispatchCx,
        topic_id: i64,
    ) -> Result<Vec<DeclaredTool>, CoreError>;

    async fn call(
        &self,
        cx: &DispatchCx,
        call_id: &str,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError>;
}

#[async_trait::async_trait]
pub trait ModRequests: Send + Sync {
    async fn call(
        &self,
        scope: &Scope,
        mod_name: &str,
        version: i64,
        request: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, CoreError>;
}

#[derive(Debug, Clone)]
pub struct DeclaredTool {
    pub origin: ToolOrigin,
    pub declaration: Tools,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use wind_event::context::DispatchCx;
    use wind_event::envelope::{Origin, Source};

    /// 一次最小派发所需的上下文
    fn todo_cx() -> DispatchCx {
        DispatchCx::root(
            Origin::Agent,
            Scope::Topic { topic_id: 1 },
            Source::core("test"),
            Duration::from_secs(1),
        )
    }

    /// 一个最小的 mod 侧实现
    struct FakeMods;

    #[async_trait::async_trait]
    impl TopicMods for FakeMods {
        async fn prompt_fragments(&self, _topic_id: i64) -> Result<Vec<String>, CoreError> {
            Ok(vec!["fragment".to_string()])
        }
        async fn agent_tools(&self, _topic_id: i64) -> Result<Vec<DeclaredTool>, CoreError> {
            Ok(vec![DeclaredTool {
                origin: ToolOrigin::ModRequest {
                    mod_name: "research".into(),
                    request: "fetch".into(),
                },
                declaration: Tools::Function(FunctionTool {
                    name: "m_research__fetch".into(),
                    description: None,
                    parameters: None,
                    strict: None,
                }),
            }])
        }
        async fn version_of(&self, _mod_name: &str) -> Result<i64, CoreError> {
            Ok(7)
        }
    }

    /// 一个最小的 MCP 侧实现
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
            call_id: &str,
            _server: &str,
            _tool: &str,
            _arguments: serde_json::Value,
        ) -> Result<serde_json::Value, CoreError> {
            Ok(serde_json::json!({ "id": call_id }))
        }
    }

    /// 一个最小的 request 执行实现
    struct FakeRequests;

    #[async_trait::async_trait]
    impl ModRequests for FakeRequests {
        async fn call(
            &self,
            _scope: &Scope,
            _mod_name: &str,
            _version: i64,
            _request: &str,
            input: serde_json::Value,
        ) -> Result<serde_json::Value, CoreError> {
            Ok(input)
        }
    }

    #[tokio::test]
    async fn the_three_seams_are_usable_as_trait_objects() {
        // 运行时的字段是 Arc<dyn …>，对象安全是这三个接口的硬要求
        let mods: Arc<dyn TopicMods> = Arc::new(FakeMods);
        let mcp: Arc<dyn McpTools> = Arc::new(FakeMcp);
        let requests: Arc<dyn ModRequests> = Arc::new(FakeRequests);
        assert_eq!(mods.agent_tools(1).await.expect("get the tools").len(), 1);
        assert!(mcp.declarations(&todo_cx(), 1).await.is_ok());
        assert!(
            requests
                .call(
                    &Scope::Topic { topic_id: 1 },
                    "research",
                    7,
                    "fetch",
                    serde_json::json!({ "q": "x" })
                )
                .await
                .is_ok()
        );
    }

    #[test]
    fn the_wire_types_are_reachable_from_this_module() {
        // 这几个名字刻意经 `super::*` 取得：删掉本模块的重导出，这条用例就编译不过
        let tool = Tools::Function(FunctionTool {
            name: "x".into(),
            description: None,
            parameters: None,
            strict: None,
        });
        let declared = DeclaredTool {
            origin: ToolOrigin::Agent { name: "x".into() },
            declaration: tool,
        };
        assert_eq!(declared.origin.exposed(), "x");
        let call = FunctionCall {
            id: "c-1".into(),
            name: "x".into(),
            arguments: "{}".into(),
        };
        let output = FunctionCallOutput {
            id: call.id.clone(),
            content: serde_json::json!({ "ok": true }),
        };
        assert_eq!(
            output.id, call.id,
            "the result is brought back under the id of its own call"
        );
    }
}
