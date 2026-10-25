use crate::conversation::events::Usage;
use crate::models::AdapterType;
use serde::{Deserialize, Serialize};
use wind_event::def::{Directory, EventDef, Mount, Shape};
use wind_wire::message::Message;
use wind_wire::tool::{FunctionCall, Tools};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PromptCollect {
    /// 话题标识
    pub topic_id: i64,
    /// 实例标识
    pub instance_id: i64,
    /// 待拼装的全部片段，顺序即最终顺序
    pub fragments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RuleApply {
    /// 该请求用的是哪一种适配器
    pub adapter: AdapterType,
    /// 提供商结构侧的请求体
    pub body: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRequest {
    pub model_id: i64,
    pub request: LlmRequestBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRequestBody {
    pub contexts: Vec<Message>,
    pub tools: Option<Vec<Tools>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResponse {
    pub model_id: i64,
    pub message: Message,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCalls {
    pub instance_id: i64,
    pub calls: Vec<FunctionCall>,
    /// 需要转交用户答复的调用 id
    #[serde(default)]
    pub suspended: Vec<String>,
}

fn schema<P: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(P)).expect("the generated schema is valid JSON")
}

fn message_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "role",
            "content",
            "reasoning_content",
            "created_at",
            "input_tokens",
            "output_tokens",
            "tool_calls",
        ],
        "properties": {
            "role": { "enum": ["system", "user", "assistant", "tool", "developer"] },
            "content": { "type": "array", "items": content_schema() },
            "reasoning_content": { "type": ["string", "null"] },
            "created_at": { "type": "integer" },
            "input_tokens": { "type": "integer" },
            "output_tokens": { "type": "integer" },
            "tool_calls": { "type": ["array", "null"], "items": function_call_schema() },
        }
    })
}

fn content_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["type", "data"],
        "properties": {
            "type": { "enum": ["text", "image", "file", "audio", "function_call"] },
            "data": {},
        }
    })
}

fn function_call_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["id", "name", "arguments"],
        "properties": {
            "id": { "type": "string" },
            "name": { "type": "string" },
            "arguments": { "type": "string" },
        }
    })
}

fn tool_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string" },
            "description": { "type": "string" },
            "parameters": {},
            "strict": { "type": "boolean" },
        }
    })
}

fn llm_request_body_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["contexts", "tools"],
        "properties": {
            "contexts": { "type": "array", "items": message_schema() },
            "tools": { "type": ["array", "null"], "items": tool_schema() },
        }
    })
}

fn llm_request_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["model_id", "request"],
        "properties": {
            "model_id": { "type": "integer" },
            "request": llm_request_body_schema(),
        }
    })
}

fn llm_response_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["model_id", "message", "usage"],
        "properties": {
            "model_id": { "type": "integer" },
            "message": message_schema(),
            "usage": schema::<Usage>(),
        }
    })
}

fn tool_calls_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["instance_id", "calls"],
        "properties": {
            "instance_id": { "type": "integer" },
            "calls": { "type": "array", "items": function_call_schema() },
            "suspended": {
                "type": "array",
                "items": { "type": "string" },
                "description": "需要转交用户答复的调用 id"
            },
        }
    })
}

pub const CORE_PROMPT_COLLECT: EventDef<PromptCollect> = EventDef::new(
    "core/prompt.collect",
    Shape::Intercept,
    true,
    Mount::UseCase,
    schema::<PromptCollect>,
);

pub const CORE_RULE_APPLY: EventDef<RuleApply> = EventDef::new(
    "core/rule.apply",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    schema::<RuleApply>,
);

pub const CORE_LLM_REQUEST: EventDef<LlmRequest> = EventDef::new(
    "core/llm.request",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    llm_request_schema,
);

pub const CORE_LLM_RESPONSE: EventDef<LlmResponse> = EventDef::new(
    "core/llm.response",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    llm_response_schema,
);

pub const CORE_LLM_TOOLCALL: EventDef<ToolCalls> = EventDef::new(
    "core/llm.toolcall",
    Shape::Intercept,
    true,
    Mount::Subsystem,
    tool_calls_schema,
);

pub fn directory() -> Directory {
    Directory::from_metas([
        CORE_PROMPT_COLLECT.meta(),
        CORE_RULE_APPLY.meta(),
        CORE_LLM_REQUEST.meta(),
        CORE_LLM_RESPONSE.meta(),
        CORE_LLM_TOOLCALL.meta(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use wind_wire::message::{Content, Role};
    use wind_wire::tool::FunctionTool;

    #[test]
    fn all_five_events_are_intercept() {
        for def in [
            CORE_PROMPT_COLLECT.name,
            CORE_RULE_APPLY.name,
            CORE_LLM_REQUEST.name,
            CORE_LLM_RESPONSE.name,
            CORE_LLM_TOOLCALL.name,
        ] {
            let meta = directory()
                .get(def)
                .expect("look up the event definition")
                .clone();
            assert_eq!(meta.shape, Shape::Intercept, "{def} must be interceptable");
            assert!(
                meta.log,
                "{def} must be logged: what was rewritten has to be inspectable"
            );
        }
    }

    #[test]
    fn the_directory_holds_exactly_these_five() {
        assert_eq!(directory().len(), 5);
    }

    #[test]
    fn the_request_payload_carries_the_model_id() {
        let schema = (CORE_LLM_REQUEST.schema)();
        let required = schema["required"]
            .as_array()
            .expect("read required as an array");
        assert!(required.contains(&serde_json::json!("model_id")));
        assert!(required.contains(&serde_json::json!("request")));
    }

    #[test]
    fn the_prompt_payload_is_a_list_of_fragments_not_a_joined_string() {
        // 改写者要能增删单个片段并知道每段的来处；拼成字符串之后这两件事都做不到
        let schema = (CORE_PROMPT_COLLECT.schema)();
        assert_eq!(
            schema["properties"]["fragments"]["type"],
            serde_json::json!("array")
        );
    }

    #[test]
    fn the_rule_payload_names_the_adapter_it_applies_to() {
        // 规则改的是提供商结构；两种结构混用会让后改的规则收到形状不同的输入
        let schema = (CORE_RULE_APPLY.schema)();
        let required = schema["required"]
            .as_array()
            .expect("read required as an array");
        assert!(required.contains(&serde_json::json!("adapter")));
        assert!(required.contains(&serde_json::json!("body")));
    }

    #[test]
    fn the_request_payload_survives_a_round_trip() {
        // 载荷解不回来，这个干预点就改不动：改写由订阅方交回一份 JSON
        let payload = LlmRequest {
            model_id: 7,
            request: LlmRequestBody {
                contexts: vec![Message {
                    role: Role::User,
                    content: vec![Content::new_text("what time is it".into())],
                    ..Message::default()
                }],
                tools: Some(vec![Tools::Function(FunctionTool {
                    name: "get_clock".into(),
                    description: None,
                    parameters: None,
                    strict: None,
                })]),
            },
        };
        let value = serde_json::to_value(&payload).expect("serialize");
        let back: LlmRequest = serde_json::from_value(value.clone()).expect("deserialize");
        assert_eq!(
            serde_json::to_value(&back).expect("serialize"),
            value,
            "the round trip must be byte-for-byte identical"
        );
    }
}
