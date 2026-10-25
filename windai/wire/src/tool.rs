use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Tools {
    Function(FunctionTool),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FunctionTool {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct FunctionCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct FunctionCallOutput {
    pub id: String,
    pub content: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_function_tool_omits_absent_options() {
        let tool = FunctionTool {
            name: "get_weather".into(),
            description: None,
            parameters: None,
            strict: None,
        };
        assert_eq!(
            serde_json::to_value(&tool).expect("serialize"),
            serde_json::json!({ "name": "get_weather" }),
            "nullable fields must be absent when unset"
        );
    }

    #[test]
    fn a_function_tool_carries_its_parameter_schema() {
        let tool = FunctionTool {
            name: "get_weather".into(),
            description: Some("look up the weather".into()),
            parameters: Some(serde_json::json!({ "type": "object" })),
            strict: Some(true),
        };
        let value = serde_json::to_value(&tool).expect("serialize");
        assert_eq!(
            value["description"],
            serde_json::json!("look up the weather")
        );
        assert_eq!(value["parameters"]["type"], serde_json::json!("object"));
        assert_eq!(value["strict"], serde_json::json!(true));
    }

    #[test]
    fn the_unified_tool_type_serializes_transparently() {
        // 标了 untagged，因此枚举外壳不出现在序列化结果里
        let wrapped = Tools::Function(FunctionTool {
            name: "get_weather".into(),
            description: None,
            parameters: None,
            strict: None,
        });
        assert_eq!(
            serde_json::to_value(&wrapped).expect("serialize"),
            serde_json::json!({ "name": "get_weather" }),
            "the enum wrapper must not appear in the serialized result"
        );
    }

    #[test]
    fn a_tool_declaration_survives_a_round_trip() {
        // 工具清单要能被改写，因此必须解得回来。解回来再序列化，结果须逐字相同。
        let cases = [
            (
                FunctionTool {
                    name: "get_weather".into(),
                    description: None,
                    parameters: None,
                    strict: None,
                },
                serde_json::json!({ "name": "get_weather" }),
            ),
            (
                FunctionTool {
                    name: "get_weather".into(),
                    description: Some("look up the weather".into()),
                    parameters: Some(serde_json::json!({ "type": "object" })),
                    strict: Some(true),
                },
                serde_json::json!({
                    "name": "get_weather",
                    "description": "look up the weather",
                    "parameters": { "type": "object" },
                    "strict": true
                }),
            ),
        ];
        for (function, wire) in cases {
            let value = serde_json::to_value(Tools::Function(function)).expect("serialize");
            assert_eq!(
                value, wire,
                "the serialized result must match the value before the round trip"
            );
            let back: Tools = serde_json::from_value(value.clone()).expect("deserialize");
            assert_eq!(
                serde_json::to_value(&back).expect("serialize"),
                value,
                "absent nullable fields must not be filled in as null during the round trip"
            );
        }
    }

    #[test]
    fn a_function_call_round_trips_through_json() {
        let call = FunctionCall {
            id: "call_abc".into(),
            name: "get_weather".into(),
            arguments: "{\"city\":\"SF\"}".into(),
        };
        let text = serde_json::to_string(&call).expect("serialize");
        let back: FunctionCall = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back.id, "call_abc");
        assert_eq!(back.name, "get_weather");
        assert_eq!(
            back.arguments, "{\"city\":\"SF\"}",
            "arguments keep their raw text; parsing is left to the commit stage"
        );
    }

    #[test]
    fn a_function_call_output_keeps_a_structured_result() {
        let output = FunctionCallOutput {
            id: "call_abc".into(),
            content: serde_json::json!({ "temp": 22 }),
        };
        let value = serde_json::to_value(&output).expect("serialize");
        assert_eq!(value["id"], serde_json::json!("call_abc"));
        assert_eq!(value["content"]["temp"], serde_json::json!(22));
    }
}
