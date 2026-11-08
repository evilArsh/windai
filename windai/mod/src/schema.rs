use crate::error::{ModError, Result};
use crate::manifest::Manifest;
use serde_json::{Map, Value, json};

const SYMBOL_PATTERN: &str = "^[a-zA-Z_][a-zA-Z0-9_-]{0,62}$";
const HANDLER_PATTERN: &str = "^[a-zA-Z_$][a-zA-Z0-9_$]*$";

/// 导出清单的结构 schema
pub fn manifest_schema() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(Manifest))
        .expect("the derived manifest schema is always serializable");
    merge(&mut schema, constraints());
    schema
}

fn constraints() -> Value {
    json!({
        "properties": {
            "version": { "maximum": 30000 },
            "description": { "maxLength": 500 },
            "mcpServers": { "propertyNames": { "pattern": SYMBOL_PATTERN } },
            "requests": { "propertyNames": { "pattern": SYMBOL_PATTERN } },
            "defs": {
                "propertyNames": { "pattern": SYMBOL_PATTERN },
                "additionalProperties": { "type": ["object", "boolean"] }
            }
        },
        "$defs": {
            "McpServer": {
                "properties": {
                    "command": { "type": "string", "minLength": 1 },
                    "url": {
                        "type": "string",
                        "minLength": 8,
                        "pattern": "^[a-zA-Z][a-zA-Z0-9+.-]*:"
                    }
                },
                "oneOf": [
                    { "required": ["command"], "not": { "required": ["url"] } },
                    { "required": ["url"], "not": { "required": ["command"] } }
                ],
                "if": { "required": ["url"] },
                "then": {
                    "properties": {
                        "args": { "maxItems": 0 },
                        "env": { "maxProperties": 0 }
                    }
                }
            },
            "Request": {
                "properties": {
                    "handler": { "pattern": HANDLER_PATTERN },
                    "inputSchema": { "type": ["object", "boolean", "null"] },
                    "outputSchema": { "type": ["object", "boolean", "null"] },
                    "timeoutMs": { "minimum": 1, "maximum": 300000 }
                }
            },
            "Events": {
                "properties": {
                    "on": { "propertyNames": { "pattern": "^core/" } },
                    "emit": { "propertyNames": { "pattern": SYMBOL_PATTERN } }
                }
            },
            "EventSub": {
                "properties": {
                    "handler": { "pattern": HANDLER_PATTERN },
                    "timeoutMs": { "minimum": 1, "maximum": 60000 }
                }
            },
            "EmitDecl": {
                "required": ["retention", "payload"],
                "properties": {
                    "eventKey": {
                        "type": ["string", "null"],
                        "pattern": SYMBOL_PATTERN,
                        "minLength": 1
                    },
                    "payload": { "type": ["object", "boolean"] }
                },
                "if": {
                    "required": ["retention"],
                    "properties": { "retention": { "const": "journal" } }
                },
                "then": {
                    "required": ["eventKey"],
                    "properties": { "eventKey": { "type": "string" } }
                }
            }
        }
    })
}

fn merge(target: &mut Value, patch: Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                match target.get_mut(&key) {
                    Some(existing) => merge(existing, value),
                    None => {
                        target.insert(key, value);
                    }
                }
            }
        }
        (target, patch) => *target = patch,
    }
}

/// 校验一个片段是合法的 Draft 2020-12 schema，且只引用本地定义
pub(crate) fn validate_fragment(
    field: &str,
    fragment: &Value,
    defs: &Map<String, Value>,
) -> Result<()> {
    match fragment {
        Value::Bool(_) => {
            validate_compiled(field, fragment)?;
        }
        Value::Object(map) => {
            if map.contains_key("defs") {
                return Err(field_error(
                    field,
                    "a fragment must not declare a top level defs keyword",
                ));
            }
            reject_external_refs(field, fragment)?;
            let mut root = fragment.clone();
            if let Value::Object(root) = &mut root {
                root.insert("defs".into(), Value::Object(defs.clone()));
            }
            validate_compiled(field, &root)?;
        }
        _ => {
            return Err(field_error(
                field,
                "a schema must be a JSON object or a boolean",
            ));
        }
    }
    Ok(())
}

fn validate_compiled(field: &str, schema: &Value) -> Result<()> {
    jsonschema::draft202012::meta::validate(schema)
        .map_err(|error| field_error(field, error.to_string()))?;
    jsonschema::draft202012::new(schema).map_err(|error| field_error(field, error.to_string()))?;
    Ok(())
}

fn reject_external_refs(field: &str, value: &Value) -> Result<()> {
    match value {
        Value::Object(map) => {
            for (key, inner) in map {
                if (key == "$ref" || key == "$dynamicRef")
                    && let Some(reference) = inner.as_str()
                    && !reference.starts_with('#')
                {
                    return Err(field_error(
                        field,
                        format!("only local references are allowed: {reference}"),
                    ));
                }
                reject_external_refs(field, inner)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                reject_external_refs(field, item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn field_error(field: &str, reason: impl Into<String>) -> ModError {
    ModError::ManifestField {
        field: field.to_string(),
        reason: reason.into(),
    }
}
