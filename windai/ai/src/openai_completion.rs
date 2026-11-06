use super::adapter::ChatAdapter;
use super::error::AdapterError;
use super::eventsource::Event;
use super::model::ChatModel;
use super::schema::openai_completion::{
    ChatCompletion, ChatCompletionContentPartImage, ChatCompletionContentPartInputAudio,
    ChatCompletionMessage, ChatCompletionMessageFunctionToolCall,
    ChatCompletionMessageFunctionToolCallFunction, ChatCompletionMessageParam,
    ChatCompletionMessageToolCall, ChatCompletionRequest, ChatStreamCompletion, Content,
    ContentObject, FileContentPart, ToolCallRequest, ToolCallRequestParams,
};
use serde_json::{Value, json};
use wind_wire::message::{Content as WireContent, Message, Role};
use wind_wire::tool::{FunctionCall, Tools};

pub struct OpenAICompletion;

impl OpenAICompletion {
    fn parse_common(
        &self,
        msg: ChatCompletionMessage,
        created_at: i64,
    ) -> Result<Message, AdapterError> {
        let text = msg.content.unwrap_or_default();
        let (transcript, audio) = match msg.audio {
            Some(audio) => {
                let transcript = audio.transcript;
                let data = audio.data.unwrap_or_default();
                let part = (!data.is_empty())
                    .then(|| WireContent::new_audio(data, String::new(), audio.id));
                (transcript, part)
            }
            None => (None, None),
        };
        let text = if text.is_empty() {
            transcript.unwrap_or_default()
        } else {
            text
        };
        let mut content = Vec::with_capacity(2);
        if !text.is_empty() {
            content.push(WireContent::new_text(text));
        }
        if let Some(audio) = audio {
            content.push(audio);
        }
        Ok(Message {
            role: msg.role.unwrap_or(Role::Assistant),
            content,
            reasoning_content: msg.reasoning_content,
            reasoning_item_id: None,
            created_at,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: msg.tool_calls.map(|tools| {
                tools
                    .into_iter()
                    .map(|tool| match tool {
                        ChatCompletionMessageToolCall::Function(tool) => {
                            let func = tool.function.unwrap_or_default();
                            FunctionCall {
                                item_id: tool.index.map(|index| index.to_string()),
                                call_id: tool.id.unwrap_or_default(),
                                name: func.name.unwrap_or_default(),
                                arguments: func.arguments.unwrap_or_default(),
                            }
                        }
                        ChatCompletionMessageToolCall::Custom(tool) => FunctionCall {
                            item_id: tool.index.map(|index| index.to_string()),
                            call_id: tool.id,
                            name: tool.custom.name,
                            arguments: tool.custom.input,
                        },
                    })
                    .collect::<Vec<FunctionCall>>()
            }),
        })
    }
}

impl ChatAdapter for OpenAICompletion {
    fn path(&self) -> &'static str {
        "/chat/completions"
    }

    fn build_request(
        &self,
        model: &ChatModel,
        contexts: &[Message],
        tools: Option<&[Tools]>,
    ) -> Result<Value, AdapterError> {
        let tools = transform_tools(tools);

        let input_messages = contexts
            .iter()
            .flat_map(|ctx| {
                if let Some(tools) = &ctx.tool_calls
                    && !tools.is_empty()
                {
                    vec![ChatCompletionMessageParam {
                        content: content_of(ctx),
                        role: ctx.role,
                        name: None,
                        audio: None,
                        reasoning_content: ctx.reasoning_content.clone(),
                        tool_calls: Some(
                            tools
                                .iter()
                                .map(|tool| {
                                    ChatCompletionMessageToolCall::Function(
                                        ChatCompletionMessageFunctionToolCall {
                                            id: Some(tool.call_id.clone()),
                                            index: None,
                                            function: Some(
                                                ChatCompletionMessageFunctionToolCallFunction {
                                                    name: Some(tool.name.clone()),
                                                    arguments: Some(tool.arguments.clone()),
                                                },
                                            ),
                                            r#type: Some(String::from("function")),
                                        },
                                    )
                                })
                                .collect::<Vec<ChatCompletionMessageToolCall>>(),
                        ),
                        tool_call_id: None,
                    }]
                } else if ctx.role == Role::Tool {
                    ctx.content
                        .iter()
                        .filter_map(|content| match content {
                            WireContent::FunctionCall { data } => {
                                Some(ChatCompletionMessageParam {
                                    content: Some(Content::Text(data.content.to_string())),
                                    role: Role::Tool,
                                    reasoning_content: None,
                                    name: None,
                                    audio: None,
                                    tool_calls: None,
                                    tool_call_id: Some(data.id.clone()),
                                })
                            }
                            _ => {
                                log::warn!(
                                    "message filtered, only tool messages are allowed. role: {}",
                                    ctx.role
                                );
                                None
                            }
                        })
                        .collect()
                } else {
                    vec![ChatCompletionMessageParam {
                        content: content_of(ctx),
                        role: ctx.role,
                        reasoning_content: ctx.reasoning_content.clone(),
                        name: None,
                        audio: None,
                        tool_calls: None,
                        tool_call_id: None,
                    }]
                }
            })
            .collect::<Vec<ChatCompletionMessageParam>>();

        let stream: Option<bool> = model
            .config
            .as_ref()
            .and_then(|config| config.get("stream"))
            .and_then(|v| v.as_bool());

        let reasoning: Option<String> = model
            .config
            .as_ref()
            .and_then(|config| config.get("reasoning"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let req = ChatCompletionRequest {
            model: model.name.clone(),
            messages: input_messages,
            temperature: None,
            top_p: None,
            max_completion_tokens: None,
            stream,
            presence_penalty: None,
            frequency_penalty: None,
            parallel_tool_calls: None,
            reasoning_effort: reasoning,
            audio: None,
            logit_bias: None,
            logprobs: None,
            metadata: None,
            modalities: None,
            n: Some(1),
            prediction: None,
            prompt_cache_key: None,
            prompt_cache_retention: None,
            response_format: None,
            safety_identifier: None,
            service_tier: None,
            stop: None,
            store: None,
            stream_options: match stream {
                Some(true) => Some(json!({"include_usage": true})),
                _ => None,
            },
            tool_choice: None,
            tools,
            top_logprobs: None,
            verbosity: None,
            web_search_options: None,
        };
        let mut body = serde_json::to_value(&req)?;
        if let Some(config) = model.config.as_ref().and_then(|config| config.as_object())
            && let Some(body) = body.as_object_mut()
        {
            for (key, value) in config {
                if key == "stream" || key == "reasoning" {
                    continue;
                }
                body.insert(key.clone(), value.clone());
            }
        }
        Ok(body)
    }

    fn parse_response(&self, data: &[u8]) -> Result<Message, AdapterError> {
        let completion: ChatCompletion = serde_json::from_slice(data)?;
        log::debug!(
            "[raw completion] id: {}, bytes: {}",
            completion.id,
            data.len()
        );
        let (input_tokens, output_tokens) = match completion.usage {
            Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
            None => (0, 0),
        };
        if let Some(choice) = completion.choices.into_iter().next() {
            let mut msg = self.parse_common(choice.message, completion.created)?;
            msg.input_tokens = input_tokens;
            msg.output_tokens = output_tokens;
            msg.created_at = completion.created;
            Ok(msg)
        } else {
            Ok(Message::default())
        }
    }

    fn parse_stream_chunk(&self, event: &Event) -> Result<Option<Message>, AdapterError> {
        let completion: ChatStreamCompletion = serde_json::from_str(&event.data)?;
        let (input_tokens, output_tokens) = match completion.usage {
            Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
            None => (0, 0),
        };
        if let Some(choice) = completion.choices.into_iter().next() {
            let mut msg = self.parse_common(choice.delta, completion.created)?;
            msg.input_tokens = input_tokens;
            msg.output_tokens = output_tokens;
            Ok(Some(msg))
        } else {
            let msg = Message {
                input_tokens,
                output_tokens,
                role: Role::Assistant,
                created_at: completion.created,
                ..Default::default()
            };
            Ok(Some(msg))
        }
    }
}

/// assistant 文本与其它内容部分，空内容不产出字段
fn content_of(ctx: &Message) -> Option<Content> {
    let mut parts: Vec<ContentObject> = ctx
        .content
        .iter()
        .filter_map(|content| match content {
            WireContent::Text { data } => Some(ContentObject {
                r#type: String::from("text"),
                text: Some(data.clone()),
                image_url: None,
                input_audio: None,
                file: None,
                refusal: None,
            }),
            WireContent::Image { data } => Some(ContentObject {
                r#type: String::from("image_url"),
                text: None,
                image_url: Some(ChatCompletionContentPartImage {
                    url: data.clone(),
                    detail: Some("auto".to_string()),
                }),
                input_audio: None,
                file: None,
                refusal: None,
            }),
            WireContent::Audio { data } => Some(ContentObject {
                r#type: String::from("input_audio"),
                text: None,
                input_audio: Some(ChatCompletionContentPartInputAudio {
                    data: data.content.clone(),
                    format: data.format.clone(),
                }),
                image_url: None,
                file: None,
                refusal: None,
            }),
            WireContent::File { data } => Some(ContentObject {
                r#type: String::from("file"),
                text: None,
                file: Some(FileContentPart {
                    file_data: Some(data.clone()),
                    file_id: None,
                    filename: None,
                }),
                input_audio: None,
                image_url: None,
                refusal: None,
            }),
            _ => {
                log::warn!(
                    "message filtered, only texts messages are allowed. role: {}",
                    ctx.role
                );
                None
            }
        })
        .collect();
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 && parts[0].r#type == "text" {
        return Some(Content::Text(parts.remove(0).text.unwrap_or_default()));
    }
    Some(Content::Objects(parts))
}

fn transform_tools(tools: Option<&[Tools]>) -> Option<Vec<ToolCallRequest>> {
    tools.map(|tools| {
        tools
            .iter()
            .map(|tool| match tool {
                Tools::Function(func_tool) => ToolCallRequest {
                    r#type: String::from("function"),
                    function: ToolCallRequestParams {
                        name: func_tool.name.clone(),
                        description: func_tool.description.clone(),
                        parameters: func_tool.parameters.clone(),
                        strict: func_tool.strict,
                    },
                },
            })
            .collect::<Vec<ToolCallRequest>>()
    })
}
