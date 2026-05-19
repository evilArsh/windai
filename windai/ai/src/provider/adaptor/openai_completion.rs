use super::schema::openai_completion::{
    ChatCompletion, ChatCompletionContentPartImage, ChatCompletionContentPartInputAudio,
    ChatCompletionMessage, ChatCompletionMessageFunctionToolCall,
    ChatCompletionMessageFunctionToolCallFunction, ChatCompletionMessageParam,
    ChatCompletionMessageToolCall, ChatCompletionRequest, ChatStreamCompletion, Content,
    ContentObject, FileContentPart, ToolCallRequest, ToolCallRequestParams,
};
use super::{Adaptor, AdaptorError, ChatAdaptor};
use crate::message::{self, Message, MessageBuilder, ReqConfig, Role};
use crate::model::AdaptorType;
use crate::provider::sse::SseBlock;
use crate::tool::{FunctionCall, Tools};
use serde_json::{Value, json};

pub struct OpenAICompletionAdaptor;

impl Adaptor for OpenAICompletionAdaptor {
    fn get_type(&self) -> AdaptorType {
        AdaptorType::OpenAICompletion
    }
}
impl OpenAICompletionAdaptor {
    fn parse_common(
        &self,
        msg: ChatCompletionMessage,
        created_at: i64,
    ) -> Result<Message, AdaptorError> {
        let content = msg.content.unwrap_or_else(|| String::new());
        let content = match msg.audio {
            Some(audio) => message::Content::new_audio(audio.data, String::new()),
            None => message::Content::new_text(content),
        };
        Ok(Message {
            role: msg.role.unwrap_or_else(|| Role::Assistant),
            content: vec![content],
            reasoning_content: Some(msg.reasoning_content.unwrap_or_else(|| String::new())),
            created_at,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: match msg.tool_calls {
                Some(tools) => Some(
                    tools
                        .into_iter()
                        .map(|tool| match tool {
                            ChatCompletionMessageToolCall::Function(tool) => {
                                let func = tool.function.unwrap_or_default();
                                FunctionCall {
                                    id: tool.id.unwrap_or_default(),
                                    name: func.name.unwrap_or_default(),
                                    arguments: func.arguments.unwrap_or_default(),
                                }
                            }
                            ChatCompletionMessageToolCall::Custom(tool) => FunctionCall {
                                id: tool.id,
                                name: tool.custom.name,
                                arguments: tool.custom.input,
                            },
                        })
                        .collect::<Vec<FunctionCall>>(),
                ),
                None => None,
            },
        })
    }
}
impl ChatAdaptor for OpenAICompletionAdaptor {
    fn build_request(
        &self,
        model_name: &str,
        config: &ReqConfig,
        contexts: &Vec<Message>,
        tools: Option<&Vec<Tools>>,
    ) -> Result<Value, AdaptorError> {
        let tools = transform_tools(tools);

        let input_messages = contexts
            .into_iter()
            .flat_map(|ctx| {
                if let Some(tools) = &ctx.tool_calls
                    && tools.len() > 0
                {
                    // 模型返回的函数调用参数
                    vec![ChatCompletionMessageParam {
                        content: None,
                        role: ctx.role,
                        name: None,
                        audio: None,
                        reasoning_content: ctx.reasoning_content.clone(),
                        tool_calls: Some(
                            tools
                                .into_iter()
                                .map(|tool| {
                                    ChatCompletionMessageToolCall::Function(
                                        ChatCompletionMessageFunctionToolCall {
                                            id: Some(tool.id.clone()),
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
                } else if ctx.is_tool_result() {
                    ctx.content
                        .iter()
                        .filter_map(|content| {
                            match content {
                                // 用户函数调用结果
                                message::Content::FunctionCall(c) => {
                                    Some(ChatCompletionMessageParam {
                                        content: Some(Content::Text(c.content.to_string())),
                                        role: Role::Tool,
                                        reasoning_content: None,
                                        name: None,
                                        audio: None,
                                        tool_calls: None,
                                        tool_call_id: Some(c.id.clone()),
                                    })
                                }
                                _ => {
                                    log::warn!(
                                        "message filtered, only tool messages are allowed. role: {}",
                                        ctx.role
                                    );
                                    None
                                }
                            }
                        })
                        .collect()
                } else {
                    let texts = ctx
                        .content
                        .iter()
                        .filter_map(|content| match content {
                            message::Content::Text(c) => Some(ContentObject {
                                r#type: String::from("text"),
                                text: Some(c.clone()),
                                image_url: None,
                                input_audio: None,
                                file: None,
                                refusal: None,
                            }),
                            message::Content::Image(c) => Some(ContentObject {
                                r#type: String::from("image_url"),
                                text: None,
                                image_url: Some(ChatCompletionContentPartImage {
                                    url: c.clone(),
                                    detail: Some("auto".to_string()),
                                }),
                                input_audio: None,
                                file: None,
                                refusal: None,
                            }),
                            message::Content::Audio(c) => Some(ContentObject {
                                r#type: String::from("input_audio"),
                                text: None,
                                image_url: None,
                                input_audio: Some(ChatCompletionContentPartInputAudio {
                                    data: c.content.clone(),
                                    format: c.format.clone(),
                                }),
                                file: None,
                                refusal: None,
                            }),
                            message::Content::File(c) => Some(ContentObject {
                                r#type: String::from("file"),
                                text: None,
                                file: Some(FileContentPart {
                                    file_data: Some(c.clone()),
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
                        .collect::<Vec<ContentObject>>();
                    let contents = if let Some(content) = texts.iter().next()
                        && texts.len() == 1
                        && content.r#type == "text"
                    {
                        Content::Text(content.text.as_ref().cloned().unwrap_or_default())
                    } else {
                        Content::Objects(texts)
                    };

                    vec![ChatCompletionMessageParam {
                        content: Some(contents),
                        role: ctx.role,
                        reasoning_content: None,
                        name: None,
                        audio: None,
                        tool_calls: None,
                        tool_call_id: None,
                    }]
                }
            })
            .collect::<Vec<ChatCompletionMessageParam>>();

        let req = ChatCompletionRequest {
            model: model_name.to_string(),
            messages: input_messages,
            temperature: config.temperature,
            top_p: config.top_p,
            max_completion_tokens: config.max_tokens,
            stream: config.stream,
            presence_penalty: config.presence_penalty,
            frequency_penalty: config.frequency_penalty,
            parallel_tool_calls: config.parallel_tool_calls,
            reasoning_effort: match config.reasoning {
                Some(val) => {
                    if val {
                        Some("medium".to_string())
                    } else {
                        None
                    }
                }
                None => None,
            },
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
            stream_options: match config.stream {
                Some(true) => Some(json!({"include_usage": true})),
                _ => None,
            },
            tool_choice: None,
            tools,
            top_logprobs: None,
            verbosity: None,
            web_search_options: None,
        };
        Ok(serde_json::to_value(&req)?)
    }

    fn parse_response(&self, data: &[u8]) -> Result<Message, AdaptorError> {
        log::debug!(
            "[raw completion]\n{}",
            serde_json::to_string_pretty(&serde_json::from_slice::<serde_json::Value>(data)?)?
        );
        let completion: ChatCompletion = serde_json::from_slice(data)?;
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
            return Ok(Message::default());
        }
    }

    fn parse_stream_chunk(&self, data: &[u8]) -> Result<Vec<Message>, AdaptorError> {
        let blocks = SseBlock::parse(data);
        blocks
            .into_iter()
            .filter_map(|block| {
                let block_data = block.data?;
                if block_data.is_empty() {
                    return None;
                }
                let completion: ChatStreamCompletion = match serde_json::from_str(&block_data) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e.into())),
                };
                let (input_tokens, output_tokens) = match completion.usage {
                    Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
                    None => (0, 0),
                };
                if let Some(choice) = completion.choices.into_iter().next() {
                    let mut msg = match self.parse_common(choice.delta, completion.created) {
                        Ok(r) => r,
                        Err(e) => return Some(Err(e)),
                    };
                    msg.input_tokens = input_tokens;
                    msg.output_tokens = output_tokens;
                    Some(Ok(msg))
                } else {
                    let msg = MessageBuilder::default()
                        .input_tokens(input_tokens)
                        .output_tokens(output_tokens)
                        .role(Role::Assistant)
                        .created_at(completion.created)
                        .build()
                        .unwrap_or_default();
                    Some(Ok(msg))
                }
            })
            .collect::<Result<Vec<Message>, AdaptorError>>()
    }
}

fn transform_tools(tools: Option<&Vec<Tools>>) -> Option<Vec<ToolCallRequest>> {
    tools
        .map(|tools| {
            tools
                .into_iter()
                .filter_map(|tool| match tool {
                    Tools::Function(func_tool) => Some(ToolCallRequest {
                        r#type: String::from("function"),
                        function: ToolCallRequestParams {
                            name: func_tool.name.clone(),
                            description: func_tool.description.clone(),
                            parameters: func_tool.parameters.clone(),
                            strict: func_tool.strict,
                        },
                    }),
                    // _ => None,
                })
                .collect::<Vec<ToolCallRequest>>()
        })
        .or(None)
}
