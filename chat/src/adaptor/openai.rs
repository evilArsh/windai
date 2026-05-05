use crate::adaptor::openai_completion::{
    ChatCompletion, ChatCompletionContentPartImage, ChatCompletionMessage,
    ChatCompletionMessageFunctionToolCall, ChatCompletionMessageFunctionToolCallFunction,
    ChatCompletionMessageParam, ChatCompletionMessageToolCall, ChatCompletionRequest,
    ChatStreamCompletion, ContentObject, FileContentPart, ToolCallRequest, ToolCallRequestParams,
};
use crate::adaptor::openai_response::{
    self, FunctionCall, FunctionCallOutput, FunctionTool, InputContent, InputItem, OutputItem,
    Response, ResponseInputFile, ResponseInputImage, ResponseInputText, ResponseOutput,
    ResponseReasoning, ResponseRequest, ResponseStream, Tools,
};
use crate::adaptor::sse::SseBlock;
use crate::adaptor::{Adaptor, AdaptorError, ChatAdaptor, openai_completion};
use crate::{
    AdaptorType, Content, ContentType, Context, Message, ReqConfig, Role, ToolCallParam,
    ToolCallRes,
};
use bytes::Bytes;
use serde_json::{Value, json};

pub struct OpenAICompletionAdaptor;

/// 只转换成 [Tools::Function]
fn transfer_response_tools(tools: Option<Vec<ToolCallParam>>) -> Option<Vec<Tools>> {
    tools
        .map(|tools| {
            tools
                .into_iter()
                .map(|tool| {
                    Tools::Function(FunctionTool {
                        r#type: String::from("function"),
                        name: tool.name,
                        description: tool.description,
                        parameters: tool.parameters,
                        // strict: tool.strict,
                        strict: None,
                        defer_loading: None,
                    })
                })
                .collect::<Vec<Tools>>()
        })
        .or(None)
}

impl Adaptor for OpenAICompletionAdaptor {
    fn get_type(&self) -> AdaptorType {
        AdaptorType::OpenAICompletion
    }
}
impl OpenAICompletionAdaptor {
    fn parse_common(
        &self,
        msg: ChatCompletionMessage,
        raw_content: Option<String>,
        usage: Option<openai_completion::TokenUsage>,
        created_at: i64,
    ) -> Result<Message, AdaptorError> {
        let content = msg.content.unwrap_or(String::new());
        let (content_type, transcript, content) = match msg.audio {
            Some(audio) => (ContentType::Audio, audio.transcript, audio.data),
            None => (ContentType::Text, None, content),
        };
        let (input_tokens, output_tokens) = match usage {
            Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
            None => (0, 0),
        };
        Ok(Message {
            role: msg.role.unwrap_or(Role::Assistant),
            raw_content,
            reasoning_content: Some(msg.reasoning_content.unwrap_or(String::new())),
            transcript,
            input_tokens,
            output_tokens,
            created_at,
            content: Some(Content::new(content_type, content)),
            tool_calls: match msg.tool_calls {
                Some(tools) => tools
                    .into_iter()
                    .map(|tool| match tool {
                        openai_completion::ChatCompletionMessageToolCall::Function(tool) => {
                            let func = tool.function.unwrap_or_default();
                            Some(ToolCallRes {
                                call_id: tool.id.unwrap_or_default(),
                                name: func.name.unwrap_or_default(),
                                arguments: func.arguments.unwrap_or_default(),
                            })
                        }
                        openai_completion::ChatCompletionMessageToolCall::Custom(tool) => {
                            Some(ToolCallRes {
                                call_id: tool.id,
                                name: tool.custom.name,
                                arguments: tool.custom.input,
                            })
                        }
                    })
                    .collect(),
                None => None,
            },
        })
    }
}
impl ChatAdaptor for OpenAICompletionAdaptor {
    fn build_request(
        &self,
        model_name: &str,
        config: ReqConfig,
        contexts: Vec<Context>,
    ) -> Result<Value, AdaptorError> {
        let tools = config
            .tools
            .map(|tools| {
                tools
                    .into_iter()
                    .map(|tool| ToolCallRequest {
                        r#type: String::from("function"),
                        function: ToolCallRequestParams {
                            name: tool.name,
                            description: tool.description,
                            parameters: tool.parameters,
                            strict: tool.strict,
                        },
                    })
                    .collect::<Vec<ToolCallRequest>>()
            })
            .or(None);

        let input_messages = contexts
            .into_iter()
            .map(|ctx| {
                if let Some(tool_calls) = ctx.tool_call_args
                    && let Some(tool_name) = ctx.tool_call_name
                    && let Some(tool_id) = ctx.tool_call_id
                {
                    // 模型返回的函数调用参数
                    ChatCompletionMessageParam {
                        content: openai_completion::Content::Text(String::new()),
                        role: Role::Assistant,
                        name: None,
                        audio: None,
                        tool_calls: Some(vec![ChatCompletionMessageToolCall::Function(
                            ChatCompletionMessageFunctionToolCall {
                                id: Some(tool_id),
                                function: Some(ChatCompletionMessageFunctionToolCallFunction {
                                    name: Some(tool_name),
                                    arguments: Some(tool_calls),
                                }),
                                r#type: Some(String::from("function")),
                            },
                        )]),
                        tool_call_id: None,
                    }
                } else if let Some(tool_id) = ctx.tool_call_id {
                    // 用户函数调用结果
                    ChatCompletionMessageParam {
                        content: openai_completion::Content::Text(
                            ctx.content
                                .into_iter()
                                .next()
                                .map(|c| c.content)
                                .unwrap_or_default(),
                        ),
                        role: Role::Tool,
                        name: None,
                        audio: None,
                        tool_calls: None,
                        tool_call_id: Some(tool_id),
                    }
                } else {
                    // 正常用户信息上下文
                    let contents = if let Some(content) = ctx.content.last()
                        && ctx.content.len() == 1
                        && content.content_type == ContentType::Text
                    {
                        openai_completion::Content::Text(ctx.content[0].content.clone())
                    } else {
                        openai_completion::Content::Objects(
                            ctx.content
                                .iter()
                                .map(|c| match c.content_type {
                                    ContentType::Text => ContentObject {
                                        r#type: "text".to_string(),
                                        text: Some(c.content.clone()),
                                        image_url: None,
                                        input_audio: None,
                                        file: None,
                                        refusal: None,
                                    },
                                    ContentType::Audio => ContentObject {
                                        r#type: "input_audio".to_string(),
                                        text: Some(c.content.clone()),
                                        image_url: None,
                                        input_audio: None,
                                        file: None,
                                        refusal: None,
                                    },
                                    ContentType::Image => ContentObject {
                                        r#type: "image_url".to_string(),
                                        text: None,
                                        image_url: Some(ChatCompletionContentPartImage {
                                            url: c.content.clone(),
                                            detail: Some("auto".to_string()),
                                        }),
                                        input_audio: None,
                                        file: None,
                                        refusal: None,
                                    },
                                    ContentType::File => ContentObject {
                                        r#type: "file".to_string(),
                                        text: None,
                                        file: Some(FileContentPart {
                                            file_data: Some(c.content.clone()),
                                            file_id: None,
                                            filename: None,
                                        }),
                                        input_audio: None,
                                        image_url: None,
                                        refusal: None,
                                    },
                                })
                                .collect::<Vec<ContentObject>>(),
                        )
                    };

                    ChatCompletionMessageParam {
                        content: contents,
                        role: ctx.role,
                        name: None,
                        audio: None,
                        tool_calls: None,
                        tool_call_id: None,
                    }
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

    fn parse_response(&self, data: Bytes) -> Result<Message, AdaptorError> {
        log::debug!(
            "[raw completion]\n{}",
            String::from_utf8_lossy(data.as_ref())
        );
        let completion: ChatCompletion = serde_json::from_slice(&data)?;
        if let Some(choice) = completion.choices.into_iter().next() {
            self.parse_common(choice.message, None, completion.usage, completion.created)
        } else {
            return Ok(Message::default_assistant());
        }
    }

    fn parse_stream_chunk(&self, data: Bytes) -> Result<Vec<Message>, AdaptorError> {
        let blocks = SseBlock::parse_all(data);
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
                if let Some(choice) = completion.choices.into_iter().next() {
                    Some(self.parse_common(
                        choice.delta,
                        None,
                        completion.usage,
                        completion.created,
                    ))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<Message>, AdaptorError>>()
    }
}

pub struct OpenAIResponseAdaptor;

impl Adaptor for OpenAIResponseAdaptor {
    fn get_type(&self) -> AdaptorType {
        AdaptorType::OpenAIResponse
    }
}

impl ChatAdaptor for OpenAIResponseAdaptor {
    fn build_request(
        &self,
        model_name: &str,
        config: ReqConfig,
        contexts: Vec<Context>,
    ) -> Result<Value, AdaptorError> {
        let tools = transfer_response_tools(config.tools);
        let input_messages = contexts
            .into_iter()
            .map(|ctx| {
                if let Some(tool_calls) = ctx.tool_call_args
                    && let Some(tool_name) = ctx.tool_call_name
                    && let Some(tool_id) = ctx.tool_call_id
                {
                    // 函数调用参数上下文
                    InputItem::FunctionCall(FunctionCall {
                        arguments: tool_calls,
                        call_id: tool_id,
                        name: tool_name,
                        r#type: String::from("function_call"),
                        id: None,
                        namespace: None,
                        status: None,
                    })
                } else if let Some(tool_id) = ctx.tool_call_id {
                    // 用户函数调用结果上下文
                    InputItem::FunctionCallOutput(FunctionCallOutput {
                        call_id: tool_id,
                        output: Value::String(
                            ctx.content
                                .into_iter()
                                .next()
                                .map(|c| c.content)
                                .unwrap_or_default(),
                        ),
                        r#type: String::from("function_call_output"),
                        id: None,
                        status: None,
                    })
                } else {
                    // 正常用户信息上下文
                    InputItem::Message(openai_response::Message {
                        content: ctx
                            .content
                            .iter()
                            .map(|c| match c.content_type {
                                ContentType::Text => {
                                    InputContent::ResponseInputText(ResponseInputText {
                                        text: c.content.clone(),
                                        r#type: "input_text".to_string(),
                                    })
                                }
                                ContentType::Image => {
                                    InputContent::ResponseInputImage(ResponseInputImage {
                                        detail: None,
                                        r#type: "input_image".to_string(),
                                        file_id: None,
                                        image_url: Some(c.content.clone()),
                                    })
                                }
                                ContentType::File | ContentType::Audio => {
                                    InputContent::ResponseInputFile(ResponseInputFile {
                                        r#type: "input_file".to_string(),
                                        file_data: Some(c.content.clone()),
                                        file_id: None,
                                        file_url: None,
                                        filename: None,
                                    })
                                }
                            })
                            .collect(),
                        role: ctx.role,
                        phase: None,
                        status: None,
                        r#type: None,
                    })
                }
            })
            .collect::<Vec<InputItem>>();

        let req = serde_json::to_value(&ResponseRequest {
            model: Some(model_name.to_string()),
            input: input_messages,
            stream: config.stream,
            temperature: config.temperature,
            top_p: config.top_p,
            max_output_tokens: config.max_tokens,
            parallel_tool_calls: config.parallel_tool_calls,
            reasoning: match config.reasoning {
                Some(true) => Some(ResponseReasoning {
                    effort: Some("medium".to_string()),
                }),
                _ => None,
            },
            background: None,
            context_management: None,
            conversation: None,
            include: None,
            instructions: None,
            max_tool_calls: None,
            metadata: None,
            previous_response_id: None,
            prompt: None,
            prompt_cache_key: None,
            prompt_cache_retention: None,
            safety_identifier: None,
            service_tier: None,
            store: None,
            stream_options: None,
            text: None,
            tool_choice: None,
            tools,
            top_logprobs: None,
            truncation: None,
        })?;
        Ok(req)
    }

    fn parse_response(&self, data: Bytes) -> Result<Message, AdaptorError> {
        log::debug!("[raw response]\n{}", String::from_utf8_lossy(data.as_ref()));
        let response: Response = serde_json::from_slice(&data)?;

        let (input_tokens, output_tokens) = match response.usage {
            Some(usage) => (usage.input_tokens, usage.output_tokens),
            None => (0, 0),
        };

        let mut output_msg: Option<Message> = None;
        let mut output_reasoning: Option<Message> = None;
        let mut output_img: Option<Message> = None;
        let mut output_toolcalls: Option<Message> = None;

        for output in response.output.into_iter() {
            match output {
                // for output in response.output{}
                OutputItem::ResponseOutputMessage(msg) => {
                    if output_msg.is_some() {
                        log::warn!("multiple output messages in response");
                        continue;
                    }
                    let c = msg.content.into_iter().next().ok_or_else(|| {
                        AdaptorError::Transfer("no output message in response".into())
                    })?;
                    output_msg = Some(Message {
                        role: msg.role,
                        raw_content: None,
                        content: Some(Content::new(
                            ContentType::Text,
                            match c {
                                ResponseOutput::ResponseOutputText(output_text) => output_text.text,
                                ResponseOutput::ResponseOutputRefusal(refusal) => refusal.refusal,
                            },
                        )),
                        reasoning_content: None,
                        transcript: None,
                        created_at: response.created_at,
                        input_tokens,
                        output_tokens,
                        tool_calls: None,
                    });
                }
                OutputItem::Reasoning(reason) => {
                    if output_reasoning.is_some() {
                        log::warn!("multiple output reasoning messages in response");
                        continue;
                    }
                    output_reasoning = Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: None,
                        reasoning_content: reason
                            .content
                            .and_then(|content_vec| content_vec.into_iter().next())
                            .map(|content| content.text)
                            .or(Some(String::new())),
                        transcript: None,
                        created_at: response.created_at,
                        input_tokens,
                        output_tokens,
                        tool_calls: None,
                    });
                }
                OutputItem::ImageGenerationCall(call) => {
                    if output_img.is_some() {
                        log::warn!("multiple output image generation in response");
                        continue;
                    }
                    output_img = Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: Some(Content::new(ContentType::Image, call.result)),
                        reasoning_content: None,
                        transcript: None,
                        created_at: response.created_at,
                        input_tokens,
                        output_tokens,
                        tool_calls: None,
                    });
                }
                OutputItem::FunctionCall(c) => {
                    let new_call = ToolCallRes {
                        call_id: c.call_id,
                        name: c.name,
                        arguments: c.arguments,
                    };
                    match &mut output_toolcalls {
                        Some(call) => match &mut call.tool_calls {
                            Some(tool_calls) => tool_calls.push(new_call),
                            None => call.tool_calls = Some(vec![new_call]),
                        },
                        None => {
                            output_toolcalls = Some(Message {
                                role: Role::Assistant,
                                raw_content: None,
                                content: None,
                                reasoning_content: None,
                                transcript: None,
                                created_at: response.created_at,
                                input_tokens,
                                output_tokens,
                                tool_calls: Some(vec![new_call]),
                            });
                        }
                    }
                }
                _ => {
                    break;
                }
            }
        }

        if let Some(toolcalls) = output_toolcalls {
            return Ok(toolcalls);
        }
        if let Some(img) = output_img {
            return Ok(img);
        }
        if let Some(reasoning) = output_reasoning {
            return Ok(reasoning);
        }
        if let Some(msg) = output_msg {
            return Ok(msg);
        }
        Ok(Message::default_assistant())
    }
    fn parse_stream_chunk(&self, data: Bytes) -> Result<Vec<Message>, AdaptorError> {
        let blocks = SseBlock::parse_all(data);
        blocks
            .into_iter()
            .filter_map(|block| {
                let block_data = block.data?;
                if block_data.is_empty() {
                    return None;
                }
                let response: ResponseStream = match serde_json::from_str(&block_data) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e.into())),
                };
                let result = match response.r#type.as_ref() {
                    "response.completed" | "response.failed" | "response.incomplete" => {
                        if let Some(resp) = response.response {
                            let (input_tokens, output_tokens) = resp
                                .usage
                                .map(|i| (i.input_tokens, i.output_tokens))
                                .unwrap_or((0, 0));

                            Ok(Some(Message {
                                role: Role::Assistant,
                                raw_content: None,
                                content: None,
                                reasoning_content: None,
                                transcript: None,
                                created_at: resp.created_at,
                                input_tokens,
                                output_tokens,
                                tool_calls: None,
                            }))
                        } else {
                            Err(AdaptorError::Transfer("no data in response".into()))
                        }
                    }

                    "response.output_item.added" => match response.item {
                        Some(item) => match item {
                            OutputItem::FunctionCall(call) => Ok(Some(Message {
                                role: Role::Assistant,
                                raw_content: None,
                                content: None,
                                reasoning_content: None,
                                transcript: None,
                                created_at: 0,
                                input_tokens: 0,
                                output_tokens: 0,
                                tool_calls: Some(vec![ToolCallRes {
                                    call_id: call.call_id,
                                    name: call.name,
                                    arguments: String::new(),
                                }]),
                            })),
                            // TODO: more
                            _ => Ok(None),
                        },
                        _ => Ok(None),
                    },

                    "response.function_call_arguments.delta" => Ok(Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: None,
                        reasoning_content: None,
                        transcript: None,
                        created_at: 0,
                        input_tokens: 0,
                        output_tokens: 0,
                        tool_calls: Some(vec![ToolCallRes {
                            call_id: String::new(),
                            name: String::new(),
                            arguments: response.delta.unwrap_or_default(),
                        }]),
                    })),

                    "response.output_text.delta" => Ok(Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: Some(Content::new(
                            ContentType::Text,
                            response.delta.unwrap_or_default(),
                        )),
                        reasoning_content: None,
                        transcript: None,
                        created_at: 0,
                        input_tokens: 0,
                        output_tokens: 0,
                        tool_calls: None,
                    })),

                    "response.reasoning_text.delta" => Ok(Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: None,
                        reasoning_content: Some(response.delta.unwrap_or_default()),
                        transcript: None,
                        created_at: 0,
                        input_tokens: 0,
                        output_tokens: 0,
                        tool_calls: None,
                    })),

                    "response.audio.delta" => Ok(Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: Some(Content::new(
                            ContentType::Audio,
                            response.delta.unwrap_or_default(),
                        )),
                        reasoning_content: None,
                        transcript: None,
                        created_at: 0,
                        input_tokens: 0,
                        output_tokens: 0,
                        tool_calls: None,
                    })),

                    "response.audio.transcript.delta" => Ok(Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: None,
                        reasoning_content: None,
                        transcript: Some(response.delta.unwrap_or_default()),
                        created_at: 0,
                        input_tokens: 0,
                        output_tokens: 0,
                        tool_calls: None,
                    })),

                    "response.image_generation_call.partial_image" => Ok(Some(Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: Some(Content::new(
                            ContentType::Image,
                            response.partial_image_b64.unwrap_or_default(),
                        )),
                        transcript: None,
                        reasoning_content: None,
                        created_at: 0,
                        input_tokens: 0,
                        output_tokens: 0,
                        tool_calls: None,
                    })),
                    _ => Ok(None),
                };

                match result {
                    Ok(Some(message)) => Some(Ok(message)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<Message>, AdaptorError>>()
    }
}
