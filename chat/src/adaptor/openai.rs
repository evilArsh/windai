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
    AdaptorType, Content, ContentType, Context, Message, ReqConfig, Role, ToolCallInfo,
    ToolCallSchema,
};
use bytes::Bytes;
use serde_json::{Value, json};

pub struct OpenAICompletionAdaptor;

/// 只转换成 [Tools::Function]
fn transfer_response_tools(tools: Option<Vec<ToolCallSchema>>) -> Option<Vec<Tools>> {
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
        created_at: i64,
    ) -> Result<Message, AdaptorError> {
        let content = msg.content.unwrap_or(String::new());
        let (content_type, transcript, content) = match msg.audio {
            Some(audio) => (ContentType::Audio, audio.transcript, audio.data),
            None => (ContentType::Text, None, content),
        };
        Ok(Message {
            role: msg.role.unwrap_or(Role::Assistant),
            raw_content,
            reasoning_content: Some(msg.reasoning_content.unwrap_or(String::new())),
            transcript,
            input_tokens: 0,
            output_tokens: 0,
            created_at,
            content: Some(Content::new(content_type, content)),
            tool_calls: match msg.tool_calls {
                Some(tools) => tools
                    .into_iter()
                    .map(|tool| match tool {
                        openai_completion::ChatCompletionMessageToolCall::Function(tool) => {
                            let func = tool.function.unwrap_or_default();
                            Some(ToolCallInfo {
                                call_id: tool.id.unwrap_or_default(),
                                name: func.name.unwrap_or_default(),
                                arguments: func.arguments.unwrap_or_default(),
                            })
                        }
                        openai_completion::ChatCompletionMessageToolCall::Custom(tool) => {
                            Some(ToolCallInfo {
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
                if ctx.role == Role::Tool
                    && let Some(tool_id) = ctx.tool_call_id
                {
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
                        reasoning_content: None,
                        name: None,
                        audio: None,
                        tool_calls: None,
                        tool_call_id: Some(tool_id),
                    }
                } else if ctx.role == Role::Assistant
                    && let Some(tools) = ctx.tool_calls
                {
                    // 模型返回的函数调用参数
                    ChatCompletionMessageParam {
                        content: openai_completion::Content::Text(String::new()),
                        role: Role::Assistant,
                        name: None,
                        audio: None,
                        reasoning_content: ctx.reasoning_content,
                        tool_calls: Some(
                            tools
                                .into_iter()
                                .map(|tool| {
                                    ChatCompletionMessageToolCall::Function(
                                        ChatCompletionMessageFunctionToolCall {
                                            id: Some(tool.call_id),
                                            function: Some(
                                                ChatCompletionMessageFunctionToolCallFunction {
                                                    name: Some(tool.name),
                                                    arguments: Some(tool.arguments),
                                                },
                                            ),
                                            r#type: Some(String::from("function")),
                                        },
                                    )
                                })
                                .collect(),
                        ),
                        tool_call_id: None,
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
                        reasoning_content: None,
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
        let (input_tokens, output_tokens) = match completion.usage {
            Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
            None => (0, 0),
        };
        if let Some(choice) = completion.choices.into_iter().next() {
            let mut msg = self.parse_common(choice.message, None, completion.created)?;
            msg.input_tokens = input_tokens;
            msg.output_tokens = output_tokens;
            msg.created_at = completion.created;
            Ok(msg)
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
                let (input_tokens, output_tokens) = match completion.usage {
                    Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
                    None => (0, 0),
                };
                if let Some(choice) = completion.choices.into_iter().next() {
                    let mut msg = match self.parse_common(choice.delta, None, completion.created) {
                        Ok(r) => r,
                        Err(e) => return Some(Err(e)),
                    };
                    msg.input_tokens = input_tokens;
                    msg.output_tokens = output_tokens;
                    Some(Ok(msg))
                } else {
                    let mut msg = Message::default_assistant();
                    msg.input_tokens = input_tokens;
                    msg.output_tokens = output_tokens;
                    msg.role = Role::Assistant;
                    msg.created_at = completion.created;
                    Some(Ok(msg))
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
            .flat_map(|ctx| {
                if ctx.role == Role::Assistant
                    && let Some(tools) = ctx.tool_calls
                {
                    tools
                        .into_iter()
                        .map(|tool| {
                            InputItem::FunctionCall(FunctionCall {
                                arguments: tool.arguments,
                                call_id: tool.call_id,
                                name: tool.name,
                                r#type: String::from("function_call"),
                                id: None,
                                namespace: None,
                                status: None,
                            })
                        })
                        .collect::<Vec<InputItem>>()

                    // 函数调用参数上下文
                } else if ctx.role == Role::Tool
                    && let Some(tool_id) = ctx.tool_call_id
                {
                    // 用户函数调用结果上下文
                    vec![InputItem::FunctionCallOutput(FunctionCallOutput {
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
                    })]
                } else {
                    // 正常用户信息上下文
                    vec![InputItem::Message(openai_response::Message {
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
                    })]
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

        let mut res = Message::default_assistant();
        for output in response.output.into_iter() {
            match output {
                OutputItem::ResponseOutputMessage(msg) => {
                    let c = msg.content.into_iter().next().ok_or_else(|| {
                        AdaptorError::Transfer("no output message in response".into())
                    })?;
                    res = Message {
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
                    };
                }
                OutputItem::ImageGenerationCall(call) => {
                    res = Message {
                        role: Role::Assistant,
                        raw_content: None,
                        content: Some(Content::new(ContentType::Image, call.result)),
                        reasoning_content: None,
                        transcript: None,
                        created_at: response.created_at,
                        input_tokens,
                        output_tokens,
                        tool_calls: None,
                    };
                }
                OutputItem::Reasoning(reason) => {
                    res.reasoning_content = reason
                        .content
                        .and_then(|content_vec| content_vec.into_iter().next())
                        .map(|content| content.text)
                        .or(Some(String::new()));
                    res.created_at = response.created_at;
                    res.input_tokens = input_tokens;
                    res.output_tokens = output_tokens;
                }
                OutputItem::FunctionCall(c) => {
                    let new_call = ToolCallInfo {
                        call_id: c.call_id,
                        name: c.name,
                        arguments: c.arguments,
                    };
                    match &mut res.tool_calls {
                        Some(tool_calls) => tool_calls.push(new_call),
                        None => {
                            res.created_at = response.created_at;
                            res.tool_calls = Some(vec![new_call]);
                            res.input_tokens = input_tokens;
                            res.output_tokens = output_tokens;
                        }
                    }
                }
                _ => {
                    break;
                }
            }
        }

        Ok(res)
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
                                tool_calls: Some(vec![ToolCallInfo {
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
                        tool_calls: Some(vec![ToolCallInfo {
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
