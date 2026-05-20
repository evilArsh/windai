use super::schema::openai_responses::{
    self, FunctionCall, FunctionCallOutput, FunctionTool, InputContent, InputItem, OutputItem,
    Response, ResponseInputFile, ResponseInputImage, ResponseInputText, ResponseOutput,
    ResponseReasoning, ResponseRequest, ResponseStream, Tools,
};
use super::{Adaptor, AdaptorError, ChatAdaptor};
use crate::message::{self, Content, Message, MessageBuilder, ReqConfig};
use crate::model::AdaptorType;
use crate::provider::sse::SseBlock;
use crate::tool;
use serde_json::Value;

fn transfer_response_tools(tools: Option<&Vec<tool::Tools>>) -> Option<Vec<Tools>> {
    tools
        .map(|tools| {
            tools
                .into_iter()
                .filter_map(|tool| match tool {
                    tool::Tools::Function(func_tool) => {
                        Some(Tools::Function(FunctionTool {
                            r#type: String::from("function"),
                            name: func_tool.name.clone(),
                            description: func_tool.description.clone(),
                            parameters: func_tool.parameters.clone(),
                            // strict: tool.strict,
                            strict: None,
                            defer_loading: None,
                        }))
                    } // _ => None,
                })
                .collect::<Vec<Tools>>()
        })
        .or(None)
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
        config: &ReqConfig,
        contexts: &Vec<Message>,
        tools: Option<&Vec<tool::Tools>>,
    ) -> Result<Value, AdaptorError> {
        let tools = transfer_response_tools(tools);
        let input_messages = contexts
            .into_iter()
            .flat_map(|ctx| {
                if let Some(tools) = &ctx.tool_calls
                    && tools.len() > 0
                {
                    tools
                        .into_iter()
                        .map(|tool| {
                            InputItem::FunctionCall(FunctionCall {
                                arguments: tool.arguments.clone(),
                                call_id: tool.id.clone(),
                                name: tool.name.clone(),
                                r#type: String::from("function_call"),
                                id: None,
                                namespace: None,
                                status: None,
                            })
                        })
                        .collect::<Vec<InputItem>>()

                    // 函数调用参数上下文
                } else if ctx.is_tool_result() {
                    ctx.content
                        .iter()
                        .filter_map(|content| {
                            match content {
                                // 用户函数调用结果
                                message::Content::FunctionCall { data } => {
                                    Some(InputItem::FunctionCallOutput(FunctionCallOutput {
                                        call_id: data.id.clone(),
                                        output: data.content.clone(),
                                        r#type: String::from("function_call_output"),
                                        id: None,
                                        status: None,
                                    }))
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
                            Content::Text { data } => {
                                Some(InputContent::ResponseInputText(ResponseInputText {
                                    text: data.clone(),
                                    r#type: "input_text".to_string(),
                                }))
                            }
                            Content::Image { data } => {
                                Some(InputContent::ResponseInputImage(ResponseInputImage {
                                    detail: None,
                                    r#type: "input_image".to_string(),
                                    file_id: None,
                                    image_url: Some(data.clone()),
                                }))
                            }
                            Content::File { data } => {
                                Some(InputContent::ResponseInputFile(ResponseInputFile {
                                    r#type: "input_file".to_string(),
                                    file_data: Some(data.clone()),
                                    file_id: None,
                                    file_url: None,
                                    filename: None,
                                }))
                            }
                            Content::Audio { data } => {
                                Some(InputContent::ResponseInputFile(ResponseInputFile {
                                    r#type: "input_file".to_string(),
                                    file_data: Some(data.content.clone()),
                                    file_id: None,
                                    file_url: None,
                                    filename: None,
                                }))
                            }
                            _ => {
                                log::warn!(
                                    "message filtered, only texts messages are allowed. role: {}",
                                    ctx.role
                                );
                                None
                            }
                        })
                        .collect();
                    vec![InputItem::Message(openai_responses::Message {
                        content: texts,
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

    fn parse_response(&self, data: &[u8]) -> Result<Message, AdaptorError> {
        log::debug!(
            "[raw completion]\n{}",
            serde_json::to_string_pretty(&serde_json::from_slice::<serde_json::Value>(data)?)?
        );
        let response: Response = serde_json::from_slice(&data)?;

        let (input_tokens, output_tokens) = match response.usage {
            Some(usage) => (usage.input_tokens, usage.output_tokens),
            None => (0, 0),
        };

        let mut res = MessageBuilder::default()
            .created_at(response.created_at)
            .input_tokens(input_tokens)
            .output_tokens(output_tokens)
            .build()
            .unwrap_or_default();
        for output in response.output.into_iter() {
            match output {
                OutputItem::ResponseOutputMessage(msg) => {
                    let c = msg.content.into_iter().next().ok_or_else(|| {
                        AdaptorError::Transfer("no output message in response".into())
                    })?;
                    res.content.push(Content::new_text(match c {
                        ResponseOutput::ResponseOutputText(output_text) => output_text.text,
                        ResponseOutput::ResponseOutputRefusal(refusal) => refusal.refusal,
                    }));
                }
                OutputItem::ImageGenerationCall(call) => {
                    res.content.push(Content::new_image(call.result));
                }
                OutputItem::Reasoning(reason) => {
                    res.reasoning_content = reason
                        .content
                        .and_then(|content_vec| content_vec.into_iter().next())
                        .map(|content| content.text)
                        .or(Some(String::new()));
                }
                OutputItem::FunctionCall(c) => {
                    let new_call = tool::FunctionCall {
                        id: c.call_id,
                        name: c.name,
                        arguments: c.arguments,
                    };
                    match &mut res.tool_calls {
                        Some(tool_calls) => tool_calls.push(new_call),
                        None => res.tool_calls = Some(vec![new_call]),
                    }
                }
                _ => {
                    break;
                }
            }
        }

        Ok(res)
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

                            Ok(Some(
                                MessageBuilder::default()
                                    .created_at(resp.created_at)
                                    .input_tokens(input_tokens)
                                    .output_tokens(output_tokens)
                                    .build()
                                    .unwrap_or_default(),
                            ))
                        } else {
                            Err(AdaptorError::Transfer("no data in response".into()))
                        }
                    }

                    "response.output_item.added" => match response.item {
                        Some(item) => match item {
                            OutputItem::FunctionCall(call) => Ok(Some(
                                MessageBuilder::default()
                                    .tool_calls(vec![tool::FunctionCall {
                                        id: call.call_id,
                                        name: call.name,
                                        arguments: String::new(),
                                    }])
                                    .build()
                                    .unwrap_or_default(),
                            )),
                            // TODO: more
                            _ => Ok(None),
                        },
                        _ => Ok(None),
                    },

                    "response.function_call_arguments.delta" => Ok(Some(
                        MessageBuilder::default()
                            .tool_calls(vec![tool::FunctionCall {
                                id: String::new(),
                                name: String::new(),
                                arguments: response.delta.unwrap_or_default(),
                            }])
                            .build()
                            .unwrap_or_default(),
                    )),

                    "response.output_text.delta" => Ok(Some(
                        MessageBuilder::default()
                            .content(vec![Content::new_text(response.delta.unwrap_or_default())])
                            .build()
                            .unwrap_or_default(),
                    )),

                    "response.reasoning_text.delta" => Ok(Some(
                        MessageBuilder::default()
                            .reasoning_content(response.delta.unwrap_or_default())
                            .build()
                            .unwrap_or_default(),
                    )),

                    "response.audio.delta" => Ok(Some(
                        MessageBuilder::default()
                            .content(vec![Content::new_audio(
                                response.delta.unwrap_or_default(),
                                String::new(),
                            )])
                            .build()
                            .unwrap_or_default(),
                    )),
                    "response.image_generation_call.partial_image" => Ok(Some(
                        MessageBuilder::default()
                            .content(vec![Content::new_image(
                                response.partial_image_b64.unwrap_or_default(),
                            )])
                            .build()
                            .unwrap_or_default(),
                    )),
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
