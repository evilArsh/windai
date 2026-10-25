use super::adapter::ChatAdapter;
use super::error::AdapterError;
use super::eventsource::Event;
use super::model::ChatModel;
use super::schema::openai_responses::{
    self, FunctionCall, FunctionCallOutput, FunctionTool, InputContent, InputItem, OutputItem,
    Response, ResponseInputFile, ResponseInputImage, ResponseInputText, ResponseOutput,
    ResponseReasoning, ResponseRequest, ResponseStream, Tools,
};
use serde_json::Value;
use wind_wire::message::{Content, Message, Role};
use wind_wire::tool;

fn transfer_response_tools(tools: Option<&[tool::Tools]>) -> Option<Vec<Tools>> {
    tools.map(|tools| {
        tools
            .iter()
            .map(|tool| match tool {
                tool::Tools::Function(func_tool) => Tools::Function(FunctionTool {
                    r#type: String::from("function"),
                    name: func_tool.name.clone(),
                    description: func_tool.description.clone(),
                    parameters: func_tool.parameters.clone(),
                    // strict: tool.strict,
                    strict: None,
                    defer_loading: None,
                }),
            })
            .collect::<Vec<Tools>>()
    })
}

pub struct OpenAIResponse;

impl ChatAdapter for OpenAIResponse {
    fn path(&self) -> &'static str {
        "/responses"
    }

    fn build_request(
        &self,
        model: &ChatModel,
        contexts: &[Message],
        tools: Option<&[tool::Tools]>,
    ) -> Result<Value, AdapterError> {
        let tools = transfer_response_tools(tools);
        let input_messages = contexts
            .iter()
            .flat_map(|ctx| {
                if let Some(tools) = &ctx.tool_calls
                    && !tools.is_empty()
                {
                    tools
                        .iter()
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
                } else if ctx.role == Role::Tool {
                    ctx.content
                        .iter()
                        .filter_map(|content| match content {
                            Content::FunctionCall { data } => {
                                Some(InputItem::FunctionCallOutput(FunctionCallOutput {
                                    call_id: data.id.clone(),
                                    output: Value::String(data.content.to_string()),
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

        let req = serde_json::to_value(&ResponseRequest {
            model: Some(model.name.clone()),
            input: input_messages,
            stream,
            temperature: None,
            top_p: None,
            max_output_tokens: None,
            parallel_tool_calls: None,
            reasoning: reasoning.map(|reason| ResponseReasoning {
                effort: Some(reason),
            }),
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
        let mut body = req;
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
        let response: Response = serde_json::from_slice(data)?;
        log::debug!(
            "[raw completion] id: {}, bytes: {}",
            response.id,
            data.len()
        );

        let (input_tokens, output_tokens) = match response.usage {
            Some(usage) => (usage.input_tokens, usage.output_tokens),
            None => (0, 0),
        };

        let mut res = Message {
            created_at: response.created_at,
            input_tokens,
            output_tokens,
            ..Default::default()
        };
        for output in response.output.into_iter() {
            match output {
                OutputItem::ResponseOutputMessage(msg) => {
                    let c = msg.content.into_iter().next().ok_or_else(|| {
                        AdapterError::Transfer("no output message in response".into())
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

    fn parse_stream_chunk(&self, event: &Event) -> Result<Option<Message>, AdapterError> {
        let response: ResponseStream = serde_json::from_str(&event.data)?;
        let result = match response.r#type.as_ref() {
            "response.completed" | "response.failed" | "response.incomplete" => {
                if let Some(resp) = response.response {
                    let (input_tokens, output_tokens) = resp
                        .usage
                        .map(|i| (i.input_tokens, i.output_tokens))
                        .unwrap_or((0, 0));

                    Ok(Some(Message {
                        created_at: resp.created_at,
                        input_tokens,
                        output_tokens,
                        ..Default::default()
                    }))
                } else {
                    Err(AdapterError::Transfer("no data in response".into()))
                }
            }

            "response.output_item.added" => match response.item {
                Some(OutputItem::FunctionCall(call)) => Ok(Some(Message {
                    tool_calls: Some(vec![tool::FunctionCall {
                        id: call.call_id,
                        name: call.name,
                        arguments: String::new(),
                    }]),
                    ..Default::default()
                })),
                // TODO: 更多类型
                _ => Ok(None),
            },

            "response.function_call_arguments.delta" => Ok(Some(Message {
                tool_calls: Some(vec![tool::FunctionCall {
                    id: String::new(),
                    name: String::new(),
                    arguments: response.delta.unwrap_or_default(),
                }]),
                ..Default::default()
            })),

            "response.output_text.delta" => Ok(Some(Message {
                content: vec![Content::new_text(response.delta.unwrap_or_default())],
                ..Default::default()
            })),

            "response.reasoning_text.delta" => Ok(Some(Message {
                reasoning_content: Some(response.delta.unwrap_or_default()),
                ..Default::default()
            })),

            "response.audio.delta" => Ok(Some(Message {
                content: vec![Content::new_audio(
                    response.delta.unwrap_or_default(),
                    String::new(),
                )],
                ..Default::default()
            })),

            "response.image_generation_call.partial_image" => Ok(Some(Message {
                content: vec![Content::new_image(
                    response.partial_image_b64.unwrap_or_default(),
                )],
                ..Default::default()
            })),

            _ => Ok(None),
        };

        match result {
            Ok(Some(message)) => Ok(Some(message)),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}
