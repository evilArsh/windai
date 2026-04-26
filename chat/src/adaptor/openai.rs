use crate::adaptor::openai_completion::{
    ChatCompletion, ChatCompletionContentPartImage, ChatCompletionMessage,
    ChatCompletionMessageParam, ChatCompletionRequest, ChatStreamCompletion, ContentObject,
    FileContentPart,
};
use crate::adaptor::openai_response::{
    self, InputContent, InputItem, OutputItem, Response, ResponseInputFile, ResponseInputImage,
    ResponseInputText, ResponseOutput, ResponseReasoning, ResponseRequest, ResponseStream,
};
use crate::adaptor::sse::SseBlock;
use crate::adaptor::{Adaptor, AdaptorError, ChatAdaptor, openai_completion};
use crate::api::request::ChatConfig;
use crate::api::request::ChatMessageContext;
use crate::api::response::ChatMessageBase;
use bytes::Bytes;
use serde_json::{Value, json};
use windai_domain::adaptor::AdaptorType;
use windai_domain::chat::ContentType;
use windai_domain::chat::Role;

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
        raw_content: String,
        usage: Option<openai_completion::TokenUsage>,
        created_at: i64,
    ) -> Result<ChatMessageBase, AdaptorError> {
        let content = msg.content.unwrap_or(String::new());
        let (content_type, transcript, content) = match msg.audio {
            Some(audio) => (ContentType::Audio, audio.transcript, audio.data),
            None => (ContentType::Text, None, content),
        };
        let (input_tokens, output_tokens) = match usage {
            Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
            None => (0, 0),
        };
        Ok(ChatMessageBase {
            role: msg.role.unwrap_or(Role::Assistant),
            raw_content,
            content,
            reasoning_content: Some(msg.reasoning_content.unwrap_or(String::new())),
            content_type,
            transcript,
            stream: false,
            input_tokens,
            output_tokens,
            created_at,
        })
    }
}
impl ChatAdaptor for OpenAICompletionAdaptor {
    fn build_request(
        &self,
        model_name: &str,
        config: &ChatConfig,
        contexts: &Vec<ChatMessageContext>,
    ) -> Result<Value, AdaptorError> {
        let input_messages = contexts
            .iter()
            .map(|ctx| {
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
                return ChatCompletionMessageParam {
                    content: contents,
                    role: ctx.role,
                    name: None,
                    audio: None,
                    tool_calls: None,
                    tool_call_id: None,
                };
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
            tools: None,
            top_logprobs: None,
            verbosity: None,
            web_search_options: None,
        };
        Ok(serde_json::to_value(&req)?)
    }

    fn parse_response(&self, data: Bytes) -> Result<ChatMessageBase, AdaptorError> {
        let completion: ChatCompletion = serde_json::from_slice(&data)?;
        let raw_content = String::new();
        let choice = completion
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| AdaptorError::Transfer("no choices in response".into()))?;

        self.parse_common(
            choice.message,
            raw_content,
            completion.usage,
            completion.created,
        )
    }

    fn parse_stream_chunk(&self, data: Bytes) -> Result<Vec<ChatMessageBase>, AdaptorError> {
        let blocks = SseBlock::parse_all(data);
        blocks
            .into_iter()
            .map(|block| {
                let completion: ChatStreamCompletion = serde_json::from_str(
                    &block
                        .data
                        .ok_or(AdaptorError::Transfer("empty sse block".into()))?,
                )?;
                let raw_content = String::new();
                let choice = completion
                    .choices
                    .into_iter()
                    .next()
                    .ok_or_else(|| AdaptorError::Transfer("no choices in response".into()))?;

                self.parse_common(
                    choice.delta,
                    raw_content,
                    completion.usage,
                    completion.created,
                )
            })
            .collect::<Result<Vec<ChatMessageBase>, AdaptorError>>()
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
        config: &ChatConfig,
        contexts: &Vec<ChatMessageContext>,
    ) -> Result<Value, AdaptorError> {
        let input_messages = contexts
            .iter()
            .map(|ctx| {
                let content: Vec<InputContent> = ctx
                    .content
                    .iter()
                    .map(|c| match c.content_type {
                        ContentType::Text => InputContent::ResponseInputText(ResponseInputText {
                            text: c.content.clone(),
                            r#type: "input_text".to_string(),
                        }),
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
                    .collect::<Vec<InputContent>>();

                Ok(openai_response::Message {
                    content,
                    role: ctx.role,
                    phase: None,
                    status: None,
                    r#type: Some("message".to_string()),
                })
            })
            .collect::<Result<Vec<openai_response::Message>, AdaptorError>>()?;

        let req = serde_json::to_value(&ResponseRequest {
            model: Some(model_name.to_string()),
            input: Some(InputItem::Message(input_messages)),
            stream: config.stream,
            temperature: config.temperature,
            top_p: config.top_p,
            max_output_tokens: config.max_tokens,
            parallel_tool_calls: config.parallel_tool_calls,
            reasoning: config.reasoning.map(|r| ResponseReasoning {
                effort: if r {
                    Some("medium".to_string())
                } else {
                    Some("none".to_string())
                },
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
            tools: None,
            top_logprobs: None,
            truncation: None,
        })?;
        Ok(req)
    }

    fn parse_response(&self, data: Bytes) -> Result<ChatMessageBase, AdaptorError> {
        let response: Response = serde_json::from_slice(&data)?;
        let raw_content = String::new();

        let mut role = Role::Assistant;
        let mut content = String::new();
        let mut reasoning_content: Option<String> = None;
        let mut content_type = ContentType::Text;

        let output = response
            .output
            .into_iter()
            .next()
            .ok_or_else(|| AdaptorError::Transfer("no output in response".into()))?;

        match output {
            OutputItem::ResponseOutputMessage(msgs) => {
                let msg = msgs.into_iter().next().ok_or_else(|| {
                    AdaptorError::Transfer("no output message in response".into())
                })?;
                role = msg.role;
                content = match msg.content {
                    ResponseOutput::ResponseOutputText(output_text) => output_text.text,
                    ResponseOutput::ResponseOutputRefusal(refusal) => refusal.refusal,
                };
            }
            OutputItem::Reasoning(reasons) => {
                role = Role::Assistant;
                reasoning_content = reasons
                    .into_iter()
                    .next()
                    .and_then(|reason_obj| reason_obj.content)
                    .and_then(|content_vec| content_vec.into_iter().next())
                    .map(|content| content.text)
                    .or(Some(String::new()));
            }
            OutputItem::ImageGenerationCall(calls) => {
                role = Role::Assistant;
                content_type = ContentType::Image;
                content = calls
                    .into_iter()
                    .next()
                    .map(|i| i.result)
                    .unwrap_or_default();
            }
            _ => {}
        }

        let (input_tokens, output_tokens) = match response.usage {
            Some(usage) => (usage.input_tokens, usage.output_tokens),
            None => (0, 0),
        };

        Ok(ChatMessageBase {
            stream: false,
            role,
            raw_content,
            content,
            reasoning_content,
            transcript: None,
            content_type,
            created_at: response.created,
            input_tokens,
            output_tokens,
        })
    }

    fn parse_stream_chunk(&self, data: Bytes) -> Result<Vec<ChatMessageBase>, AdaptorError> {
        let blocks = SseBlock::parse_all(data);
        blocks
            .into_iter()
            .map(|block| {
                let response: ResponseStream = serde_json::from_str(
                    &block
                        .data
                        .ok_or_else(|| AdaptorError::Transfer("empty sse block".into()))?,
                )?;
                let raw_content = String::new();
                let mut role = Role::Assistant;
                let mut content = String::new();
                let mut reasoning_content: Option<String> = None;
                let mut content_type = ContentType::Text;
                let mut input_tokens = 0;
                let mut output_tokens = 0;
                let mut created_at = 0;

                match response.r#type.as_ref() {
                    "response.completed" | "response.failed" | "response.incomplete" => {
                        if let Some(resp) = response.response {
                            role = Role::Assistant;
                            (input_tokens, output_tokens) = resp
                                .usage
                                .map(|i| (i.input_tokens, i.output_tokens))
                                .unwrap_or((0, 0));
                            created_at = resp.created;
                        }
                    }

                    "response.output_text.delta" | "response.function_call_arguments.delta" => {
                        role = Role::Assistant;
                        content = response.delta.unwrap_or_default();
                    }

                    "response.reasoning_text.delta" => {
                        role = Role::Assistant;
                        reasoning_content = Some(response.delta.unwrap_or_default());
                    }

                    "response.audio.transcript.delta" => {
                        role = Role::Assistant;
                        content_type = ContentType::Audio;
                    }

                    "response.image_generation_call.partial_image" => {
                        role = Role::Assistant;
                        content_type = ContentType::Image;
                        content = response.partial_image_b64.unwrap_or_default();
                    }

                    _ => {}
                }
                Ok(ChatMessageBase {
                    stream: true,
                    role,
                    raw_content,
                    content,
                    reasoning_content,
                    transcript: None,
                    content_type,
                    created_at,
                    input_tokens,
                    output_tokens,
                })
            })
            .collect::<Result<Vec<ChatMessageBase>, AdaptorError>>()
    }
}
