use super::{AdaptorError, AdaptorType, ChatAdaptor};
use crate::{
    adaptor::Adaptor,
    dto::{
        chat::{MessageCommon, MessageResponse, RequestConfig},
        openai_completion, openai_response,
    },
    models::ContentType,
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::str::FromStr;

pub struct OpenAICompletionAdaptor;

impl Adaptor for OpenAICompletionAdaptor {
    fn get_type(&self) -> AdaptorType {
        AdaptorType::OpenAICompletion
    }
}
impl OpenAICompletionAdaptor {
    fn parse_common(
        &self,
        msg: openai_completion::ChatCompletionMessage,
        raw_content: String,
        usage: Option<openai_completion::TokenUsage>,
        created_at: i64,
    ) -> Result<MessageCommon, AdaptorError> {
        let content = msg.content.unwrap_or(String::new());
        // 如果存在音频，则将音频数据作为 content，并丢弃原始 content
        let (content_type, transcript, content) = match msg.audio {
            Some(audio) => (ContentType::Audio, audio.transcript, audio.data),
            None => (ContentType::Text, None, content),
        };
        let (input_tokens, output_tokens) = match usage {
            Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
            None => (0, 0),
        };
        Ok(MessageCommon {
            role: msg.role.unwrap_or_default().to_string(),
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
        config: &RequestConfig,
        contexts: &Vec<MessageResponse>,
    ) -> Result<Value, AdaptorError> {
        let input_messages = contexts
            .into_iter()
            .map(|m| match m.base.content_type {
                ContentType::Text => Ok(openai_completion::ChatCompletionMessageParam {
                    content: openai_completion::Content::Text(m.base.content.clone()),
                    role: openai_completion::Role::from_str(&m.base.role)?,
                    name: None,
                    audio: None,
                    tool_calls: None,
                    tool_call_id: None,
                }),
                ContentType::Audio => Ok(openai_completion::ChatCompletionMessageParam {
                    content: openai_completion::Content::Objects(vec![
                        openai_completion::ContentObject {
                            r#type: "input_audio".to_string(),
                            text: Some(m.base.content.clone()),
                            image_url: None,
                            input_audio: None,
                            file: None,
                            refusal: None,
                        },
                    ]),
                    role: openai_completion::Role::from_str(&m.base.role)?,
                    name: None,
                    audio: None,
                    tool_calls: None,
                    tool_call_id: None,
                }),
                ContentType::Image => Ok(openai_completion::ChatCompletionMessageParam {
                    content: openai_completion::Content::Objects(vec![
                        openai_completion::ContentObject {
                            r#type: "image_url".to_string(),
                            text: None,
                            image_url: Some(openai_completion::ChatCompletionContentPartImage {
                                url: m.base.content.clone(),
                                detail: Some("auto".to_string()),
                            }),
                            input_audio: None,
                            file: None,
                            refusal: None,
                        },
                    ]),
                    role: openai_completion::Role::from_str(&m.base.role)?,
                    name: None,
                    audio: None,
                    tool_calls: None,
                    tool_call_id: None,
                }),
                ContentType::File => Ok(openai_completion::ChatCompletionMessageParam {
                    content: openai_completion::Content::Objects(vec![
                        openai_completion::ContentObject {
                            r#type: "file".to_string(),
                            text: None,
                            file: Some(openai_completion::FileContentPart {
                                file_data: Some(m.base.content.clone()),
                                file_id: None,
                                filename: None,
                            }),
                            input_audio: None,
                            image_url: None,
                            refusal: None,
                        },
                    ]),
                    role: openai_completion::Role::from_str(&m.base.role)?,
                    name: None,
                    audio: None,
                    tool_calls: None,
                    tool_call_id: None,
                }),
            })
            .collect::<Result<Vec<openai_completion::ChatCompletionMessageParam>, AdaptorError>>(
            )?;

        let req = openai_completion::ChatCompletionRequest {
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
            audio: None, // TODO: 音频参数补全
            logit_bias: None,
            logprobs: None,
            metadata: None,
            modalities: None, // TODO: 音频参数补全
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
                Some(true) => Some(json!({"include_usage":true})),
                _ => None,
            },
            tool_choice: None,
            tools: None, // TODO: 函数调用参数补全
            top_logprobs: None,
            verbosity: None,
            web_search_options: None,
        };
        Ok(serde_json::to_value(&req)?)
    }

    fn parse_response(&self, data: Bytes) -> Result<MessageCommon, AdaptorError> {
        let completion: openai_completion::ChatCompletion = serde_json::from_slice(&data)?;
        // let raw_content = serde_json::to_string(&completion).unwrap_or_default();
        let raw_content = String::new();
        let choice = completion
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| AdaptorError::Transfer("no choices in response".into()))?;

        return self.parse_common(
            choice.message,
            raw_content,
            completion.usage,
            completion.created,
        );
    }
    fn parse_stream_response(&self, data: Bytes) -> Result<MessageCommon, AdaptorError> {
        let completion: openai_completion::ChatStreamCompletion = serde_json::from_slice(&data)?;
        // let raw_content = serde_json::to_string(&completion).unwrap_or_default();
        let raw_content = String::new();

        let choice = completion
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| AdaptorError::Transfer("no choices in response".into()))?;

        return self.parse_common(
            choice.delta,
            raw_content,
            completion.usage,
            completion.created,
        );
    }
}

impl Adaptor for OpenAIResponseAdaptor {
    fn get_type(&self) -> AdaptorType {
        AdaptorType::OpenAIResponse
    }
}

impl ChatAdaptor for OpenAIResponseAdaptor {
    fn build_request(
        &self,
        model_name: &str,
        config: &RequestConfig,
        contexts: &Vec<MessageResponse>,
    ) -> Result<Value, AdaptorError> {
        let input_messages = contexts
            .into_iter()
            .map(|m| {
                let content = match m.base.content_type {
                    ContentType::Text => openai_response::InputContent::ResponseInputText(vec![
                        openai_response::ResponseInputText {
                            text: m.base.content.clone(),
                            r#type: "input_text".to_string(),
                        },
                    ]),
                    ContentType::Image => openai_response::InputContent::ResponseInputImage(vec![
                        openai_response::ResponseInputImage {
                            detail: None,
                            r#type: "input_image".to_string(),
                            file_id: None,
                            image_url: Some(m.base.content.clone()),
                        },
                    ]),
                    ContentType::File | ContentType::Audio => {
                        openai_response::InputContent::ResponseInputFile(vec![
                            openai_response::ResponseInputFile {
                                r#type: "input_file".to_string(),
                                file_data: Some(m.base.content.clone()),
                                file_id: None,
                                file_url: None,
                                filename: None,
                            },
                        ])
                    }
                };
                Ok(openai_response::Message {
                    content,
                    role: openai_completion::Role::from_str(&m.base.role)?,
                    phase: None,
                    status: None,
                    r#type: Some("message".to_string()),
                })
            })
            .collect::<Result<Vec<openai_response::Message>, AdaptorError>>()?;

        let req = serde_json::to_value(&openai_response::ResponseRequest {
            model: Some(model_name.to_string()),
            input: Some(openai_response::InputItem::Message(input_messages)),
            stream: config.stream,
            temperature: config.temperature,
            top_p: config.top_p,
            max_output_tokens: config.max_tokens,
            parallel_tool_calls: config.parallel_tool_calls,
            reasoning: config
                .reasoning
                .map(|r| openai_response::ResponseReasoning {
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
            tools: None, // TODO: 函数调用参数补全
            top_logprobs: None,
            truncation: None,
        })?;
        Ok(req)
    }

    fn parse_response(&self, data: Bytes) -> Result<MessageCommon, AdaptorError> {
        let response: openai_response::Response = serde_json::from_slice(&data)?;
        // let raw_content = serde_json::to_string(&response).unwrap_or_default();
        let raw_content = String::new();

        let mut role = String::new();
        let mut content = String::new();
        let mut reasoning_content: Option<String> = None;
        let mut content_type = ContentType::Text;

        let output = response
            .output
            .into_iter()
            .next()
            .ok_or_else(|| AdaptorError::Transfer("no output in response".into()))?;

        match output {
            openai_response::OutputItem::ResponseOutputMessage(msgs) => {
                let msg = msgs.into_iter().next().ok_or_else(|| {
                    AdaptorError::Transfer("no output message in response".into())
                })?;
                role = msg.role.to_string();
                content = match msg.content {
                    openai_response::ResponseOutput::ResponseOutputText(output_text) => {
                        output_text.text
                    }
                    openai_response::ResponseOutput::ResponseOutputRefusal(refusal) => {
                        refusal.refusal
                    }
                };
            }
            openai_response::OutputItem::Reasoning(reasons) => {
                role = openai_completion::Role::Assistant.to_string();
                reasoning_content = reasons
                    .into_iter()
                    .next()
                    .and_then(|reason_obj| reason_obj.content)
                    .and_then(|content_vec| content_vec.into_iter().next())
                    .map(|content| content.text)
                    .or(Some(String::new()));
            }
            // TODO: 函数调用
            // openai_response::OutputItem::FunctionCall(calls) => {
            //     // TODO: handle function calls
            //     let _ = calls;
            // }
            // openai_response::OutputItem::FunctionCallOutput(calls) => {
            //     // TODO: handle function call outputs
            //     let _ = calls;
            // }
            openai_response::OutputItem::ImageGenerationCall(calls) => {
                role = openai_completion::Role::Assistant.to_string();
                content_type = ContentType::Image;
                content = calls
                    .into_iter()
                    .next()
                    .and_then(|i| Some(i.result))
                    .unwrap_or(String::new());
            }
            // TODO:
            _ => {}
        }

        let (input_tokens, output_tokens) = match response.usage {
            Some(usage) => (usage.input_tokens, usage.output_tokens),
            None => (0, 0),
        };

        Ok(MessageCommon {
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
    fn parse_stream_response(&self, data: Bytes) -> Result<MessageCommon, AdaptorError> {
        let response: openai_response::ResponseStream = serde_json::from_slice(&data)?;
        let raw_content = String::new();
        let mut role = String::new();
        let mut content = String::new();
        let mut reasoning_content: Option<String> = None;
        let mut content_type = ContentType::Text;
        let mut input_tokens = 0;
        let mut output_tokens = 0;
        let mut created_at = 0;

        match response.r#type.as_ref() {
            // TODO: 事件触发后会返回空的 content，reasoning_content
            "response.completed" | "response.failed" | "response.incomplete" => {
                if let Some(resp) = response.response {
                    role = openai_completion::Role::Assistant.to_string();
                    (input_tokens, output_tokens) = resp
                        .usage
                        .and_then(|i| Some((i.input_tokens, i.output_tokens)))
                        .unwrap_or((0, 0));
                    created_at = resp.created;
                }
            }

            "response.output_text.delta" | "response.function_call_arguments.delta" => {
                role = openai_completion::Role::Assistant.to_string();
                content = response.delta.unwrap_or_default();
            }

            "response.reasoning_text.delta" => {
                role = openai_completion::Role::Assistant.to_string();
                reasoning_content = Some(response.delta.unwrap_or_default());
            }

            "response.audio.transcript.delta" => {
                role = openai_completion::Role::Assistant.to_string();
                content_type = ContentType::Audio;
            }

            "response.image_generation_call.partial_image" => {
                role = openai_completion::Role::Assistant.to_string();
                content_type = ContentType::Image;
                content = response.partial_image_b64.unwrap_or_default();
            }

            _ => {}
        }
        Ok(MessageCommon {
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
    }
}
