// use futures::StreamExt;
// use reqwest::Method;
// use serde_json::json;
// use windai::adaptor::openai_chat::{self, ChatCompletion, ChatStreamCompletion};
// use windai::adaptor::sse;
// use windai::provider::Credentials;
// use windai::proxy::error::{ProxyError, RequestError};
// use windai::proxy::forward::{request, request_sse};

// fn get_openai_chat_request(stream: bool) -> (openai_chat::ChatCompletionRequest, Credentials) {
//     dotenvy::dotenv().ok();
//     let req_body = openai_chat::ChatCompletionRequestBuilder::default()
//         .stream(stream)
//         .temperature(1)
//         .top_p(1)
//         .frequency_penalty(0)
//         .presence_penalty(0)
//         .max_tokens(8192)
//         .model("deepseek-chat")
//         .reasoning(true)
//         .messages(vec![
//             openai_chat::ChatCompletionRequestMessageBuilder::default()
//                 .role(openai_chat::Role::System)
//                 .content(openai_chat::Content::Text(
//                     "you are a helpful assistant, and MUST answer me in Chinese".to_string(),
//                 ))
//                 .build()
//                 .unwrap(),
//             openai_chat::ChatCompletionRequestMessageBuilder::default()
//                 .role(openai_chat::Role::User)
//                 .content(openai_chat::Content::Text("who are you".to_string()))
//                 .build()
//                 .unwrap(),
//         ])
//         .build()
//         .unwrap();

//     let credential = Credentials::from_env();
//     return (req_body, credential);
// }

// #[tokio::test]
// async fn openai_chat_compatible() {
//     let (req_body, credential) = get_openai_chat_request(false);

//     let response = request(&credential.url, Method::POST, |req| {
//         let mut body_json = serde_json::to_value(&req_body).unwrap();
//         body_json["thinking"] = json!({"type":"enabled"});
//         // let req_body: openai_chat::ChatCompletionRequest = req_body.into();
//         req.bearer_auth(&credential.key).json(&body_json)
//     })
//     .await;
//     match response {
//         Ok(data) => {
//             let raw = data.bytes().await.unwrap();
//             let response: ChatCompletion = serde_json::from_slice(&raw).unwrap();
//             dbg!(&response);
//         }
//         Err(err) => {
//             dbg!(&err);
//             match err {
//                 ProxyError::Request(e) => match e {
//                     RequestError::Http { code, .. } => {
//                         if credential.key.is_empty() {
//                             assert_eq!(code, 401);
//                         } else {
//                             assert_eq!(code >= 300, true);
//                         }
//                     }
//                     _ => {}
//                 },
//                 _ => {}
//             }
//         }
//     }
// }

// #[tokio::test]
// async fn openai_chat_stream_compatible() {
//     let (req_body, credential) = get_openai_chat_request(true);

//     let response = request_sse(&credential.url, Method::POST, |req| {
//         let mut body_json = serde_json::to_value(&req_body).unwrap();
//         body_json["thinking"] = json!({"type":"enabled"});
//         // let req_body: openai_chat::ChatCompletionRequest = req_body.into();
//         req.bearer_auth(&credential.key).json(&body_json)
//     })
//     .await;
//     match response {
//         Ok(data) => {
//             let raw = data.bytes_stream();
//             tokio::pin!(raw);
//             while let Some(item) = raw.next().await {
//                 match item {
//                     Ok(item) => {
//                         let block = sse::SSEBlock::parse_all(item);
//                         for block_item in block.iter() {
//                             let s = block_item.data.as_ref().unwrap();
//                             dbg!(&s);
//                             let response: ChatStreamCompletion =
//                                 serde_json::from_str(s.as_str()).unwrap();
//                             dbg!(&response);
//                         }
//                     }
//                     Err(err) => {
//                         dbg!(&err);
//                     }
//                 }
//             }
//         }
//         Err(err) => {
//             dbg!(&err);
//             match err {
//                 ProxyError::Request(e) => match e {
//                     RequestError::Http { code, .. } => {
//                         if credential.key.is_empty() {
//                             assert_eq!(code, 401);
//                         } else {
//                             assert_eq!(code >= 300, true);
//                         }
//                     }
//                     _ => {}
//                 },
//                 _ => {}
//             }
//         }
//     }
// }
