// use futures::StreamExt;
// use wind_ai::{
//     chat::{self, ResEventStatus},
//     message::{Content, Message, ReqConfig, Role},
//     model::Model,
//     provider::adapter::{self},
// };
// #[path = "./common/lib.rs"]
// mod common;

// #[tokio::test]
// #[ignore = "need to complete .env config file"]
// async fn test_handle_chat() {
//     let env = common::load_env();

//     let chat_config = ReqConfig {
//         temperature: None,
//         top_p: None,
//         max_tokens: None,
//         stream: Some(env.test_stream),
//         presence_penalty: None,
//         frequency_penalty: None,
//         parallel_tool_calls: None,
//         reasoning: Some(true),
//     };
//     let model = Model {
//         name: env.test_model,
//         adapter: env.test_adapter,
//         endpoint: env.test_endpoint,
//     };
//     let contexts = vec![
//         Message::new_simple(
//             Role::System,
//             vec![Content::new_text(String::from(
//                 "you are a helpful assistant, response in Chinese",
//             ))],
//             None,
//         ),
//         Message::new_simple(
//             Role::User,
//             vec![Content::new_text(String::from("who are you"))],
//             None,
//         ),
//     ];
//     let mut seen_stream = false;
//     let chat_adapter = adapter::get_chat_adapter(model.adapter);
//     let req_body =
//         chat::build_request(chat_adapter.as_ref(), &model, &chat_config, &contexts, None).unwrap();

//     let res = chat::handle_chat(
//         chat_adapter.as_ref(),
//         &req_body,
//         &env.test_base_url,
//         &env.test_key,
//         None,
//     );
//     let mut res = Box::pin(res);
//     while let Some(value) = res.next().await {
//         if value.status == ResEventStatus::Partial {
//             seen_stream = true;
//         }
//         if value.error.is_some() {
//             panic!("{}", value.error.unwrap());
//         }
//     }
//     assert_eq!(chat_config.stream.unwrap(), seen_stream);
// }
