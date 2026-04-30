// /// 创建用户消息
// /// - 将用户消息的 id 和 index 初始化为0
// fn create_user_message(
//     content: Vec<ChatInput>,
//     is_stream: bool,
//     model: &Model,
//     topic: &Topic,
//     provider: &Provider,
// ) -> ChatMessage {
//     return ChatMessage {
//         base: Message {
//             id: 0,
//             stream: is_stream,
//             from_id: None,
//             role: Role::User,
//             raw_content: String::new(),
//             content: content.into_iter().map(|c| c.to_content()).collect(),
//             reasoning_content: None,
//             transcript: None,
//             created_at: Utc::now().timestamp(),
//             model_id: model.id,
//             topic_id: topic.id,
//             index: 0,
//             is_boundary: false,
//             input_tokens: 0,
//             output_tokens: 0,
//         },
//         model_name: model.name.clone(),
//         provider_name: provider.name.clone(),
//         provider_id: provider.id,
//         adaptor: model.adaptor,
//     };
// }


// fn build_stream_context(
//     user_input: Vec<ChatInput>,
//     topic_id: i64,
//     model_id: i64,
//     config: &ChatConfigParams,
// ) -> Result<StreamContext, ChatError> {
//     let db = storage::global();
//     let is_stream = config.stream.unwrap_or(false);
//     let model = db
//         .get_model(model_id)?
//         .ok_or_else(|| ChatError::Internal(format!("cannot find model: {}", model_id)))?;

//     let chat_adaptor = get_chat_adaptor(model.adaptor);

//     let topic = db
//         .get_topic(topic_id)?
//         .ok_or_else(|| ChatError::Internal(format!("cannot find topic: {}", model.provider_id)))?;

//     let provider = db.get_provider(model.provider_id)?.ok_or_else(|| {
//         ChatError::Internal(format!("cannot find provider: {}", model.provider_id))
//     })?;

//     let mut messages = db.list_chat_messages_by_topic(topic_id)?;
//     let mut user_msg = create_user_message(user_input, is_stream, &model, &topic, &provider);
//     db.create_message(&mut user_msg.base)?;
//     let mut chat_msg = ChatMessage {
//         base: MessageBuilder::default()
//             .stream(is_stream)
//             .role(Role::Assistant)
//             .from_id(user_msg.base.id)
//             .model_id(model_id)
//             .topic_id(topic_id)
//             .is_boundary(false)
//             .build()
//             .map_err(|e| ChatError::Internal(e.to_string()))?,
//         model_name: model.name.clone(),
//         provider_name: provider.name,
//         provider_id: provider.id,
//         adaptor: model.adaptor,
//     };
//     db.create_message(&mut chat_msg.base)?;

//     messages.push(user_msg.clone());
//     let messages_contexts = filter_chat_contexts(messages, topic.max_context as usize);
//     let req_body = chat_adaptor.build_request(
//         &model.name,
//         config,
//         &messages_contexts
//             .into_iter()
//             .map(|m| m.to_context())
//             .collect(),
//     )?;
//     let api_key = db
//         .get_credentials_by_provider(model.provider_id)?
//         .into_iter()
//         .next()
//         .map(|credent| credent.key)
//         .ok_or_else(|| {
//             ChatError::Internal(format!("no credentials found for model: {}", model.name))
//         })?;

//     let base_url = provider.base_url.ok_or_else(|| {
//         ChatError::Internal(format!(
//             "base_url is not configured for provider: {}",
//             model.provider_id
//         ))
//     })?;
//     let endpoint = model
//         .endpoint
//         .unwrap_or_else(|| adaptor::get_default_endpoint(model.adaptor));

//     let url = Url::parse(&base_url)?.join(&endpoint)?;

//     Ok(StreamContext {
//         chat_msg,
//         user_msg,
//         chat_adaptor,
//         is_stream,
//         req_body,
//         url,
//         api_key,
//     })
// }
