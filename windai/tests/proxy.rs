use windai::api::response::ChatMessage;
use windai::domain::adaptor::AdaptorType;
use windai::domain::chat::{MessageBuilder, Role};

#[test]
fn filter_chat_contexts() {
    fn build(id: i64, role: Role) -> ChatMessage {
        ChatMessage {
            base: MessageBuilder::default()
                .id(id)
                .index(id * 10)
                .role(role)
                .build()
                .unwrap(),
            model_name: String::new(),
            provider_name: String::new(),
            provider_id: 0,
            adaptor: AdaptorType::OpenAICompletion,
        }
    }
    let messages = vec![
        build(1, Role::User),
        build(2, Role::User),
        build(3, Role::Assistant),
        build(4, Role::User),
        build(5, Role::Assistant),
        build(6, Role::User),
        build(7, Role::Assistant),
        build(8, Role::User),
    ];
    
}
