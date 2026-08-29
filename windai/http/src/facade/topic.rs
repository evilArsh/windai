use std::sync::Arc;
use wind_ai::message::ReqConfig;
use wind_core::WindCore;
use wind_core::models::{
    ChatConfig, CreateTopic, Message, Topic, UpdateAgentBinding, UpdateMessage, UpdateTopic,
};

use crate::dto::approval::ApproveToolCallsRequest;
use crate::dto::envelope::{ApiResponse, map_core_error};
use crate::dto::message::{CreateChatRequest, SubmitChatResponse};

pub struct TopicFacade {
    core: Arc<WindCore>,
}

impl TopicFacade {
    pub fn new(core: Arc<WindCore>) -> Self {
        Self { core }
    }

    pub async fn list_topics(&self) -> ApiResponse<Vec<Topic>> {
        match self.core.storage().topic().list_topics().await {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_topic(&self, input: CreateTopic) -> ApiResponse<Topic> {
        match self.core.storage().topic().create(input).await {
            Ok(t) => ApiResponse::ok(t),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_topic(&self, topic_id: i64) -> ApiResponse<Topic> {
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(Some(t)) => ApiResponse::ok(t),
            Ok(None) => ApiResponse::not_found("topic not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_topic(&self, topic_id: i64, input: UpdateTopic) -> ApiResponse<Topic> {
        if let Err(e) = self.core.storage().topic().update(topic_id, input).await {
            return map_core_error(e);
        }
        self.get_topic(topic_id).await
    }

    pub async fn delete_topic(&self, topic_id: i64) -> ApiResponse<()> {
        // delete_topics 不校验 rows-affected，先确认存在再删，保持与 get/update 一致的 404 语义。
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        match self.core.storage().topic().delete_topics(&[topic_id]).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_topic_by_binding(
        &self,
        binding_id: i64,
        parent_topic_id: i64,
    ) -> ApiResponse<Topic> {
        match self
            .core
            .storage()
            .topic()
            .get_topic_by_binding_id(parent_topic_id, binding_id)
            .await
        {
            Ok(Some(t)) => ApiResponse::ok(t),
            Ok(None) => ApiResponse::not_found("topic not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_topic_messages(&self, topic_id: i64) -> ApiResponse<Vec<Message>> {
        match self.core.storage().message().list_by_topic(topic_id).await {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn list_message_context(&self, topic_id: i64) -> ApiResponse<Vec<Message>> {
        match self.core.storage().message().list_contexts(topic_id).await {
            Ok(rows) => ApiResponse::ok(rows),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_message(&self, message_id: i64) -> ApiResponse<Message> {
        match self.core.storage().message().get(message_id).await {
            Ok(Some(m)) => ApiResponse::ok(m),
            Ok(None) => ApiResponse::not_found("message not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_message_from_message(&self, message_id: i64) -> ApiResponse<Message> {
        match self.core.storage().message().get_from_msg(message_id).await {
            Ok(Some(m)) => ApiResponse::ok(m),
            Ok(None) => ApiResponse::not_found("message not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn update_message(
        &self,
        message_id: i64,
        input: UpdateMessage,
    ) -> ApiResponse<Message> {
        if let Err(e) = self
            .core
            .storage()
            .message()
            .update(message_id, input)
            .await
        {
            return map_core_error(e);
        }
        self.get_message(message_id).await
    }

    pub async fn create_chat(
        &self,
        topic_id: i64,
        input: CreateChatRequest,
    ) -> ApiResponse<SubmitChatResponse> {
        // 先确认 topic 存在，再发送到 runtime（不做 get-or-create）。
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        let handle = self.core.fetch_topic(topic_id);
        match handle.create_chat(input.content).await {
            Ok(()) => ApiResponse::ok(SubmitChatResponse { accepted: true }),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn cancel_task(&self, topic_id: i64, binding_id: i64) -> ApiResponse<()> {
        // 先确认 topic 存在，再发送到 runtime（不做 get-or-create）。
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        let handle = self.core.fetch_topic(topic_id);
        match handle.cancel_task(binding_id).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn approve_tool_calls(
        &self,
        topic_id: i64,
        message_id: i64,
        input: ApproveToolCallsRequest,
    ) -> ApiResponse<()> {
        // 1. 读 message 下 pending 审批；2. 校验属于 topic；3. 确定 binding_id；4. handle.approve。
        let records = match self
            .core
            .storage()
            .approval()
            .list_pending_by_message(message_id)
            .await
        {
            Ok(r) => r,
            Err(e) => return map_core_error(e),
        };
        if records.is_empty() {
            return ApiResponse::not_found("no pending approvals");
        }
        if records.iter().any(|r| r.parent_topic_id != topic_id) {
            return ApiResponse::not_found("approval not found for topic");
        }
        let binding_id = records[0].binding_id;
        let handle = self.core.fetch_topic(topic_id);
        match handle
            .approve(binding_id, input.allow_ids, input.deny_ids)
            .await
        {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn get_chat_config(&self, binding_id: i64) -> ApiResponse<ChatConfig> {
        let binding = match self.core.storage().agent().get_binding(binding_id).await {
            Ok(Some(b)) => b,
            Ok(None) => return ApiResponse::not_found("binding not found"),
            Err(e) => return map_core_error(e),
        };
        let Some(config_id) = binding.chat_config_id else {
            return ApiResponse::not_found("chat config not found");
        };
        match self.core.storage().topic().get_chat_config(config_id).await {
            Ok(Some(c)) => ApiResponse::ok(c),
            Ok(None) => ApiResponse::not_found("chat config not found"),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn create_chat_config(
        &self,
        binding_id: i64,
        input: ReqConfig,
    ) -> ApiResponse<ChatConfig> {
        // 先确认 binding 存在，避免插入 chat_config 后 binding 缺失导致孤儿行。
        match self.core.storage().agent().get_binding(binding_id).await {
            Ok(None) => return ApiResponse::not_found("binding not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        let created = match self.core.storage().topic().create_chat_config(input).await {
            Ok(c) => c,
            Err(e) => return map_core_error(e),
        };
        let update = UpdateAgentBinding {
            chat_config_id: Some(created.id),
            ..Default::default()
        };
        if let Err(e) = self
            .core
            .storage()
            .agent()
            .update_binding(binding_id, update)
            .await
        {
            return map_core_error(e);
        }
        ApiResponse::ok(created)
    }

    pub async fn update_chat_config(
        &self,
        binding_id: i64,
        input: ReqConfig,
    ) -> ApiResponse<ChatConfig> {
        let binding = match self.core.storage().agent().get_binding(binding_id).await {
            Ok(Some(b)) => b,
            Ok(None) => return ApiResponse::not_found("binding not found"),
            Err(e) => return map_core_error(e),
        };
        let Some(config_id) = binding.chat_config_id else {
            return ApiResponse::not_found("chat config not found");
        };
        if let Err(e) = self
            .core
            .storage()
            .topic()
            .update_chat_config(config_id, input)
            .await
        {
            return map_core_error(e);
        }
        self.get_chat_config(binding_id).await
    }
}
