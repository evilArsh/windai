use std::sync::Arc;
use wind_core::WindCore;
use wind_core::models::{
    AgentInstance, AgentRole, CreateTopic, Message, Topic, UpdateMessage, UpdateTopic,
};

use crate::dto::ApproveToolCallsRequest;
use crate::dto::CreateChatRequest;
use crate::dto::{ApiResponse, map_core_error};

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

    pub async fn list_child_topics(&self, topic_id: i64) -> ApiResponse<Vec<Topic>> {
        // 先确认父话题存在，再列出其直接子话题，保持 404 语义一致
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        match self
            .core
            .storage()
            .topic()
            .list_child_topics(topic_id)
            .await
        {
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
        // delete_topics 不校验 rows-affected，先确认存在再删，保持与 get/update 一致的 404 语义
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

    /// 获取实例的全部消息
    pub async fn list_instance_messages(&self, instance_id: i64) -> ApiResponse<Vec<Message>> {
        let instance = match self.require_child_instance(instance_id).await {
            Ok(instance) => instance,
            Err(err) => return err.without_data(),
        };
        match self
            .core
            .storage()
            .message()
            .list_by_instance(instance.id)
            .await
        {
            Ok(messages) => ApiResponse::ok(messages),
            Err(err) => map_core_error(err),
        }
    }

    /// 获取话题的对话列表，内部解析该话题唯一的主实例
    ///
    /// 尚未发起过对话时主实例不存在，返回空列表
    pub async fn list_topic_messages(&self, topic_id: i64) -> ApiResponse<Vec<Message>> {
        // 先确认话题存在，避免「话题不存在」与「话题未对话」都返回空列表
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Err(err) => return map_core_error(err),
        }

        let main_id = match self
            .core
            .storage()
            .agent()
            .get_main_instance(topic_id)
            .await
        {
            Ok(Some(instance)) => instance.id,
            Ok(None) => return ApiResponse::ok(Vec::new()),
            Err(err) => return map_core_error(err),
        };

        match self
            .core
            .storage()
            .message()
            .list_by_instance(main_id)
            .await
        {
            Ok(messages) => ApiResponse::ok(messages),
            Err(err) => map_core_error(err),
        }
    }

    /// 校验实例存在且不是主实例，主实例不对外暴露
    ///
    /// 错误载体用 `ApiResponse<()>`，调用方以 `without_data()` 转成自己的 data 类型
    async fn require_child_instance(
        &self,
        instance_id: i64,
    ) -> Result<AgentInstance, ApiResponse<()>> {
        let instance = match self.core.storage().agent().get_instance(instance_id).await {
            Ok(Some(instance)) => instance,
            Ok(None) => return Err(ApiResponse::not_found("agent instance not found")),
            Err(err) => return Err(map_core_error(err)),
        };
        if instance.role == AgentRole::Main {
            return Err(ApiResponse::not_found("agent instance not found"));
        }
        Ok(instance)
    }

    pub async fn get_message(&self, message_id: i64) -> ApiResponse<Message> {
        match self.core.storage().message().get(message_id).await {
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

    pub async fn create_chat(&self, topic_id: i64, input: CreateChatRequest) -> ApiResponse<()> {
        // 先确认 topic 存在且已配置模型，再发送到 runtime（不做 get-or-create）。
        // topic 没有模型时 runtime 会在首轮初始化失败，因此这里必须在
        // 返回 `accepted: true` 之前拦下，把原因直接告诉调用方
        let topic = match self.core.storage().topic().get_topic(topic_id).await {
            Ok(Some(topic)) => topic,
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Err(e) => return map_core_error(e),
        };
        if topic.model_id.is_none() {
            return ApiResponse::bad_request("topic has no model configured");
        }
        let handle = self.core.fetch_topic(topic_id);
        match handle.create_task(input.content).await {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }

    pub async fn cancel_task(&self, topic_id: i64, instance_id: i64) -> ApiResponse<()> {
        // 先确认 topic 存在，再发送到 runtime（不做 get-or-create）
        match self.core.storage().topic().get_topic(topic_id).await {
            Ok(None) => return ApiResponse::not_found("topic not found"),
            Ok(Some(_)) => {}
            Err(e) => return map_core_error(e),
        }
        let handle = self.core.fetch_topic(topic_id);
        match handle.cancel_task(instance_id).await {
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
        // 1. 读 message 下 pending 审批；2. 校验属于 topic；3. 确定 instance_id；4. handle.approve
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
        if records.iter().any(|r| r.topic_id != topic_id) {
            return ApiResponse::not_found("approval not found for topic");
        }
        let instance_id = records[0].instance_id;
        let handle = self.core.fetch_topic(topic_id);
        match handle
            .approve(instance_id, input.allow_ids, input.deny_ids)
            .await
        {
            Ok(()) => ApiResponse::ok(()),
            Err(e) => map_core_error(e),
        }
    }
}
