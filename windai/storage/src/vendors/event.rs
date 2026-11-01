use serde_json::Value;
use wind_event::{Origin, Scope, Shape, Source};

use crate::error::Result;
use crate::{Storage, Topic};

impl Storage {
    /// 创建事件日志
    ///
    /// TODO: 1. scope 为global时，存topic_id=null，否则存实际topic_id,和instance_id
    /// 2. Source 应该被筛选：来自的子系统，来自的mod，因此schema不能只存一个笼统的source text
    pub async fn create_event_log(
        &self,
        name: String,
        shape: Shape,
        source: Source,
        scope: Scope,
        origin: Origin,
        payload: Value,
    ) -> Result<i64> {
        todo!()
    }
}
