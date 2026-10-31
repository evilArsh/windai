use serde_json::Value;

use crate::Storage;
use crate::error::Result;

impl Storage {
    pub async fn create_external_wait(
        &self,
        instance_id: i64,
        message_id: i64,
        payload: Value,
    ) -> Result<i64> {
        todo!()
    }

    pub async fn answer_external_wait(&self, id: i64, decisions: Value) -> Result<()> {
        todo!()
    }
}
