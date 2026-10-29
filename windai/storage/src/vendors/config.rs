use crate::error::Result;
use crate::{Storage, Topic};

impl Storage {
    pub async fn get_topic_by_id(&self, _topic_id: i64) -> Result<Option<Topic>> {
        todo!()
    }

    pub async fn get_topic_by_instance(&self, _instance_id: i64) -> Result<Option<Topic>> {
        todo!()
    }
}
