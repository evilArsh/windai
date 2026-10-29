use crate::error::Result;
use crate::{AgentInstance, Storage};

impl Storage {
    pub async fn get_instance_by_id(&self, _instance_id: i64) -> Result<Option<AgentInstance>> {
        todo!()
    }
}
