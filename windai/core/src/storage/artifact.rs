use crate::db::DbPool;

pub struct AgentArtifactStorage {
    db: DbPool,
}

impl AgentArtifactStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    pub fn pool(&self) -> &DbPool {
        &self.db
    }
}
