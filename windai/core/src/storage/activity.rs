use crate::db::DbPool;

pub struct TopicActivityStorage {
    db: DbPool,
}

impl TopicActivityStorage {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }

    pub fn pool(&self) -> &DbPool {
        &self.db
    }
}
