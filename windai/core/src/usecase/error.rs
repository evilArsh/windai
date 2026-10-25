use crate::error::CoreError;

#[derive(Debug, thiserror::Error)]
pub enum UseCaseError {
    #[error("{entity} {id} does not exist")]
    NotFound { entity: &'static str, id: i64 },
    #[error(transparent)]
    Storage(#[from] CoreError),
}
