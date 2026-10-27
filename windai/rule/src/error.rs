use evalexpr::EvalexprError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid rule: {0}")]
    InvalidRule(String),

    #[error("path: {0}")]
    Path(String),

    #[error(transparent)]
    Expr(#[from] EvalexprError),

    #[error("condition: {0}")]
    Condition(String),

    #[error("type mismatch: {0}")]
    Type(String),
}

pub type Result<T> = std::result::Result<T, Error>;
