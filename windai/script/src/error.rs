/// 脚本引擎的错误
#[derive(Debug, thiserror::Error)]
pub enum ScriptsError {
    #[error("the script engine config is invalid: {0}")]
    Config(String),
    #[error("the mod scripts do not compile: {0}")]
    Compile(String),
    #[error("the handler failed: {0}")]
    Handler(String),
    #[error("the handler did not finish in time")]
    Timeout,
    #[error("the interpreter pool is busy")]
    Busy,
    #[error("the payload exceeds {limit} bytes")]
    PayloadTooLarge { limit: usize },
    #[error("the mod is suspended after consecutive timeouts")]
    Suspended,
    #[error("the script engine failed: {0}")]
    Internal(String),
}
