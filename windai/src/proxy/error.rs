use std::error::Error;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Request(#[from] RequestError),

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("[request error] code: {code}: {msg}")]
    Http {
        code: u16,
        msg: String,
        #[source]
        source: Option<reqwest::Error>,
    },

    #[error("[request error] {msg}")]
    Other {
        msg: String,
        #[source]
        source: Option<reqwest::Error>,
    },
}
impl RequestError {
    pub fn from_reqwest(error: reqwest::Error) -> Self {
        match error.status() {
            Some(status) => Self::Http {
                code: status.as_u16(),
                msg: error.to_string(),
                source: Some(error),
            },
            None => match error.is_timeout() {
                true => Self::Other {
                    msg: "request timeout".to_string(),
                    source: Some(error),
                },
                false => Self::Other {
                    msg: error
                        .source()
                        .map(|e| e.to_string())
                        .unwrap_or(error.to_string()),
                    source: Some(error),
                },
            },
        }
    }
}
impl From<reqwest::Error> for ProxyError {
    fn from(err: reqwest::Error) -> Self {
        ProxyError::Request(RequestError::from_reqwest(err))
    }
}
