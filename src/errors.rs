use std::{error::Error as StdError, fmt, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum RmqError {
    #[error("Redis connection error: {0}")]
    ConnectionError(String),

    #[error("Message serialization error: {0}")]
    SerializationError(String),

    #[error("Message deserialization error: {0}")]
    DeserializationError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

pub type RmqResult<T> = Result<T, RmqError>;

impl From<fred::error::Error> for RmqError {
    fn from(err: fred::error::Error) -> Self {
        RmqError::ConnectionError(err.to_string())
    }
}

impl From<serde_json::Error> for RmqError {
    fn from(err: serde_json::Error) -> Self {
        match err.classify() {
            serde_json::error::Category::Io => RmqError::SerializationError(err.to_string()),
            serde_json::error::Category::Syntax | serde_json::error::Category::Data => {
                RmqError::DeserializationError(err.to_string())
            }
            serde_json::error::Category::Eof => RmqError::DeserializationError(err.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerError(Arc<dyn StdError + Send + Sync>);

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl StdError for ConsumerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl ConsumerError {
    pub fn new<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        ConsumerError(Arc::new(error))
    }
}

impl From<RmqError> for ConsumerError {
    fn from(error: RmqError) -> Self {
        ConsumerError(Arc::new(error))
    }
}
