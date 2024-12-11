use async_trait::async_trait;
use eyre::Result;
use serde::{de::DeserializeOwned, Serialize};

use crate::delivery::Delivery;

#[async_trait]
pub trait Consumer: Send + Sync + 'static {
    type Message: Serialize + DeserializeOwned + Send + Sync + Clone;

    async fn process(&self, delivery: &mut Delivery<Self::Message>) -> Result<()>;

    /// Determines whether to retry after a failure.
    /// Default behavior:
    /// - If `retry_config` is present, retry as long as `delivery.retry_count < max_retries`.
    /// - If `retry_config` is not present, returns false (no retries).
    /// Consumers can override this method to implement custom logic.
    async fn should_retry(&self, delivery: &Delivery<Self::Message>) -> bool {
        if let Some(max_retries) = delivery.max_retries {
            delivery.retry_count < max_retries
        } else {
            false
        }
    }
}
