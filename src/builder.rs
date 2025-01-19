use fred::prelude::{Client, ClientLike, Config};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, sync::Arc};

use crate::{
    errors::{RmqError, RmqResult},
    Queue, QueueOptions, RetryConfig,
};

pub struct QueueBuilder<M> {
    url: Option<String>,
    client: Option<Arc<Client>>,
    stream: Option<String>,
    group: Option<String>,
    options: QueueOptions,
    marker: PhantomData<M>,
}

impl<M> QueueBuilder<M>
where
    M: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            url: None,
            client: None,
            stream: None,
            group: None,
            options: QueueOptions::default(),
            marker: PhantomData,
        }
    }

    /// Provide a Redis URL that will be turned into a new client.
    pub fn url(mut self, url: &str) -> Self {
        self.url = Some(url.to_owned());

        self
    }

    /// If you want to pass an already-initialized `fred::Client`.
    pub fn client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);

        self
    }

    pub fn stream(mut self, stream: &str) -> Self {
        self.stream = Some(stream.to_owned());

        self
    }

    pub fn group(mut self, group: &str) -> Self {
        self.group = Some(group.to_owned());

        self
    }

    pub fn options(mut self, options: QueueOptions) -> Self {
        self.options = options;

        self
    }

    /// Convenience: set just the pending timeout.
    pub fn pending_timeout(mut self, timeout: u64) -> Self {
        self.options.pending_timeout = Some(timeout);

        self
    }

    /// Convenience: set a retry config in one call.
    pub fn retry_config(mut self, max_retries: u32, delay_ms: u64) -> Self {
        self.options.retry_config = Some(RetryConfig {
            max_retries,
            retry_delay: delay_ms,
        });

        self
    }

    /// Convenience: set DLQ config in one call.
    pub fn with_dlq(mut self, dlq_name: &str) -> Self {
        self.options.dlq_name = Some(dlq_name.to_string());

        self
    }

    /// Convenience: set just the poll interval.
    pub fn poll_interval(mut self, ms: u64) -> Self {
        self.options.poll_interval = Some(ms);

        self
    }

    /// Finally build the queue.
    pub async fn build(self) -> RmqResult<Queue<M>> {
        let stream = self
            .stream
            .ok_or_else(|| RmqError::ConfigError("`stream` not specified".to_string()))?;
        let group = self.group;
        let options = self.options;

        let client = match self.client {
            Some(c) => c,
            None => {
                let url = self.url.ok_or_else(|| {
                    RmqError::ConfigError("No Redis client or URL provided".to_string())
                })?;
                let config = Config::from_url(&url)?;
                let new_client = Arc::new(Client::new(config, None, None, None));
                new_client.connect();
                new_client.wait_for_connect().await?;

                new_client
            }
        };

        Queue::new(client, stream, group, options).await
    }
}
