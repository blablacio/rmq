use fred::prelude::{Client, ClientLike, Config};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, sync::Arc};
use tracing::warn;

use crate::{
    consumer::Consumer,
    errors::{RmqError, RmqResult},
    options::{PrefetchConfig, ScalingConfig},
    scaling::{DefaultScalingStrategy, ScalingStrategy},
    Queue, QueueOptions, RetryConfig, RetrySyncPolicy,
};

pub struct QueueBuilder<M>
where
    M: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    url: Option<String>,
    client: Option<Arc<Client>>,
    stream: Option<String>,
    group: Option<String>,
    options: QueueOptions,
    factory: Option<Arc<dyn Fn() -> Arc<dyn Consumer<Message = M>> + Send + Sync>>,
    scaling_strategy: Option<Arc<dyn ScalingStrategy>>,
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
            factory: None,
            scaling_strategy: None,
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

    /// Sets queue options, merging with any already configured settings.
    ///
    /// This is a convenience method for bulk configuration. Settings already
    /// configured via specific builder methods (like `prefetch_count()`,
    /// `auto_recovery()`, etc.) will be preserved and take precedence.
    ///
    /// For more fine-grained control, use the individual setter methods instead.
    pub fn options(mut self, options: QueueOptions) -> Self {
        // Save existing settings that might have been set via builder methods
        let existing = self.options;

        // Update with provided options, but preserve any explicitly set values
        self.options = QueueOptions {
            // For simple Option<T> fields, use existing value if it's Some, otherwise use new value
            initial_consumers: existing.initial_consumers.or(options.initial_consumers),
            pending_timeout: existing.pending_timeout.or(options.pending_timeout),
            poll_interval: existing.poll_interval.or(options.poll_interval),
            retry_config: existing.retry_config.or(options.retry_config),
            dlq_name: existing.dlq_name.or(options.dlq_name),
            auto_recovery: existing.auto_recovery.or(options.auto_recovery),

            // For prefetch_config, more complex merging might be needed
            prefetch_config: match (existing.prefetch_config, options.prefetch_config) {
                // If existing has value, preserve it (builder methods took precedence)
                (Some(config), _) => Some(config),
                // Otherwise use the new value
                (None, new_config) => new_config,
            },

            // For non-Option fields, prefer existing value if it was explicitly changed from default
            retry_sync: if existing.retry_sync != QueueOptions::default().retry_sync {
                existing.retry_sync
            } else {
                options.retry_sync
            },

            // Add other fields as needed
            ..options
        };

        self
    }

    /// Set the initial number of consumers to start with
    pub fn initial_consumers(mut self, count: u32) -> Self {
        self.options.initial_consumers = Some(count);

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

    /// Set the prefetch count - how many messages to prefetch from Redis
    ///
    /// - A value of 1 disables prefetching (direct Redis polling)
    /// - Values above 1 enable prefetching with the specified batch size
    /// - Higher values reduce CPU usage with many consumers
    ///
    /// This initializes or updates the prefetch configuration.
    pub fn prefetch_count(mut self, count: u32) -> Self {
        let buffer_size = self.options.prefetch_config.as_ref().map_or(
            QueueOptions::default().prefetch_config.unwrap().buffer_size, // Use default buffer size if not set
            |config| config.buffer_size,
        );
        self.options.prefetch_config = Some(PrefetchConfig {
            count,
            buffer_size,
            scaling: None,
        });

        self
    }

    /// Set the consumer buffer size when prefetching is enabled.
    ///
    /// This determines the capacity of the channel buffer for each consumer.
    ///
    /// This initializes or updates the prefetch configuration.
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        let count = self.options.prefetch_config.as_ref().map_or(
            QueueOptions::default().prefetch_config.unwrap().count, // Use default count if not set
            |config| config.count,
        );
        self.options.prefetch_config = Some(PrefetchConfig {
            count,
            buffer_size,
            scaling: None,
        });

        self
    }

    /// Enable and configure auto-scaling.
    ///
    /// Requires prefetching to be enabled.
    /// Requires a `consumer_factory` to be set.
    pub fn scaling_config(
        mut self,
        min_consumers: u32,
        max_consumers: u32,
        scale_interval: u64,
    ) -> Self {
        let mut config = self
            .options
            .prefetch_config
            .unwrap_or_else(|| QueueOptions::default().prefetch_config.unwrap());

        config.scaling = Some(ScalingConfig {
            min_consumers,
            max_consumers,
            scale_interval,
        });
        self.options.prefetch_config = Some(config);

        self
    }

    /// Provide a factory function to create new consumer instances for auto-scaling.
    ///
    /// This is required if auto-scaling is enabled.
    /// The factory itself should be wrapped in an Arc.
    pub fn with_factory<F, C>(mut self, factory: F) -> Self
    where
        // F is a closure that returns a concrete Consumer type C
        F: Fn() -> C + Send + Sync + 'static,
        // C is the concrete Consumer type
        C: Consumer<Message = M> + Send + Sync + 'static,
    {
        if self.factory.is_some() {
            warn!("with_factory called multiple times. Overwriting previous factory.");
        }

        // Create a new closure that calls the user's closure and wraps the result
        let wrapped_factory = Arc::new(move || {
            let consumer_instance: C = factory();
            // Wrap the concrete instance in Arc and cast to Arc<dyn Consumer<...>>
            Arc::new(consumer_instance) as Arc<dyn Consumer<Message = M>>
        });

        self.factory = Some(wrapped_factory);

        self
    }

    /// Sets a template consumer instance to be cloned for creating new consumers during auto-scaling.
    ///
    /// The provided instance must implement `Clone`.
    pub fn with_instance<C>(mut self, instance: C) -> Self
    where
        C: Consumer<Message = M> + Clone + Send + Sync + 'static,
    {
        if self.factory.is_some() {
            warn!("with_instance called after with_factory or multiple times. Overwriting previous factory.");
        }

        // Create a factory closure that clones the provided instance
        let factory = Arc::new(move || {
            // Clone the captured instance each time the factory is called
            let cloned_instance = instance.clone();
            // Wrap the cloned instance in Arc and cast to the trait object
            Arc::new(cloned_instance) as Arc<dyn Consumer<Message = M>>
        });

        // Store the cloning factory closure
        self.factory = Some(factory);

        self
    }

    /// Provide a custom scaling strategy.
    ///
    /// If not provided and scaling is enabled, `DefaultScalingStrategy` will be used.
    pub fn scaling_strategy(mut self, strategy: impl ScalingStrategy + 'static) -> Self {
        self.scaling_strategy = Some(Arc::new(strategy));

        self
    }

    /// Convenience: set the retry sync policy.
    pub fn retry_sync(mut self, policy: RetrySyncPolicy) -> Self {
        self.options.retry_sync = policy;

        self
    }

    /// Set auto-recovery timeout in milliseconds.
    pub fn auto_recovery(mut self, timeout_ms: u64) -> Self {
        self.options.auto_recovery = Some(timeout_ms);

        self
    }

    /// Enable/disable auto-recovery.
    pub fn with_auto_recovery(mut self, enabled: bool) -> Self {
        if enabled {
            // Use default value if enabling without specific timeout
            self.options.auto_recovery = Some(5000); // Default value
        } else {
            self.options.auto_recovery = None;
        }

        self
    }

    /// Finally build the queue.
    pub async fn build(self) -> RmqResult<Queue<M>> {
        let stream = self
            .stream
            .ok_or_else(|| RmqError::ConfigError("`stream` not specified".to_string()))?;
        let group = self.group;
        let options = self.options;
        let factory = self.factory.clone(); // Store factory reference for later use

        // Validate that if initial_consumers is set, we have a factory
        if let Some(count) = options.initial_consumers {
            if count > 0 && factory.is_none() {
                return Err(RmqError::ConfigError(
                    "initial_consumers specified without providing a consumer factory or instance"
                        .to_string(),
                ));
            }
        }

        // Validate scaling configuration and get strategy
        let scaling_strategy = if let Some(prefetch_config) = &options.prefetch_config {
            if let Some(scaling) = &prefetch_config.scaling {
                // Check min/max relationship
                if scaling.min_consumers > scaling.max_consumers {
                    return Err(RmqError::ConfigError(format!(
                        "min_consumers ({}) cannot be greater than max_consumers ({})",
                        scaling.min_consumers, scaling.max_consumers
                    )));
                }

                // Scaling enabled, factory is required
                if factory.is_none() {
                    return Err(RmqError::ConfigError(
                        "Consumer factory must be provided when scaling is enabled".to_string(),
                    ));
                }

                // Use default strategy if none provided
                Some(
                    self.scaling_strategy
                        .unwrap_or_else(|| Arc::new(DefaultScalingStrategy)),
                )
            } else {
                // Scaling disabled
                None
            }
        } else {
            // Prefetch disabled (implies scaling disabled)
            None
        };

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

        // Pass the factory to Queue::new regardless of scaling configuration
        Queue::new(
            client,
            stream,
            group,
            options,
            factory, // Always pass the factory if it exists
            scaling_strategy,
        )
        .await
    }
}
