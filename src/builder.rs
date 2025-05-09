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
    /// `auto_recovery()`, `producer_only()`, etc.) will be preserved if they
    /// differ from the default, otherwise the values from the provided `options`
    /// argument will be used.
    ///
    /// For more fine-grained control, use the individual setter methods instead.
    pub fn options(mut self, options: QueueOptions) -> Self {
        let existing = self.options; // Current options set by builder methods
        let defaults = QueueOptions::default();

        self.options = QueueOptions {
            initial_consumers: if existing.initial_consumers != defaults.initial_consumers {
                existing.initial_consumers
            } else {
                options.initial_consumers
            },
            pending_timeout: if existing.pending_timeout != defaults.pending_timeout {
                existing.pending_timeout
            } else {
                options.pending_timeout
            },
            poll_interval: if existing.poll_interval != defaults.poll_interval {
                existing.poll_interval
            } else {
                options.poll_interval
            },
            retry_config: if existing.retry_config != defaults.retry_config {
                existing.retry_config
            } else {
                options.retry_config
            },
            dlq_name: if existing.dlq_name != defaults.dlq_name {
                existing.dlq_name
            } else {
                options.dlq_name
            },
            auto_recovery: if existing.auto_recovery != defaults.auto_recovery {
                existing.auto_recovery
            } else {
                options.auto_recovery
            },
            prefetch_config: if existing.prefetch_config != defaults.prefetch_config {
                existing.prefetch_config
            } else {
                options.prefetch_config
            },
            // ... keep the existing logic for retry_sync, producer_only, delete_on_ack
            // as it already matches this pattern and the documentation.
            retry_sync: if existing.retry_sync != defaults.retry_sync {
                existing.retry_sync
            } else {
                options.retry_sync
            },
            producer_only: if existing.producer_only != defaults.producer_only {
                existing.producer_only
            } else {
                options.producer_only
            },
            delete_on_ack: if existing.delete_on_ack != defaults.delete_on_ack {
                existing.delete_on_ack
            } else {
                options.delete_on_ack
            },
        };

        self
    }

    /// Set the initial number of consumers to start with
    pub fn initial_consumers(mut self, count: usize) -> Self {
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
    /// Higher values reduce CPU usage with many consumers
    ///
    /// This initializes or updates the prefetch configuration.
    pub fn prefetch_count(mut self, count: usize) -> Self {
        let buffer_size = self.options.prefetch_config.as_ref().map_or(
            QueueOptions::default().prefetch_config.unwrap().buffer_size,
            |config| config.buffer_size,
        );
        self.options.prefetch_config = Some(PrefetchConfig {
            count,
            buffer_size,
            scaling: self.options.prefetch_config.and_then(|pc| pc.scaling), // Preserve existing scaling
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
            QueueOptions::default().prefetch_config.unwrap().count,
            |config| config.count,
        );
        self.options.prefetch_config = Some(PrefetchConfig {
            count,
            buffer_size,
            scaling: self.options.prefetch_config.and_then(|pc| pc.scaling), // Preserve existing scaling
        });

        self
    }

    /// Enable and configure auto-scaling.
    ///
    /// Requires prefetching to be enabled.
    /// Requires a `consumer_factory` to be set.
    pub fn scaling_config(
        mut self,
        min_consumers: usize,
        max_consumers: usize,
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
            self.options.auto_recovery = Some(
                self.options
                    .auto_recovery
                    .unwrap_or(QueueOptions::default().auto_recovery.unwrap_or(5000)),
            );
        } else {
            self.options.auto_recovery = None;
        }

        self
    }

    /// Disables message prefetching for this queue instance.
    ///
    /// When prefetching is disabled:
    /// - Each consumer will poll Redis directly for messages.
    /// - This may increase Redis load and CPU usage compared to prefetching,
    ///   especially with many consumers.
    /// - Auto-scaling will be disabled, as it relies on the prefetching mechanism.
    /// - Any previous calls to `.prefetch_count()`, `.buffer_size()`, or `.scaling_config()`
    ///   will have their effects nullified. Subsequent calls to those methods will
    ///   re-enable prefetching with the specified configurations.
    ///
    /// This is intended for consuming queues where direct polling is preferred.
    /// For queues that only produce messages, use `.producer_only(true)` which also
    /// disables prefetching along with all other consumer functionalities.
    pub fn disable_prefetch(mut self) -> Self {
        self.options.prefetch_config = None;

        self
    }

    /// Configures the queue instance to be producer-only.
    ///
    /// If set to `true`:
    /// - No consumers will be started by this queue instance.
    /// - Prefetching will be disabled (any `prefetch_config` will be ignored).
    /// - Scaling will be disabled (any `scaling_config` will be ignored).
    /// - Calls to `add_consumers`, `remove_consumers`, `register_consumer` on the `Queue` will fail.
    /// - Any provided `consumer_factory`, `scaling_strategy`, or `initial_consumers`
    ///   will be ignored for this instance and may produce warnings during build.
    ///
    /// This setting takes precedence. If `options()` is called later with a
    /// `QueueOptions` struct where `producer_only` is false, this explicit
    /// setting via `producer_only(true)` will still be respected.
    pub fn producer_only(mut self, producer_only: bool) -> Self {
        self.options.producer_only = producer_only;

        self
    }

    /// Finally build the queue.
    pub async fn build(self) -> RmqResult<Queue<M>> {
        let stream = self
            .stream
            .ok_or_else(|| RmqError::ConfigError("`stream` not specified".to_string()))?;
        let group = self.group;

        // If producer_only is true, warn about consumer-specific configurations
        if self.options.producer_only {
            if self.factory.is_some() {
                warn!("QueueBuilder: 'producer_only' is true, but a 'consumer_factory' was provided. It will be ignored for this queue instance.");
            }
            if self.scaling_strategy.is_some() {
                warn!("QueueBuilder: 'producer_only' is true, but a 'scaling_strategy' was provided. It will be ignored for this queue instance.");
            }
            if self.options.initial_consumers.map_or(false, |c| c > 0) {
                warn!("QueueBuilder: 'producer_only' is true, but 'initial_consumers' > 0. No consumers will be started.");
            }
            if self.options.prefetch_config.is_some() {
                warn!("QueueBuilder: 'producer_only' is true, but 'prefetch_config' was provided. Prefetching will be disabled.");
                // Queue::new will set prefetch_config to None if producer_only is true.
            }
            // No need to nullify them here, Queue::new will handle it based on options.producer_only
        } else {
            // Validations for consumer-based queue
            if let Some(count) = self.options.initial_consumers {
                if count > 0 && self.factory.is_none() {
                    return Err(RmqError::ConfigError(
                        "initial_consumers specified without providing a consumer factory or instance"
                            .to_string(),
                    ));
                }
            }
        }

        // Validate scaling configuration and get strategy (only if not producer_only)
        let mut scaling_strategy = None;

        if !self.options.producer_only {
            // Scaling is only relevant if the queue is not producer-only.
            if let Some(prefetch_config) = &self.options.prefetch_config {
                // Scaling also requires prefetching to be enabled.
                if let Some(scaling_opts) = &prefetch_config.scaling {
                    // Now, perform validations specific to scaling.
                    if scaling_opts.min_consumers > scaling_opts.max_consumers {
                        return Err(RmqError::ConfigError(format!(
                            "min_consumers ({}) cannot be greater than max_consumers ({})",
                            scaling_opts.min_consumers, scaling_opts.max_consumers
                        )));
                    }

                    if self.factory.is_none() {
                        return Err(RmqError::ConfigError(
                            "Consumer factory must be provided when scaling is enabled".to_string(),
                        ));
                    }

                    // If scaling is configured, use the strategy from the builder's `self.scaling_strategy`
                    // if it was explicitly set. Otherwise, fall back to the default scaling strategy.
                    scaling_strategy = self
                        .scaling_strategy
                        .clone()
                        .or_else(|| Some(Arc::new(DefaultScalingStrategy)));
                }
            }
        }

        let client = match self.client {
            Some(c) => c,
            None => {
                let url = self.url.ok_or_else(|| {
                    RmqError::ConfigError("No Redis client or URL provided".to_string())
                })?;
                let config = Config::from_url(&url)?;
                let new_client = Arc::new(Client::new(config, None, None, None));
                new_client.connect();

                if let Err(e) = new_client.wait_for_connect().await {
                    return Err(e.into());
                }

                new_client
            }
        };

        Queue::new(
            client,
            stream,
            group,
            self.options,     // Pass the potentially modified options
            self.factory,     // Pass factory; Queue::new will ignore if producer_only
            scaling_strategy, // Pass strategy; Queue::new will ignore if producer_only
        )
        .await
    }
}
