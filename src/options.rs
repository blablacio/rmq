pub struct QueueDefaults;

impl QueueDefaults {
    pub const POLL_INTERVAL: u64 = 100; // Default poll interval in milliseconds
}

#[derive(Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub retry_delay: u64, // in milliseconds
}

#[derive(Clone)]
pub struct QueueOptions {
    pub pending_timeout: Option<u64>, // Timeout after which a message is reclaimed
    pub retry_config: Option<RetryConfig>, // Retry configuration
    pub poll_interval: Option<u64>,   // Interval for queue polling
    pub dlq_name: Option<String>,     // Optional DLQ stream name
    pub auto_recovery: Option<u64>,   // Automatically recover messages on startup after timeout
    pub delete_on_ack: bool,          // Automatically delete messages from queue after ack
    pub prefetch_count: Option<u32>,  // None=disabled, Some(n)=enabled with batch size n
    pub retry_sync: RetrySyncPolicy,  // When to sync retry counts with Redis
}

#[derive(Clone, PartialEq)]
pub enum RetrySyncPolicy {
    OnEachRetry,   // Sync immediately on each retry (current behavior)
    OnAcknowledge, // Sync only when message is acknowledged
    OnShutdown,    // Sync only on queue shutdown
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            pending_timeout: None,
            retry_config: None,
            poll_interval: Some(QueueDefaults::POLL_INTERVAL),
            dlq_name: None,
            auto_recovery: None,
            delete_on_ack: false,
            prefetch_count: Some(100), // Enable prefetching with a reasonable batch size
            retry_sync: RetrySyncPolicy::OnEachRetry, // Keep current behavior as default
        }
    }
}

impl QueueOptions {
    pub fn poll_interval(&self) -> u64 {
        self.poll_interval.unwrap_or(QueueDefaults::POLL_INTERVAL)
    }

    pub(crate) fn max_retries(&self) -> Option<u32> {
        self.retry_config.as_ref().map(|c| c.max_retries)
    }

    pub fn retry_delay(&self) -> u64 {
        self.retry_config.as_ref().map_or(0, |cfg| cfg.retry_delay)
    }
}
