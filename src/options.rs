pub struct QueueDefaults;

impl QueueDefaults {
    pub const POLL_INTERVAL: u64 = 100; // Default poll interval in milliseconds
    pub const DLQ_NAME: &str = "dead_letter_queue"; // Default DLQ stream name
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
    pub enable_dlq: bool,             // Toggle for using Dead-Letter Queue
    pub dlq_name: Option<String>,     // Optional DLQ stream name
    pub auto_recovery: bool,          // Automatically recover messages on startup
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            pending_timeout: None,
            retry_config: None,
            poll_interval: Some(QueueDefaults::POLL_INTERVAL),
            enable_dlq: false,
            dlq_name: Some(QueueDefaults::DLQ_NAME.to_string()),
            auto_recovery: false,
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

    pub(crate) fn dlq_name(&self) -> String {
        self.dlq_name
            .clone()
            .unwrap_or(QueueDefaults::DLQ_NAME.to_string())
    }

    pub fn retry_delay(&self) -> u64 {
        self.retry_config.as_ref().map_or(0, |cfg| cfg.retry_delay)
    }
}
