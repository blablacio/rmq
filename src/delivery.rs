use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use fred::prelude::{Client, StreamsInterface};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};
use tracing::{debug, error};

use crate::{errors::RmqResult, options::RetrySyncPolicy, ConsumerError};

struct DeliveryState {
    // Tracks Redis's delivery count (1-based)
    delivery_count: AtomicU32,
    error: Mutex<Option<Arc<ConsumerError>>>,
    keep_alive_stop: AtomicBool,
    keep_alive_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct Delivery<M> {
    pub(crate) client: Arc<Client>,
    pub stream: String,
    pub group: String,
    pub queue_id: String,
    pub message_id: String,
    pub message: M,
    pub max_retries: Option<u32>,
    delete_on_ack: bool,
    pub retry_sync_policy: RetrySyncPolicy,
    state: Arc<DeliveryState>,
}

impl<M> Delivery<M>
where
    M: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    pub fn new(
        client: Arc<Client>,
        stream: String,
        group: String,
        queue_id: String,
        message_id: String,
        message: M,
        max_retries: Option<u32>,
        delivery_count: u32, // Changed from retry_count
        delete_on_ack: bool,
        retry_sync_policy: RetrySyncPolicy,
    ) -> Self {
        Self {
            client,
            stream,
            group,
            queue_id,
            message_id,
            message,
            max_retries,
            delete_on_ack,
            retry_sync_policy,
            state: Arc::new(DeliveryState {
                // Store delivery count directly (no conversion)
                delivery_count: AtomicU32::new(delivery_count),
                error: Mutex::new(None),
                keep_alive_stop: AtomicBool::new(false),
                keep_alive_handle: Mutex::new(None),
            }),
        }
    }

    /// Get the current delivery count (1-based, matching Redis)
    ///
    /// This returns the raw delivery count as tracked by Redis:
    /// - 1 = first delivery
    /// - 2 = second delivery (first retry)
    /// - 3 = third delivery (second retry)
    /// and so on.
    pub fn delivery_count(&self) -> u32 {
        self.state.delivery_count.load(Ordering::Relaxed)
    }

    /// Calculate retry count from delivery count
    ///
    /// This converts the 1-based delivery count to a 0-based retry count:
    /// - delivery_count 1 → retry_count 0 (initial delivery, no retries)
    /// - delivery_count 2 → retry_count 1 (first retry)
    /// - delivery_count 3 → retry_count 2 (second retry)
    pub fn retry_count(&self) -> u32 {
        let delivery_count = self.delivery_count();
        // Ensure we don't underflow if delivery_count is 0 for some reason
        if delivery_count > 0 {
            delivery_count - 1
        } else {
            0
        }
    }

    /// Increment the delivery count and sync with Redis based on policy
    pub async fn inc_delivery_count(&self) -> RmqResult<()> {
        let new_count = self.state.delivery_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Only sync with Redis if the policy requires it
        if self.retry_sync_policy == RetrySyncPolicy::OnEachRetry {
            self.sync_delivery_count_with_redis(new_count).await?;
        }

        Ok(())
    }

    /// Synchronize the delivery count with Redis
    async fn sync_delivery_count_with_redis(&self, delivery_count: u32) -> RmqResult<()> {
        // Use XCLAIM with RETRYCOUNT option to update delivery count in Redis
        // No conversion needed since we're storing delivery_count directly
        self.client
            .xclaim::<String, _, _, _, _>(
                &self.stream,
                &self.group,
                &self.queue_id,
                0, // min-idle-time: 0 to immediately claim
                &self.message_id,
                None,                        // idle
                None,                        // time
                Some(delivery_count.into()), // retrycount = delivery_count
                false,                       // force
                true,                        // justid: retrieve only the message ID
            )
            .await?;

        Ok(())
    }

    /// Explicitly sync the current delivery count with Redis
    /// This is useful for OnShutdown policy
    pub async fn sync_delivery_count(&self) -> RmqResult<()> {
        let current_count = self.state.delivery_count.load(Ordering::Relaxed);
        self.sync_delivery_count_with_redis(current_count).await
    }

    /// Retrieve the current error, if any.
    pub async fn error(&self) -> Option<Arc<ConsumerError>> {
        self.state.error.lock().await.clone()
    }

    /// Set an error message.
    pub async fn set_error(&self, err: ConsumerError) {
        self.state.error.lock().await.replace(Arc::new(err));
    }

    /// Start the keep-alive mechanism
    pub async fn start_keep_alive(&self, interval_millis: u64) {
        // Step 1: Check if a keep-alive task is already running
        if self.state.keep_alive_handle.lock().await.is_some() {
            debug!(
                "Keep-alive task is already running for message_id: {}",
                self.message_id
            );

            return;
        }

        // Step 2: Clone necessary fields for the async task
        let client = self.client.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();
        let consumer_id = self.queue_id.clone();
        let message_id = self.message_id.clone();
        let state = self.state.clone();

        // Step 3: Spawn the keep-alive async task
        let join_handle = tokio::spawn(async move {
            while !state.keep_alive_stop.load(Ordering::Relaxed) {
                // Sleep for the specified interval
                sleep(Duration::from_millis(interval_millis)).await;

                // Execute the xclaim command to renew the claim on the message
                if let Err(e) = client
                    .xclaim::<String, _, _, _, _>(
                        &stream,
                        &group,
                        &consumer_id,
                        0, // min-idle-time: 0 to immediately reclaim if needed
                        &message_id,
                        None,  // idle
                        None,  // time
                        None,  // retrycount
                        false, // force
                        true,  // justid: retrieve only the message ID
                    )
                    .await
                {
                    error!(
                        "Failed to execute xclaim for message_id {}: {}",
                        message_id, e
                    );
                }
            }

            debug!("Keep-alive task stopped for message_id: {}", message_id);
        });

        // Step 4: Store the JoinHandle in `keep_alive_handle`
        self.state
            .keep_alive_handle
            .lock()
            .await
            .replace(join_handle);

        debug!(
            "Keep-alive task started for message_id: {} with interval: {} ms",
            self.message_id, interval_millis
        );
    }

    /// Stop the keep-alive mechanism
    pub async fn stop_keep_alive(&self) {
        // Signal the keep-alive task to stop
        self.state.keep_alive_stop.store(true, Ordering::Relaxed);

        // Acquire a lock to access `keep_alive_handle`
        let mut handle_lock = self.state.keep_alive_handle.lock().await;

        // If there's an active keep-alive task, await its completion
        if let Some(handle) = handle_lock.take() {
            if let Err(e) = handle.await {
                error!(
                    "Keep-alive task for message_id {} panicked: {}",
                    self.message_id, e
                );
            } else {
                debug!(
                    "Keep-alive task completed for message_id: {}",
                    self.message_id
                );
            }
        } else {
            debug!(
                "No active keep-alive task to stop for message_id: {}",
                self.message_id
            );
        }
    }

    /// Acknowledge the message, potentially sync retry count, and stop keep-alive
    pub async fn ack(&self) -> RmqResult<()> {
        self.stop_keep_alive().await;

        // Sync retry count before acknowledgment if policy requires it
        if self.retry_sync_policy == RetrySyncPolicy::OnAcknowledge {
            let current_count = self.state.delivery_count.load(Ordering::Relaxed);
            self.sync_delivery_count_with_redis(current_count).await?;
        }

        self.client
            .xack::<i64, _, _, _>(&self.stream, &self.group, &self.message_id)
            .await?;

        if self.delete_on_ack {
            self.client
                .xdel::<(), _, _>(&self.stream, &self.message_id)
                .await?;
        }

        Ok(())
    }

    /// Reject the message and stop keep-alive (if needed)
    pub async fn reject(&self) -> RmqResult<()> {
        self.stop_keep_alive().await;
        // Implement rejection logic if needed
        Ok(())
    }
}
