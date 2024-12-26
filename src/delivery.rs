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

use crate::{errors::RmqResult, ConsumerError};

struct DeliveryState {
    retry_count: AtomicU32,
    error: Mutex<Option<Arc<ConsumerError>>>,
    keep_alive_stop: AtomicBool,
    keep_alive_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct Delivery<M> {
    pub(crate) client: Arc<Client>,
    pub stream: String,
    pub group: String,
    pub consumer_id: String,
    pub message_id: String,
    pub message: M,
    pub max_retries: Option<u32>,
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
        consumer_id: String,
        message_id: String,
        message: M,
        max_retries: Option<u32>,
        retry_count: u32,
    ) -> Self {
        Self {
            client,
            stream,
            group,
            consumer_id,
            message_id,
            message,
            max_retries,
            state: Arc::new(DeliveryState {
                retry_count: AtomicU32::new(retry_count),
                error: Mutex::new(None),
                keep_alive_stop: AtomicBool::new(false),
                keep_alive_handle: Mutex::new(None),
            }),
        }
    }

    /// Read the current `retry_count`.
    pub fn retry_count(&self) -> u32 {
        self.state.retry_count.load(Ordering::Relaxed)
    }

    /// Increment the retry count by 1
    pub fn inc_retry_count(&self) {
        self.state.retry_count.fetch_add(1, Ordering::Relaxed);
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
        let consumer_id = self.consumer_id.clone();
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

    /// Acknowledge the message and stop keep-alive
    pub async fn ack(&self) -> RmqResult<()> {
        self.stop_keep_alive().await;
        self.client
            .xack::<i64, _, _, _>(&self.stream, &self.group, &self.message_id)
            .await?;
        Ok(())
    }

    /// Reject the message and stop keep-alive (if needed)
    pub async fn reject(&self) -> RmqResult<()> {
        self.stop_keep_alive().await;
        // Implement rejection logic if needed
        Ok(())
    }
}
