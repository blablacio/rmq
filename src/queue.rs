use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use dashmap::DashMap;
use fred::{
    error::Error,
    prelude::{Client, ClientLike, Config, LuaInterface, StreamsInterface},
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::{
    sync::{
        watch::{self, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
    time::{interval, sleep, timeout},
};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    consumer::Consumer,
    delivery::Delivery,
    errors::RmqResult,
    options::{QueueOptions, RetrySyncPolicy},
    prefetch::{ConsumerChannel, MessageBuffer},
};

#[derive(Clone)]
pub struct Queue<M> {
    client: Arc<Client>,
    stream: String,
    group: String,
    options: QueueOptions,
    script_hash: String,
    shutdown_tx: Sender<bool>,
    shutdown_rx: Receiver<bool>,
    tasks: Arc<DashMap<Uuid, TaskState<M>>>, // Store both handle and delivery
    message_buffer: Option<Arc<MessageBuffer<M>>>, // Make this optional
    prefetch_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    queue_id: String, // Unique queue ID for all operations
    marker: PhantomData<M>,
}

// Add a new struct to track both task handles and deliveries
struct TaskState<M> {
    handle: JoinHandle<()>,
    delivery: Option<Arc<Mutex<Option<Delivery<M>>>>>,
}

impl<M> Queue<M>
where
    M: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    pub async fn new(
        client: Arc<Client>,
        stream: String,
        group: Option<String>,
        options: QueueOptions,
    ) -> RmqResult<Self> {
        let group = group.unwrap_or_else(|| "default_group".to_string());

        // Only create the group if group_name is provided
        if !group.is_empty() {
            client
                .xgroup_create(&stream, &group, "$", true)
                .await
                .or_else(|e| {
                    if e.details().contains("BUSYGROUP") {
                        debug!(
                            "Consumer group '{}' for stream '{}' already exists. Ignoring error.",
                            group, stream
                        );
                        Ok(())
                    } else {
                        Err(e)
                    }
                })?;
        }

        let script = include_str!("../scripts/claim.lua");
        let script_hash;

        if client.is_clustered() {
            script_hash = client.script_load_cluster(script).await?;
        } else {
            script_hash = client.script_load(script).await?;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Initialize MessageBuffer only if prefetching is enabled
        let message_buffer = if let Some(config) = &options.prefetch_config {
            Some(Arc::new(MessageBuffer::new(config.buffer_size)))
        } else {
            None
        };

        // Create a single consumer ID for all operations with this queue
        // Format: "queue-{group_name}-{unique_id}"
        let queue_id = format!("queue-{}-{}", group, Uuid::now_v7());

        let queue = Self {
            client: client.clone(),
            stream: stream.clone(),
            group: group.clone(),
            options: options.clone(),
            script_hash,
            tasks: Arc::new(DashMap::new()),
            shutdown_tx,
            shutdown_rx,
            message_buffer, // Assign the Option<Arc<MessageBuffer>>
            prefetch_task: Arc::new(Mutex::new(None)),
            queue_id,
            marker: PhantomData,
        };

        // Start the prefetch task only if prefetch_config is Some (and thus message_buffer is Some)
        if options.prefetch_config.is_some() {
            let prefetch_task = queue.start_prefetch_task().await;
            *queue.prefetch_task.lock().await = Some(prefetch_task);
        }

        Ok(queue)
    }

    async fn start_prefetch_task(&self) -> JoinHandle<()> {
        let client = self.client.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();
        let options = self.options.clone();
        let script_hash = self.script_hash.clone();
        let message_buffer = self
            .message_buffer
            .clone()
            .expect("Prefetch task started without message buffer");
        // Use the queue's consumer ID instead of generating a new one
        let prefetcher_id = self.queue_id.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut auto_recovery_done = false;

        // Ensure prefetch config exists before starting the task
        let prefetch_config = self
            .options
            .prefetch_config
            .clone()
            .expect("Prefetch task started without prefetch config");

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(options.poll_interval()));

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        debug!("Prefetcher received shutdown signal. Exiting.");

                        break;
                    }
                    _ = interval.tick() => {
                        // Get current consumer count to scale prefetch appropriately
                        let consumer_count = message_buffer.consumer_count().await;

                        if consumer_count == 0 {
                            // No consumers, no need to prefetch
                            continue;
                        }

                        // Calculate the appropriate prefetch amount based on consumer count
                        // Use the count from the captured prefetch_config
                        let effective_prefetch = prefetch_config.count + consumer_count as u32;

                        // Get current buffer state
                        let current_len = message_buffer.get_overflow_size().await;

                        // Only fetch more if there's space for a meaningful number of messages
                        // This is more aggressive than before and keeps the buffer fuller
                        if current_len >= effective_prefetch as usize {
                            continue;
                        }

                        // Calculate how many messages to fetch to fill the buffer
                        // Ensure subtraction doesn't underflow
                        let fetch_count = effective_prefetch.saturating_sub(current_len as u32);


                        // Skip tiny fetches
                        if fetch_count < 10 {
                            continue;
                        }

                        let pending_timeout = if !auto_recovery_done && options.auto_recovery.is_some() {
                            auto_recovery_done = true;

                            options.auto_recovery
                        } else {
                            options.pending_timeout
                        };

                        let pending_timeout_arg = match pending_timeout {
                            Some(timeout) => timeout.to_string(),
                            None => "nil".to_string(),
                        };

                        debug!(
                            "Prefetcher fetching {} messages for group '{}' with timeout {}",
                            fetch_count,
                            group,
                            pending_timeout_arg
                        );

                        let batch_size = fetch_count.to_string();

                        // Fetch from Redis
                        let result: Result<Vec<(String, HashMap<String, Value>, u32)>, Error> = client
                            .evalsha::<Vec<(String, HashMap<String, Value>, u32)>, _, _, _>(
                                &script_hash,
                                &[&stream],
                                &[&group, &prefetcher_id, &pending_timeout_arg, &batch_size],
                            )
                            .await;

                        match result {
                            Ok(messages) => {
                                debug!(
                                    "Prefetcher fetched {} messages for group '{}'",
                                    messages.len(),
                                    group
                                );

                                // If we got close to our requested batch size, check if we need to immediately
                                // fetch more to keep the buffer full (don't wait for next interval)
                                let fetched_count = messages.len();
                                message_buffer.push(messages).await;

                                // If we got a significant number of messages and are close to our requested
                                // batch size, immediately reset the interval to fetch more messages without waiting
                                if fetched_count >= (fetch_count as usize * 3/4) && fetched_count > 50 {
                                    interval.reset();
                                }
                            }
                            Err(e) => {
                                error!("Error executing Lua script: {}", e);
                            }
                        }
                    }
                }
            }

            debug!("Prefetcher task completed.");
        })
    }

    pub async fn from_url(
        url: &str,
        stream: &str,
        group: Option<&str>,
        options: QueueOptions,
    ) -> RmqResult<Self> {
        // Initialize client from config
        let config = Config::from_url(url)?;
        let client = Arc::new(Client::new(config, None, None, None));
        // Connect client
        client.connect();
        client.wait_for_connect().await?;

        Self::new(
            client,
            stream.to_string(),
            group.map(|s| s.to_string()),
            options,
        )
        .await
    }

    pub fn client(&self) -> Arc<Client> {
        self.client.clone()
    }

    pub async fn shutdown(&self, shutdown_timeout: Option<u64>) {
        // Signal all consumers to shut down
        if self.shutdown_tx.send(true).is_err() {
            warn!("Failed to send shutdown signal; consumers may not stop.");
        }

        // Wait for the prefetcher using the same pattern as wait_tasks()
        if let Some(handle) = self.prefetch_task.lock().await.take() {
            debug!("Waiting for prefetcher to terminate...");

            // Apply a short timeout specifically for the prefetcher
            match shutdown_timeout {
                Some(_) => {
                    // Always use a short timeout for prefetcher (200ms max)
                    let prefetcher_timeout = Duration::from_millis(200);

                    match timeout(prefetcher_timeout, handle).await {
                        Ok(_) => debug!("Prefetcher terminated within timeout."),
                        Err(_) => {
                            debug!("Prefetcher didn't terminate in time; aborting it.");
                        }
                    }
                }
                None => {
                    // Wait indefinitely (match wait_tasks behavior)
                    let _ = handle.await;

                    debug!("Prefetcher terminated gracefully.");
                }
            }
        }

        // Handle retry sync policy for OnShutdown
        if let RetrySyncPolicy::OnShutdown = self.options.retry_sync {
            // Sync any active deliveries
            for entry in self.tasks.iter() {
                if let Some(task_state) = entry.value().delivery.as_ref() {
                    if let Some(delivery) = task_state.lock().await.as_ref() {
                        // Attempt to sync the delivery count, but don't block shutdown if it fails
                        if let Err(e) = delivery.sync_delivery_count().await {
                            warn!("Failed to sync delivery count on shutdown: {}", e);
                        }
                    }
                }
            }
        }

        // Wait for all consumer tasks to terminate
        match shutdown_timeout {
            Some(timeout_millis) => {
                let duration = Duration::from_millis(timeout_millis);

                match timeout(duration, self.wait_tasks()).await {
                    Ok(_) => debug!("All consumer tasks terminated within timeout."),
                    Err(_) => {
                        debug!("Timeout reached; aborting remaining tasks.");

                        self.abort_tasks().await;
                    }
                }
            }
            None => {
                self.wait_tasks().await;

                debug!("All consumer tasks terminated gracefully.");
            }
        }
    }

    async fn wait_tasks(&self) {
        let mut tasks = Vec::new();

        // Collect and remove task handles
        for key in self
            .tasks
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>()
        {
            if let Some((_, task_state)) = self.tasks.remove(&key) {
                tasks.push(task_state.handle);
            }
        }

        // Await all tasks
        for handle in tasks {
            let _ = handle.await;
        }
    }

    async fn abort_tasks(&self) {
        for key in self
            .tasks
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>()
        {
            if let Some((_, task_state)) = self.tasks.remove(&key) {
                task_state.handle.abort();
            }
        }
    }

    pub async fn register_consumer<C>(&self, consumer: C) -> RmqResult<Uuid>
    where
        C: Consumer<Message = M>,
    {
        let id = Uuid::now_v7();

        // Spawn the consumer task as a method on Queue
        let (handle, delivery_holder) = self.start_consumer(Arc::new(consumer)).await?;

        self.tasks.insert(
            id,
            TaskState {
                handle,
                delivery: delivery_holder,
            },
        );

        Ok(id)
    }

    async fn start_consumer(
        &self,
        consumer: Arc<dyn Consumer<Message = M>>,
    ) -> RmqResult<(JoinHandle<()>, Option<Arc<Mutex<Option<Delivery<M>>>>>)> {
        let client = self.client.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();
        let options = self.options.clone();
        let script_hash = self.script_hash.clone();
        let shutdown_rx = self.shutdown_rx.clone();

        // Create a shared delivery holder if we need to track active deliveries
        let delivery_holder = if self.options.retry_sync == RetrySyncPolicy::OnShutdown {
            Some(Arc::new(Mutex::new(None)))
        } else {
            None
        };

        // Clone it to pass to the consumer task
        let task_delivery_holder = delivery_holder.clone();

        // Generate a unique consumer task ID
        let consumer_id = Uuid::now_v7();

        // Use the queue's ID for Redis operations
        let queue_id = self.queue_id.clone();

        // Register with prefetch system only if prefetch_config is Some
        let consumer_channel = if let Some(buffer) = &self.message_buffer {
            Some(buffer.register_consumer(consumer_id).await)
        } else {
            None
        };

        // Pass message_buffer only if prefetch_config is Some
        let message_buffer = if options.prefetch_config.is_some() {
            Some(self.message_buffer.clone().unwrap())
        } else {
            None
        };

        let handle = tokio::spawn(async move {
            Self::consumer_task(
                client,
                stream,
                group,
                consumer,
                queue_id,    // Use queue_id for Redis operations
                consumer_id, // Pass the consumer-specific ID
                options,
                script_hash,
                shutdown_rx,
                consumer_channel,
                task_delivery_holder,
                message_buffer,
            )
            .await;
        });

        Ok((handle, delivery_holder))
    }

    // Produce a message to the queue (Redis Stream)
    pub async fn produce(&self, message: &M) -> RmqResult<()> {
        let msg = serde_json::to_string(message)?;

        self.client
            .xadd::<(), _, _, _, _>(&self.stream, true, None, "*", ("data", msg))
            .await?;

        Ok(())
    }

    async fn consumer_task(
        client: Arc<Client>,
        stream: String,
        group: String,
        consumer: Arc<dyn Consumer<Message = M>>,
        queue_id: String,
        consumer_id: Uuid,
        options: QueueOptions,
        script_hash: String,
        mut shutdown_rx: Receiver<bool>,
        mut consumer_channel: Option<ConsumerChannel<M>>,
        delivery_holder: Option<Arc<Mutex<Option<Delivery<M>>>>>,
        message_buffer: Option<Arc<MessageBuffer<M>>>,
    ) {
        let mut auto_recovery_done = false;

        loop {
            let shutdown_clone = shutdown_rx.clone();

            tokio::select! {
                _ = shutdown_rx.changed() => {
                    debug!("Consumer Task '{}' received shutdown signal. Exiting.", &queue_id);

                    break;
                }
                _ = async {
                    // Get message either from prefetch channel or directly from Redis
                    let message_result = if let Some(channel) = &mut consumer_channel {
                        match channel.receive().await {
                            Some(message) => Ok(vec![message]),
                            None => {
                                // Channel closed, likely due to queue shutdown
                                debug!("Consumer channel closed for {}", queue_id);

                                return;
                            }
                        }
                    } else {
                        // Traditional direct fetch for prefetch_count = 1
                        let pending_timeout = if !auto_recovery_done && options.auto_recovery.is_some() {
                            auto_recovery_done = true;

                            options.auto_recovery
                        } else {
                            options.pending_timeout
                        };

                        let pending_timeout_arg = match pending_timeout {
                            Some(timeout) => timeout.to_string(),
                            None => "nil".to_string(),
                        };

                        debug!(
                            "Consumer Task '{}' fetching messages with timeout {}",
                            queue_id, pending_timeout_arg
                        );

                        client
                            .evalsha::<Vec<(String, HashMap<String, Value>, u32)>, _, _, _>(
                                &script_hash,
                                &[&stream],
                                &[&group, &queue_id, &pending_timeout_arg, "1"],
                            )
                            .await
                    };

                    // Determine if we need to sleep (before using message_result in the match)
                    let is_empty = match &message_result {
                        Ok(msgs) => msgs.is_empty(),
                        Err(_) => true,
                    };

                    match message_result {
                        Ok(messages) => {
                            for (message_id, fields, delivery_count) in messages {
                                Self::process_message(
                                    client.clone(),
                                    &stream,
                                    &group,
                                    consumer.clone(),
                                    queue_id.clone(),
                                    message_id,
                                    fields,
                                    delivery_count,
                                    &options,
                                    shutdown_clone.clone(),
                                    delivery_holder.clone(),
                                )
                                .await;
                            }
                        }
                        Err(e) => {
                            error!("Error executing Lua script: {}", e);
                        }
                    }

                    // Small sleep to avoid CPU spin when no messages
                    if is_empty {
                        sleep(Duration::from_millis(options.poll_interval())).await;
                    }
                } => {}
            }
        }

        // Unregister from prefetch system when shutting down
        if let Some(buffer) = &message_buffer {
            // Check if buffer exists
            if consumer_channel.is_some() {
                // Check if it was actually registered
                buffer.unregister_consumer(&consumer_id).await;
            }
        }

        debug!("Consumer Task '{}' terminated.", queue_id);
    }

    async fn process_message(
        client: Arc<Client>,
        stream: &str,
        group: &str,
        consumer: Arc<dyn Consumer<Message = M>>,
        queue_id: String,
        message_id: String,
        fields: HashMap<String, Value>,
        delivery_count: u32,
        options: &QueueOptions,
        shutdown_rx: Receiver<bool>,
        delivery_holder: Option<Arc<Mutex<Option<Delivery<M>>>>>,
    ) {
        if *shutdown_rx.borrow() {
            debug!("Shutdown signal received before processing message {}, returning immediately without acking.", message_id);

            return; // Exit immediately, DO NOT ACK here
        }

        if let Some(data) = fields.get("data") {
            match serde_json::from_value::<M>(data.clone()) {
                Ok(message) => {
                    // Use queue_id for delivery
                    let delivery = Delivery::new(
                        client.clone(),
                        stream.to_string(),
                        group.to_string(),
                        queue_id.clone(),
                        message_id.clone(),
                        message,
                        options.max_retries(),
                        delivery_count,
                        options.delete_on_ack,
                        options.retry_sync.clone(),
                    );

                    // If this is a manual queue (no pending_timeout), start keep-alive
                    if options.pending_timeout.is_none() {
                        delivery.start_keep_alive(options.poll_interval()).await;
                    }

                    // When processing a message, store the delivery in the holder if needed
                    if let Some(holder) = &delivery_holder {
                        let mut lock = holder.lock().await;
                        *lock = Some(delivery.clone());
                    }

                    loop {
                        let result = consumer.process(&delivery).await;

                        match result {
                            Ok(_) => {
                                // Succeeded, ack and break out
                                if let Err(e) = delivery.ack().await {
                                    error!("Error acknowledging message {}: {}", message_id, e);
                                }

                                break;
                            }
                            Err(e) => {
                                error!(
                                    "Error processing message {}: {}",
                                    message_id,
                                    e.to_string()
                                );

                                // Check for shutdown signal before retry
                                if *shutdown_rx.borrow() {
                                    debug!("Shutdown signal received before retry check for message {}, skipping retry.", message_id);

                                    return; // Exit immediately, DO NOT RETRY or set_error
                                }

                                delivery.set_error(e.clone()).await;

                                // Add this line to determine if we should retry
                                let should_retry = consumer.should_retry(&delivery).await;

                                if should_retry {
                                    // Check if this is a manual queue scenario
                                    if options.pending_timeout.is_none() {
                                        // Increase delivery_count after a failed attempt only for manual queue
                                        if let Err(e) = delivery.inc_delivery_count().await {
                                            error!(
                                                "Error incrementing delivery count for message {}: {}",
                                                message_id, e
                                            );
                                        }
                                        // Sleep if retry_delay is specified in retry_config
                                        if options.retry_delay() > 0 {
                                            sleep(Duration::from_millis(options.retry_delay()))
                                                .await;
                                        }

                                        // Loop again to retry processing
                                        continue;
                                    } else {
                                        // Stealing queue scenario:
                                        // We rely on `pending_timeout` & XAUTOCLAIM to retry eventually
                                        // Do not ack, just break. The message remains pending.
                                        break;
                                    }
                                } else {
                                    // should_retry = false
                                    if let Some(dlq_name) = options.dlq_name.clone() {
                                        // Move the message to the Dead-Letter Queue
                                        if let Err(e) = client
                                            .xadd::<String, _, _, _, _>(
                                                &dlq_name,
                                                false,
                                                None,
                                                "*", // Let Redis generate a new ID
                                                vec![
                                                    ("data", data.to_string()),
                                                    ("error", e.to_string()),
                                                ],
                                            )
                                            .await
                                        {
                                            error!(
                                                "Error moving message {} to DLQ: {}",
                                                message_id, e
                                            );
                                        }

                                        // Acknowledge the original message to remove it from pending
                                        if let Err(e) = delivery.ack().await {
                                            error!(
                                                "Error acknowledging message {}: {}",
                                                message_id, e
                                            );
                                        }
                                    } else {
                                        // Without DLQ: Acknowledge the message to remove it from pending
                                        if let Err(e) = delivery.ack().await {
                                            error!(
                                                "Error acknowledging message {}: {}",
                                                message_id, e
                                            );
                                        }
                                    }

                                    break;
                                }
                            }
                        }
                    }

                    // When message processing is done, clear the delivery
                    if let Some(holder) = &delivery_holder {
                        let mut lock = holder.lock().await;
                        *lock = None;
                    }
                }
                Err(e) => {
                    error!("Failed to deserialize message {}: {}", message_id, e);
                    // Acknowledge the message to remove it from the pending list
                    let _ = client.xack::<(), _, _, _>(stream, group, &message_id).await;
                }
            }
        } else {
            error!("Message {} missing 'data' field", message_id);
            // Acknowledge the message to remove it from the pending list
            let _ = client.xack::<(), _, _, _>(stream, group, &message_id).await;
        }
    }
}
