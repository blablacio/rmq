use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

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
    scaling::{ScaleAction, ScalingContext, ScalingStrategy},
    RmqError,
};

#[derive(Clone)]
pub struct Queue<M>
where
    M: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    client: Arc<Client>,
    stream: String,
    group: String,
    options: QueueOptions,
    script_hash: String,
    shutdown_tx: Sender<bool>,
    shutdown_rx: Receiver<bool>,
    tasks: Arc<DashMap<Uuid, TaskState<M>>>, // Store both handle and delivery
    message_buffer: Option<Arc<MessageBuffer<M>>>,
    prefetch_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    scaling_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    consumer_factory: Option<Arc<dyn Fn() -> Arc<dyn Consumer<Message = M>> + Send + Sync>>,
    scaling_strategy: Option<Arc<dyn ScalingStrategy>>,
    queue_id: String, // Unique queue ID for all operations
    marker: PhantomData<M>,
}

// Add a new struct to track both task handles and deliveries
struct TaskState<M> {
    handle: JoinHandle<()>,
    delivery: Option<Arc<Mutex<Option<Delivery<M>>>>>,
    processing: Arc<AtomicBool>,
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
        consumer_factory: Option<Arc<dyn Fn() -> Arc<dyn Consumer<Message = M>> + Send + Sync>>,
        scaling_strategy: Option<Arc<dyn ScalingStrategy>>,
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
            // Validate scaling config dependency
            if config.scaling.is_some() && consumer_factory.is_none() {
                return Err(crate::errors::RmqError::ConfigError(
                    "Consumer factory must be provided when scaling is enabled".to_string(),
                ));
            }
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
            scaling_task: Arc::new(Mutex::new(None)),
            consumer_factory,
            scaling_strategy,
            queue_id,
            marker: PhantomData,
        };

        // Start the prefetch task only if prefetch_config is Some (and thus message_buffer is Some)
        if options.prefetch_config.is_some() {
            let prefetch_task = queue.start_prefetch_task().await;
            *queue.prefetch_task.lock().await = Some(prefetch_task);
        }

        // Start the scaling task if configured
        if queue
            .options
            .prefetch_config
            .as_ref()
            .map_or(false, |pc| pc.scaling.is_some())
        {
            let scaling_task_handle = queue.start_scaling_task().await?;
            *queue.scaling_task.lock().await = Some(scaling_task_handle);
        }

        // Start initial consumers if configured
        if let Some(initial_count) = queue.options.initial_consumers {
            if initial_count > 0 {
                if let Some(factory) = &queue.consumer_factory {
                    debug!("Starting {} initial consumers...", initial_count);

                    if let Err(e) = queue.scale_up(initial_count, factory).await {
                        error!("Failed to start initial consumers: {}", e);

                        return Err(crate::errors::RmqError::ConfigError(format!(
                            "Failed to start initial consumers: {}",
                            e
                        )));
                    }
                } else {
                    warn!(
                        "Initial consumers configured ({}), but no consumer factory provided. Ignoring.",
                        initial_count
                    );
                }
            }
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
                        // Use debug! for regular logging
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
                        if current_len >= effective_prefetch as usize {
                            // If we skipped fetching because the buffer is full,
                            // try distributing existing overflow to potentially idle consumers.
                            message_buffer.distribute_overflow().await;

                            continue; // Continue to next loop iteration
                        }

                        // Calculate how many messages to fetch to fill the buffer
                        // Ensure subtraction doesn't underflow
                        let fetch_count = effective_prefetch.saturating_sub(current_len as u32);

                        // Ensure we only fetch if fetch_count is positive
                        if fetch_count == 0 {
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

                        let batch_size = fetch_count.to_string();

                        // Fetch from Redis using EVALSHA
                        let result: Result<Vec<(String, HashMap<String, Value>, u32)>, Error> = client
                            .evalsha::<Vec<(String, HashMap<String, Value>, u32)>, _, _, _>(
                                &script_hash,
                                &[&stream], // KEYS[1] = stream name
                                &[&group, &prefetcher_id, &pending_timeout_arg, &batch_size], // ARGV[1]=group, ARGV[2]=consumer, ARGV[3]=timeout, ARGV[4]=count
                            )
                            .await;

                        match result {
                            Ok(messages) => {
                                let fetched_count = messages.len();

                                // After pushing new messages, immediately try distributing any overflow
                                message_buffer.distribute_overflow().await;

                                if !messages.is_empty() {
                                    message_buffer.push(messages).await;

                                    // If we got close to our requested batch size, check if we need to immediately
                                    // fetch more to keep the buffer full (don't wait for next interval)
                                    if fetched_count >= (fetch_count as usize * 3/4) && fetched_count > 50 { // Check thresholds
                                        interval.reset();
                                    }
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
            None,
            None,
        )
        .await
    }

    pub fn client(&self) -> Arc<Client> {
        self.client.clone()
    }

    /// Returns the current number of registered consumer tasks.
    pub fn consumer_count(&self) -> usize {
        self.tasks.len()
    }

    async fn start_scaling_task(&self) -> RmqResult<JoinHandle<()>> {
        // Ensure scaling is configured
        let scaling_config = self
            .options
            .prefetch_config
            .as_ref()
            .and_then(|pc| pc.scaling.clone())
            .ok_or_else(|| {
                crate::errors::RmqError::ConfigError(
                    "Attempted to start scaling task without scaling configuration".to_string(),
                )
            })?;

        // Ensure necessary components are present
        let strategy = self.scaling_strategy.clone().ok_or_else(|| {
            crate::errors::RmqError::ConfigError(
                "Scaling strategy missing for scaling task".to_string(),
            )
        })?;
        let factory = self.consumer_factory.clone().ok_or_else(|| {
            crate::errors::RmqError::ConfigError(
                "Consumer factory missing for scaling task".to_string(),
            )
        })?;
        let message_buffer = self.message_buffer.clone().ok_or_else(|| {
            crate::errors::RmqError::ConfigError(
                "Message buffer missing for scaling task".to_string(),
            )
        })?;
        let tasks = self.tasks.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let queue_clone = self.clone(); // Clone self for scale_up/scale_down calls

        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(scaling_config.scale_interval));
            debug!(
                "Scaling task started. Interval: {}ms, Min: {}, Max: {}",
                scaling_config.scale_interval,
                scaling_config.min_consumers,
                scaling_config.max_consumers
            );

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        debug!("Scaling task received shutdown signal. Exiting.");

                        break;
                    }
                    _ = interval.tick() => {
                        let current_consumers = tasks.len();
                        // Calculate idle consumers more accurately
                        let idle_consumers = tasks
                            .iter()
                            .filter(|entry| !entry.value().processing.load(Ordering::Relaxed))
                            .count();
                        let overflow_size = message_buffer.get_overflow_size().await;

                        let context = ScalingContext {
                            current_consumers,
                            idle_consumers,
                            overflow_size,
                            min_consumers: scaling_config.min_consumers as usize,
                            max_consumers: scaling_config.max_consumers as usize,
                        };

                        let action = strategy.decide(context).await;

                        match action {
                            ScaleAction::ScaleUp(count) => {
                                let actual_count = std::cmp::min(
                                    count,
                                    scaling_config.max_consumers.saturating_sub(current_consumers as u32)
                                );
                                if actual_count > 0 {
                                    debug!("Scaling up by {} consumers", actual_count);

                                    if let Err(e) = queue_clone.scale_up(actual_count, &factory).await {
                                        error!("Failed to scale up: {}", e);
                                    }
                                }
                            }
                            ScaleAction::ScaleDown(count) => {
                                let actual_count = std::cmp::min(
                                    count,
                                    (current_consumers as u32).saturating_sub(scaling_config.min_consumers)
                                );

                                if actual_count > 0 {
                                    debug!("Scaling down by {} consumers", actual_count);

                                    if let Err(e) = queue_clone.scale_down(actual_count).await {
                                        error!("Failed to scale down: {}", e);
                                    }
                                }
                            }
                            ScaleAction::Hold => {
                                 debug!("Holding.");
                            }
                        }
                    }
                }
            }

            debug!("Scaling task completed.");
        });

        Ok(handle)
    }

    async fn scale_up(
        &self,
        count: u32,
        factory: &Arc<dyn Fn() -> Arc<dyn Consumer<Message = M>> + Send + Sync>,
    ) -> RmqResult<()> {
        debug!("Attempting to scale up by {} consumers", count);

        for _ in 0..count {
            let new_consumer = factory();
            let id = Uuid::now_v7();

            match self.start_consumer(new_consumer).await {
                Ok(task_state) => {
                    self.tasks.insert(id, task_state);
                    debug!("Successfully added consumer {}", id);
                }
                Err(e) => {
                    error!("Failed to start new consumer during scale up: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn scale_down(&self, count: u32) -> RmqResult<()> {
        debug!("Attempting to scale down by {} consumers", count);

        let mut removed_count = 0;
        let idle_keys: Vec<Uuid> = self
            .tasks
            .iter()
            .filter(|entry| !entry.value().processing.load(Ordering::Relaxed))
            .map(|entry| *entry.key())
            .take(count as usize)
            .collect();

        for key in idle_keys {
            if removed_count >= count {
                break;
            }

            if let Some((_key, task_state)) = self.tasks.remove(&key) {
                debug!("Aborting idle consumer task {}", key);
                task_state.handle.abort();
                // The consumer_task loop should handle unregistering from message_buffer on exit
                removed_count += 1;
            } else {
                warn!(
                    "Attempted to remove task {} during scale down, but it was already gone.",
                    key
                );
            }
        }

        debug!("Successfully scaled down by {} consumers", removed_count);

        Ok(())
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
                        Err(_) => debug!("Prefetcher didn't terminate in time; aborting it."),
                    }
                }
                None => {
                    // Wait indefinitely (match wait_tasks behavior)
                    let _ = handle.await;

                    debug!("Prefetcher terminated gracefully.");
                }
            }
        }

        // Wait for the scaling task
        if let Some(handle) = self.scaling_task.lock().await.take() {
            debug!("Waiting for scaling task to terminate...");
            // Scaling task should shut down quickly, use short timeout like prefetcher
            Self::wait_or_abort_task("Scaling task", handle, shutdown_timeout, Some(200)).await;
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

    // Helper for waiting/aborting tasks during shutdown
    async fn wait_or_abort_task(
        task_name: &str,
        handle: JoinHandle<()>,
        shutdown_timeout: Option<u64>,
        specific_timeout_ms: Option<u64>,
    ) {
        let wait_duration = match specific_timeout_ms {
            Some(ms) => Duration::from_millis(ms),
            None => match shutdown_timeout {
                Some(ms) => Duration::from_millis(ms),
                None => Duration::from_secs(u64::MAX), // Effectively wait indefinitely
            },
        };

        if wait_duration.as_secs() == u64::MAX {
            // Wait indefinitely
            let _ = handle.await;

            debug!("{} terminated gracefully.", task_name);
        } else {
            match timeout(wait_duration, handle).await {
                Ok(_) => debug!("{} terminated within timeout.", task_name),
                Err(_) => debug!("{} didn't terminate in time; aborting it.", task_name),
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

    /// Adds a specified number of consumers to the queue.
    pub async fn add_consumers(&self, count: u32) -> RmqResult<()> {
        if count == 0 {
            return Ok(());
        }

        let factory = self.consumer_factory.clone().ok_or_else(|| {
            RmqError::ConfigError("Consumer factory must be provided to add consumers.".to_string())
        })?;

        debug!("Attempting to add {} consumers via add_consumers", count);

        self.scale_up(count, &factory).await
    }

    /// Removes a specified number of consumers from the queue.
    /// It will attempt to remove idle consumers first.
    pub async fn remove_consumers(&self, count: u32) -> RmqResult<()> {
        if count == 0 {
            return Ok(());
        }

        debug!(
            "Attempting to remove {} consumers via remove_consumers",
            count
        );

        self.scale_down(count).await
    }

    /// Registers a single consumer with the queue.
    pub async fn register_consumer<C>(&self, consumer: C) -> RmqResult<Uuid>
    where
        C: Consumer<Message = M>,
    {
        let id = Uuid::now_v7();

        // Spawn the consumer task as a method on Queue
        let task_state = self.start_consumer(Arc::new(consumer)).await?;

        self.tasks.insert(id, task_state);

        Ok(id)
    }

    async fn start_consumer(
        &self,
        consumer: Arc<dyn Consumer<Message = M>>,
    ) -> RmqResult<TaskState<M>> {
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

        let delivery_holder_clone = delivery_holder.clone();

        // Generate a unique consumer task ID
        let consumer_id = Uuid::now_v7();

        let queue_id = self.queue_id.clone(); // Use the queue's ID for Redis operations
        let processing = Arc::new(AtomicBool::new(false));
        let processing_clone = processing.clone();

        // Register with prefetch system only if prefetch_config is Some
        let consumer_channel = if let Some(buffer) = &self.message_buffer {
            // Pass the processing flag along with the ID
            Some(
                buffer
                    .register_consumer(consumer_id, processing.clone())
                    .await,
            ) // Pass processing_clone here
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
                delivery_holder_clone,
                processing_clone,
                message_buffer,
            )
            .await;
        });

        Ok(TaskState {
            handle,
            delivery: delivery_holder,
            processing,
        })
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
        processing_status: Arc<AtomicBool>,
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
                                    processing_status.clone(),
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
        processing_status: Arc<AtomicBool>,
    ) {
        if *shutdown_rx.borrow() {
            debug!("Shutdown signal received before processing message {}, returning immediately without acking.", message_id);

            return; // Exit immediately, DO NOT ACK here
        }

        let data = match fields.get("data") {
            Some(value) => value,
            None => {
                error!("Message {} missing 'data' field", message_id);
                // Acknowledge the message to remove it from the pending list
                let _ = client.xack::<(), _, _, _>(stream, group, &message_id).await;

                return;
            }
        };

        let message = match serde_json::from_value::<M>(data.clone()) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to deserialize message {}: {}", message_id, e);
                // Acknowledge the message to remove it from the pending list
                let _ = client.xack::<(), _, _, _>(stream, group, &message_id).await;

                return;
            }
        };

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

        // Set processing status to true
        processing_status.store(true, Ordering::Relaxed);

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
                    error!("Error processing message {}: {}", message_id, e.to_string());

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
                                sleep(Duration::from_millis(options.retry_delay())).await;
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
                                    vec![("data", data.to_string()), ("error", e.to_string())],
                                )
                                .await
                            {
                                error!("Error moving message {} to DLQ: {}", message_id, e);
                            }

                            // Acknowledge the original message to remove it from pending
                            if let Err(e) = delivery.ack().await {
                                error!("Error acknowledging message {}: {}", message_id, e);
                            }
                        } else {
                            // Without DLQ: Acknowledge the message to remove it from pending
                            if let Err(e) = delivery.ack().await {
                                error!("Error acknowledging message {}: {}", message_id, e);
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

        // Reset processing status
        processing_status.store(false, Ordering::Relaxed);
    }
}
