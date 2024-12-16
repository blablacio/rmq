use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use dashmap::DashMap;
use eyre::Result;
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
    time::{sleep, timeout},
};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{consumer::Consumer, delivery::Delivery, options::QueueOptions};

#[derive(Clone)]
pub struct Queue<M> {
    client: Arc<Client>,
    stream: String,
    group: String,
    options: QueueOptions,
    script_hash: String,
    shutdown_tx: Sender<bool>,
    shutdown_rx: Receiver<bool>,
    tasks: Arc<DashMap<Uuid, JoinHandle<()>>>,
    marker: PhantomData<M>,
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
    ) -> Result<Self> {
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

        Ok(Self {
            client,
            stream,
            group,
            options,
            script_hash,
            shutdown_tx,
            shutdown_rx,
            tasks: Arc::new(DashMap::new()),
            marker: PhantomData,
        })
    }

    pub async fn from_url(
        url: &str,
        stream: &str,
        group: Option<&str>,
        options: QueueOptions,
    ) -> Result<Self> {
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
            if let Some((_, handle)) = self.tasks.remove(&key) {
                tasks.push(handle);
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
            if let Some((_, handle)) = self.tasks.remove(&key) {
                handle.abort();
            }
        }
    }

    pub async fn register_consumer<C>(&self, consumer: C) -> Result<Uuid>
    where
        C: Consumer<Message = M>,
    {
        let id = Uuid::now_v7();

        // Spawn the consumer task as a method on Queue
        let handle = self.start_consumer(Arc::new(consumer)).await?;

        self.tasks.insert(id, handle);

        Ok(id)
    }

    async fn start_consumer(
        &self,
        consumer: Arc<dyn Consumer<Message = M>>,
    ) -> Result<JoinHandle<()>> {
        let client = self.client.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();
        let options = self.options.clone();
        let script_hash = self.script_hash.clone();
        let shutdown_rx = self.shutdown_rx.clone();

        let handle = tokio::spawn(async move {
            Self::consumer_task(
                client,
                stream,
                group,
                consumer,
                options,
                script_hash,
                shutdown_rx,
            )
            .await;
        });

        Ok(handle)
    }

    // Produce a message to the queue (Redis Stream)
    pub async fn produce(&self, message: &M) -> Result<()> {
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
        options: QueueOptions,
        script_hash: String,
        mut shutdown_rx: Receiver<bool>,
    ) {
        let consumer_id = Uuid::now_v7().to_string();
        let mut interval = tokio::time::interval(Duration::from_millis(options.poll_interval()));

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    debug!("Consumer Task '{}' received shutdown signal. Exiting.", &consumer_id);
                    break;
                }
                _ = interval.tick() => {
                    // Prepare the arguments for the Lua script
                    let pending_timeout = options.pending_timeout.map_or("nil".to_string(), |t| t.to_string());

                    // Execute the Lua script using evalsha
                    let result: Result<Vec<(String, HashMap<String, Value>, u32)>, Error> = client
                        .evalsha(
                            script_hash.clone(),
                            vec![stream.clone()],
                            vec![
                                group.clone(),
                                consumer_id.clone(),
                                pending_timeout,
                            ],
                        )
                        .await;

                    match result {
                        Ok(messages) => {
                            if !messages.is_empty() {
                                // Process messages
                                for (message_id, fields, retry_count) in messages {
                                    Self::process_message(
                                        client.clone(),
                                        &stream,
                                        &group,
                                        consumer.clone(),
                                        consumer_id.clone(),
                                        message_id,
                                        fields,
                                        retry_count,
                                        &options,
                                    )
                                    .await;
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

        debug!("Consumer Task '{}' terminated.", consumer_id);
    }

    async fn process_message(
        client: Arc<Client>,
        stream: &str,
        group: &str,
        consumer: Arc<dyn Consumer<Message = M>>,
        consumer_id: String,
        message_id: String,
        fields: HashMap<String, Value>,
        retry_count: u32,
        options: &QueueOptions,
    ) {
        if let Some(data) = fields.get("data") {
            match serde_json::from_value::<M>(data.clone()) {
                Ok(message) => {
                    let mut delivery = Delivery {
                        client: client.clone(),
                        stream: stream.to_string(),
                        group: group.to_string(),
                        consumer_id: consumer_id.clone(),
                        message_id: message_id.clone(),
                        message,
                        max_retries: options.max_retries(),
                        retry_count,
                        error: None,
                        keep_alive_handle: Arc::new(Mutex::new(None)),
                        keep_alive_stop: None,
                    };

                    // If this is a manual queue (no pending_timeout), start keep-alive
                    if options.pending_timeout.is_none() {
                        delivery.start_keep_alive(options.poll_interval()).await;
                    }

                    loop {
                        let result = consumer.process(&mut delivery).await;

                        match result {
                            Ok(_) => {
                                // Succeeded, ack and break out
                                if let Err(e) = delivery.ack().await {
                                    error!("Error acknowledging message {}: {}", message_id, e);
                                }

                                break;
                            }
                            Err(e) => {
                                error!("Error processing message {}: {}", message_id, e);
                                delivery.error = Some(e.to_string());

                                let should_retry = consumer.should_retry(&delivery).await;

                                if should_retry {
                                    // Check if this is a manual queue scenario
                                    if options.pending_timeout.is_none() {
                                        // Manual queue with retries: We handle retry in-process
                                        // Increase retry_count after a failed attempt
                                        delivery.retry_count += 1;

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
                                    if options.enable_dlq {
                                        // Move the message to the Dead-Letter Queue
                                        let dlq_stream = options
                                            .dlq_name
                                            .clone()
                                            .unwrap_or_else(|| "dead_letter_stream".to_string());

                                        if let Err(e) = client
                                            .xadd::<String, _, _, _, _>(
                                                &dlq_stream,
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
                }
                Err(e) => {
                    error!("Failed to deserialize message {}: {}", message_id, e);
                    // Acknowledge the message to remove it from the pending list
                    let _ = client
                        .xack::<i64, _, _, _>(stream, group, &message_id)
                        .await;
                }
            }
        } else {
            error!("Message {} missing 'data' field", message_id);
            // Acknowledge the message to remove it from the pending list
            let _ = client
                .xack::<i64, _, _, _>(stream, group, &message_id)
                .await;
        }
    }
}
