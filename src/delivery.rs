use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use eyre::Result;
use fred::prelude::{Client, StreamsInterface};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};

#[derive(Clone)]
pub struct Delivery<M> {
    pub(crate) client: Arc<Client>,
    pub stream: String,
    pub group: String,
    pub consumer_id: String,
    pub message_id: String,
    pub message: M,
    pub max_retries: Option<u32>,
    pub retry_count: u32,
    pub error: Option<String>,
    pub(crate) keep_alive_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(crate) keep_alive_stop: Option<Arc<AtomicBool>>,
}

impl<M> Delivery<M>
where
    M: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    // Start the keep-alive mechanism
    pub async fn start_keep_alive(&mut self, interval_millis: u64) {
        let client = self.client.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();
        let consumer_id = self.consumer_id.clone();
        let message_id = self.message_id.clone();
        let stop_signal = Arc::new(AtomicBool::new(false));
        self.keep_alive_stop = Some(stop_signal.clone());

        self.keep_alive_handle = Arc::new(Mutex::new(Some(tokio::spawn(async move {
            while !stop_signal.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(interval_millis)).await;

                let _ = client
                    .xclaim::<String, _, _, _, _>(
                        &stream,
                        &group,
                        &consumer_id,
                        0, // min-idle-time
                        &message_id,
                        None,  // idle
                        None,  // time
                        None,  // retrycount
                        false, // force
                        true,  // justid
                    )
                    .await;
            }
        }))));
    }

    // Stop the keep-alive mechanism
    pub async fn stop_keep_alive(&mut self) {
        if let Some(stop_signal) = &self.keep_alive_stop {
            stop_signal.store(true, Ordering::Relaxed);
        }

        if let Some(handle) = self.keep_alive_handle.lock().await.take() {
            let _ = handle.await;
        }
    }

    // Acknowledge the message and stop keep-alive
    pub async fn ack(&mut self) -> Result<()> {
        self.stop_keep_alive().await;
        self.client
            .xack::<i64, _, _, _>(&self.stream, &self.group, &self.message_id)
            .await?;
        Ok(())
    }

    // Reject the message and stop keep-alive (if needed)
    pub async fn reject(&mut self) -> Result<()> {
        self.stop_keep_alive().await;
        // Implement rejection logic if needed
        Ok(())
    }
}
