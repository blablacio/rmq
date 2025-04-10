use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;

type Message = (String, HashMap<String, Value>, u32); // Message ID, fields, retry count

pub struct ConsumerChannel<M> {
    receiver: mpsc::Receiver<Message>,
    marker: PhantomData<M>,
}

impl<M> ConsumerChannel<M> {
    pub fn new(receiver: mpsc::Receiver<Message>) -> Self {
        Self {
            receiver,
            marker: PhantomData,
        }
    }

    pub async fn receive(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }
}

// Simple, direct implementation with minimal overhead
pub struct MessageBuffer<M> {
    // Consumers map - protected by RwLock for concurrent access
    consumers: Arc<RwLock<HashMap<Uuid, mpsc::Sender<Message>>>>,
    // Overflow buffer - protected by Mutex for exclusive access
    overflow: Arc<Mutex<VecDeque<Message>>>,
    // Channel capacity per consumer
    capacity: usize,
    marker: PhantomData<M>,
}

impl<M> MessageBuffer<M> {
    pub fn new(channel_capacity: usize) -> Self {
        Self {
            consumers: Arc::new(RwLock::new(HashMap::new())),
            overflow: Arc::new(Mutex::new(VecDeque::new())),
            capacity: std::cmp::max(channel_capacity, 1000),
            marker: PhantomData,
        }
    }

    pub async fn get_overflow_size(&self) -> usize {
        let overflow = self.overflow.lock().await;

        overflow.len()
    }

    pub async fn register_consumer(&self, consumer_id: Uuid) -> ConsumerChannel<M> {
        // Create channel with appropriate capacity
        let (sender, receiver) = mpsc::channel(self.capacity);

        // Register the sender
        {
            let mut consumers = self.consumers.write().await;
            consumers.insert(consumer_id, sender);
        }

        // Check if there are messages in overflow that can be sent to this consumer
        self.distribute_overflow().await;

        // Return the consumer channel
        ConsumerChannel::new(receiver)
    }

    pub async fn unregister_consumer(&self, consumer_id: &Uuid) {
        let mut consumers = self.consumers.write().await;
        consumers.remove(consumer_id);
    }

    pub async fn consumer_count(&self) -> usize {
        let consumers = self.consumers.read().await;

        consumers.len()
    }

    pub async fn push(&self, messages: Vec<Message>) {
        if messages.is_empty() {
            return;
        }

        // Fast path optimization - direct distribution
        let consumers = self.consumers.read().await;
        let consumer_count = consumers.len();

        if consumer_count == 0 {
            // No consumers, store all in overflow
            let mut overflow = self.overflow.lock().await;
            overflow.extend(messages);

            return;
        }

        // Get consumers as a vector for distribution
        let consumer_vec: Vec<(&Uuid, &mpsc::Sender<Message>)> = consumers.iter().collect();

        // Single overflow lock acquisition - only once if needed
        let mut overflow_msgs = Vec::new();

        // Distribute messages round-robin
        for (i, message) in messages.into_iter().enumerate() {
            let (_, sender) = consumer_vec[i % consumer_count];

            // Non-blocking send - collect failures for batch overflow processing
            if let Err(mpsc::error::TrySendError::Full(msg)) = sender.try_send(message) {
                overflow_msgs.push(msg);
            }
            // Closed channels are ignored - consumer is being removed
        }

        // Only acquire overflow lock once if needed
        if !overflow_msgs.is_empty() {
            let mut overflow = self.overflow.lock().await;
            overflow.extend(overflow_msgs);
        }
    }

    async fn distribute_overflow(&self) {
        let consumers = self.consumers.read().await;

        if consumers.is_empty() {
            return;
        }

        let mut overflow = self.overflow.lock().await;

        if overflow.is_empty() {
            return;
        }

        let consumer_vec: Vec<(&Uuid, &mpsc::Sender<Message>)> = consumers.iter().collect();
        let consumer_count = consumer_vec.len();

        // Take messages from overflow
        let messages: Vec<Message> = overflow.drain(..).collect();

        // Distribute to consumers (reuse our overflow vec for failed messages)
        let mut failed_msgs = Vec::new();

        for (i, message) in messages.into_iter().enumerate() {
            let (_, sender) = consumer_vec[i % consumer_count];

            if let Err(mpsc::error::TrySendError::Full(msg)) = sender.try_send(message) {
                failed_msgs.push(msg);
            }
        }

        // Put back any messages that couldn't be distributed
        overflow.extend(failed_msgs);
    }
}
