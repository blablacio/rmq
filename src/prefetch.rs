use dashmap::DashMap;
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex};
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
    // Use DashMap for concurrent consumer management
    consumers: Arc<DashMap<Uuid, mpsc::Sender<Message>>>,
    // Overflow buffer - protected by Mutex for exclusive access
    overflow: Arc<Mutex<VecDeque<Message>>>,
    // Channel capacity per consumer
    consumer_capacity: usize,
    marker: PhantomData<M>,
}

impl<M> MessageBuffer<M> {
    pub fn new(consumer_capacity: usize) -> Self {
        Self {
            // Initialize DashMap
            consumers: Arc::new(DashMap::new()),
            overflow: Arc::new(Mutex::new(VecDeque::new())),
            consumer_capacity,
            marker: PhantomData,
        }
    }

    pub async fn get_overflow_size(&self) -> usize {
        self.overflow.lock().await.len()
    }

    pub async fn register_consumer(&self, consumer_id: Uuid) -> ConsumerChannel<M> {
        // Create channel with appropriate capacity
        let (sender, receiver) = mpsc::channel(self.consumer_capacity);

        // Insert directly into DashMap (no write lock needed)
        self.consumers.insert(consumer_id, sender);

        // Check if there are messages in overflow that can be sent to this consumer
        self.distribute_overflow().await;

        // Return the consumer channel
        ConsumerChannel::new(receiver)
    }

    pub async fn unregister_consumer(&self, consumer_id: &Uuid) {
        // Remove directly from DashMap (no write lock needed)
        self.consumers.remove(consumer_id);
    }

    pub async fn consumer_count(&self) -> usize {
        // Get length directly from DashMap (no read lock needed)
        self.consumers.len()
    }

    pub async fn push(&self, messages: Vec<Message>) {
        if messages.is_empty() {
            return;
        }

        // Check consumer count directly
        let consumer_count = self.consumers.len();

        if consumer_count == 0 {
            // No consumers, store all in overflow
            let mut overflow = self.overflow.lock().await;
            overflow.extend(messages);

            return;
        }

        // Collect senders to avoid holding iterator during sends
        let consumer_senders: Vec<mpsc::Sender<Message>> = self
            .consumers
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        // Single overflow lock acquisition - only once if needed
        let mut overflow_msgs = Vec::new();

        // Distribute messages round-robin using the collected senders
        for (i, message) in messages.into_iter().enumerate() {
            // Ensure we don't panic if consumer_senders became empty unexpectedly
            if consumer_senders.is_empty() {
                overflow_msgs.push(message);

                continue;
            }

            let sender = &consumer_senders[i % consumer_senders.len()]; // Use len() of collected vec

            // Non-blocking send - collect failures for batch overflow processing
            if let Err(e) = sender.try_send(message) {
                // Push the message to overflow regardless of Full or Closed error
                let msg = e.into_inner();
                overflow_msgs.push(msg);
            }
        }

        // Only acquire overflow lock once if needed
        if !overflow_msgs.is_empty() {
            let mut overflow = self.overflow.lock().await;
            overflow.extend(overflow_msgs);
        }

        // Attempt to distribute any messages currently in overflow AFTER pushing new ones.
        self.distribute_overflow().await;
    }

    pub async fn distribute_overflow(&self) {
        let mut overflow_guard = self.overflow.lock().await;
        let initial_size = overflow_guard.len();

        if initial_size == 0 {
            return; // Release lock implicitly
        }

        let consumer_count = self.consumers.len();

        if consumer_count == 0 {
            return; // Release lock implicitly
        }

        // Create a temporary list of consumer senders to iterate over without holding the DashMap lock
        let senders: Vec<_> = self
            .consumers
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        // Iterate while there are messages and we haven't tried all consumers in this pass
        let mut consumer_idx = 0;
        let mut attempts = 0; // Track attempts to avoid infinite loops if all channels are full

        // Loop while there are messages AND we haven't fruitlessly tried every consumer
        while !overflow_guard.is_empty() && attempts < senders.len() {
            let current_sender_idx = consumer_idx % senders.len(); // Ensure index wraps around
            let sender = &senders[current_sender_idx];

            // Peek at the front message without removing it yet
            if let Some(message) = overflow_guard.front() {
                // Attempt to send the message
                if sender.try_send(message.clone()).is_ok() {
                    // Success! Remove the message from the front of the queue
                    overflow_guard.pop_front();
                    // Move to the next consumer index for the *next* message
                    consumer_idx = (consumer_idx + 1) % senders.len(); // Maintain round-robin
                    attempts = 0; // Reset attempts since we successfully sent one

                    // Continue to the next message in the overflow buffer
                    continue;
                }
            } else {
                // Should not happen if !overflow_guard.is_empty(), but break defensively
                break;
            }

            // If try_send failed, move to the next consumer for *this* message
            consumer_idx = (consumer_idx + 1) % senders.len();
            attempts += 1; // Increment attempts since this consumer failed
        }
    }
}
