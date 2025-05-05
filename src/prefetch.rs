use dashmap::DashMap;
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

type Message = (String, HashMap<String, Value>, u32); // Message ID, fields, retry count

struct ConsumerState {
    sender: mpsc::Sender<Message>,
    processing: Arc<AtomicBool>,
}

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
    // Store both sender and processing flag
    consumers: Arc<DashMap<Uuid, ConsumerState>>,
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

    pub async fn register_consumer(
        &self,
        consumer_id: Uuid,
        processing: Arc<AtomicBool>, // Add processing flag parameter
    ) -> ConsumerChannel<M> {
        // Create channel with appropriate capacity
        let (sender, receiver) = mpsc::channel(self.consumer_capacity);

        // Store the sender and the processing flag
        let state = ConsumerState {
            sender,
            processing, // Store the passed flag
        };
        self.consumers.insert(consumer_id, state);

        // Check if there are messages in overflow that can be sent to this new consumer
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

        if self.consumers.is_empty() {
            let mut overflow = self.overflow.lock().await;
            overflow.extend(messages);

            return;
        }

        // Create a single list prioritizing idle consumers
        // Cloning Arc<AtomicBool> and mpsc::Sender is cheap
        let mut prioritized_consumers = Vec::new();
        let mut busy_consumers_temp = Vec::new(); // Temp storage for busy ones

        for entry in self.consumers.iter() {
            let state = entry.value();

            if !state.processing.load(Ordering::Relaxed) {
                prioritized_consumers.push(state.sender.clone()); // Idle go first
            } else {
                busy_consumers_temp.push(state.sender.clone()); // Busy collected separately
            }
        }

        prioritized_consumers.extend(busy_consumers_temp); // Append busy consumers

        // If no consumers ended up in the list (e.g., map is empty after check)
        if prioritized_consumers.is_empty() {
            let mut overflow = self.overflow.lock().await;
            overflow.extend(messages);

            return;
        }

        let mut overflow_msgs = VecDeque::new();

        'message_loop: for message in messages {
            // Try sending to the first available consumer in the prioritized list
            for sender in &prioritized_consumers {
                // Use try_send directly. If it succeeds, the message is sent.
                if sender.try_send(message.clone()).is_ok() {
                    // Clone message for the attempt
                    continue 'message_loop; // Sent successfully, move to the next message
                }
                // If try_send fails (Err), continue to the next consumer in the list
            }

            // If the loop completes without sending, add to overflow
            overflow_msgs.push_back(message);
        }

        // Add any overflowed messages to the main overflow queue
        if !overflow_msgs.is_empty() {
            let mut overflow = self.overflow.lock().await;
            overflow.extend(overflow_msgs);
        }

        // Attempt to distribute any messages currently in overflow
        self.distribute_overflow().await;
    }

    pub async fn distribute_overflow(&self) {
        let mut overflow_guard = self.overflow.lock().await;

        if overflow_guard.is_empty() {
            return; // Nothing to distribute
        }

        // Create prioritized consumer list (idle first)
        let mut prioritized_consumers = Vec::new();
        let mut busy_consumers_temp = Vec::new();

        for entry in self.consumers.iter() {
            let state = entry.value();

            if !state.processing.load(Ordering::Relaxed) {
                prioritized_consumers.push(state.sender.clone());
            } else {
                busy_consumers_temp.push(state.sender.clone());
            }
        }

        prioritized_consumers.extend(busy_consumers_temp);

        // No consumers available to distribute to
        if prioritized_consumers.is_empty() {
            return;
        }

        let initial_len = overflow_guard.len();
        let mut processed_count = 0;

        // Iterate through the overflow queue once
        while processed_count < initial_len && !overflow_guard.is_empty() {
            processed_count += 1;

            if let Some(message) = overflow_guard.front() {
                let mut sent = false;

                // Try sending to the first available prioritized consumer
                for sender in &prioritized_consumers {
                    if sender.try_send(message.clone()).is_ok() {
                        overflow_guard.pop_front(); // Sent, remove from overflow
                        sent = true;

                        break; // Stop trying consumers for this message
                    }
                }

                if sent {
                    continue; // Message was sent, process next in overflow
                } else if let Some(msg_to_move) = overflow_guard.pop_front() {
                    // Message could not be sent to *any* consumer, move it to the back
                    overflow_guard.push_back(msg_to_move);
                }
            } else {
                // Should not happen if !overflow_guard.is_empty() passed
                break;
            }
        }
        // MutexGuard is dropped here, releasing the lock automatically.
    }
}
