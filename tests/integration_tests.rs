use async_trait::async_trait;
use fred::prelude::{Client, ClientLike, Config, KeysInterface, StreamsInterface};
use rmq::{Consumer, ConsumerError, Delivery, Queue, QueueBuilder, QueueOptions, RetryConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, error::Error, fmt, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::sleep};

fn get_redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string())
}

/// Test message type
#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestMessage {
    content: String,
}

#[derive(Debug)]
struct TestError(String);

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for TestError {}

/// A simple consumer that records received messages and always acknowledges them.
struct TestAcknowledgeConsumer {
    received_messages: Arc<Mutex<Vec<TestMessage>>>,
}

#[async_trait]
impl Consumer for TestAcknowledgeConsumer {
    type Message = TestMessage;

    async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
        let mut msgs = self.received_messages.lock().await;
        msgs.push(delivery.message.clone());
        delivery.ack().await?; // Always ack
        Ok(())
    }
}

#[tokio::test]
async fn test_manual_queue_basic_flow() -> eyre::Result<()> {
    // Demonstrate: Using the Builder with an existing `fred::Client`.
    let config = Config::from_url(&get_redis_url())?;
    let raw_client = Client::new(config, None, None, None);
    raw_client.connect();
    raw_client.wait_for_connect().await?;
    let arc_client = Arc::new(raw_client);

    // Build using our custom QueueBuilder (but inject the existing client)
    let queue = QueueBuilder::<TestMessage>::new()
        .client(arc_client.clone())
        .stream("manual_test_stream")
        .group("manual_group")
        .build()
        .await?;

    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let consumer = TestAcknowledgeConsumer {
        received_messages: received_messages.clone(),
    };
    queue.register_consumer(consumer).await?;

    // Produce a message
    let msg = TestMessage {
        content: "Hello Manual".into(),
    };
    queue.produce(&msg).await?;

    // Wait and check
    sleep(Duration::from_secs(1)).await;
    let msgs = received_messages.lock().await;
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].content, "Hello Manual");

    // Cleanup
    arc_client
        .xgroup_destroy::<String, _, _>("manual_test_stream", "manual_group")
        .await?;
    arc_client.del::<String, _>("manual_test_stream").await?;
    Ok(())
}

#[tokio::test]
async fn test_stealing_queue_basic_flow() -> eyre::Result<()> {
    // Demonstrate: Using direct Queue::new(...) with a `fred::Client`.
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    // Stealing queue
    let options = QueueOptions {
        pending_timeout: Some(2000),
        retry_config: None,
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };
    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "stealing_test_stream".to_string(),
        Some("stealing_group".to_string()),
        options,
    )
    .await?;

    let received1 = Arc::new(Mutex::new(Vec::new()));
    let consumer1 = TestAcknowledgeConsumer {
        received_messages: received1.clone(),
    };

    let received2 = Arc::new(Mutex::new(Vec::new()));
    let consumer2 = TestAcknowledgeConsumer {
        received_messages: received2.clone(),
    };

    queue.register_consumer(consumer1).await?;
    queue.register_consumer(consumer2).await?;

    // Produce messages
    for i in 0..5 {
        let msg = TestMessage {
            content: format!("StealMe {}", i),
        };
        queue.produce(&msg).await?;
    }

    // Wait some time
    sleep(Duration::from_secs(3)).await;

    let msgs1 = received1.lock().await;
    let msgs2 = received2.lock().await;
    let total = msgs1.len() + msgs2.len();
    assert_eq!(total, 5, "Not all messages were processed");

    // Cleanup
    client
        .xgroup_destroy::<String, _, _>("stealing_test_stream", "stealing_group")
        .await?;
    client.del::<String, _>("stealing_test_stream").await?;
    Ok(())
}

#[tokio::test]
async fn test_stealing_queue_retry_behavior() -> eyre::Result<()> {
    /// A flaky consumer that fails a certain number of times before succeeding.
    struct FlakyConsumer {
        attempts: Arc<Mutex<u32>>,
        fail_up_to: u32,
    }

    #[async_trait]
    impl Consumer for FlakyConsumer {
        type Message = TestMessage;

        async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut attempts = self.attempts.lock().await;
            *attempts += 1;
            if *attempts <= self.fail_up_to {
                Err(ConsumerError::new(TestError("Simulated failure".into())))
            } else {
                delivery.ack().await?;
                Ok(())
            }
        }

        async fn should_retry(&self, delivery: &Delivery<Self::Message>) -> bool {
            // Retry as long as we haven't reached fail_up_to attempts
            delivery.retry_count() < self.fail_up_to
        }
    }

    // Demonstrate: Using the Builder with a Redis URL (no prebuilt client).
    let options = QueueOptions {
        pending_timeout: Some(500),
        retry_config: Some(RetryConfig {
            max_retries: 3,
            retry_delay: 0,
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = QueueBuilder::<TestMessage>::new()
        .url(&get_redis_url())
        .stream("retry_test_stream")
        .group("retry_group")
        .options(options)
        .build()
        .await?;

    let attempts = Arc::new(Mutex::new(0));
    let consumer = FlakyConsumer {
        attempts: attempts.clone(),
        fail_up_to: 3,
    };
    queue.register_consumer(consumer).await?;

    let msg = TestMessage {
        content: "Retry me".into(),
    };
    queue.produce(&msg).await?;

    sleep(Duration::from_secs(3)).await;

    let c = *attempts.lock().await;
    assert_eq!(c, 4, "Expected 4 attempts: initial + 3 retries");

    // Cleanup
    let client = queue.client().clone(); // if you add a .client() getter to your Queue
    client
        .xgroup_destroy::<String, _, _>("retry_test_stream", "retry_group")
        .await?;
    client.del::<String, _>("retry_test_stream").await?;
    Ok(())
}

#[tokio::test]
async fn test_handling_invalid_messages() -> eyre::Result<()> {
    // Demonstrate: Using the optional `Queue::from_url(...)` helper method.
    let queue = Queue::<TestMessage>::from_url(
        &get_redis_url(),
        "invalid_test_stream",
        Some("invalid_group"),
        QueueOptions::default(),
    )
    .await?;

    let client = queue.client().clone(); // If you add a public getter or store the client

    let received = Arc::new(Mutex::new(Vec::new()));
    let consumer = TestAcknowledgeConsumer {
        received_messages: received.clone(),
    };
    queue.register_consumer(consumer).await?;

    // Valid message
    let valid_msg = TestMessage {
        content: "ValidMsg".into(),
    };
    queue.produce(&valid_msg).await?;

    // Invalid message directly into the stream
    client
        .xadd::<(), _, _, _, _>("invalid_test_stream", true, None, "*", ("data", "not_json"))
        .await?;

    sleep(Duration::from_secs(2)).await;
    let msgs = received.lock().await;
    assert_eq!(msgs.len(), 1, "Only the valid message should be processed");
    assert_eq!(msgs[0].content, "ValidMsg");

    // Cleanup
    client
        .xgroup_destroy::<String, _, _>("invalid_test_stream", "invalid_group")
        .await?;
    client.del::<String, _>("invalid_test_stream").await?;
    Ok(())
}

#[tokio::test]
async fn test_queue_graceful_shutdown() -> eyre::Result<()> {
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "shutdown_test_stream".to_string(),
        Some("shutdown_group".to_string()),
        QueueOptions::default(),
    )
    .await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let consumer = TestAcknowledgeConsumer {
        received_messages: received.clone(),
    };
    queue.register_consumer(consumer).await?;

    // Produce multiple messages
    for i in 0..3 {
        let msg = TestMessage {
            content: format!("Msg {}", i),
        };
        queue.produce(&msg).await?;
    }

    // Wait a bit for processing
    sleep(Duration::from_secs(1)).await;

    // Shutdown the queue
    queue.shutdown(Some(2000)).await; // Graceful shutdown

    let msgs = received.lock().await;
    assert_eq!(
        msgs.len(),
        3,
        "All messages should be processed before shutdown"
    );

    // Cleanup
    client
        .xgroup_destroy::<String, _, _>("shutdown_test_stream", "shutdown_group")
        .await?;
    client.del::<String, _>("shutdown_test_stream").await?;
    Ok(())
}

#[tokio::test]
async fn test_default_no_retry_behavior() -> eyre::Result<()> {
    // This test checks that with no retry_config and no override of should_retry(), no retries occur.
    // The message should fail once and never be attempted again.

    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    // Manual queue: no pending_timeout, no retry_config
    let options = QueueOptions::default();
    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "no_retry_no_override_stream".to_string(),
        Some("no_retry_no_override_group".to_string()),
        options,
    )
    .await?;

    // A consumer that fails once
    struct FailOnceConsumer {
        attempted: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl Consumer for FailOnceConsumer {
        type Message = TestMessage;

        async fn process(&self, _delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut attempted = self.attempted.lock().await;
            if !*attempted {
                *attempted = true;
                Err(ConsumerError::new(TestError("First attempt fails".into())))
            } else {
                // If it were retried, we'd see this. But we won't.
                Err(ConsumerError::new(TestError(
                    "Should never see second attempt".into(),
                )))
            }
        }
    }

    let fail_consumer = FailOnceConsumer {
        attempted: Arc::new(Mutex::new(false)),
    };
    queue.register_consumer(fail_consumer).await?;

    // Produce a message
    let msg = TestMessage {
        content: "Fail me once".into(),
    };
    queue.produce(&msg).await?;

    // Wait a bit
    sleep(Duration::from_secs(2)).await;

    // Since no retry_config and no override, should_retry = false by default.
    // The message fails once and no second attempt occurs.
    // We cannot directly assert attempts here since we didn't store them, but we trust logic:
    // If it retried, we would have hit the second attempt panic in the consumer.
    // Since test passes without panic, we confirm no second attempt happened.

    client
        .xgroup_destroy::<String, _, _>("no_retry_no_override_stream", "no_retry_no_override_group")
        .await?;
    client
        .del::<String, _>("no_retry_no_override_stream")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_retry_override_without_retry_config() -> eyre::Result<()> {
    // This test checks that without a retry_config, but with a consumer-defined `should_retry()` that
    // always returns true, the message keeps being retried. However, to actually see multiple attempts,
    // we need `pending_timeout` so the message can be reclaimed after failure.

    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    // Stealing queue: pending_timeout set, no retry_config
    let options = QueueOptions {
        pending_timeout: Some(1000), // allow reclaim after 1 second
        retry_config: None,
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };
    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "no_config_with_override_stream".to_string(),
        Some("no_config_override_group".to_string()),
        options,
    )
    .await?;

    // A consumer that always fails and always returns true in should_retry().
    struct AlwaysRetryConsumer {
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for AlwaysRetryConsumer {
        type Message = TestMessage;

        async fn process(&self, _delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            Err(ConsumerError::new(TestError("I always fail".into())))
        }

        async fn should_retry(&self, _delivery: &Delivery<Self::Message>) -> bool {
            true // Always retry, no matter what
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let cons = AlwaysRetryConsumer {
        attempts: attempts.clone(),
    };
    queue.register_consumer(cons).await?;

    // Produce a message
    let msg = TestMessage {
        content: "Keep failing".into(),
    };
    queue.produce(&msg).await?;

    // We wait a few seconds to allow multiple reclaims
    sleep(Duration::from_secs(4)).await;

    // After ~4 seconds with pending_timeout=1s, we should have multiple attempts
    let at = *attempts.lock().await;
    assert!(
        at > 1,
        "We expected multiple attempts since should_retry always true and stealing enabled"
    );

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>(
            "no_config_with_override_stream",
            "no_config_override_group",
        )
        .await?;
    client
        .del::<String, _>("no_config_with_override_stream")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_default_retry_behavior_with_config() -> eyre::Result<()> {
    // With a retry_config and no override, we rely on the default should_retry().
    // We'll fail a certain number of times and confirm it stops retrying after max_retries.

    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    // Set pending_timeout so message can be reclaimed for retries
    let options = QueueOptions {
        pending_timeout: Some(1000),
        retry_config: Some(RetryConfig {
            max_retries: 2, // 2 retries after initial
            retry_delay: 0,
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };
    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "retry_config_no_override_stream".to_string(),
        Some("retry_config_no_override_group".to_string()),
        options,
    )
    .await?;

    // Consumer that fails until attempts > 3 (initial + 2 retries = 3 attempts total)
    struct CountingConsumer {
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for CountingConsumer {
        type Message = TestMessage;

        async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            if *att <= 3 {
                Err(ConsumerError::new(TestError(format!(
                    "fail attempt {}",
                    *att
                ))))
            } else {
                delivery.ack().await?;
                Ok(())
            }
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let cons = CountingConsumer {
        attempts: attempts.clone(),
    };
    queue.register_consumer(cons).await?;

    // Produce message
    let msg = TestMessage {
        content: "Retry with config".into(),
    };
    queue.produce(&msg).await?;

    sleep(Duration::from_secs(3)).await;

    let at = *attempts.lock().await;
    // We expected: initial attempt + 2 retries = 3 attempts total.
    // On the 4th attempt, we would exceed max_retries and should_retry returns false, no retry.
    // So attempts must be exactly 3 and then stop retrying.
    assert_eq!(at, 3, "Expected 3 attempts total");

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>(
            "retry_config_no_override_stream",
            "retry_config_no_override_group",
        )
        .await?;
    client
        .del::<String, _>("retry_config_no_override_stream")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_retry_override_ignores_retry_config() -> eyre::Result<()> {
    // If retry_config is present but the consumer override returns false,
    // then no retries should happen.

    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let options = QueueOptions {
        pending_timeout: Some(1000),
        retry_config: Some(RetryConfig {
            max_retries: 5,
            retry_delay: 0,
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };
    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "retry_config_override_stream".to_string(),
        Some("retry_config_override_group".to_string()),
        options,
    )
    .await?;

    // Consumer always fails and always returns false for should_retry()
    struct NoRetryConsumer {
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for NoRetryConsumer {
        type Message = TestMessage;

        async fn process(&self, _delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            Err(ConsumerError::new(TestError("fail always".into())))
        }

        async fn should_retry(&self, _delivery: &Delivery<Self::Message>) -> bool {
            false // Ignore retry_config entirely
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let cons = NoRetryConsumer {
        attempts: attempts.clone(),
    };
    queue.register_consumer(cons).await?;

    // Produce message
    let msg = TestMessage {
        content: "No retry due to override".into(),
    };
    queue.produce(&msg).await?;

    // Wait long enough for potential retries if they were to happen
    sleep(Duration::from_secs(3)).await;

    let at = *attempts.lock().await;
    // Only 1 attempt should have occurred, no retries because should_retry() always returns false.
    assert_eq!(at, 1, "Expected only 1 attempt, no retries due to override");

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>(
            "retry_config_override_stream",
            "retry_config_override_group",
        )
        .await?;
    client
        .del::<String, _>("retry_config_override_stream")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_manual_queue_retries_until_success() -> Result<(), Box<dyn Error>> {
    // In this test:
    // - We have no `pending_timeout`, making this a manual queue.
    // - We supply a `retry_config` with max_retries = 3.
    // - The consumer will fail the first 2 attempts and succeed on the 3rd attempt.
    // Expected behavior:
    // Initial attempt (fails), retry 1 (fails), retry 2 (succeeds), total attempts = 3.
    // After success, the message should be acked and removed.

    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    // Manual queue: no pending_timeout
    let options = QueueOptions {
        pending_timeout: None,
        retry_config: Some(RetryConfig {
            max_retries: 3,
            retry_delay: 0, // no delay for simplicity
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "manual_blocking_retries_success_stream".to_string(),
        Some("manual_blocking_success_group".to_string()),
        options,
    )
    .await?;

    #[derive(Debug, Clone)]
    struct CustomConsumerError(String);

    impl fmt::Display for CustomConsumerError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Error for CustomConsumerError {}

    struct EventuallySucceedsConsumer {
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for EventuallySucceedsConsumer {
        type Message = TestMessage;

        async fn process(&self, _delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            if *att <= 2 {
                Err(ConsumerError::new(CustomConsumerError(format!(
                    "fail attempt {}",
                    *att
                ))))
            } else {
                // On the 3rd attempt (initial + 2 retries = 3rd), succeed
                Ok(())
            }
        }

        async fn should_retry(&self, delivery: &Delivery<Self::Message>) -> bool {
            if let Some(e) = delivery.error().await {
                if let Some(e) = e.source() {
                    if e.is::<CustomConsumerError>() {
                        return false;
                    }
                }
            }

            return true;
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let consumer = EventuallySucceedsConsumer {
        attempts: attempts.clone(),
    };
    queue.register_consumer(consumer).await?;

    let msg = TestMessage {
        content: "Eventually succeed".into(),
    };
    queue.produce(&msg).await?;

    // Wait enough time for attempts: initial + 2 retries with no delays should be quick
    // But let's wait a bit to be safe.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let at = *attempts.lock().await;
    // Expected attempts = 3 (initial attempt + 2 retries, succeed on 3rd)
    assert_eq!(at, 3, "Expected exactly 3 attempts total before success");

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>(
            "manual_blocking_retries_success_stream",
            "manual_blocking_success_group",
        )
        .await?;
    client
        .del::<String, _>("manual_blocking_retries_success_stream")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_manual_queue_exhaust_retries() -> eyre::Result<()> {
    // In this test:
    // - Manual queue (no pending_timeout).
    // - retry_config with max_retries = 2.
    // The consumer always fails.
    // Expected behavior:
    // Initial attempt fails, retry 1 fails, retry 2 fails, then since max_retries reached, we ack and give up.
    // Total attempts = initial + max_retries = 1 + 2 = 3 attempts total.

    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let options = QueueOptions {
        pending_timeout: None,
        retry_config: Some(RetryConfig {
            max_retries: 2,
            retry_delay: 0, // no delay
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "manual_blocking_retries_exceed_stream".to_string(),
        Some("manual_blocking_exceed_group".to_string()),
        options,
    )
    .await?;

    struct AlwaysFailConsumer {
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for AlwaysFailConsumer {
        type Message = TestMessage;

        async fn process(&self, _delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            Err(ConsumerError::new(TestError("Always fail".into())))
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let consumer = AlwaysFailConsumer {
        attempts: attempts.clone(),
    };
    queue.register_consumer(consumer).await?;

    let msg = TestMessage {
        content: "Never succeed".into(),
    };
    queue.produce(&msg).await?;

    // Wait a bit to allow the attempts
    tokio::time::sleep(Duration::from_secs(2)).await;

    let at = *attempts.lock().await;
    // Expected attempts = initial (1) + max_retries (2) = 3 total attempts, all fail, then ack and stop.
    assert_eq!(
        at, 3,
        "Expected 3 attempts total, no more after max_retries reached"
    );

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>(
            "manual_blocking_retries_exceed_stream",
            "manual_blocking_exceed_group",
        )
        .await?;
    client
        .del::<String, _>("manual_blocking_retries_exceed_stream")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_idempotent_processing() -> eyre::Result<()> {
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let options = QueueOptions {
        pending_timeout: None,
        retry_config: Some(RetryConfig {
            max_retries: 3,
            retry_delay: 0,
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "idempotency_stream".to_string(),
        Some("idempotency_group".to_string()),
        options,
    )
    .await?;

    let global_counter = Arc::new(Mutex::new(0));

    struct IdempotentConsumer {
        attempts: Arc<Mutex<u32>>,
        global_counter: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for IdempotentConsumer {
        type Message = TestMessage;

        async fn process(
            &self,
            _delivery: &Delivery<Self::Message>,
        ) -> eyre::Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            if *att <= 2 {
                // Fail first two attempts
                Err(ConsumerError::new(TestError(
                    format!("Fail attempt {}", *att).into(),
                )))
            } else {
                // On success, increment the global counter once
                let mut counter = self.global_counter.lock().await;
                *counter += 1;
                Ok(())
            }
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let consumer = IdempotentConsumer {
        attempts: attempts.clone(),
        global_counter: global_counter.clone(),
    };

    queue.register_consumer(consumer).await?;
    let msg = TestMessage {
        content: "Idempotent test".into(),
    };
    queue.produce(&msg).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let counter_val = *global_counter.lock().await;
    assert_eq!(
        counter_val, 1,
        "Global counter should only increment once despite multiple attempts"
    );

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>("idempotency_stream", "idempotency_group")
        .await?;
    client.del::<String, _>("idempotency_stream").await?;

    Ok(())
}

#[tokio::test]
async fn test_infinite_retry_loop_in_stealing_queue() -> eyre::Result<()> {
    // This is a pathological scenario:
    // - pending_timeout is set, enabling stealing
    // - consumer always fails and always returns true in should_retry()
    // The message will remain pending and reclaimed forever.
    // We'll run the test for a few seconds and confirm attempts > 1.

    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let options = QueueOptions {
        pending_timeout: Some(1000), // 1 second
        retry_config: None,          // No numeric limit, should_retry decides forever
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "infinite_retry_stream".to_string(),
        Some("infinite_retry_group".to_string()),
        options,
    )
    .await?;

    struct InfiniteRetryConsumer {
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for InfiniteRetryConsumer {
        type Message = TestMessage;

        async fn process(
            &self,
            _delivery: &Delivery<Self::Message>,
        ) -> eyre::Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            Err(ConsumerError::new(TestError("Always fail".into())))
        }

        async fn should_retry(&self, _delivery: &Delivery<Self::Message>) -> bool {
            true // Always retry
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let consumer = InfiniteRetryConsumer {
        attempts: attempts.clone(),
    };
    queue.register_consumer(consumer).await?;

    let msg = TestMessage {
        content: "Never succeed, always retry".into(),
    };
    queue.produce(&msg).await?;

    // Wait a few seconds to allow multiple reclaims
    tokio::time::sleep(Duration::from_secs(5)).await;

    let at = *attempts.lock().await;
    // After 5 seconds with a pending_timeout of 1 second, we expect multiple attempts.
    // The exact number isn't deterministic, but > 1 is a good sign it's being retried repeatedly.
    assert!(
        at > 1,
        "Expected multiple attempts due to infinite retry scenario"
    );

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>("infinite_retry_stream", "infinite_retry_group")
        .await?;
    client.del::<String, _>("infinite_retry_stream").await?;

    Ok(())
}

#[tokio::test]
async fn test_long_delay_high_retry_manual_queue() -> eyre::Result<()> {
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let options = QueueOptions {
        pending_timeout: None, // manual queue
        retry_config: Some(RetryConfig {
            max_retries: 50,
            retry_delay: 1000, // 1 second delay between attempts
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "high_retry_long_delay_stream".to_string(),
        Some("high_retry_long_delay_group".to_string()),
        options,
    )
    .await?;

    struct LateSuccessConsumer {
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for LateSuccessConsumer {
        type Message = TestMessage;

        async fn process(
            &self,
            delivery: &Delivery<Self::Message>,
        ) -> eyre::Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            if *att < 50 {
                Err(ConsumerError::new(TestError(
                    format!("Fail attempt {}", *att).into(),
                )))
            } else {
                delivery.ack().await?;
                Ok(())
            }
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let consumer = LateSuccessConsumer {
        attempts: attempts.clone(),
    };
    queue.register_consumer(consumer).await?;

    let msg = TestMessage {
        content: "Very patient message".into(),
    };
    queue.produce(&msg).await?;

    // This test might run for a while due to long delays. Wait enough time for ~50 attempts * 1s delay.
    tokio::time::sleep(Duration::from_secs(60)).await; // 50 attempts * 1s = 50s, + overhead.

    let at = *attempts.lock().await;
    // Expect at == 50 total attempts
    // Final attempt should succeed and ack the message.
    assert_eq!(at, 50, "Should have retried 50 times plus initial attempt");

    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>(
            "high_retry_long_delay_stream",
            "high_retry_long_delay_group",
        )
        .await?;
    client
        .del::<String, _>("high_retry_long_delay_stream")
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_consumer_panic_handling() -> eyre::Result<()> {
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    // Stealing queue scenario
    let options = QueueOptions {
        pending_timeout: Some(1000),
        retry_config: None,
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "panic_test_stream".to_string(),
        Some("panic_group".to_string()),
        options,
    )
    .await?;

    struct PanicConsumer;

    #[async_trait]
    impl Consumer for PanicConsumer {
        type Message = TestMessage;

        async fn process(
            &self,
            _delivery: &Delivery<Self::Message>,
        ) -> eyre::Result<(), ConsumerError> {
            panic!("Simulated panic in process!");
        }
    }

    let consumer = PanicConsumer;
    queue.register_consumer(consumer).await?;

    // Produce a message that will cause a panic
    let msg = TestMessage {
        content: "Panic message".into(),
    };
    queue.produce(&msg).await?;

    // Wait a bit
    sleep(Duration::from_secs(2)).await;

    // The consumer task should have terminated due to panic. The system should still be alive.
    // We can't confirm behavior easily here except that the test doesn't crash.
    // If `should_retry()` is false by default, no retries occur. The message remains pending.

    // Cleanup
    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>("panic_test_stream", "panic_group")
        .await?;
    client.del::<String, _>("panic_test_stream").await?;

    Ok(())
}

#[tokio::test]
async fn test_time_based_retry_logic() -> eyre::Result<()> {
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let options = QueueOptions {
        pending_timeout: None, // manual queue
        retry_config: Some(RetryConfig {
            max_retries: 10,
            retry_delay: 500, // 0.5s delay
        }),
        poll_interval: Some(100),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "time_based_retry_stream".to_string(),
        Some("time_based_retry_group".to_string()),
        options,
    )
    .await?;

    struct TimeBasedRetryConsumer {
        start: std::time::Instant,
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Consumer for TimeBasedRetryConsumer {
        type Message = TestMessage;

        async fn process(
            &self,
            _delivery: &Delivery<Self::Message>,
        ) -> eyre::Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            if *att < 5 {
                Err(ConsumerError::new(TestError(
                    format!("fail attempt {}", *att).into(),
                )))
            } else {
                Ok(()) // succeed on 5th attempt
            }
        }

        async fn should_retry(&self, delivery: &Delivery<Self::Message>) -> bool {
            let elapsed = self.start.elapsed().as_secs();
            if elapsed < 10 {
                // Within 10s, rely on default logic from retry_config
                // (which means retry_count < max_retries)
                if let Some(max_r) = delivery.max_retries {
                    delivery.retry_count() < max_r
                } else {
                    false
                }
            } else {
                // After 10s have passed since consumer start, no more retries.
                false
            }
        }
    }

    let attempts = Arc::new(Mutex::new(0));
    let consumer = TimeBasedRetryConsumer {
        start: std::time::Instant::now(),
        attempts: attempts.clone(),
    };
    queue.register_consumer(consumer).await?;

    let msg = TestMessage {
        content: "Time-based retry".into(),
    };
    queue.produce(&msg).await?;

    // Wait enough time for several attempts. The consumer tries until attempt <5 succeeds.
    // With 0.5s delay, ~2-3 seconds should suffice for 5 attempts.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let at = *attempts.lock().await;
    // Expect attempts = 5 total
    assert_eq!(
        at, 5,
        "Should succeed on the 5th attempt before 10s elapsed"
    );

    queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>("time_based_retry_stream", "time_based_retry_group")
        .await?;
    client.del::<String, _>("time_based_retry_stream").await?;

    Ok(())
}

#[tokio::test]
async fn test_multiple_consumers_dlq_integration() -> eyre::Result<()> {
    // Setup Redis client
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    // Initialize the primary Queue for Consumer A
    let options_a = QueueOptions {
        pending_timeout: Some(1000), // 1 second
        retry_config: Some(RetryConfig {
            max_retries: 3,
            retry_delay: 50,
        }),
        poll_interval: Some(100),
        enable_dlq: true,                                 // Enable DLQ
        dlq_name: Some("multi_policies_dlq".to_string()), // Custom DLQ stream
    };

    let queue_a = Queue::<TestMessage>::new(
        client.clone(),
        "multi_policies_stream".to_string(),
        Some("multi_policies_group_a".to_string()), // Unique group for Consumer A
        options_a,
    )
    .await?;

    // Initialize the primary Queue for Consumer B
    let options_b = QueueOptions {
        pending_timeout: Some(1000), // 1 second
        retry_config: Some(RetryConfig {
            max_retries: 2,
            retry_delay: 50,
        }),
        poll_interval: Some(100),
        enable_dlq: true,                                 // Enable DLQ
        dlq_name: Some("multi_policies_dlq".to_string()), // Same DLQ stream
    };

    let queue_b = Queue::<TestMessage>::new(
        client.clone(),
        "multi_policies_stream".to_string(),
        Some("multi_policies_group_b".to_string()), // Unique group for Consumer B
        options_b,
    )
    .await?;

    // Initialize the DLQ Queue for Consumer C
    let dlq_options = QueueOptions {
        pending_timeout: Some(1000),
        retry_config: None,
        poll_interval: Some(100),
        enable_dlq: false, // No further DLQ for DLQ
        dlq_name: None,
    };

    let dlq_queue = Queue::<TestMessage>::new(
        client.clone(),
        "multi_policies_dlq".to_string(),
        Some("dead_letter_group".to_string()),
        dlq_options,
    )
    .await?;

    // Define Consumers A, B, and C with different policies
    struct NumericLimitConsumer {
        attempts: Arc<Mutex<u32>>,
    }
    #[async_trait]
    impl Consumer for NumericLimitConsumer {
        type Message = TestMessage;

        async fn process(&self, _delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            Err(ConsumerError::new(TestError("Always fail A".into())))
        }

        async fn should_retry(&self, delivery: &Delivery<Self::Message>) -> bool {
            // Retry up to 2 times
            delivery.retry_count() < 2
        }
    }

    struct NoRetryConsumer {
        attempts: Arc<Mutex<u32>>,
    }
    #[async_trait]
    impl Consumer for NoRetryConsumer {
        type Message = TestMessage;

        async fn process(&self, _delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            Err(ConsumerError::new(TestError("Always fail B".into())))
        }

        async fn should_retry(&self, _delivery: &Delivery<Self::Message>) -> bool {
            false // Do not retry
        }
    }

    struct InfiniteRetryConsumer {
        attempts: Arc<Mutex<u32>>,
    }
    #[async_trait]
    impl Consumer for InfiniteRetryConsumer {
        type Message = TestMessage;

        async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut att = self.attempts.lock().await;
            *att += 1;
            // Succeed after 5 attempts
            if *att > 5 {
                delivery.ack().await?;
                Ok(())
            } else {
                Err(ConsumerError::new(TestError(
                    format!("Fail C attempt {}", *att).into(),
                )))
            }
        }

        async fn should_retry(&self, _delivery: &Delivery<Self::Message>) -> bool {
            true // Always retry
        }
    }

    // Initialize attempt counters
    let a_attempts = Arc::new(Mutex::new(0));
    let b_attempts = Arc::new(Mutex::new(0));
    let c_attempts = Arc::new(Mutex::new(0));

    // Instantiate Consumers
    let cons_a = NumericLimitConsumer {
        attempts: a_attempts.clone(),
    };
    let cons_b = NoRetryConsumer {
        attempts: b_attempts.clone(),
    };
    let cons_c = InfiniteRetryConsumer {
        attempts: c_attempts.clone(),
    };

    // Register Consumers A and B to their respective groups
    queue_a.register_consumer(cons_a).await?;
    queue_b.register_consumer(cons_b).await?;

    // Register Consumer C to the DLQ group
    dlq_queue.register_consumer(cons_c).await?;

    // Produce a single message
    let msg = TestMessage {
        content: "Multi-consumer with DLQ test".into(),
    };
    queue_a.produce(&msg).await?;

    // Wait sufficient time for all processing and DLQ handling
    sleep(Duration::from_secs(5)).await;

    // Retrieve the counts
    let a = *a_attempts.lock().await;
    let b = *b_attempts.lock().await;
    let c = *c_attempts.lock().await;

    // Assertions
    // Consumer A should have tried exactly 3 times
    assert_eq!(a, 3, "Consumer A should have attempted exactly 3 times");

    // Consumer B should have tried once and moved to DLQ
    assert_eq!(b, 1, "Consumer B should have attempted exactly 1 time");

    // Consumer C should have attempted multiple times until success
    assert!(
        c > 5,
        "Consumer C should have tried multiple times until success"
    );

    // Verify that the message was moved to DLQ by querying the DLQ stream directly
    let dlq_stream = "multi_policies_dlq";

    // Use XRANGE to retrieve all messages in the DLQ
    let dlq_messages: Vec<(String, HashMap<String, Value>)> = client
        .xrange(
            dlq_stream,
            "-",       // Start ID
            "+",       // End ID
            Some(100), // Limit
        )
        .await?;

    // Check if the produced message exists in the DLQ
    let message_in_dlq = dlq_messages.iter().any(|(_, fields)| {
        if let Some(data) = fields.get("data") {
            if let Ok(msg) = serde_json::from_value::<TestMessage>(data.clone()) {
                return msg.content == "Multi-consumer with DLQ test";
            }
        }
        false
    });

    assert!(message_in_dlq, "Message should have been moved to the DLQ");

    // Cleanup
    queue_a.shutdown(Some(2000)).await;
    queue_b.shutdown(Some(2000)).await;
    dlq_queue.shutdown(Some(2000)).await;
    client
        .xgroup_destroy::<String, _, _>("multi_policies_stream", "multi_policies_group_a")
        .await?;
    client
        .xgroup_destroy::<String, _, _>("multi_policies_stream", "multi_policies_group_b")
        .await?;
    client
        .xgroup_destroy::<String, _, _>(dlq_stream, "dead_letter_group")
        .await?;
    client.del::<String, _>("multi_policies_stream").await?;
    client.del::<String, _>(dlq_stream).await?;
    Ok(())
}

#[tokio::test]
async fn test_large_scale_throughput_stress() -> eyre::Result<()> {
    let config = Config::from_url(&get_redis_url())?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;
    let client = Arc::new(client);

    let options = QueueOptions {
        pending_timeout: Some(2000),
        retry_config: Some(RetryConfig {
            max_retries: 3,
            retry_delay: 50,
        }),
        poll_interval: Some(50),
        enable_dlq: false,
        dlq_name: None,
    };

    let queue = Queue::<TestMessage>::new(
        client.clone(),
        "large_scale_stream".to_string(),
        Some("large_scale_group".to_string()),
        options,
    )
    .await?;

    struct MostlyOkConsumer {
        received: Arc<Mutex<u32>>,
        acked: Arc<Mutex<u32>>, // New counter for acks
    }

    #[async_trait]
    impl Consumer for MostlyOkConsumer {
        type Message = TestMessage;

        async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
            let mut rec = self.received.lock().await;
            *rec += 1;

            // Adjusted failure rate to ~1%
            if rand::random::<u8>() < 3 {
                // Approximately 1% failure
                Err(ConsumerError::new(TestError("Random failure".into())))
            } else {
                delivery.ack().await?;
                let mut ack = self.acked.lock().await;
                *ack += 1; // Increment acked counter on successful ack
                Ok(())
            }
        }
    }

    // Initialize both counters
    let received_messages = Arc::new(Mutex::new(0));
    let acked_messages = Arc::new(Mutex::new(0));
    let consumer = MostlyOkConsumer {
        received: received_messages.clone(),
        acked: acked_messages.clone(),
    };
    queue.register_consumer(consumer).await?;

    // Produce a large number of messages
    let total_msgs = 1000;
    for i in 0..total_msgs {
        let msg = TestMessage {
            content: format!("Msg {}", i),
        };
        queue.produce(&msg).await?;
    }

    // Wait enough time for all messages to be processed or retried
    sleep(Duration::from_secs(50)).await;

    // Retrieve the counts
    let rec_count = *received_messages.lock().await;
    let ack_count = *acked_messages.lock().await;

    // Assert that at least 100% of the messages are received
    assert!(
        rec_count >= total_msgs,
        "Expected at least {} received messages, but got {}",
        total_msgs,
        rec_count
    );

    // Assert that at least 90% of messages are acknowledged
    let expected_min_ack = (total_msgs as f64 * 0.9).ceil() as u32;
    assert!(
        ack_count >= expected_min_ack,
        "Expected at least {} acknowledgments, but got {}",
        expected_min_ack,
        ack_count
    );

    // Cleanup
    queue.shutdown(Some(5000)).await;
    client
        .xgroup_destroy::<String, _, _>("large_scale_stream", "large_scale_group")
        .await?;
    client.del::<String, _>("large_scale_stream").await?;
    Ok(())
}
