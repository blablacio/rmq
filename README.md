# rmq

**rmq** is a Redis-based message queue library designed for simplicity, flexibility, and performance. It leverages Redis Streams to provide reliable message delivery, consumer groups, retries, optional dead-letter queues (DLQ), and more. With **rmq**, you can easily produce, consume, and manage messages across distributed systems in Rust.

## Features

- **Consumer Groups**
  Utilize Redis Streams consumer groups to coordinate multiple consumers working on the same stream.

- **Retries & Backoff**
  Configure retry policies, delays, and custom logic for when and how messages should be retried.

- **Dead-Letter Queues (DLQ)**
  Optionally route failed messages that exceed the maximum retry count to a dead-letter queue for manual inspection.

- **Keep-Alive & Manual Queues**
  Keep messages claimed if processing takes longer than expected, or use manual queues for more control over message acknowledgment.

- **Multiple Initialization Modes**
  Use a single `fred::Client`, pass a Redis URL directly, or use our fluent `QueueBuilder` to configure the queue step by step.

- **Clonable Queues & Deliveries**
  Both `Queue` and `Delivery` structs can be freely cloned and passed around, enabling flexible concurrency patterns and easier integration with async workflows.

- **Message Prefetching**
  Efficient message prefetching reduces Redis calls and CPU usage, especially with many consumers.

- **Manual Consumer Scaling**
  Manually add or remove consumers as needed based on external configuration or workload demands.

- **Auto-Scaling**
  Dynamically adjust the number of consumers based on workload to optimize resource usage and throughput.

- **Integration-Ready**
  Easily run integration tests against a local or containerized Redis instance for reliable testing in CI/CD pipelines.

---

## Getting Started

### Prerequisites

- **Rust Toolchain**
  Install Rust (via [rustup](https://rustup.rs/)) to build the library.

- **Redis**
  A running Redis instance is required. For local development, you can quickly spin one up with Docker:
  ```bash
  docker run --rm -p 6379:6379 redis:latest
  ```

### Installation

Add **rmq** to your `Cargo.toml`:

```toml
[dependencies]
rmq = { git = "https://github.com/blablacio/rmq.git", branch = "main" }
```

---

## Usage

### 1. Direct Creation with an Existing `fred::Client`

If you already have a `fred` client configured, you can build a `Queue` directly:

```rust
use rmq::{Queue, QueueOptions, Consumer, Delivery};
use fred::prelude::*;
use async_trait::async_trait;
use eyre::Result;
use std::sync::Arc;

#[derive(Clone)]
struct MyMessage {
    content: String,
}

#[derive(Clone)]
struct MyConsumer;

#[async_trait]
impl Consumer for MyConsumer {
    type Message = MyMessage;

    async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<()> {
        println!("Processing message: {}", delivery.message.content);
        // Acknowledge on success
        delivery.ack().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create & connect a Fred client
    let config = Config::from_url("redis://127.0.0.1:6379")?;
    let client = Client::new(config, None, None, None);
    client.init().await;

    // Build a queue from the client
    let options = QueueOptions::default();
    let queue = Queue::new(
        Arc::new(client),
        "my_stream".to_string(),
        Some("my_group".to_string()),
        options,
        None, // consumer_factory
        None, // scaling_strategy
    ).await?;

    // Register consumer
    queue.register_consumer(MyConsumer).await?;

    // Produce a message
    let msg = MyMessage { content: "Hello, Direct!".into() };
    queue.produce(&msg).await?;

    // Shutdown gracefully
    queue.shutdown(None).await;

    Ok(())
}
```

### 2. Using `Queue::from_url(...)`

For a simpler approach—**without** manually configuring a `fred::Client`—you can call a convenience constructor:

```rust
use rmq::{Queue, QueueOptions};
use async_trait::async_trait;
use eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let options = QueueOptions::default();

    // Initialize a queue from a Redis URL
    let queue = Queue::<String>::from_url(
        "redis://127.0.0.1:6379",
        "my_stream",
        Some("my_group"),
        options
    ).await?;

    // Register consumer, produce messages, etc.
    // ...
    queue.shutdown(None).await;

    Ok(())
}
```

### 3. Builder Pattern

**rmq** also supports a fluent builder API, which is helpful if you want to customize your queue step-by-step or prefer the style:

```rust
use rmq::{Queue, QueueOptions, RetryConfig, Consumer, Delivery};
use fred::prelude::*;
use async_trait::async_trait;
use eyre::Result;
use std::sync::Arc;

#[derive(Clone)]
struct MyConsumer;

#[async_trait]
impl Consumer for MyConsumer {
    type Message = String;
    async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<()> {
        println!("Processing: {}", delivery.message);
        delivery.ack().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Example 1: Provide an existing Fred client
    let config = Config::from_url("redis://127.0.0.1:6379")?;
    let raw_client = Client::new(config, None, None, None);
    raw_client.connect();
    raw_client.wait_for_connect().await?;
    let existing_client = Arc::new(raw_client);

    let queue = QueueBuilder::<String>::new()
        .client(existing_client)
        .stream("builder_stream")
        .group("builder_group")
        .retry_config(5, 1000) // 5 retries, 1 second delay
        .build()
        .await?;

    queue.register_consumer(MyConsumer).await?;
    queue.produce(&"Hello from the builder!".to_string()).await?;
    queue.shutdown(None).await;

    Ok(())
}
```

**Or** if you'd rather not create a `fred::Client` yourself, you can rely purely on the builder's Redis URL:

```rust
#[tokio::main]
async fn main() -> eyre::Result<()> {
    let queue = QueueBuilder::<String>::new()
        .url("redis://127.0.0.1:6379")
        .stream("builder_stream")
        .group("builder_group")
        .options(QueueOptions {
            retry_config: Some(RetryConfig {
                max_retries: 3,
                retry_delay: 500,
            }),
            enable_dlq: true,
            auto_recovery: Some(30000), // Auto-recover messages pending for 30 seconds on startup
            delete_on_ack: true,        // Delete messages after successful acknowledgment
            poll_interval: Some(50),      // Custom poll interval
            pending_timeout: Some(5000),  // Custom pending timeout
            dlq_name: Some("my_dlq_stream".to_string()), // Or use a Dead-Letter Queue
            ..Default::default()
        })
        .build()
        .await?;

    // Use the queue
    // ...
    queue.shutdown(None).await;
    Ok(())
}
```

---

## Advanced Configuration

### Retry & DLQ

Use `QueueOptions` and `RetryConfig` to fine-tune retry behavior and dead-letter queues:

```rust
use rmq::{QueueOptions, RetryConfig};

let options = QueueOptions {
    pending_timeout: Some(2000), // 2-second reclaim for a 'stealing' queue
    retry_config: Some(RetryConfig {
        max_retries: 5,
        retry_delay: 1000, // 1-second delay before each retry
    }),
    enable_dlq: true,
    auto_recovery: Some(30000), // Auto-recover messages pending for 30 seconds on startup
    delete_on_ack: true,        // Delete messages after successful acknowledgment
    poll_interval: Some(50),      // Custom poll interval
    dlq_name: Some("my_dlq_stream".to_string()), // Or use a Dead-Letter Queue
    ..Default::default()
};
```

When a consumer fails, the queue decides (based on `should_retry()`) if the message is retried or moved to the DLQ.

### Understanding Delivery Counts & Retries

**rmq** uses a consistent approach to track message delivery attempts:

- **Delivery Count**: 1-based count of how many times a message has been delivered

  - Initial delivery = 1
  - First retry = 2
  - Second retry = 3

- **Retry Count**: 0-based count of how many retries have occurred
  - Initial delivery = 0 (no retries yet)
  - First retry = 1
  - Second retry = 2

When configuring `max_retries: 2`, you'll get a total of 3 processing attempts:

- Initial delivery (retry_count=0)
- First retry (retry_count=1)
- Second retry (retry_count=2)

### Manual vs. Stealing Queues

**rmq** supports two distinct queue operation modes:

#### 1. Manual Queues (when `pending_timeout` is `None`)

- Messages won't be automatically reclaimed by other consumers
- Delivery counts are incremented explicitly by your queue after failed attempts
- Retries happen in-process (same consumer task)
- The consumer must explicitly `ack()` or fail to release the message

#### 2. Stealing Queues (when `pending_timeout` is `Some(...)`)

- Redis Streams will auto-claim messages that a consumer has taken too long to process
- Delivery counts are incremented by Redis's XAUTOCLAIM mechanism
- Failed messages can be claimed by any available consumer
- Good for workload distribution and fault tolerance

**Important**: With stealing queues, a message may be processed by different consumers during its retry sequence. This provides better system resilience but means you shouldn't rely on the same consumer handling all retries of a specific message.

### Auto-Recovery

The `auto_recovery` option, when set to `Some(timeout_ms)`, enables automatic recovery of pending messages when a consumer starts up. If messages in the pending queue have been pending for longer than `timeout_ms`, the consumer will attempt to claim and re-process them on startup. This is useful in scenarios where consumers might crash or become unavailable, ensuring messages are not stuck indefinitely in the pending state.

### Delete on Ack

Setting `delete_on_ack: true` in `QueueOptions` will automatically delete messages from the Redis stream after they are successfully acknowledged by a consumer. By default, messages remain in the stream even after acknowledgment, which can be useful for audit trails or data retention, but enabling this option provides true "queue" semantics where messages are removed after processing.

### Manual vs. Stealing Queues

If `pending_timeout` is `None`, the queue is a **manual queue** (messages won't be reclaimed automatically). The consumer must **ack** or **retry** in-process.
If `pending_timeout` is `Some(...)`, it's a **stealing queue**—Redis Streams will auto-claim messages that a consumer has taken too long to acknowledge.

### Prefetching

**rmq** implements a prefetching mechanism similar to RabbitMQ that can significantly reduce CPU usage, especially with many consumers:

- With prefetching enabled (`prefetch_config: Some(...)`), a single task fetches messages from Redis in batches.
- These messages are then distributed to consumers via internal channels.
- This reduces the number of Redis calls and dramatically improves CPU efficiency.

You can control prefetching with the `prefetch_config` option, which takes a `PrefetchConfig` struct:

```rust
use rmq::{QueueOptions, PrefetchConfig};

// Enable prefetching (default in v0.2+)
let options = QueueOptions {
    prefetch_config: Some(PrefetchConfig {
        count: 100,      // Prefetch up to 100 messages at once
        buffer_size: 50, // Buffer up to 50 messages per consumer channel
        scaling: None,   // No auto-scaling (default)
    }),
    ..Default::default()
};

// Or disable prefetching for direct consumer polling
let options = QueueOptions {
    prefetch_config: None, // Disable prefetching
    ..Default::default()
};
```

You can also set these values individually using the `QueueBuilder`:

```rust
use rmq::QueueBuilder;

let queue = QueueBuilder::<String>::new()
    .url("redis://127.0.0.1:6379")
    .stream("my_stream")
    .group("my_group")
    .prefetch_count(150) // Set the number of messages to prefetch
    .buffer_size(75)     // Set the buffer size for each consumer
    .build()
    .await?;
```

#### Prefetching Performance Characteristics:

- **CPU Usage**: Significantly lower (20-40% reduction observed in tests) - especially valuable with many consumers.
- **Throughput vs. CPU Trade-off**: May have a small impact on raw processing speed in exchange for CPU efficiency.
- **Optimal Settings**:
  - For high throughput: Set `prefetch_config.count` roughly equal to your expected active consumer count. Adjust `buffer_size` based on message processing time and desired latency.
  - For idle scenarios: Prefetching is especially valuable with many idle consumers, keeping CPU usage low.

Prefetching is particularly beneficial when you have many consumers (>10) or need to minimize CPU usage in systems with sporadic message activity.

### Initial Consumers

You can specify the number of consumers to start with when creating the queue using the `initial_consumers` option:

```rust
use rmq::{QueueBuilder, Consumer, Delivery};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

// Define a consumer that can be cloned
#[derive(Clone)]
struct MyConsumer {
    counter: Arc<AtomicU32>,
}

#[async_trait]
impl Consumer for MyConsumer {
    type Message = String;

    async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<(), ConsumerError> {
        println!("Processing: {}", delivery.message);
        self.counter.fetch_add(1, Ordering::SeqCst);
        delivery.ack().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let counter = Arc::new(AtomicU32::new(0));

    let queue = QueueBuilder::<String>::new()
        .url("redis://127.0.0.1:6379")
        .stream("my_stream")
        .group("my_group")
        .initial_consumers(5) // Start with 5 consumer instances
        .with_instance(MyConsumer { counter: counter.clone() })
        .build()
        .await?;

    // The queue already has 5 consumers running, no need to register them manually

    // Produce some messages
    for i in 0..10 {
        queue.produce(&format!("Message {}", i)).await?;
    }

    // Wait for processing to complete...

    queue.shutdown(Some(2000)).await;
    Ok(())
}
```

The `initial_consumers` option requires that you also provide a consumer factory or instance using either `with_factory()` or `with_instance()`.

### Manual Consumer Scaling

In addition to auto-scaling, you can manually control the number of consumers using the `add_consumers` and `remove_consumers` methods:

```rust
// Add 3 more consumers
queue.add_consumers(3).await?;

// Remove 2 consumers
queue.remove_consumers(2).await?;
```

This is useful for scenarios where consumer counts are controlled by external configuration or when you want to implement your own scaling logic based on application-specific metrics.

When removing consumers, the queue will prioritize removing idle consumers first to minimize disruption to ongoing processing.

### Auto-Scaling

**rmq** provides an automatic consumer scaling system that dynamically adjusts the number of active consumers based on workload:

- Scale up when message backlog builds and no idle consumers are available
- Scale down when consumers are idle and no backlog exists
- Respect minimum and maximum consumer bounds
- Use custom scaling strategies for specific workload patterns

Auto-scaling requires prefetching to be enabled and works best with cloneable consumers or a consumer factory.

#### Enabling Auto-Scaling

Using the builder pattern:

```rust
use rmq::{QueueBuilder, Consumer};
use async_trait::async_trait;
use std::sync::Arc;

// Define a consumer that can be cloned for auto-scaling
#[derive(Clone)]
struct MyScalableConsumer;

#[async_trait]
impl Consumer for MyScalableConsumer {
    type Message = String;

    async fn process(&self, delivery: &Delivery<Self::Message>) -> Result<()> {
        // Process the message
        delivery.ack().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let queue = QueueBuilder::<String>::new()
        .url("redis://127.0.0.1:6379")
        .stream("scaling_stream")
        .group("scaling_group")
        // Configure prefetching (required for scaling)
        .prefetch_count(10)
        .buffer_size(5)
        // Configure auto-scaling
        .scaling_config(
            2,      // min_consumers
            10,     // max_consumers
            1000,   // scale_interval_ms
        )
        // Provide a consumer instance that can be cloned
        .with_instance(MyScalableConsumer)
        .build()
        .await?;

    // Auto-scaling is now enabled - no need to manually register consumers

    // Produce messages
    for i in 0..100 {
        queue.produce(&format!("Message {}", i)).await?;
    }

    // The queue will automatically scale between 2-10 consumers based on load

    queue.shutdown(Some(5000)).await;
    Ok(())
}
```

#### Alternative: Factory Function

Instead of providing a cloneable consumer instance, you can provide a factory function:

```rust
let queue = QueueBuilder::<String>::new()
    // ...other configuration...
    .scaling_config(2, 10, 1000)
    .with_factory(|| {
        // Create and return a new consumer instance
        MyConsumer::new()
    })
    .build()
    .await?;
```

#### Custom Scaling Strategies

You can implement the `ScalingStrategy` trait to customize how scaling decisions are made:

```rust
use rmq::{ScalingStrategy, ScalingContext, ScaleAction};
use async_trait::async_trait;

struct MyCustomStrategy;

#[async_trait]
impl ScalingStrategy for MyCustomStrategy {
    async fn decide(&self, context: ScalingContext) -> ScaleAction {
        // Implement your custom scaling logic
        if context.overflow_size > context.current_consumers * 10 {
            // Scale up faster if backlog is large
            let scale_up = (context.max_consumers - context.current_consumers).min(3);
            ScaleAction::ScaleUp(scale_up as u32)
        } else if context.idle_consumers > context.current_consumers / 2 {
            // Scale down more aggressively if more than half consumers are idle
            let scale_down = context.idle_consumers.min(context.current_consumers - context.min_consumers);
            ScaleAction::ScaleDown(scale_down as u32)
        } else {
            ScaleAction::Hold
        }
    }
}

// Use with the builder
let queue = QueueBuilder::<String>::new()
    // ...other configuration...
    .scaling_config(2, 10, 1000)
    .with_instance(MyScalableConsumer)
    .scaling_strategy(MyCustomStrategy)
    .build()
    .await?;
```

#### Auto-Scaling Benefits

- **Resource Efficiency**: Only use as many consumers as needed
- **Automatic Load Handling**: Adapt to varying message rates
- **Simplified Operations**: No need to manually tune consumer counts

---

## Testing

### Run Tests With Docker Compose

A typical workflow for integration tests:

```bash
docker-compose up --build --abort-on-container-exit
```

This will start a Redis service and run `cargo test` inside a container, ensuring tests run in a controlled environment.

If you prefer a local approach, you can just run:

```bash
cargo test
```

_(As long as you have a local Redis instance accessible via `REDIS_URL` or the default `redis://127.0.0.1:6379`.)_

---

## Configuration Summary

- **REDIS_URL**
  An environment variable used by your code/tests to point to the Redis instance (if you want).
- **`QueueOptions`**
  Set up pending timeout, poll interval, and optional `RetryConfig`.
- **`Consumer`**
  Implement custom logic in `process()` and optionally override `should_retry()` to control message handling.
- **`PrefetchConfig`**
  Configure message prefetching and optional auto-scaling.
- **`ScalingStrategy`**
  Implement custom scaling logic by implementing this trait.
- **`initial_consumers`**
  Start with a specific number of consumers when the queue is created.
- **`add_consumers`/`remove_consumers`**
  Manually scale the number of consumers up or down based on external conditions.

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request at [GitHub](https://github.com/blablacio/rmq.git). We'd love to see your ideas on advanced features, improved scheduling, or broader use cases.

---

## License

This project is licensed under [The Unlicense](https://unlicense.org).
You can find the license text in the [LICENSE](LICENSE) file.

---

**Happy messaging with Redis Streams & rmq!**
