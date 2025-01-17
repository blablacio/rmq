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
    let queue = Queue::new(Arc::new(client), "my_stream".to_string(), Some("my_group".to_string()), options).await?;

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

**Or** if you’d rather not create a `fred::Client` yourself, you can rely purely on the builder’s Redis URL:

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
            dlq_name: Some("my_dlq_stream".to_string()),
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
    dlq_name: Some("my_dlq_stream".to_string()),
    ..Default::default()
};
```

When a consumer fails, the queue decides (based on `should_retry()`) if the message is retried or moved to the DLQ.

### Manual vs. Stealing Queues

If `pending_timeout` is `None`, the queue is a **manual queue** (messages won’t be reclaimed automatically). The consumer must **ack** or **retry** in-process.  
If `pending_timeout` is `Some(...)`, it’s a **stealing queue**—Redis Streams will auto-claim messages that a consumer has taken too long to acknowledge.

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

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request at [GitHub](https://github.com/blablacio/rmq). We’d love to see your ideas on advanced features, improved scheduling, or broader use cases.

---

## License

This project is licensed under [The Unlicense](https://unlicense.org).  
You can find the license text in the [LICENSE](LICENSE) file.

---

**Happy messaging with Redis Streams & rmq!**
