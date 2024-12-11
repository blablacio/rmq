# rmq

**rmq** is a Redis-based message queue library designed for simplicity, flexibility, and performance. It leverages Redis Streams to provide reliable message delivery, consumer groups, retries, optional dead-letter queues (DLQ), and more. With **rmq**, you can easily produce, consume, and manage messages across distributed systems.

## Features

- **Consumer Groups:**  
  Utilize Redis Streams consumer groups to coordinate multiple consumers working on the same stream.
- **Retries & Backoff:**  
  Configure retry policies, delays, and custom logic for when and how messages should be retried.
- **Dead-Letter Queues (DLQ):**  
  Optionally route failed messages that exceed the maximum retry count to a dead-letter queue for manual inspection.
- **Keep-Alive & Manual Queues:**  
  Keep messages claimed if processing takes longer than expected, and use manual queues for more control over message acknowledgment.
- **Clonable Queues & Deliveries:**  
  Both `Queue` and `Delivery` structs can be freely cloned and passed around, enabling flexible concurrency patterns and easier integration with async workflows.
- **Integration-Ready:**  
  Easily run integration tests against a local or containerized Redis instance for reliable testing in CI/CD pipelines.

## Getting Started

### Prerequisites

- **Rust Toolchain:**  
  Install Rust (via [rustup](https://rustup.rs/)) to build the library.
- **Redis:**  
  A running Redis instance is required. You can run it locally or in a container:

  ```bash
  docker run --rm -p 6379:6379 redis:latest
  ```

### Installation

Add rmq to your Cargo.toml:

```toml
[dependencies]
rmq = { git = "https://github.com/blablacio/rmq.git", branch = "main" }
```

### Basic Usage

1. Implement a Consumer:

```rust
use rmq::{Consumer, Delivery};
use async_trait::async_trait;
use eyre::Result;

struct MyConsumer;

#[async_trait]
impl Consumer for MyConsumer {
    type Message = String;

    async fn process(&self, delivery: &mut Delivery<Self::Message>) -> Result<()> {
        println!("Processing message: {}", delivery.message);
        // Perform your logic here, delivery.ack() will be called on success
        Ok(())
    }
}
```

2. Create a Queue and Produce Messages:

```rust
use rmq::{Queue, QueueOptions};
use fred::prelude::*;
use std::sync::Arc;
use eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Arc::new(Client::connect("redis://127.0.0.1:6379", None).await?);
    let options = QueueOptions::default();

    let queue: Queue<String> = Queue::new(client.clone(), "my_stream".to_string(), None, options).await?;

    // Produce a message
    queue.produce(&"Hello, World!".to_string()).await?;

    // Register a consumer
    queue.register_consumer(MyConsumer).await?;

    // Run until you're ready to shut down
    queue.shutdown(None).await;

    Ok(())
}
```

3. Retry Configuration:

```rust
use rmq::{QueueOptions, RetryConfig};

let options = QueueOptions {
    retry_config: Some(RetryConfig {
        max_retries: 5,
        retry_delay: 1000, // 1 second delay before each retry
    }),
    enable_dlq: true,
    dlq_name: Some("my_dlq_stream".to_string()),
    ..Default::default()
};
```

## Testing

### Run Tests:

```bash
docker-compose up --build --abort-on-container-exit
```

This will start Redis and run cargo test inside the rmq-test container, ensuring tests run in a controlled environment.

## Configuration

- **REDIS_URL**: Set this environment variable to point tests or runtime code to a Redis instance.
- **QueueOptions**: Configure retries, delays, and DLQs.
- **Consumer**: Implement custom logic in process() and should_retry() to control how messages are handled and retried.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request at https://github.com/blablacio/rmq.

## License

This project is licensed under [The Unlicense](https://unlicense.org).

You can find the license text in the [LICENSE](LICENSE) file.
