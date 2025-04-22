mod builder;
mod consumer;
mod delivery;
mod errors;
mod options;
mod prefetch;
mod queue;
mod scaling;

pub use builder::QueueBuilder;
pub use consumer::Consumer;
pub use delivery::Delivery;
pub use errors::{ConsumerError, RmqError, RmqResult};
pub use options::{PrefetchConfig, QueueOptions, RetryConfig, RetrySyncPolicy, ScalingConfig};
pub use queue::Queue;
pub use scaling::{DefaultScalingStrategy, ScaleAction, ScalingContext, ScalingStrategy};
