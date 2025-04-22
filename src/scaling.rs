use async_trait::async_trait;

#[derive(Debug)]
pub enum ScaleAction {
    ScaleUp(u32),   // Number of consumers to add
    ScaleDown(u32), // Number of consumers to remove
    Hold,
}

pub struct ScalingContext {
    pub current_consumers: usize,
    pub idle_consumers: usize, // Consumers not currently processing a message
    pub overflow_size: usize,  // Messages in MessageBuffer's overflow
    pub min_consumers: usize,
    pub max_consumers: usize,
}

#[async_trait]
pub trait ScalingStrategy: Send + Sync + 'static {
    /// Decides the scaling action based on current state.
    async fn decide(&self, context: ScalingContext) -> ScaleAction;
}

// Default implementation
pub struct DefaultScalingStrategy;

#[async_trait]
impl ScalingStrategy for DefaultScalingStrategy {
    async fn decide(&self, context: ScalingContext) -> ScaleAction {
        // --- Scale Up Logic ---
        if context.overflow_size > 0
            && context.idle_consumers == 0
            && context.current_consumers < context.max_consumers
        {
            // Scale up if buffer is overflowing, no consumers are idle, and not at max
            let scale_up_by = (context.max_consumers - context.current_consumers).min(1); // Example: scale up by 1

            return ScaleAction::ScaleUp(scale_up_by as u32);
        } else if context.current_consumers < context.min_consumers {
            // Ensure minimum consumers are running
            let scale_up_by = context.min_consumers - context.current_consumers;

            return ScaleAction::ScaleUp(scale_up_by as u32);
        }

        // --- Scale Down Logic (Modified) ---
        // Only scale down if there are idle consumers, we are above the minimum,
        // AND there is no buffer overflow.
        if context.idle_consumers > 0
            && context.current_consumers > context.min_consumers
            && context.overflow_size == 0
        {
            // <-- Added overflow check
            // Scale down if consumers are idle, above min, and no overflow
            let scale_down_by = context
                .idle_consumers
                .min(context.current_consumers - context.min_consumers);

            return ScaleAction::ScaleDown(scale_down_by as u32);
        }

        // --- Hold Logic ---
        ScaleAction::Hold
    }
}
