pub mod reader;

use async_trait::async_trait;

#[async_trait]
pub trait EpochSupplier: Send + Sync + 'static {
    // Values returned must satisfy the Global Epoch Invariant:
    // If a call returns a value e, then all subsequent calls must return a value
    // greater than or equal to e-1.
    // In particular this means that the value returned from here could be one less
    // than the true epoch.
    async fn read_epoch(&self) -> u64;

    async fn wait_until_epoch(&self, epoch: u64, timeout: chrono::Duration);
}
