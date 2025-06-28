use async_trait::async_trait;
use frontend::error::Error as FrontendError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait]
pub trait Transaction: Send + Sync + 'static {
    async fn execute(
        &self,
        value_per_key: Arc<Mutex<HashMap<usize, u64>>>,
    ) -> Result<(), FrontendError>;
}
