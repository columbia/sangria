use std::sync::Arc;

use super::*;
use async_trait::async_trait;
use common::network::fast_network::FastNetwork;
use epoch_reader::reader::EpochReader;
use tokio_util::sync::CancellationToken;

pub struct Reader {
    epoch_reader: EpochReader,
}

impl Reader {
    pub fn new(
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        bg_runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) -> Self {
        Reader {
            epoch_reader: EpochReader::new(fast_network, runtime, bg_runtime, cancellation_token),
        }
    }
}

#[async_trait]
impl EpochSupplier for Reader {
    async fn read_epoch(&self) -> u64 {
        self.epoch_reader.read_epoch().await
    }

    async fn wait_until_epoch(&self, target_epoch: u64, timeout: chrono::Duration) {}
}
