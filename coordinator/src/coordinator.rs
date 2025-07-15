use std::sync::Arc;

use crate::transaction::Transaction;
use common::{
    config::{CommitStrategy, Config},
    keyspace::Keyspace,
    membership::range_assignment_oracle::RangeAssignmentOracle,
    network::fast_network::FastNetwork,
    region::Zone,
    transaction_info::TransactionInfo,
};
use coordinator_rangeclient::rangeclient::RangeClient;
use epoch_reader::reader::EpochReader;
use resolver::resolver_client::ResolverClient;
use tokio_util::sync::CancellationToken;
use tx_state_store::client::Client as TxStateStoreClient;

pub struct Coordinator {
    range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
    runtime: tokio::runtime::Handle,
    range_client: Arc<RangeClient>,
    epoch_reader: Arc<EpochReader>,
    tx_state_store: Arc<TxStateStoreClient>,
    commit_strategy: CommitStrategy,
    pub resolver: Arc<dyn ResolverClient>,
}

impl Coordinator {
    pub async fn new(
        config: &Config,
        zone: Zone,
        range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        bg_runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
        tx_state_store: Arc<TxStateStoreClient>,
        range_client: Arc<RangeClient>,
        resolver: Arc<dyn ResolverClient>,
    ) -> Coordinator {
        let epoch_reader = Arc::new(EpochReader::new(
            fast_network.clone(),
            runtime.clone(),
            bg_runtime.clone(),
            cancellation_token.clone(),
        ));
        Coordinator {
            range_assignment_oracle,
            runtime,
            range_client,
            epoch_reader,
            tx_state_store,
            commit_strategy: config.commit_strategy.clone(),
            resolver,
        }
    }

    pub async fn start_transaction(
        &self,
        transaction_info: Arc<TransactionInfo>,
        keyspace: Keyspace,
    ) -> Transaction {
        self.tx_state_store
            .start_transaction(transaction_info.id)
            .await
            .unwrap();

        Transaction::new(
            transaction_info,
            self.range_client.clone(),
            self.range_assignment_oracle.clone(),
            self.epoch_reader.clone(),
            self.tx_state_store.clone(),
            self.runtime.clone(),
            self.resolver.clone(),
            self.commit_strategy.clone(),
            keyspace,
        )
    }
}
