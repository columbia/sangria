use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

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

/// Tracks reverse dependencies and aborting transactions for cascading aborts.
/// Only used for Pipelined and Adaptive commit strategies.
pub struct DependencyTracker {
    /// Maps transaction_id -> set of transactions that depend on it
    reverse_deps: RwLock<HashMap<Uuid, HashSet<Uuid>>>,
    /// Set of transactions currently aborting (prevents new dependencies)
    aborting_transactions: RwLock<HashSet<Uuid>>,
    /// Maps transaction_id -> set of participant ranges
    participant_ranges: RwLock<HashMap<Uuid, HashSet<common::full_range_id::FullRangeId>>>,
}

impl DependencyTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(DependencyTracker {
            reverse_deps: RwLock::new(HashMap::new()),
            aborting_transactions: RwLock::new(HashSet::new()),
            participant_ranges: RwLock::new(HashMap::new()),
        })
    }

    /// Register that dependent_tx depends on dependency_tx
    pub async fn add_reverse_dep(&self, dependency_tx: Uuid, dependent_tx: Uuid) {
        let mut reverse_deps = self.reverse_deps.write().await;
        reverse_deps
            .entry(dependency_tx)
            .or_insert_with(HashSet::new)
            .insert(dependent_tx);
    }

    /// Check if a transaction is currently aborting
    pub async fn is_aborting(&self, tx_id: Uuid) -> bool {
        let aborting = self.aborting_transactions.read().await;
        aborting.contains(&tx_id)
    }

    /// Get all transactions that depend on the given transaction
    pub async fn get_dependents(&self, tx_id: Uuid) -> Option<HashSet<Uuid>> {
        let reverse_deps = self.reverse_deps.read().await;
        reverse_deps.get(&tx_id).cloned()
    }

    /// Mark transactions as aborting
    pub async fn mark_aborting(&self, tx_ids: &[Uuid]) {
        let mut aborting = self.aborting_transactions.write().await;
        for tx_id in tx_ids {
            aborting.insert(*tx_id);
        }
    }

    /// Remove transaction from aborting set
    pub async fn unmark_aborting(&self, tx_id: Uuid) {
        let mut aborting = self.aborting_transactions.write().await;
        aborting.remove(&tx_id);
    }

    /// Clean up reverse dependencies for a transaction
    pub async fn remove_reverse_deps(&self, tx_id: Uuid) {
        let mut reverse_deps = self.reverse_deps.write().await;
        reverse_deps.remove(&tx_id);
    }

    /// Register a participant range for a transaction
    pub async fn add_participant_range(
        &self,
        tx_id: Uuid,
        range_id: common::full_range_id::FullRangeId,
    ) {
        let mut participant_ranges = self.participant_ranges.write().await;
        participant_ranges
            .entry(tx_id)
            .or_insert_with(HashSet::new)
            .insert(range_id);
    }

    /// Get participant ranges for a transaction
    pub async fn get_participant_ranges(
        &self,
        tx_id: Uuid,
    ) -> Option<HashSet<common::full_range_id::FullRangeId>> {
        let participant_ranges = self.participant_ranges.read().await;
        participant_ranges.get(&tx_id).cloned()
    }

    /// Remove participant ranges for a transaction
    pub async fn remove_participant_ranges(&self, tx_id: Uuid) {
        let mut participant_ranges = self.participant_ranges.write().await;
        participant_ranges.remove(&tx_id);
    }
}

pub struct Coordinator {
    range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
    runtime: tokio::runtime::Handle,
    range_client: Arc<RangeClient>,
    epoch_reader: Arc<EpochReader>,
    tx_state_store: Arc<TxStateStoreClient>,
    commit_strategy: CommitStrategy,
    pub resolver: Arc<dyn ResolverClient>,
    pub dependency_tracker: Arc<DependencyTracker>,
    enable_cascading_abort: bool,
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
        let dependency_tracker = DependencyTracker::new();
        Coordinator {
            range_assignment_oracle,
            runtime,
            range_client,
            epoch_reader,
            tx_state_store,
            commit_strategy: config.commit_strategy.clone(),
            resolver,
            dependency_tracker,
            enable_cascading_abort: config.enable_cascading_abort,
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
            self.dependency_tracker.clone(),
            self.enable_cascading_abort,
        )
    }
}
