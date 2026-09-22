use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    sync::Arc,
};
use tokio::sync::{oneshot, RwLock};
use tracing::info;
use uuid::Uuid;

use crate::{
    core::{group_commit::GroupCommit, statistics::StatisticsTracker},
    participant_range_info::ParticipantRangeInfo,
};
use coordinator_rangeclient::error::Error;

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub id: Uuid,
    pub num_dependencies: u32,
    pub dependents: HashSet<Uuid>,
    // These edges are retained for measurement even after the operational
    // `dependents` set is drained as transactions become unblocked.
    pub dependency_ids: HashSet<Uuid>,
    pub depth_dependents: HashSet<Uuid>,
    pub dependency_depth: usize,
    pub registered: bool,
    pub participant_ranges_info: Vec<ParticipantRangeInfo>,
    pub fake: bool,
}

impl TransactionInfo {
    pub fn default(id: Uuid, fake: bool) -> Self {
        TransactionInfo {
            id,
            num_dependencies: 0,
            dependents: HashSet::new(),
            dependency_ids: HashSet::new(),
            depth_dependents: HashSet::new(),
            dependency_depth: 0,
            registered: false,
            participant_ranges_info: Vec::new(),
            fake,
        }
    }
}

#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,
}

impl State {
    fn unresolved_dependency_depth(&self, transaction_id: Uuid) -> usize {
        self.unresolved_dependency_depth_inner(transaction_id, &mut HashSet::new())
    }

    fn unresolved_dependency_depth_inner(
        &self,
        transaction_id: Uuid,
        visiting: &mut HashSet<Uuid>,
    ) -> usize {
        if !visiting.insert(transaction_id) {
            return 0;
        }

        let dependency_ids = self
            .info_per_transaction
            .get(&transaction_id)
            .map(|info| info.dependency_ids.clone())
            .unwrap_or_default();
        let mut depth = 0;
        for dependency_id in dependency_ids {
            if !self.resolved_transactions.contains(&dependency_id) {
                depth =
                    depth.max(1 + self.unresolved_dependency_depth_inner(dependency_id, visiting));
            }
        }
        visiting.remove(&transaction_id);
        depth
    }

    /// Installs the unresolved dependency set and refreshes active depths.
    ///
    /// A dependency can initially be represented by a depth-zero placeholder.
    /// If its own commit request arrives later, the retained `depth_dependents`
    /// edges refresh every already-registered successor. The returned entries
    /// are real transactions whose current active depths are statistics samples.
    fn update_dependency_depths(
        &mut self,
        transaction_id: Uuid,
        dependencies: &HashSet<Uuid>,
    ) -> Vec<(Uuid, usize)> {
        for dependency in dependencies {
            self.info_per_transaction
                .entry(*dependency)
                .or_insert(TransactionInfo::default(*dependency, false))
                .depth_dependents
                .insert(transaction_id);
        }

        self.info_per_transaction
            .get_mut(&transaction_id)
            .expect("transaction info must exist before recording its depth")
            .dependency_ids = dependencies.clone();

        let mut queue = VecDeque::from([transaction_id]);
        let mut affected = HashSet::new();
        while let Some(current_id) = queue.pop_front() {
            if !affected.insert(current_id) {
                continue;
            }
            let new_depth = self.unresolved_dependency_depth(current_id);

            let current_info = self
                .info_per_transaction
                .get_mut(&current_id)
                .expect("depth propagation referenced an unknown transaction");
            current_info.dependency_depth = new_depth;
            let depth_dependents = current_info.depth_dependents.clone();
            for dependent_id in depth_dependents {
                queue.push_back(dependent_id);
            }
        }

        affected
            .into_iter()
            .filter_map(|affected_id| {
                let info = self.info_per_transaction.get(&affected_id)?;
                (info.registered && !info.fake).then_some((affected_id, info.dependency_depth))
            })
            .collect()
    }
}

pub struct Resolver {
    state: RwLock<State>,
    group_commit: GroupCommit,
    waiting_transactions: RwLock<HashMap<Uuid, oneshot::Sender<()>>>,
    bg_runtime: tokio::runtime::Handle,
    stats_tracker: RwLock<StatisticsTracker>,
    measure_dependency_depth: bool,
}

impl Resolver {
    pub fn new(group_commit: GroupCommit, bg_runtime: tokio::runtime::Handle) -> Self {
        Resolver {
            state: RwLock::new(State {
                info_per_transaction: HashMap::new(),
                resolved_transactions: HashSet::new(),
            }),
            group_commit,
            waiting_transactions: RwLock::new(HashMap::new()),
            bg_runtime,
            stats_tracker: RwLock::new(StatisticsTracker::new()),
            measure_dependency_depth: std::env::var_os("SANGRIA_MEASURE_DEPENDENCY_DEPTH")
                .is_some(),
        }
    }

    pub async fn commit(
        resolver: Arc<Self>,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges_info: Vec<ParticipantRangeInfo>,
        fake: bool,
    ) -> Result<(), Error> {
        // A transaction that is read-only across all participant ranges will
        // not have a commit phase and so we also ignore any dependencies it may have
        if participant_ranges_info.iter().all(|info| !info.has_writes) {
            return Ok(());
        }
        let (s, r) = oneshot::channel();
        let mut num_pending_dependencies = 0;
        let mut pending_dependencies = HashSet::new();

        // Acquire the write lock and update the state with new dependencies
        info!("Updating dependencies for transaction {:?}", transaction_id);
        {
            let mut state = resolver.state.write().await;
            for dependency in &dependencies {
                if !state.resolved_transactions.contains(dependency) {
                    // Dependency is not yet resolved, so we need to wait for it
                    num_pending_dependencies += 1;
                    pending_dependencies.insert(*dependency);
                    // Add the transaction as a dependent of the dependency
                    state
                        .info_per_transaction
                        .entry(*dependency)
                        .or_insert(TransactionInfo::default(*dependency, fake))
                        .dependents
                        .insert(transaction_id);
                } else {
                    info!(
                        "Dependency {:?} was already resolved in the meantime",
                        dependency
                    );
                }
            }

            {
                let transaction_info = state
                    .info_per_transaction
                    .entry(transaction_id)
                    .or_insert(TransactionInfo::default(transaction_id, fake));

                transaction_info.num_dependencies = num_pending_dependencies;
                transaction_info.participant_ranges_info = participant_ranges_info;
                transaction_info.fake = fake;
                transaction_info.registered = true;
            }

            let depth_updates = if resolver.measure_dependency_depth && !fake {
                state.update_dependency_depths(transaction_id, &pending_dependencies)
            } else {
                Vec::new()
            };
            let mut waiting_transactions = resolver.waiting_transactions.write().await;
            waiting_transactions.insert(transaction_id, s);
            // {
            //     let mut stats_tracker = resolver.stats_tracker.write().await;
            //     stats_tracker.record_request();
            // }
            {
                let mut stats_tracker = resolver.stats_tracker.write().await;
                stats_tracker.record_waiting_transactions_count(waiting_transactions.len());
                for (depth_transaction_id, depth) in depth_updates {
                    stats_tracker.record_dependency_depth(depth_transaction_id, depth);
                }
            }
            drop(waiting_transactions);

            info!("Updated dependencies for transaction {:?}", transaction_id);
            if num_pending_dependencies == 0 {
                // If there are no pending dependencies, we can commit the transaction
                info!(
                    "No pending dependencies, committing transaction {:?}",
                    transaction_id
                );
                // Add transaction while holding the write lock
                let transaction_info_clone = state
                    .info_per_transaction
                    .get(&transaction_id)
                    .expect("registered transaction info must exist")
                    .clone();
                resolver
                    .group_commit
                    .add_transactions(&vec![transaction_info_clone.clone()])
                    .await?;
                let resolver_clone = resolver.clone();
                resolver.bg_runtime.spawn(async move {
                    let _ =
                        Self::trigger_commit(resolver_clone, vec![transaction_info_clone]).await;
                });
            }
        }

        // Block until the transaction is actually committed
        r.await.unwrap();
        info!("Transaction {} finally committed!", transaction_id);
        Ok(())
    }

    async fn trigger_commit(
        resolver: Arc<Self>,
        transactions: Vec<TransactionInfo>,
    ) -> Result<(), Error> {
        info!(
            "Triggering commit for transactions {:?}",
            transactions.iter().map(|tx| tx.id).collect::<Vec<_>>()
        );
        let finished_transactions = resolver.group_commit.commit().await?;
        let finished_transactions_ids = finished_transactions
            .iter()
            .map(|tx| tx.id)
            .collect::<Vec<_>>();
        // Notify the transactions currently waiting for messages in the channels so that they unblock
        {
            info!("Notifying transactions");
            let mut waiting_transactions = resolver.waiting_transactions.write().await;
            for transaction in finished_transactions {
                let sender = waiting_transactions.remove(&transaction.id).unwrap();
                sender.send(()).unwrap();
            }
            // TODO: Clean up other state too here?
        }
        info!("Registering transactions as committed");
        // Register the transactions as committed so that more dependencies can be resolved
        if !finished_transactions_ids.is_empty() {
            let _ =
                Self::spawn_register_committed_transactions(resolver, finished_transactions_ids);
        }
        Ok(())
    }

    pub fn spawn_register_committed_transactions(
        resolver: Arc<Self>,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        // Helper function to bypass compiler's "cycle detected" inference error
        // caused by our recursive call: trigger_commit -> register_committed_transactions -> trigger_commit
        let resolver_clone = resolver.clone();
        resolver.bg_runtime.spawn(async move {
            let _ =
                Resolver::register_committed_transactions(resolver_clone, transaction_ids).await;
        });
        Ok(())
    }

    pub async fn register_committed_transactions(
        resolver: Arc<Self>,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        let mut new_ready_to_commit = Vec::new();
        {
            let mut new_resolved_dependencies = Vec::new();

            let mut state = resolver.state.write().await;
            // TODO: When is it ok to remove transactions from resolved_transactions?
            for transaction_id in transaction_ids {
                state.resolved_transactions.insert(transaction_id);
                new_resolved_dependencies.push(transaction_id);
            }

            // Find iteratively all transactions that are now ready to commit until the new_resolved_dependencies vector is empty
            while !new_resolved_dependencies.is_empty() {
                let transaction_id = new_resolved_dependencies.pop().unwrap();

                if !state.info_per_transaction.contains_key(&transaction_id) {
                    // Transaction has no dependents
                    continue;
                }

                let transaction_info = state.info_per_transaction.get_mut(&transaction_id).unwrap();
                // Move dependents out of the transaction info
                let dependents = mem::take(&mut transaction_info.dependents);
                assert!(transaction_info.dependents.is_empty());

                // Check if any dependencies are now resolved and if any new transactions are ready to commit
                if !dependents.is_empty() {
                    for dependent in dependents.iter() {
                        let dependent_transaction_info =
                            state.info_per_transaction.get_mut(&dependent).unwrap();
                        assert!(
                            dependent_transaction_info.num_dependencies > 0,
                            "Dependent transaction {} has no pending dependencies",
                            dependent
                        );
                        dependent_transaction_info.num_dependencies -= 1;
                        if dependent_transaction_info.num_dependencies == 0 {
                            // Transaction is now unblocked and ready to commit
                            new_ready_to_commit.push(dependent_transaction_info.clone());
                            new_resolved_dependencies.push(*dependent);
                        }
                    }
                }
            }

            // Add transactions to the group commit while holding the write lock so that dependencies order is respected
            if !new_ready_to_commit.is_empty() {
                let _ = resolver
                    .group_commit
                    .add_transactions(&new_ready_to_commit)
                    .await;
            }
        }
        // Trigger a commit so that the new ready transactions are added to the group commit and get committed
        if !new_ready_to_commit.is_empty() {
            info!(
                "New ready to commit transactions: {:?}",
                new_ready_to_commit
                    .iter()
                    .map(|tx| tx.id)
                    .collect::<Vec<_>>()
            );
            let resolver_clone = resolver.clone();
            resolver.bg_runtime.spawn(async move {
                let _ = Resolver::trigger_commit(resolver_clone, new_ready_to_commit).await;
            });
        }
        Ok(())
    }

    // ---------------------- Statistics ----------------------
    pub async fn sample_waiting_transactions(&self) {
        let waiting_count = self.waiting_transactions.read().await.len();
        let mut stats_tracker = self.stats_tracker.write().await;
        stats_tracker.record_waiting_transactions_sample(waiting_count);
    }

    pub async fn get_average_waiting_transactions(&self) -> f64 {
        let stats_tracker = self.stats_tracker.read().await;
        stats_tracker.get_average_waiting_transactions()
    }

    pub async fn get_stats(resolver: Arc<Self>) -> HashMap<String, f64> {
        // Call this function only at the end of the workload since it's reseting the stats!!
        let mut group_commit_stats = resolver.group_commit.get_stats().await;
        let mut stats_tracker = resolver.stats_tracker.write().await;
        let resolver_stats = stats_tracker.get_stats();
        stats_tracker.reset();
        group_commit_stats.extend(resolver_stats);
        group_commit_stats
    }

    pub async fn reset_stats(&self) {
        let mut stats_tracker = self.stats_tracker.write().await;
        stats_tracker.reset();
    }

    pub async fn get_transaction_info_status(&self) -> String {
        let mut status = String::new();
        status.push_str("Info per transaction:\n");

        let state = self.state.read().await;
        for (tx_id, tx_info) in &state.info_per_transaction {
            status.push_str(&format!(
                "  Transaction {}: dependencies={}, dependents={:?}, fake={}\n",
                tx_id, tx_info.num_dependencies, tx_info.dependents, tx_info.fake
            ));
        }
        status
    }

    pub async fn get_resolved_transactions_status(&self) -> String {
        let state = self.state.read().await;
        format!("Resolved transactions: {:?}\n", state.resolved_transactions)
    }

    pub async fn get_waiting_transactions_status(&self) -> String {
        let waiting = self.waiting_transactions.read().await;
        format!(
            "Waiting transactions: {:?}\n",
            waiting.keys().collect::<Vec<_>>()
        )
    }

    pub async fn get_num_waiting_transactions(&self) -> usize {
        let waiting = self.waiting_transactions.read().await;
        waiting.len()
    }

    pub async fn get_group_commit_status(&self) -> String {
        self.group_commit.get_status().await
    }
    // ---------------------- / Statistics ----------------------
}
