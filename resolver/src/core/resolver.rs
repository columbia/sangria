use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};
use tokio::sync::{RwLock, oneshot};
use tracing::info;
use uuid::Uuid;

use crate::{
    core::{group_commit::GroupCommit, statistics::StatisticsTracker},
    participant_range_info::ParticipantRangeInfo,
};
use coordinator_rangeclient::error::{Error, TransactionAbortReason};

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub id: Uuid,
    pub num_dependencies: u32,
    pub dependents: HashSet<Uuid>,
    pub dependencies: HashSet<Uuid>,  // Track actual dependency IDs for tree visualization
    pub participant_ranges_info: Vec<ParticipantRangeInfo>,
    pub fake: bool,
}

impl TransactionInfo {
    pub fn default(id: Uuid, fake: bool) -> Self {
        TransactionInfo {
            id,
            num_dependencies: 0,
            dependents: HashSet::new(),
            dependencies: HashSet::new(),
            participant_ranges_info: Vec::new(),
            fake,
        }
    }
}

#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,  // Both commits AND aborts are resolved
    committed_transactions: HashSet<Uuid>,  // Only successful commits
}

pub struct Resolver {
    state: RwLock<State>,
    group_commit: GroupCommit,
    waiting_transactions: RwLock<HashMap<Uuid, oneshot::Sender<Result<(), Error>>>>,
    bg_runtime: tokio::runtime::Handle,
    stats_tracker: RwLock<StatisticsTracker>,
}

impl Resolver {
    pub fn new(group_commit: GroupCommit, bg_runtime: tokio::runtime::Handle) -> Self {
        Resolver {
            state: RwLock::new(State {
                info_per_transaction: HashMap::new(),
                resolved_transactions: HashSet::new(),
                committed_transactions: HashSet::new(),
            }),
            group_commit,
            waiting_transactions: RwLock::new(HashMap::new()),
            bg_runtime,
            stats_tracker: RwLock::new(StatisticsTracker::new()),
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
        let mut has_aborted_dependency = false;

        // Acquire the write lock and update the state with new dependencies
        info!("Updating dependencies for transaction {:?}", transaction_id);
        {
            let mut state = resolver.state.write().await;

            // IMPORTANT: Create our own entry in info_per_transaction FIRST, before adding
            // ourselves as a dependent of any dependency. This ensures that if
            // register_committed_transactions runs concurrently, it can find our entry
            // and properly decrement our num_dependencies count.
            let transaction_info = state
                .info_per_transaction
                .entry(transaction_id)
                .or_insert(TransactionInfo::default(transaction_id, fake));
            transaction_info.participant_ranges_info = participant_ranges_info;

            for dependency in dependencies {
                // Track this dependency for tree visualization
                state
                    .info_per_transaction
                    .get_mut(&transaction_id)
                    .unwrap()
                    .dependencies
                    .insert(dependency);

                if !state.resolved_transactions.contains(&dependency) {
                    // Dependency is not yet resolved, so we need to wait for it
                    num_pending_dependencies += 1;
                    // Add the transaction as a dependent of the dependency
                    state
                        .info_per_transaction
                        .entry(dependency)
                        .or_insert(TransactionInfo::default(dependency, fake))
                        .dependents
                        .insert(transaction_id);
                } else if !state.committed_transactions.contains(&dependency) {
                    // Dependency was resolved but NOT committed - it was aborted
                    // This transaction must also abort
                    info!(
                        "Dependency {:?} was aborted, transaction {:?} must also abort",
                        dependency, transaction_id
                    );
                    has_aborted_dependency = true;
                    break;
                } else {
                    info!(
                        "Dependency {:?} was already committed in the meantime",
                        dependency
                    );
                }
            }

            // If any dependency was aborted, abort this transaction immediately
            // BUT we must register this transaction as resolved (aborted) so future
            // transactions depending on it will also know to abort
            if has_aborted_dependency {
                state.resolved_transactions.insert(transaction_id);
                // Do NOT insert into committed_transactions - this marks it as aborted
                info!(
                    "Transaction {:?} marked as resolved (aborted due to dependency)",
                    transaction_id
                );

                // CRITICAL: Check if any transactions have already registered as dependents
                // of this transaction. If so, we need to notify them that we're aborting.
                // This handles the race where TX B registers on TX A, then TX A discovers
                // its dependency was aborted - TX B is waiting and needs to be notified.
                let dependents_to_notify: Vec<Uuid> = state
                    .info_per_transaction
                    .get(&transaction_id)
                    .map(|info| info.dependents.iter().cloned().collect())
                    .unwrap_or_default();

                drop(state);  // Release the lock before spawning

                // Spawn a task to cascade the abort to dependents
                if !dependents_to_notify.is_empty() {
                    info!(
                        "Transaction {:?} cascading abort to {} dependents: {:?}",
                        transaction_id,
                        dependents_to_notify.len(),
                        dependents_to_notify
                    );
                    let resolver_clone = resolver.clone();
                    let mut all_to_abort = dependents_to_notify;
                    all_to_abort.push(transaction_id);  // Include self to notify any waiters
                    resolver.bg_runtime.spawn(async move {
                        let _ = Self::register_aborted_transactions(resolver_clone, all_to_abort).await;
                    });
                }

                return Err(Error::TransactionAborted(TransactionAbortReason::DependencyAborted));
            }

            // Now set the final num_dependencies count
            let transaction_info = state
                .info_per_transaction
                .get_mut(&transaction_id)
                .unwrap();
            transaction_info.num_dependencies = num_pending_dependencies;
            resolver
                .waiting_transactions
                .write()
                .await
                .insert(transaction_id, s);

            // {
            //     let mut stats_tracker = resolver.stats_tracker.write().await;
            //     stats_tracker.record_request();
            // }

            info!("Updated dependencies for transaction {:?}", transaction_id);
            if num_pending_dependencies == 0 {
                // If there are no pending dependencies, we can commit the transaction
                info!(
                    "No pending dependencies, committing transaction {:?}",
                    transaction_id
                );
                // Add transaction while holding the write lock
                let transaction_info_clone = transaction_info.clone();
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

        // Block until the transaction is actually committed (or aborted)
        let result = r.await.unwrap();
        match &result {
            Ok(_) => info!("Transaction {} finally committed!", transaction_id),
            Err(_) => info!("Transaction {} was aborted", transaction_id),
        }
        result
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
                // Use if-let to handle case where transaction was already notified (e.g., by abort cascade)
                if let Some(sender) = waiting_transactions.remove(&transaction.id) {
                    let _ = sender.send(Ok(()));  // Send Ok to indicate successful commit
                }
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
                state.committed_transactions.insert(transaction_id);  // Mark as successfully committed
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
                        // Skip dependents that have already been resolved (e.g., aborted due to another dependency)
                        // This handles the race where TX B registers as dependent of TX A, then TX B discovers
                        // another dependency was aborted and marks itself as resolved. When TX A commits,
                        // TX B is still in A's dependents list but has num_dependencies=0 and is already resolved.
                        if state.resolved_transactions.contains(dependent) {
                            info!(
                                "Skipping dependent {:?} - already resolved (likely aborted)",
                                dependent
                            );
                            continue;
                        }

                        let dependent_transaction_info =
                            state.info_per_transaction.get_mut(&dependent).unwrap();

                        // Double-check: if num_dependencies is 0 but TX wasn't marked resolved,
                        // this is a bug. But we'll handle gracefully by skipping.
                        if dependent_transaction_info.num_dependencies == 0 {
                            info!(
                                "Warning: Dependent transaction {:?} has num_dependencies=0 but isn't resolved - skipping",
                                dependent
                            );
                            continue;
                        }

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

    pub async fn abort(
        resolver: Arc<Self>,
        transaction_id: Uuid,
    ) -> Result<(), Error> {
        info!("Aborting transaction {:?} and cascading to dependents", transaction_id);

        // Walk the dependency graph to find ALL transitive dependents
        let mut transactions_to_abort = HashSet::new();
        transactions_to_abort.insert(transaction_id);

        {
            let state = resolver.state.read().await;
            let mut to_explore = vec![transaction_id];

            while let Some(tx_id) = to_explore.pop() {
                if let Some(tx_info) = state.info_per_transaction.get(&tx_id) {
                    for dependent in &tx_info.dependents {
                        if transactions_to_abort.insert(*dependent) {
                            info!("Cascading abort to dependent transaction {:?}", dependent);
                            to_explore.push(*dependent);  // Recursively find their dependents
                        }
                    }
                }
            }
        }

        info!("Collected {} transactions to abort (including dependents)", transactions_to_abort.len());

        // Register all as aborted
        Self::register_aborted_transactions(resolver, transactions_to_abort.into_iter().collect()).await
    }

    pub async fn register_aborted_transactions(
        resolver: Arc<Self>,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        info!("Registering {} transactions as aborted", transaction_ids.len());

        {
            let mut state = resolver.state.write().await;

            // Mark all as resolved but NOT committed
            for transaction_id in &transaction_ids {
                state.resolved_transactions.insert(*transaction_id);
                // Explicitly do NOT add to committed_transactions
                info!("Transaction {:?} marked as resolved (aborted)", transaction_id);
            }

            // Unblock waiting transactions (notify them they're aborted)
            let mut waiting_transactions = resolver.waiting_transactions.write().await;
            for transaction_id in &transaction_ids {
                if let Some(sender) = waiting_transactions.remove(transaction_id) {
                    let _ = sender.send(Err(Error::TransactionAborted(
                        TransactionAbortReason::DependencyAborted
                    )));  // Send error to indicate abort
                    info!("Notified transaction {:?} of abort", transaction_id);
                }
            }

            // Check if any dependents can now proceed (or must also abort)
            // For each aborted transaction, check its dependents
            let mut new_ready_to_check = Vec::new();
            for transaction_id in &transaction_ids {
                if let Some(tx_info) = state.info_per_transaction.get(transaction_id) {
                    for dependent in &tx_info.dependents {
                        new_ready_to_check.push(*dependent);
                    }
                }
            }

            // For each dependent, they must also abort since their dependency was aborted
            // First, mark them as resolved (aborted) and notify them
            for dependent_id in &new_ready_to_check {
                // Skip if already resolved (already aborted via cascading or direct abort)
                if state.resolved_transactions.contains(dependent_id) {
                    continue;
                }

                // Mark as resolved (aborted due to dependency)
                state.resolved_transactions.insert(*dependent_id);
                info!("Dependent {:?} marked as resolved (aborted due to dependency cascade)", dependent_id);

                // Notify the dependent that it's aborted
                if let Some(sender) = waiting_transactions.remove(dependent_id) {
                    let _ = sender.send(Err(Error::TransactionAborted(
                        TransactionAbortReason::DependencyAborted
                    )));
                    info!("Notified dependent {:?} of abort due to dependency cascade", dependent_id);
                }
            }
        }

        Ok(())
    }

    // ---------------------- Statistics ----------------------
    pub async fn sample_waiting_transactions(&self) {
        let mut stats_tracker = self.stats_tracker.write().await;
        let waiting_count = self.waiting_transactions.read().await.len();
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

    /// Returns the full dependency tree for visualization
    /// Each transaction includes: id, status (committed/aborted), dependencies, dependents
    pub async fn get_dependency_tree(&self) -> DependencyTree {
        let state = self.state.read().await;

        let mut transactions = Vec::new();
        let mut num_committed = 0u32;
        let mut num_aborted = 0u32;

        for (tx_id, tx_info) in &state.info_per_transaction {
            let is_resolved = state.resolved_transactions.contains(tx_id);
            let is_committed = state.committed_transactions.contains(tx_id);

            let status = if !is_resolved {
                "pending".to_string()
            } else if is_committed {
                num_committed += 1;
                "committed".to_string()
            } else {
                num_aborted += 1;
                "aborted".to_string()
            };

            transactions.push(TransactionNode {
                id: tx_id.to_string(),
                status,
                dependencies: tx_info.dependencies.iter().map(|id| id.to_string()).collect(),
                dependents: tx_info.dependents.iter().map(|id| id.to_string()).collect(),
            });
        }

        DependencyTree {
            transactions,
            num_committed,
            num_aborted,
        }
    }
    // ---------------------- / Statistics ----------------------
}

/// Struct to hold dependency tree data for visualization
#[derive(Debug)]
pub struct TransactionNode {
    pub id: String,
    pub status: String,
    pub dependencies: Vec<String>,
    pub dependents: Vec<String>,
}

#[derive(Debug)]
pub struct DependencyTree {
    pub transactions: Vec<TransactionNode>,
    pub num_committed: u32,
    pub num_aborted: u32,
}
