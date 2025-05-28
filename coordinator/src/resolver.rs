use crate::{error::Error, group_commit::GroupCommit};
use common::full_range_id::FullRangeId;
use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};
use tokio::sync::{oneshot, RwLock};
use tracing::info;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ParticipantRangeInfo {
    pub participant_range: FullRangeId,
    pub has_writes: bool,
}
#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub id: Uuid,
    pub num_dependencies: u32,
    pub dependents: HashSet<Uuid>,
    pub participant_ranges_info: Vec<ParticipantRangeInfo>,
}

impl TransactionInfo {
    pub fn default(id: Uuid) -> Self {
        TransactionInfo {
            id,
            num_dependencies: 0,
            dependents: HashSet::new(),
            participant_ranges_info: Vec::new(),
        }
    }
}

#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,
}

pub struct Resolver {
    state: RwLock<State>,
    group_commit: GroupCommit,
    waiting_transactions: RwLock<HashMap<Uuid, oneshot::Sender<()>>>,
    bg_runtime: tokio::runtime::Handle,
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
        }
    }

    pub async fn commit(
        resolver: Arc<Self>,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges_info: Vec<ParticipantRangeInfo>,
    ) -> Result<(), Error> {
        // A transaction that is read-only across all participant ranges will
        // not have a commit phase and so we also ignore any dependencies it may have
        if participant_ranges_info.iter().all(|info| !info.has_writes) {
            return Ok(());
        }

        let (s, r) = oneshot::channel();
        let mut num_pending_dependencies = 0;

        // Acquire the write lock and update the state with new dependencies
        info!("Updating dependencies for transaction {:?}", transaction_id);
        let transaction_info = {
            let mut state = resolver.state.write().await;
            for dependency in dependencies {
                if !state.resolved_transactions.contains(&dependency) {
                    // Dependency is not yet resolved, so we need to wait for it
                    num_pending_dependencies += 1;
                    // Add the transaction as a dependent of the dependency
                    state
                        .info_per_transaction
                        .entry(dependency)
                        .or_insert(TransactionInfo::default(dependency))
                        .dependents
                        .insert(transaction_id);
                }
            }

            let transaction_info = state
                .info_per_transaction
                .entry(transaction_id)
                .or_insert(TransactionInfo::default(transaction_id));

            transaction_info.num_dependencies = num_pending_dependencies;
            transaction_info.participant_ranges_info = participant_ranges_info;
            let transaction_info_clone = transaction_info.clone();
            resolver
                .waiting_transactions
                .write()
                .await
                .insert(transaction_id, s);
            transaction_info_clone
        };
        info!("Updated dependencies for transaction {:?}", transaction_id);
        if num_pending_dependencies == 0 {
            // If there are no pending dependencies, we can commit the transaction
            info!(
                "No pending dependencies, committing transaction {:?}",
                transaction_id
            );
            let resolver_clone = resolver.clone();
            resolver.bg_runtime.spawn(async move {
                let _ = Self::trigger_commit(resolver_clone, vec![transaction_info]).await;
            });
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
        let _ = resolver.group_commit.add_transactions(&transactions).await;
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
            Self::spawn_register_committed_transactions(resolver, finished_transactions_ids);
        }
        Ok(())
    }

    fn spawn_register_committed_transactions(
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

            //  Find iteratively all transactions that are now ready to commit until the new_resolved_dependencies vector is empty
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
                            state.resolved_transactions.insert(*dependent);
                        }
                    }
                }
            }
        }
        // Trigger a commit so that the new ready transactions are added to the group commit and get committed
        if !new_ready_to_commit.is_empty() {
        let resolver_clone = resolver.clone();
            resolver.bg_runtime.spawn(async move {
                let _ = Resolver::trigger_commit(resolver_clone, new_ready_to_commit).await;
            });
        }
        Ok(())
    }
}
