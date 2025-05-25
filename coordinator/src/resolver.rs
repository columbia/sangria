use common::full_range_id::FullRangeId;
use std::{collections::{HashMap, HashSet}, mem};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use uuid::Uuid;
use crate::{error::Error, group_commit::GroupCommit};

#[derive(Clone)]
pub struct ParticipantRangeInfo {
    pub participant_range: FullRangeId,
    pub has_writes: bool,
}
#[derive(Default, Clone)]
pub struct TransactionInfo {
    pub id: Uuid,
    pub num_dependencies: u32,
    pub dependents: HashSet<Uuid>,
    pub participant_ranges_info: Vec<ParticipantRangeInfo>,
}

struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,
    waiting_transactions: HashMap<Uuid, mpsc::Sender<()>>,
}

pub struct Resolver {
    state: RwLock<State>,
    group_commit: GroupCommit,
}

impl Resolver {
    pub fn new(group_commit: GroupCommit) -> Self {
        Resolver {
            state: RwLock::new(State {
                info_per_transaction: HashMap::new(),
                resolved_transactions: HashSet::new(),
                waiting_transactions: HashMap::new(),
            }),
            group_commit,
        }
    }

    pub async fn commit(
        &self,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges_info: Vec<ParticipantRangeInfo>,
    ) -> Result<(), Error> {
        let (s, mut r) = mpsc::channel(1);

        // A transaction that is read-only across all participant ranges will not have a commit phase and so we also ignore any dependencies it may have
        if participant_ranges_info.iter().all(|info| !info.has_writes) {
            return Ok(());
        }

        let mut num_pending_dependencies = 0;

        // Acquire the write lock and update the state with new dependencies
        let transaction_info = {
            let mut state = self.state.write().await;
            for dependency in dependencies {
                if !state.resolved_transactions.contains(&dependency) {
                    // Dependency is not yet resolved, so we need to wait for it
                    num_pending_dependencies += 1;
                    // Add the transaction as a dependent of the dependency
                    state
                        .info_per_transaction
                        .entry(dependency)
                        .or_insert(TransactionInfo::default())
                        .dependents
                        .insert(transaction_id);
                }
            }

            let transaction_info = state
                .info_per_transaction
                .entry(transaction_id)
                .or_insert(TransactionInfo::default());

            let transaction_info_clone = transaction_info.clone();
            transaction_info.num_dependencies = num_pending_dependencies;
            transaction_info.participant_ranges_info = participant_ranges_info;

            state.waiting_transactions.insert(transaction_id, s);
            transaction_info_clone
        };

        if num_pending_dependencies == 0 {
            // If there are no pending dependencies, we can commit the transaction
            let _ = self.group_commit
                .add_transactions(&vec![transaction_info.clone()])
                .await;
            // Trigger a commit, no need to wait for it here
            let _ = self.group_commit.commit().await;
        }

        // Block until the transaction is actually committed
        // Wait to read as many messages as there are participant ranges with non empty writesets
        for participant_range_info in transaction_info.participant_ranges_info {
            if participant_range_info.has_writes {
                r.recv().await.unwrap();
            }
        }
        Ok(())
    }

    pub async fn register_committed_transactions(
        &self,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        let mut new_ready_to_commit = Vec::new();
        {
            let mut new_resolved_dependencies = Vec::new();

            let mut state = self.state.write().await;
            // TODO: When is it ok to remove transactions from resolved_transactions?
            for transaction_id in transaction_ids {
                state.resolved_transactions.insert(transaction_id);
                new_resolved_dependencies.push(transaction_id);
            }

            //  Find iteratively all transactions that are now ready to commit until the new_resolved_dependencies vector is empty
            while !new_resolved_dependencies.is_empty() {
                let transaction_id = new_resolved_dependencies.pop().unwrap();

                if !state.info_per_transaction.contains_key(&transaction_id) {
                    continue;
                }

                let transaction_info = state
                    .info_per_transaction
                    .get_mut(&transaction_id)
                    .unwrap();
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

        let _ = self.group_commit
            .add_transactions(&new_ready_to_commit)
            .await;
        let _ = self.group_commit.commit().await;

        Ok(())
    }
}
