use crate::error::Error;
use crate::full_range_id::FullRangeId;
use crate::resolver::TransactionInfo;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use uuid::Uuid;

struct State {
    tx_ids: RwLock<HashSet<Uuid>>,
    group_per_participant: HashMap<FullRangeId, RwLock<Vec<Uuid>>>,
}

// Transactions ready to commit grouped by participant range
struct GroupCommit {
    state: RwLock<State>,
    range_client: Arc<RangeClient>,
    tx_state_store: Arc<TxStateStoreClient>,
}

impl GroupCommit {
    pub fn new() -> Self {
        GroupCommit {
            state: RwLock::new(State {
                tx_ids: RwLock::new(HashSet::new()),
                group_per_participant: HashMap::new(),
            }),
            range_client: Arc::new(RangeClient::new()),
            tx_state_store: Arc::new(TxStateStoreClient::new()),
        }
    }

    async fn maybe_insert_new_participant_ranges(
        self,
        participant_ranges: Vec<FullRangeId>,
    ) -> Result<(), Error> {
        // Double-checked pattern: First read to avoid unnecessary write lock;
        // then write only if any participants are missing.
        let mut participants_to_insert = Vec::new();
        {
            let group_per_participant = self.group_per_participant.read().await;
            for participant_range in participant_ranges {
                if !group_per_participant.contains_key(&participant_range) {
                    participants_to_insert.push(participant_range);
                }
            }
        }
        // Acquire the write lock only if necessary
        if !participants_to_insert.is_empty() {
            let mut state = self.state.write().await;
            for participant_range in participants_to_insert {
                state
                    .group_per_participant
                    .entry(participant_range)
                    .or_insert_with(|| Arc::new(RwLock::new(Vec::new())));
            }
        }
        Ok(())
    }

    pub async fn add_transactions(self, transactions: Vec<&TransactionInfo>) -> Result<(), Error> {
        // Group transactions by participant range
        let mut group_per_participant = HashMap::new();
        for transaction in transactions {
            for participant_range in transaction.participant_ranges_info.iter() {
                // If a transaction has no writes in a participant range, it is not added to that participant range's group
                if participant_range.has_writes {
                    group_per_participant
                        .entry(participant_range.participant_range)
                        .or_insert_with(|| Vec::new())
                        .push(transaction.id);
                }
            }
        }

        self.maybe_insert_new_participant_ranges(group_per_participant.keys().collect())
            .await;

        // Extend tx_ids with the tx_ids of the transactions that are ready to commit
        let mut state = self.state.write().await;
        state.tx_ids.extend(transactions.iter().map(|t| t.id));

        // Update the participant ranges with the new transactions that are ready to commit
        let mut state = self.state.read().await;
        for (participant_range, transactions) in group_per_participant.iter() {
            let mut group = state
                .group_per_participant
                .get_mut(participant_range)
                .unwrap();
            group.write().await.extend(transactions);
        }
        Ok(())
    }

    async fn commit(self) -> Result<(), Error> {
        // Commit all groups in parallel
        let mut state = self.state.read().await;

        // Log all transactions ready to commit as committed in the tx_state_store
        {
            let tx_ids = state.tx_ids.write().await;
            let tx_ids = tx_ids.iter().collect();
            self.tx_state_store
                .batch_commit_transactions(tx_ids, 0)
                .await
                .unwrap();
            // TODO: Handle cascading aborts
            // {
            //     OpResult::TransactionIsAborted => {
            //         // Somebody must have aborted the transaction (maybe due to timeout)
            //         // so unfortunately the commit was not successful.
            //         return Err(Error::TransactionAborted(TransactionAbortReason::Other));
            //     }
            // };
            tx_ids.clear();
            // Transactions Committed!
        }

        let mut commit_join_set = JoinSet::new();

        // Notify all participants so that:
        // - they apply the TXs' PrepareRecords in storage
        // - and then quickly update their PendingCommitTables to stop treating these transactions as dependencies for other transactions
        for (participant_range, group) in state.group_per_participant.iter() {
            let group = group.read().await;

            let range_client = self.range_client.clone();
            commit_join_set.spawn(async move {
                range_client
                    .commit_transactions(group, participant_range, 0)
                    .await;
            });
        }

        while commit_join_set.join_next().await.is_some() {}
    }
}
