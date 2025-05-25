use common::full_range_id::FullRangeId;
use tx_state_store::client::Client as TxStateStoreClient;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{error::Error, resolver::TransactionInfo, rangeclient::RangeClient};

struct State {
    tx_ids: RwLock<HashSet<Uuid>>,
    group_per_participant: HashMap<FullRangeId, Arc<RwLock<Vec<Uuid>>>>,
}

// Transactions ready to commit grouped by participant range
pub struct GroupCommit {
    state: RwLock<State>,
    range_client: Arc<RangeClient>,
    tx_state_store: Arc<TxStateStoreClient>,
}

impl GroupCommit {
    pub fn new(range_client: Arc<RangeClient>, tx_state_store: Arc<TxStateStoreClient>) -> Self {
        GroupCommit {
            state: RwLock::new(State {
                tx_ids: RwLock::new(HashSet::new()),
                group_per_participant: HashMap::new(),
            }),
            range_client,
            tx_state_store,
        }
    }

    async fn maybe_insert_new_participant_ranges(
        &self,
        participant_ranges: Vec<FullRangeId>,
    ) -> Result<(), Error> {
        // Double-checked pattern: First read to avoid unnecessary write lock;
        // then write only if any participants are missing.
        let mut participants_to_insert = Vec::new();
        {
            let state = self.state.read().await;
            let group_per_participant = &state.group_per_participant;
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

    pub async fn add_transactions(
        &self,
        transactions: &Vec<TransactionInfo>,
    ) -> Result<(), Error> {
        // Group transactions by participant range
        let mut tmp_group_per_participant = HashMap::new();
        for transaction in transactions {
            for participant_range in transaction.participant_ranges_info.iter() {
                // If a transaction has no writes in a participant range, it is not added to that participant range's group
                if participant_range.has_writes {
                    tmp_group_per_participant
                        .entry(participant_range.participant_range)
                        .or_insert_with(|| Vec::new())
                        .push(transaction.id);
                }
            }
        }

        let _ = self
            .maybe_insert_new_participant_ranges(tmp_group_per_participant.keys().cloned().collect())
            .await;

        // Extend tx_ids with the tx_ids of the transactions that are ready to commit
        let state = self.state.read().await;
        let mut tx_ids = state.tx_ids.write().await;
        tx_ids.extend(transactions.iter().map(|t| t.id));

        // Update the participant ranges with the new transactions that are ready to commit
        let state = self.state.read().await;
        for (participant_range, transactions) in tmp_group_per_participant.iter() {
            let mut group = state
                .group_per_participant
                .get(participant_range)
                .unwrap()
                .write()
                .await;
            group.extend(transactions.iter().cloned());
        }
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), Error> {
        // Commit all groups in parallel
        let state = self.state.read().await;

        // Log all transactions ready to commit as committed in the tx_state_store
        {
            let mut tx_ids = state.tx_ids.write().await;
            let tx_ids_vec = tx_ids.iter().cloned().collect();

            self.tx_state_store
                .try_batch_commit_transactions(&tx_ids_vec, 0)
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
        for (participant_range, group_guard) in state.group_per_participant.iter() {
            let range_client = self.range_client.clone();
            let group_clone = group_guard.clone();
            let participant_range_clone = participant_range.clone();
            commit_join_set.spawn(async move {
                let group_clone = group_clone.read().await;
                let _ = range_client
                    .commit_transactions(group_clone.to_vec(), &participant_range_clone, 0)
                    .await;
            });
        }

        while commit_join_set.join_next().await.is_some() {}
        Ok(())
    }
}
