use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use crate::error::Error;
use crate::full_range_id::FullRangeId;
use tokio::task::JoinSet;
use crate::resolver::TransactionInfo;


struct State {
    group_per_participant: HashMap<FullRangeId, RwLock<Vec<Uuid>>>,
    num_participants_per_transaction: HashMap<Uuid, u32>,
}

// Transactions ready to commit grouped by participant range
struct GroupCommit {
    state: RwLock<State>,
}

impl GroupCommit {
    pub fn new() -> Self {
        GroupCommit {
            state: RwLock::new(State {
                group_per_participant: HashMap::new(),
                num_participants_per_transaction: HashMap::new(),
            }),
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
                state.group_per_participant
                    .entry(participant_range)
                    .or_insert_with(|| Arc::new(RwLock::new(Vec::new())));
            }
        }
        Ok(())
    }

    pub async fn add_transactions(
        self,
        transactions: Vec<&TransactionInfo>,
    ) -> Result<(), Error> {

        // Group transactions by participant range
        let mut group_per_participant = HashMap::new();
        for transaction in transactions {
            for participant_range in transaction.participant_ranges {
                group_per_participant.entry(participant_range).or_insert_with(|| Vec::new()).push(transaction.id);
            }
        }

        self.maybe_insert_new_participant_ranges(group_per_participant.keys().collect()).await;

        // Update the participant ranges with the new transactions that are ready to commit
        let mut state = self.state.read().await;
        for (participant_range, transactions) in group_per_participant.iter() {
            let mut group = state.group_per_participant.get_mut(participant_range).unwrap();
            group.write().await.extend(transactions);
        }
        Ok(())
    }

    async fn commit(self) -> Result<(), Error> {
        //  Commit each group in parallel

        // Mark transaction as committed in the transaction storage
        // match self
        //     .tx_state_store
        //     .try_commit_transaction(self.id, epoch)
        //     .await
        //     .unwrap()
        // {
        //     OpResult::TransactionIsAborted => {
        //         // Somebody must have aborted the transaction (maybe due to timeout)
        //         // so unfortunately the commit was not successful.
        //         return Err(Error::TransactionAborted(TransactionAbortReason::Other));
        //     }
        //     OpResult::TransactionIsCommitted(i) => assert!(i.epoch == epoch),
        // };


        // Transaction Committed!
        // self.state = State::Committed;
        // notify participants so they can quickly release locks.
        // let mut commit_join_set = JoinSet::new();
        // for range_id in self.participant_ranges.keys() {
        //     let range_id = *range_id;
        //     let range_client = self.range_client.clone();
        //     let transaction_info = self.transaction_info.clone();
        //     commit_join_set.spawn_on(
        //         async move {
        //             range_client
        //                 .commit_transaction(transaction_info, &range_id, epoch)
        //                 .await
        //         },
        //         &self.runtime,
        //     );
        // }
        // while commit_join_set.join_next().await.is_some() {}
    }
}

