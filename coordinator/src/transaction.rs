use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use colored::Colorize;
use common::{
    config::CommitStrategy, constants, full_range_id::FullRangeId, keyspace::Keyspace,
    membership::range_assignment_oracle::RangeAssignmentOracle, record::Record,
    transaction_info::TransactionInfo,
};
use epoch_reader::reader::EpochReader;
use tokio::task::JoinSet;
use tracing::info;
use uuid::Uuid;

use coordinator_rangeclient::{
    error::{Error, TransactionAbortReason},
    rangeclient::RangeClient,
};
use resolver::participant_range_info::ParticipantRangeInfo;
use resolver::resolver_client::ResolverClient;
use tx_state_store::client::{Client as TxStateStoreClient, OpResult};

enum State {
    Running,
    Preparing,
    Aborted,
    Committed,
}

struct ParticipantRange {
    readset: HashSet<Bytes>,
    writeset: HashMap<Bytes, Bytes>,
    deleteset: HashSet<Bytes>,
    leader_sequence_number: u64,
}

pub struct Transaction {
    id: Uuid,
    transaction_info: Arc<TransactionInfo>,
    state: State,
    participant_ranges: HashMap<FullRangeId, ParticipantRange>,
    dependencies: HashSet<Uuid>,
    range_client: Arc<RangeClient>,
    range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
    epoch_reader: Arc<EpochReader>,
    tx_state_store: Arc<TxStateStoreClient>,
    runtime: tokio::runtime::Handle,
    commit_strategy: CommitStrategy,
    resolver: Arc<dyn ResolverClient>,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct FullRecordKey {
    pub range_id: FullRangeId,
    pub key: Bytes,
}

impl Transaction {
    async fn resolve_full_record_key(
        &mut self,
        keyspace: &Keyspace,
        key: Bytes,
    ) -> Result<FullRecordKey, Error> {
        let range_id = match self
            .range_assignment_oracle
            .full_range_id_of_key(keyspace, key.clone())
            .await
        {
            None => return Err(Error::KeyspaceDoesNotExist),
            Some(id) => id,
        };
        let full_record_key = FullRecordKey {
            key: key.clone(),
            range_id,
        };
        Ok(full_record_key)
    }

    fn check_still_running(&self) -> Result<(), Error> {
        match self.state {
            State::Running => Ok(()),
            State::Aborted => Err(Error::TransactionAborted(TransactionAbortReason::Other)),
            State::Preparing | State::Committed => Err(Error::TransactionNoLongerRunning),
        }
    }

    fn get_participant_range(&mut self, range_id: FullRangeId) -> &mut ParticipantRange {
        self.participant_ranges
            .entry(range_id)
            .or_insert_with(|| ParticipantRange {
                readset: HashSet::new(),
                writeset: HashMap::new(),
                deleteset: HashSet::new(),
                leader_sequence_number: 0,
            });
        self.participant_ranges.get_mut(&range_id).unwrap()
    }

    pub async fn get(&mut self, keyspace: &Keyspace, key: Bytes) -> Result<Option<Bytes>, Error> {
        self.check_still_running()?;
        let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;
        let participant_range = self.get_participant_range(full_record_key.range_id);
        // Read-your-writes.
        if let Some(v) = participant_range.writeset.get(&key) {
            return Ok(Some(v.clone()));
        }
        if participant_range.deleteset.contains(&key) {
            return Ok(None);
        }
        // TODO(tamer): errors.
        let get_result = self
            .range_client
            .get(
                self.transaction_info.clone(),
                &full_record_key.range_id,
                vec![key.clone()],
            )
            .await
            .unwrap();

        //  Update the transaction's dependencies with those returned by the Get in the range.
        self.dependencies.extend(get_result.dependencies);
        info!(
            "Transaction {} has dependencies: {:?}",
            self.id, self.dependencies
        );

        let participant_range = self.get_participant_range(full_record_key.range_id);
        let current_range_leader_seq_num = get_result.leader_sequence_number;
        if current_range_leader_seq_num != constants::INVALID_LEADER_SEQUENCE_NUMBER
            && participant_range.leader_sequence_number
                == constants::UNSET_LEADER_SEQUENCE_NUMBER as u64
        {
            participant_range.leader_sequence_number = current_range_leader_seq_num as u64;
        };
        if current_range_leader_seq_num != participant_range.leader_sequence_number as i64 {
            let _ = self.record_abort().await;
            return Err(Error::TransactionAborted(
                TransactionAbortReason::RangeLeadershipChanged,
            ));
        }
        participant_range.readset.insert(key.clone());

        let val = get_result.vals.first().unwrap().clone();
        Ok(val)
    }

    pub async fn put(&mut self, keyspace: &Keyspace, key: Bytes, val: Bytes) -> Result<(), Error> {
        self.check_still_running()?;
        let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;
        let participant_range = self.get_participant_range(full_record_key.range_id);
        participant_range.deleteset.remove(&key);
        participant_range.writeset.insert(key, val.clone());
        Ok(())
    }

    pub async fn del(&mut self, keyspace: &Keyspace, key: Bytes) -> Result<(), Error> {
        self.check_still_running()?;
        let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;
        let participant_range = self.get_participant_range(full_record_key.range_id);
        participant_range.writeset.remove(&key);
        participant_range.deleteset.insert(key);
        Ok(())
    }

    async fn record_abort(&mut self) -> Result<(), Error> {
        // We can directly set the state to Aborted here since given a transaction
        //  cannot commit on its own without us deciding to commit it.
        self.state = State::Aborted;
        // Record the abort.
        // TODO(tamer): handle errors here.
        let mut abort_join_set = JoinSet::new();
        for range_id in self.participant_ranges.keys() {
            let range_id = *range_id;
            let range_client = self.range_client.clone();
            let transaction_info = self.transaction_info.clone();
            abort_join_set.spawn_on(
                async move {
                    range_client
                        .abort_transaction(transaction_info, &range_id)
                        .await
                },
                &self.runtime,
            );
        }
        let outcome = self
            .tx_state_store
            .try_abort_transaction(self.id)
            .await
            .unwrap();
        match outcome {
            OpResult::TransactionIsAborted => (),
            OpResult::TransactionIsCommitted(_) => {
                panic!("transaction committed without coordinator consent!")
            }
        }
        while abort_join_set.join_next().await.is_some() {}
        Ok(())
    }

    pub async fn abort(&mut self) -> Result<(), Error> {
        match self.state {
            State::Aborted => return Ok(()),
            _ => {
                self.check_still_running()?;
            }
        };
        self.record_abort().await
    }

    fn error_from_rangeclient_error(_err: rangeclient::client::Error) -> Error {
        // TODO(tamer): handle
        panic!("encountered rangeclient error, translation not yet implemented.")
    }

    pub async fn commit(&mut self) -> Result<(), Error> {
        self.check_still_running()?;

        // 1. --- PREPARE PHASE ---
        self.state = State::Preparing;
        let mut prepare_join_set = JoinSet::new();
        for (range_id, info) in &self.participant_ranges {
            let range_id = *range_id;
            let range_client = self.range_client.clone();
            let transaction_info = self.transaction_info.clone();
            let has_reads = !info.readset.is_empty();
            let writes: Vec<Record> = info
                .writeset
                .iter()
                .map(|(k, v)| Record {
                    key: k.clone(),
                    val: v.clone(),
                })
                .collect();
            let deletes: Vec<Bytes> = info.deleteset.iter().cloned().collect();
            prepare_join_set.spawn_on(
                async move {
                    range_client
                        .prepare_transaction(
                            transaction_info,
                            &range_id,
                            has_reads,
                            &writes,
                            &deletes,
                        )
                        .await
                },
                &self.runtime,
            );
        }
        let mut any_early_lock_releases = false;
        while let Some(res) = prepare_join_set.join_next().await {
            let res = match res {
                Err(_) => {
                    let _ = self.record_abort().await;
                    return Err(Error::TransactionAborted(
                        TransactionAbortReason::PrepareFailed,
                    ));
                }
                Ok(res) => res,
            };
            let res = res.map_err(Self::error_from_rangeclient_error)?;
            // Update transaction's dependencies with those returned by the Prepare in each range.
            self.dependencies.extend(res.dependencies);
            if res.released_lock_early {
                any_early_lock_releases = true;
            }
            // epoch_leases.push(res.epoch_lease);
            // if res.highest_known_epoch > epoch {
            //     epoch = res.highest_known_epoch;
            // }
        }
        info!(
            "Transaction {} has dependencies: {:?}",
            self.id, self.dependencies
        );

        // At this point we are prepared!
        match self.commit_strategy {
            CommitStrategy::Traditional => {
                let epoch = 0;
                // Attempt to commit.
                match self
                    .tx_state_store
                    .try_commit_transaction(self.id, epoch)
                    .await
                    .unwrap()
                {
                    OpResult::TransactionIsAborted => {
                        // Somebody must have aborted the transaction (maybe due to timeout)
                        // so unfortunately the commit was not successful.
                        return Err(Error::TransactionAborted(TransactionAbortReason::Other));
                    }
                    OpResult::TransactionIsCommitted(i) => assert!(i.epoch == epoch),
                };

                // Transaction Committed!
                self.state = State::Committed;
                // notify participants so they can quickly release locks.
                let mut commit_join_set = JoinSet::new();
                for (range_id, info) in self.participant_ranges.iter() {
                    let range_id = *range_id;
                    let has_writes = !info.writeset.is_empty();
                    let range_client = self.range_client.clone();
                    let transaction_info = self.transaction_info.clone();

                    if has_writes {
                        commit_join_set.spawn_on(
                            async move {
                                range_client
                                    .commit_transactions(
                                        vec![transaction_info.id],
                                        &range_id,
                                        epoch,
                                    )
                                    .await
                            },
                            &self.runtime,
                        );
                    }
                }
                while commit_join_set.join_next().await.is_some() {}
            }
            // CommitStrategy::Pipelined => {
            //     // 2. --- COMMIT PHASE ---
            //     info!(
            //         "{}",
            //         format!("Delegating commit to resolver for transaction {}", self.id)
            //             .italic()
            //             .bold()
            //             .yellow()
            //     );
            //     let participants_info = self
            //         .participant_ranges
            //         .iter()
            //         .map(|(range_id, info)| ParticipantRangeInfo {
            //             participant_range: *range_id,
            //             has_writes: !info.writeset.is_empty(),
            //         })
            //         .collect();
            //     self.resolver
            //         .commit(self.id, self.dependencies.clone(), participants_info)
            //         .await?;
            // }
            CommitStrategy::Adaptive | CommitStrategy::Pipelined => {
                if !self.dependencies.is_empty() {
                    info!(
                        "{}",
                        format!("Delegating commit to resolver for transaction {}", self.id)
                            .italic()
                            .bold()
                            .yellow()
                    );
                    let participants_info = self
                        .participant_ranges
                        .iter()
                        .map(|(range_id, info)| {
                            ParticipantRangeInfo::new(*range_id, !info.writeset.is_empty())
                        })
                        .collect();
                    self.resolver
                        .commit(self.id, self.dependencies.clone(), participants_info)
                        .await?;
                } else {
                    // Send commit directly to the participant ranges.
                    info!(
                        "{}",
                        format!("Committing transaction {:?} without Resolver", self.id)
                            .italic()
                            .bold()
                            .green()
                    );
                    let _ = self
                        .tx_state_store
                        .try_commit_transaction(self.id, 0)
                        .await
                        .unwrap();
                    self.state = State::Committed;

                    // Notify participants so they can quickly release locks.
                    let mut commit_join_set = JoinSet::new();
                    for (range_id, info) in self.participant_ranges.iter() {
                        let range_id = *range_id;
                        let has_writes = !info.writeset.is_empty();
                        let range_client = self.range_client.clone();
                        let transaction_info = self.transaction_info.clone();
                        if has_writes {
                            commit_join_set.spawn_on(
                                async move {
                                    range_client
                                        .commit_transactions(
                                            vec![transaction_info.id],
                                            &range_id,
                                            0,
                                        )
                                        .await
                                },
                                &self.runtime,
                            );
                        }
                    }
                    while commit_join_set.join_next().await.is_some() {}
                    info!("Transaction {:?} has committed", self.id);

                    // Now that commit is complete, we must register the transaction as committed in the resolver.
                    info!(
                        "Registering transaction {:?} as committed in the resolver",
                        self.id
                    );

                    if any_early_lock_releases {
                        info!("At least one early lock release happened, registering transaction as committed in the resolver");
                        // Spawn async and don't wait for it to complete.
                        let resolver = self.resolver.clone();
                        let tx_id = self.id;
                        self.runtime.spawn(async move {
                            let _ = resolver.register_committed_transactions(vec![tx_id]).await;
                        });
                    }
                }
                info!("COMMIT OF TRANSACTION {:?} DONE!", self.id);
            }
        }
        Ok(())
    }

    pub(crate) fn new(
        transaction_info: Arc<TransactionInfo>,
        range_client: Arc<RangeClient>,
        range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
        epoch_reader: Arc<EpochReader>,
        tx_state_store: Arc<TxStateStoreClient>,
        runtime: tokio::runtime::Handle,
        resolver: Arc<dyn ResolverClient>,
        commit_strategy: CommitStrategy,
    ) -> Transaction {
        Transaction {
            id: transaction_info.id,
            transaction_info,
            state: State::Running,
            participant_ranges: HashMap::new(),
            dependencies: HashSet::new(),
            range_client,
            range_assignment_oracle,
            epoch_reader,
            tx_state_store,
            runtime,
            commit_strategy,
            resolver,
        }
    }
}
