use super::{GetResult, PrepareResult, RangeManager as Trait};

use crate::{
    epoch_supplier::EpochSupplier, error::Error, key_version::KeyVersion,
    range_manager::lock_table, storage::RangeInfo, storage::Storage,
    transaction_abort_reason::TransactionAbortReason, wal::Wal,
};
use bytes::Bytes;
use common::config::{CommitStrategy, Config, Heuristic};
use common::full_range_id::FullRangeId;
use common::transaction_info::TransactionInfo;

use colored::Colorize;
use uuid::Uuid;

use crate::prefetching_buffer::KeyState;
use crate::prefetching_buffer::PrefetchingBuffer;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tonic::async_trait;
use tracing::info;

struct PrepareRecord {
    changes: HashMap<Bytes, Option<Bytes>>, // key to change -> new value
}

struct PendingState {
    pending_prepare_records: HashMap<Uuid, Arc<PrepareRecord>>,
    // key -> id of the last transaction that has updated this key
    pending_commit_table: HashMap<Bytes, Uuid>,
}

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: HighestKnownEpoch,
    lock_table: lock_table::LockTable,
    pending_state: RwLock<PendingState>,
}

enum State {
    NotLoaded,
    Loading(tokio::sync::broadcast::Sender<Result<(), Error>>),
    Loaded(LoadedState),
    Unloaded,
}

struct HighestKnownEpoch {
    val: RwLock<u64>,
}

impl HighestKnownEpoch {
    fn new(e: u64) -> HighestKnownEpoch {
        HighestKnownEpoch {
            val: RwLock::new(e),
        }
    }

    async fn read(&self) -> u64 {
        *self.val.read().await
    }

    async fn maybe_update(&self, new_epoch: u64) {
        let mut v = self.val.write().await;
        *v = std::cmp::max(*v, new_epoch)
    }
}

pub struct RangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    range_id: FullRangeId,
    config: Config,
    storage: Arc<S>,
    epoch_supplier: Arc<dyn EpochSupplier>,
    wal: Arc<W>,
    state: Arc<RwLock<State>>,
    prefetching_buffer: PrefetchingBuffer,
    bg_runtime: tokio::runtime::Handle,
}

#[async_trait]
impl<S, W> Trait for RangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    async fn load(&self) -> Result<(), Error> {
        if let State::Loaded(_) = self.state.read().await.deref() {
            return Ok(());
        }
        let sender = {
            let mut state = self.state.write().await;
            match state.deref_mut() {
                State::Loaded(_) => return Ok(()),
                State::Loading(sender) => {
                    let mut receiver = sender.subscribe();
                    drop(state);
                    return receiver.recv().await.unwrap();
                }
                State::NotLoaded => {
                    let (sender, _) = tokio::sync::broadcast::channel(1);
                    *state = State::Loading(sender.clone());
                    sender
                }
                State::Unloaded => return Err(Error::RangeIsNotLoaded),
            }
        };

        let load_result = self.load_inner().await;

        let mut state = self.state.write().await;
        match load_result {
            Err(e) => {
                *state = State::Unloaded;
                sender.send(Err(e.clone())).unwrap();
                Err(e)
            }
            Ok(loaded_state) => {
                *state = State::Loaded(loaded_state);
                // TODO(tamer): Ignoring the error here seems kind of sketchy.
                let _ = sender.send(Ok(()));
                Ok(())
            }
        }
    }

    async fn unload(&self) {
        let mut state = self.state.write().await;
        *state = State::Unloaded;
    }

    async fn is_unloaded(&self) -> bool {
        let state = self.state.read().await;
        match state.deref() {
            State::Unloaded => true,
            State::NotLoaded | State::Loading(_) | State::Loaded(_) => false,
        }
    }

    async fn prefetch(&self, transaction_id: Uuid, key: Bytes) -> Result<(), Error> {
        // Request prefetch from the prefetching buffer
        let keystate = self
            .prefetching_buffer
            .process_prefetch_request(transaction_id, key.clone())
            .await;

        match keystate {
            KeyState::Fetched => Ok(()), // key has previously been fetched
            KeyState::Loading(_) => Err(Error::PrefetchError), // Something is wrong if loading was returned
            KeyState::Requested(fetch_sequence_number) =>
            // key has just been requested - start fetch
            {
                // Fetch from database
                let val = match self.prefetch_get(key.clone()).await {
                    Ok(value) => value,
                    Err(_) => {
                        self.prefetching_buffer
                            .fetch_failed(key.clone(), fetch_sequence_number)
                            .await;
                        return Err(Error::PrefetchError);
                    }
                };
                // Successfully fetched from database -> add to buffer and update records
                self.prefetching_buffer
                    .fetch_complete(key.clone(), val, fetch_sequence_number)
                    .await;
                Ok(())
            }
        }
    }

    async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                if !state.range_info.key_range.includes(key.clone()) {
                    return Err(Error::KeyIsOutOfRange);
                };
                self.acquire_range_lock(state, tx.clone()).await?;

                let mut get_result = GetResult {
                    val: None,
                    leader_sequence_number: state.range_info.leader_sequence_number as i64,
                    dependencies: vec![],
                };

                if self.config.commit_strategy != CommitStrategy::Traditional {
                    info!("Checking dependencies for transaction {:?}", tx.id);
                    // First, check if operation has any dependencies with another transaction
                    let pending_state = state.pending_state.read().await;
                    if let Some(dependency) = pending_state.pending_commit_table.get(&key) {
                        get_result.dependencies = vec![dependency.clone()];
                        // Read the value from the pending_prepare_record of the dependee transaction
                        info!(
                            "Reading value from pending_prepare_record of transaction {:?}",
                            dependency
                        );
                        let prepare_record = pending_state
                            .pending_prepare_records
                            .get(&dependency)
                            .unwrap();
                        if prepare_record.changes.contains_key(&key) {
                            get_result.val = prepare_record.changes.get(&key).unwrap().clone();
                            return Ok(get_result);
                        } else {
                            panic!("Key not found in pending_prepare_records but dependency found");
                        }
                    }
                }
                info!("No dependencies found for transaction {:?}", tx.id);
                // Check prefetch buffer
                let value = self
                    .prefetching_buffer
                    .get_from_buffer(key.clone())
                    .await
                    .map_err(|_| Error::PrefetchError)?;
                if let Some(val) = value {
                    get_result.val = Some(val);
                } else {
                    let val = self
                        .storage
                        .get(self.range_id, key.clone())
                        .await
                        .map_err(Error::from_storage_error)?;

                    get_result.val = val.clone();
                }

                Ok(get_result)
            }
        }
    }

    async fn prepare(
        &self,
        tx: Arc<TransactionInfo>,
        prepare: PrepareRequest<'_>,
    ) -> Result<PrepareResult, Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => {
                return Err(Error::RangeIsNotLoaded)
            }
            State::Loaded(state) => {
                // Check if transaction has no writes
                let has_writes = !(prepare.puts().unwrap_or_default().is_empty()
                    && prepare.deletes().unwrap_or_default().is_empty());

                // --- For read-only transactions ---
                if !has_writes {
                    // If transaction has reads, verify lock and release it
                    if prepare.has_reads() {
                        if !state.lock_table.is_currently_holding(&vec![tx.id]).await {
                            return Err(Error::TransactionAborted(
                                TransactionAbortReason::TransactionLockLost,
                            ));
                        }
                        // TODO: Should I leave an entry in the PendingCommitTable for this transaction here?
                        // Probably not, since this is a read-only transaction and future reads or writes should not depend on its success.
                        // state.pending_commit_table.write().await.insert(key, tx.id);
                        // TODO: Make sure somehow we don't call the commit method for read-only transactions.
                        state.lock_table.release().await;
                    }

                    // Return early for read-only transactions
                    return Ok(PrepareResult {
                        highest_known_epoch: state.highest_known_epoch.read().await,
                        epoch_lease: state.range_info.epoch_lease,
                        dependencies: vec![],
                    });
                }

                // --- For transactions with writes ---
                // 1) Validation Checks
                // Sanity check that the written keys are all within this range. Build the prepare record while you are at it.
                // TODO: check delete and write sets are non-overlapping.
                let prepare_record: Arc<PrepareRecord> = {
                    let mut prepare_record = PrepareRecord {
                        changes: HashMap::new(),
                    };
                    for put in prepare.puts().iter() {
                        for put in put.iter() {
                            // TODO: too much copying :(
                            let key =
                                Bytes::copy_from_slice(put.key().unwrap().k().unwrap().bytes());
                            if !state.range_info.key_range.includes(key.clone()) {
                                return Err(Error::KeyIsOutOfRange);
                            }
                            let val = Bytes::copy_from_slice(put.value().unwrap().bytes());
                            prepare_record.changes.insert(key, Some(val));
                        }
                    }
                    for del in prepare.deletes().iter() {
                        for del in del.iter() {
                            let key = Bytes::copy_from_slice(del.k().unwrap().bytes());
                            if !state.range_info.key_range.includes(key.clone()) {
                                return Err(Error::KeyIsOutOfRange);
                            }
                            prepare_record.changes.insert(key, None);
                        }
                    }
                    Arc::new(prepare_record)
                };
                // Validate the transaction lock is not lost, this is essential to ensure 2PL
                // invariants still hold.
                if prepare.has_reads() && !state.lock_table.is_currently_holding(&vec![tx.id]).await
                {
                    return Err(Error::TransactionAborted(
                        TransactionAbortReason::TransactionLockLost,
                    ));
                }

                // 2) Acquire the range lock if it hasn't already been acquired by previous Gets
                self.acquire_range_lock(state, tx.clone()).await?;

                let mut dependencies = HashSet::new();
                {
                    // Decide whether we should release the lock early for each key based on the commit strategy and the heuristic
                    let mut early_lock_release_per_key = HashMap::new();
                    for key in prepare_record.changes.keys() {
                        early_lock_release_per_key.insert(
                            key.clone(),
                            self.do_early_lock_release(state, key.clone()).await,
                        );
                    }
                    info!(
                        "Early lock release per key:\n{}",
                        early_lock_release_per_key
                            .iter()
                            .map(|(k, v)| format!(
                                "{}: {}",
                                format!("{:?}", k).blue(),
                                v.to_string().red()
                            ))
                            .collect::<Vec<_>>()
                            .join("\n")
                    );
                    // 3) Save the prepare record to the pending_prepare_records
                    let mut pending_state = state.pending_state.write().await;
                    pending_state
                        .pending_prepare_records
                        .insert(tx.id, prepare_record.clone());

                    // NOTE: CommitStrategy::Traditional will return no dependencies, will not update the pending_commit_table, and will not release the lock
                    // so it could bypass the rest of this codeblock for efficiency.

                    if self.config.commit_strategy != CommitStrategy::Traditional {
                        // 4) Get any Write-Write dependencies for the transaction and update the pending_commit_table if ran in "early-lock-release" mode
                        info!("Checking dependencies for transaction {:?}", tx.id);
                        for (key, early_lock_release) in early_lock_release_per_key.iter() {
                            if pending_state.pending_commit_table.contains_key(key) {
                                dependencies.insert(
                                    pending_state.pending_commit_table.get(key).unwrap().clone(),
                                );
                            }
                            // Update the pending_commit_table only for keys that we will release the lock early for
                            // For the keys that we will not release the lock early for, we clear the entry in the pending_commit_table
                            if *early_lock_release {
                                pending_state
                                    .pending_commit_table
                                    .insert(key.clone(), tx.id);
                            } else {
                                pending_state.pending_commit_table.remove(key);
                            }
                        }
                    }
                    drop(pending_state);
                    info!(
                        "Dependencies for transaction {:?}: {:?}",
                        tx.id, dependencies
                    );

                    // 5) Maybe Release the range lock
                    // NOTE: This is for when we have per-key locking.
                    // for (key, early_lock_release) in early_lock_release_per_key {
                    //     if early_lock_release {
                    //         state.lock_table.release(key).await;
                    //     }
                    // }
                    // Now since we have one lock for all keys in the range, the heuristic is guaranteed to return the same early lock release decision for all keys.
                    // So, we just check the early-lock-release flag for the first key and release the lock if it is true.
                    if *early_lock_release_per_key.values().next().unwrap() {
                        info!(
                            "Early lock release for transaction {:?} on range {:?}",
                            tx.id, self.range_id.range_id
                        );
                        state.lock_table.release().await;
                    }
                }

                // 6) Log prepare record to WAL
                self.wal
                    .append_prepare(prepare)
                    .await
                    .map_err(Error::from_wal_error)?;

                // let highest_known_epoch = state.highest_known_epoch.read().await;
                Ok(PrepareResult {
                    highest_known_epoch: 0,
                    epoch_lease: state.range_info.epoch_lease,
                    dependencies: dependencies.into_iter().collect(),
                })
            }
        }
    }

    async fn abort(&self, tx: Arc<TransactionInfo>, abort: AbortRequest<'_>) -> Result<(), Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => {
                return Err(Error::RangeIsNotLoaded)
            }
            State::Loaded(state) => {
                if !state.lock_table.is_currently_holding(&vec![tx.id]).await {
                    return Ok(());
                }
                {
                    // TODO: We can skip aborting to the log if we never appended a prepare record.
                    // TODO: It's possible the WAL already contains this record in case this is a retry
                    // so avoid re-inserting in that case.
                    self.wal
                        .append_abort(abort)
                        .await
                        .map_err(Error::from_wal_error)?;
                }
                state.lock_table.release().await;

                // let _ = self
                //     .prefetching_buffer
                //     .process_transaction_complete(tx.id)
                //     .await;
                Ok(())
            }
        }
    }

    async fn commit(
        &self,
        transactions: Vec<Uuid>,
        commit: CommitRequest<'_>,
    ) -> Result<(), Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => {
                return Err(Error::RangeIsNotLoaded)
            }
            State::Loaded(state) => {
                // if !state.lock_table.is_currently_holding(tx.clone()).await {
                //     // it must be that we already finished committing, but perhaps the coordinator didn't
                //     // realize that, so we just return success.
                //     return Ok(());
                // }
                // state.highest_known_epoch.maybe_update(commit.epoch()).await;

                // Write commit record to WAL
                self.wal
                    .append_commit(commit)
                    .await
                    .map_err(Error::from_wal_error)?;

                // Collect all pending prepare records for the transactions to be committed
                let mut pending_prepare_record_per_tx = HashMap::new();
                {
                    let pending_state = state.pending_state.read().await;
                    for tx_id in &transactions {
                        match pending_state.pending_prepare_records.get(tx_id) {
                            Some(prepare_record) => {
                                _ = pending_prepare_record_per_tx
                                    .insert(tx_id, prepare_record.clone())
                            }
                            None => panic!("Prepare record not found for transaction {:?}", tx_id),
                            // TODO: Panic for now, but maybe it's just that the TX had already been committed?
                        }
                    }
                }

                let mut all_changes = HashMap::new();
                let mut last_tx_per_key = HashMap::new();
                for tx_id in &transactions {
                    // Apply the changes to the keys in the correct order to respect the commit dependencies.
                    // For every key, collect the last transaction that has updated it. This will help us update the pending_commit_table later.
                    // TODO: overlaps should have been handled in the prepare phase.
                    let prepare_record = pending_prepare_record_per_tx.get(tx_id).unwrap();
                    for (key, val) in prepare_record.changes.iter() {
                        all_changes.insert(key.clone(), val.clone());
                        last_tx_per_key.insert(key.clone(), tx_id);
                    }
                }

                let version = KeyVersion {
                    epoch: commit.epoch(),
                    // TODO: version counter should be an internal counter per range.
                    // Remove from the commit message.
                    version_counter: commit.vid() as u64,
                };

                // Batch upsert for all puts and deletes
                if !all_changes.is_empty() {
                    self.storage
                        .batch_upsert(self.range_id, &all_changes, version)
                        .await
                        .map_err(Error::from_storage_error)?;
                    // Update the prefetch buffer for all puts and deletes
                    for (key, val) in all_changes.iter() {
                        if let Some(val) = val {
                            self.prefetching_buffer
                                .upsert(key.clone(), val.clone())
                                .await;
                        } else {
                            self.prefetching_buffer.delete(key.clone()).await;
                        }
                    }
                }

                {
                    let mut pending_state = state.pending_state.write().await;
                    if self.config.commit_strategy != CommitStrategy::Traditional {
                        // We update the pending_commit_table to remove the transactions that have committed and are listed as dependencies for other transactions
                        for (key, tx_id) in last_tx_per_key {
                            if pending_state.pending_commit_table.contains_key(&key) {
                                // The last dependee per key might have changed since the prepare phase, so we need to check if it is still a dependency before removing it.
                                let last_dependee =
                                    pending_state.pending_commit_table.get(&key).unwrap();
                                info!(
                                    "Last dependee for key {:?} is {:?} and last tx is {:?}",
                                    key, last_dependee, tx_id
                                );
                                info!("All transaction ids: {:?}", transactions);
                                if *tx_id == *last_dependee {
                                    info!("Removing key {:?} from pending_commit_table", key);
                                    pending_state.pending_commit_table.remove(&key);
                                }
                                // Otherwise, the transaction that last updated the key in the pending_commit_table has not committed yet.
                            }
                        }
                    }

                    // Remove the prepare records for the transactions that have committed.
                    for tx_id in &transactions {
                        info!("Removing prepare record for transaction {:?}", tx_id);
                        pending_state.pending_prepare_records.remove(tx_id);
                    }
                }

                info!("Done committing transactions {:?}", transactions);

                // // For CommitStrategy::Traditional, this is the only transaction that can hold the lock.
                // if self.config.commit_strategy == CommitStrategy::Traditional {
                //     assert!(transactions.len() == 1);
                //     assert!(
                //         transactions[0].id
                //             == state.lock_table.get_current_holder_id().await.unwrap()
                //     );
                // }

                // If any of the committed transactions is the one holding the lock, release it.
                if let Some(current_holder_id) = state.lock_table.get_current_holder_id().await {
                    if transactions.iter().any(|tx| *tx == current_holder_id) {
                        state.lock_table.release().await;
                        info!(
                            "Lock released for transaction {:?} on range {:?}",
                            current_holder_id, self.range_id.range_id
                        );
                    }
                }

                Ok(())
            }
        }
    }
}

impl<S, W> RangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    pub fn new(
        range_id: FullRangeId,
        config: Config,
        storage: Arc<S>,
        epoch_supplier: Arc<dyn EpochSupplier>,
        wal: W,
        bg_runtime: tokio::runtime::Handle,
    ) -> Arc<Self> {
        Arc::new(RangeManager {
            range_id,
            config,
            storage,
            epoch_supplier,
            wal: Arc::new(wal),
            state: Arc::new(RwLock::new(State::NotLoaded)),
            prefetching_buffer: PrefetchingBuffer::new(),
            bg_runtime,
        })
    }

    async fn load_inner(&self) -> Result<LoadedState, Error> {
        let epoch_supplier = self.epoch_supplier.clone();
        let storage = self.storage.clone();
        let wal = self.wal.clone();
        let range_id = self.range_id;
        let bg_runtime = self.bg_runtime.clone();
        let state = self.state.clone();
        let lease_renewal_interval = self.config.range_server.range_maintenance_duration;
        let epoch_duration = self.config.epoch.epoch_duration;
        // Calculate how many epochs we need for the desired lease duration.
        // TODO(yanniszark): Put this in the config.
        let intended_lease_duration = Duration::from_secs(2);
        let num_epochs_per_lease = intended_lease_duration
            .as_nanos()
            .checked_div(epoch_duration.as_nanos())
            .and_then(|n| u64::try_from(n).ok())
            .unwrap();
        // Ensure that we have at least one epoch per lease.
        let num_epochs_per_lease = std::cmp::max(1, num_epochs_per_lease);

        self.bg_runtime
            .spawn(async move {
                // TODO: handle all errors instead of panicking.
                let epoch = epoch_supplier
                    .read_epoch()
                    .await
                    .map_err(Error::from_epoch_supplier_error)?;
                let mut range_info = storage
                    .take_ownership_and_load_range(range_id)
                    .await
                    .map_err(Error::from_storage_error)?;
                // Epoch read from the provider can be 1 less than the true epoch. The highest known epoch
                // of a range cannot move backward even across range load/unloads, so to maintain that guarantee
                // we just wait for the epoch to advance once.
                epoch_supplier
                    .wait_until_epoch(epoch + 1, chrono::Duration::seconds(10))
                    .await
                    .map_err(Error::from_epoch_supplier_error)?;
                // Get a new epoch lease.
                let highest_known_epoch = epoch + 1;
                let new_epoch_lease_lower_bound =
                    std::cmp::max(highest_known_epoch, range_info.epoch_lease.1 + 1);
                let new_epoch_lease_upper_bound =
                    new_epoch_lease_lower_bound + num_epochs_per_lease;
                storage
                    .renew_epoch_lease(
                        range_id,
                        (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                        range_info.leader_sequence_number,
                    )
                    .await
                    .map_err(Error::from_storage_error)?;
                range_info.epoch_lease = (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound);
                wal.sync().await.map_err(Error::from_wal_error)?;
                // // Create a recurrent task to renew.
                // bg_runtime.spawn(async move {
                //     Self::renew_epoch_lease_task(
                //         range_id,
                //         epoch_supplier,
                //         storage,
                //         state,
                //         lease_renewal_interval,
                //     )
                //     .await
                // });
                // TODO: apply WAL here!
                Ok(LoadedState {
                    range_info,
                    highest_known_epoch: HighestKnownEpoch::new(highest_known_epoch),
                    lock_table: lock_table::LockTable::new(),
                    pending_state: RwLock::new(PendingState {
                        pending_prepare_records: HashMap::new(),
                        pending_commit_table: HashMap::new(),
                    }),
                })
            })
            .await
            .unwrap()
    }

    async fn renew_epoch_lease_task(
        range_id: FullRangeId,
        epoch_supplier: Arc<dyn EpochSupplier>,
        storage: Arc<S>,
        state: Arc<RwLock<State>>,
        lease_renewal_interval: std::time::Duration,
        num_epochs_per_lease: u64,
    ) -> Result<(), Error> {
        loop {
            let leader_sequence_number: u64;
            let old_lease: (u64, u64);
            let epoch = epoch_supplier
                .read_epoch()
                .await
                .map_err(Error::from_epoch_supplier_error)?;
            let highest_known_epoch = epoch + 1;
            if let State::Loaded(state) = state.read().await.deref() {
                old_lease = state.range_info.epoch_lease;
                leader_sequence_number = state.range_info.leader_sequence_number;
            } else {
                tokio::time::sleep(lease_renewal_interval).await;
                continue;
            }
            // How far are we from the current lease expiring? Check so we don't
            // end up taking the lease for an unbounded amount of epochs.
            let num_epochs_left = old_lease.1.saturating_sub(epoch);
            if num_epochs_left > 2 * num_epochs_per_lease {
                tokio::time::sleep(lease_renewal_interval).await;
                continue;
            }
            let new_epoch_lease_lower_bound = std::cmp::max(highest_known_epoch, old_lease.1 + 1);
            let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + num_epochs_per_lease;
            // TODO: We should handle some errors here. For example:
            // - If the error seems transient (e.g., a timeout), we should retry.
            // - If the error is something like RangeOwnershipLost, we should unload the range.
            storage
                .renew_epoch_lease(
                    range_id,
                    (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                    leader_sequence_number,
                )
                .await
                .map_err(Error::from_storage_error)?;

            // Update the state.
            // If our new lease continues from our old lease, merge the ranges.
            let mut new_lease = (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound);
            if (new_epoch_lease_lower_bound - old_lease.1) == 1 {
                new_lease = (old_lease.0, new_epoch_lease_upper_bound);
            }
            if let State::Loaded(state) = state.write().await.deref_mut() {
                // This should never happen as only this task changes the epoch lease.
                assert_eq!(
                    state.range_info.epoch_lease, old_lease,
                    "Epoch lease changed by someone else, but only this task should be changing it!"
                );
                state.range_info.epoch_lease = new_lease;
                state
                    .highest_known_epoch
                    .maybe_update(highest_known_epoch)
                    .await;
            } else {
                return Err(Error::RangeIsNotLoaded);
            }
            // Sleep for a while before renewing the lease again.
            tokio::time::sleep(lease_renewal_interval).await;
        }
    }

    async fn do_early_lock_release(&self, state: &LoadedState, _key: Bytes) -> bool {
        match self.config.commit_strategy {
            CommitStrategy::Pipelined => true,
            CommitStrategy::Traditional => false,
            CommitStrategy::Adaptive => {
                // When we have per-key locking, we can check contention for each key and allow early-lock-release for that key if it is contended.
                // e.g. return self.lock_table.is_contended(&key).await;
                // But for now, we just check the contention for the entire range based on how many transactions are currently waiting for the lock and return the same decision for all keys in the range.
                match self.config.heuristic {
                    Heuristic::LockContention => state.lock_table.is_contended().await,
                    // Heuristic::Static => map(state.range_info.range_id) -> Yes or No,
                }
            }
        }
    }

    pub async fn print_lock_table_state(&self) {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => {
                return;
            }
            State::Loaded(state) => {
                state.lock_table.print_state(self.range_id.range_id).await;
            }
        }
    }

    async fn acquire_range_lock(
        &self,
        state: &LoadedState,
        tx: Arc<TransactionInfo>,
    ) -> Result<(), Error> {
        let receiver = state.lock_table.acquire(tx.clone()).await?;
        // TODO: allow timing out locks when transaction timeouts are implemented.
        receiver.await.unwrap();
        Ok(())
    }

    /// Get from database without acquiring any locks
    /// This is a very basic copy of the 'get' function
    pub async fn prefetch_get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        let val = self
            .storage
            .get(self.range_id, key.clone())
            .await
            .map_err(Error::from_storage_error)?;
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use common::config::{
        CassandraConfig, EpochConfig, FrontendConfig, HostPort, RangeServerConfig, UniverseConfig,
    };
    use common::transaction_info::TransactionInfo;
    use common::util;
    use core::time;
    use flatbuffers::FlatBufferBuilder;
    use std::str::FromStr;
    use uuid::Uuid;

    use super::*;
    use crate::for_testing::epoch_supplier::EpochSupplier;
    use crate::for_testing::in_memory_wal::InMemoryWal;
    use crate::storage::cassandra::Cassandra;

    type RM = RangeManager<Cassandra, InMemoryWal>;

    impl RM {
        async fn abort_transaction(&self, tx: Arc<TransactionInfo>) {
            let mut fbb = FlatBufferBuilder::new();
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let fbb_root = AbortRequest::create(
                &mut fbb,
                &AbortRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                },
            );
            fbb.finish(fbb_root, None);
            let abort_record_bytes = fbb.finished_data();
            let abort_record = flatbuffers::root::<AbortRequest>(abort_record_bytes).unwrap();
            self.abort(tx.clone(), abort_record).await.unwrap()
        }

        async fn prepare_transaction(
            &self,
            tx: Arc<TransactionInfo>,
            writes: Vec<(Bytes, Bytes)>,
            deletes: Vec<Bytes>,
            has_reads: bool,
        ) -> Result<(), Error> {
            let mut fbb = FlatBufferBuilder::new();
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let mut puts_vector = Vec::new();
            for (k, v) in writes {
                let k = Some(fbb.create_vector(k.to_vec().as_slice()));
                let key = Key::create(&mut fbb, &KeyArgs { k });
                let value = fbb.create_vector(v.to_vec().as_slice());
                puts_vector.push(Record::create(
                    &mut fbb,
                    &RecordArgs {
                        key: Some(key),
                        value: Some(value),
                    },
                ));
            }
            let puts = Some(fbb.create_vector(&puts_vector));
            let mut del_vector = Vec::new();
            for k in deletes {
                let k = Some(fbb.create_vector(k.to_vec().as_slice()));
                let key = Key::create(&mut fbb, &KeyArgs { k });
                del_vector.push(key);
            }
            let deletes = Some(fbb.create_vector(&del_vector));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let fbb_root = PrepareRequest::create(
                &mut fbb,
                &PrepareRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                    has_reads,
                    puts,
                    deletes,
                },
            );
            fbb.finish(fbb_root, None);
            let prepare_record_bytes = fbb.finished_data();
            let prepare_record = flatbuffers::root::<PrepareRequest>(prepare_record_bytes).unwrap();
            self.prepare(tx.clone(), prepare_record).await.map(|_| ())
        }

        async fn commit_transaction(&self, tx: Arc<TransactionInfo>) -> Result<(), Error> {
            let epoch = self.epoch_supplier.read_epoch().await.unwrap();
            let mut fbb = FlatBufferBuilder::new();
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let fbb_root = CommitRequest::create(
                &mut fbb,
                &CommitRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                    epoch,
                    vid: 0,
                },
            );
            fbb.finish(fbb_root, None);
            let commit_record_bytes = fbb.finished_data();
            let commit_record = flatbuffers::root::<CommitRequest>(commit_record_bytes).unwrap();
            self.commit(tx.id, commit_record).await
        }
    }

    struct TestContext {
        rm: Arc<RM>,
        storage_context: crate::storage::cassandra::for_testing::TestContext,
    }

    async fn init() -> TestContext {
        let epoch_supplier = Arc::new(EpochSupplier::new());
        let storage_context: crate::storage::cassandra::for_testing::TestContext =
            crate::storage::cassandra::for_testing::init().await;
        let cassandra = storage_context.cassandra.clone();
        let prefetching_buffer = Arc::new(PrefetchingBuffer::new());
        let range_id = FullRangeId {
            keyspace_id: storage_context.keyspace_id,
            range_id: storage_context.range_id,
        };
        let epoch_config = EpochConfig {
            // Not used in these tests.
            proto_server_addr: "127.0.0.1:50052".parse().unwrap(),
            epoch_duration: time::Duration::from_millis(10),
        };
        let config = Config {
            range_server: RangeServerConfig {
                range_maintenance_duration: time::Duration::from_secs(1),
                proto_server_addr: HostPort::from_str("127.0.0.1:50054").unwrap(),
                fast_network_addr: HostPort::from_str("127.0.0.1:50055").unwrap(),
                fast_network_polling_core_id: 1,
                background_runtime_core_ids: vec![1],
            },
            universe: UniverseConfig {
                proto_server_addr: "127.0.0.1:123".parse().unwrap(),
            },
            frontend: FrontendConfig {
                proto_server_addr: "127.0.0.1:124".parse().unwrap(),
                fast_network_addr: HostPort::from_str("127.0.0.1:125").unwrap(),
                fast_network_polling_core_id: 1,
                background_runtime_core_ids: vec![1],
                transaction_overall_timeout: time::Duration::from_secs(10),
            },
            cassandra: CassandraConfig {
                cql_addr: HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 9042,
                },
            },
            regions: std::collections::HashMap::new(),
            epoch: epoch_config,
        };
        let rm = Arc::new(RM {
            range_id,
            config,
            storage: cassandra,
            wal: Arc::new(InMemoryWal::new()),
            epoch_supplier: epoch_supplier.clone(),
            state: Arc::new(RwLock::new(State::NotLoaded)),
            prefetching_buffer,
            bg_runtime: tokio::runtime::Handle::current().clone(),
        });
        let rm_copy = rm.clone();
        let init_handle = tokio::spawn(async move { rm_copy.load().await.unwrap() });
        // Give some delay so the RM can see the epoch advancing.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        epoch_supplier.set_epoch(1).await;
        init_handle.await.unwrap();
        TestContext {
            rm,
            storage_context,
        }
    }

    fn start_transaction() -> Arc<TransactionInfo> {
        Arc::new(TransactionInfo {
            id: Uuid::new_v4(),
            started: chrono::Utc::now(),
            overall_timeout: time::Duration::from_secs(10),
        })
    }

    #[tokio::test]
    async fn basic_get_put() {
        let context = init().await;
        let rm = context.rm.clone();
        let key = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
        let tx1 = start_transaction();
        assert!(rm
            .get(tx1.clone(), key.clone())
            .await
            .unwrap()
            .val
            .is_none());
        rm.abort_transaction(tx1.clone()).await;
        let tx2 = start_transaction();
        let val = Bytes::from_static(b"I have a value!");
        rm.prepare_transaction(
            tx2.clone(),
            Vec::from([(key.clone(), val.clone())]),
            Vec::new(),
            false,
        )
        .await
        .unwrap();
        rm.commit_transaction(tx2.clone()).await.unwrap();
        let tx3 = start_transaction();
        let val_after_commit = rm.get(tx3.clone(), key.clone()).await.unwrap().val.unwrap();
        assert!(val_after_commit == val);
    }

    // #[tokio::test]
    // async fn test_recurring_lease_renewal() {
    //     let context = init().await;
    //     let rm = context.rm.clone();
    //     // Get the current lease bounds.
    //     let initial_lease = match rm.state.read().await.deref() {
    //         State::Loaded(state) => state.range_info.epoch_lease,
    //         _ => panic!("Range is not loaded"),
    //     };
    //     // Sleep for 2 seconds to allow the lease renewal task to run at least once.
    //     tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    //     let final_lease = match rm.state.read().await.deref() {
    //         State::Loaded(state) => state.range_info.epoch_lease,
    //         _ => panic!("Range is not loaded"),
    //     };
    //     // Check that the upper bound has increased.
    //     assert!(
    //         final_lease.1 > initial_lease.1,
    //         "Lease upper bound did not increase"
    //     );
    // }
}
