use crate::{error::Error, transaction_abort_reason::TransactionAbortReason};
use chrono::DateTime;
use common::transaction_info::TransactionInfo;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

type UtcDateTime = DateTime<chrono::Utc>;
pub struct CurrentLockHolder {
    transaction: Arc<TransactionInfo>,
    when_acquired: UtcDateTime,
    when_requested: UtcDateTime,
}

pub struct LockRequest {
    transaction: Arc<TransactionInfo>,
    sender: oneshot::Sender<()>,
    when_requested: UtcDateTime,
}

struct State {
    open: bool, // Lock is open either when there is no holder or when the current-holder has marked the lock as violatable.
    current_holder: Option<CurrentLockHolder>,
    previous_holder: Option<CurrentLockHolder>, // We keep the previous-holder around to track dependencies.
    waiting_to_acquire: VecDeque<LockRequest>,
    // waiting_for_release: VecDeque<LockRequest>,
}

// Implements transaction lock table for the range.
// Currently there is just a single lock for the entire range despite having
// "Table" in the name, but we might partition the lock to allow for more
// concurrency down the line.
pub struct LockTable {
    state: RwLock<State>,
}

impl LockTable {
    pub fn new() -> LockTable {
        LockTable {
            state: RwLock::new(State {
                open: true,
                current_holder: None,
                previous_holder: None,
                waiting_to_acquire: VecDeque::new(),
                // waiting_for_release: VecDeque::new(),
            }),
        }
    }
    // pub async fn maybe_wait_for_current_holder(
    //     &self,
    //     tx: Arc<TransactionInfo>,
    // ) -> oneshot::Receiver<()> {
    //     let (s, r) = oneshot::channel();
    //     let mut state = self.state.write().await;
    //     match &state.current_holder {
    //         None => s.send(()).unwrap(),
    //         Some(_) => {
    //             let req = LockRequest {
    //                 transaction: tx.clone(),
    //                 sender: s,
    //                 when_requested: chrono::Utc::now(),
    //             };
    //             state.waiting_for_release.push_back(req);
    //         }
    //     };
    //     r
    // }

    fn grant_lock_to_next_waiter(&self, state: &mut State) {
        assert!(state.open);
        match state.waiting_to_acquire.pop_front() {
            None => (),
            Some(req) => {
                let when_acquired = chrono::Utc::now();
                let new_holder = CurrentLockHolder {
                    transaction: req.transaction.clone(),
                    when_requested: req.when_requested,
                    when_acquired,
                };
                state.previous_holder = state.current_holder.take();
                state.current_holder = Some(new_holder);
                state.open = false;
                req.sender.send(()).unwrap();
            }
        }
    }

    pub async fn mark_as_violatable(&self) {
        let mut state = self.state.write().await;
        state.open = true;
        self.grant_lock_to_next_waiter(&mut state);
    }

    pub async fn acquire(&self, tx: Arc<TransactionInfo>) -> Result<oneshot::Receiver<()>, Error> {
        let when_requested = chrono::Utc::now();
        let (s, r) = oneshot::channel();
        let mut state = self.state.write().await;

        // If the same transaction is trying to acquire the lock again, ignore.
        if let Some(current_holder) = &state.current_holder {
            if current_holder.transaction.id == tx.id {
                s.send(()).unwrap();
                return Ok(r);
            }
        }

        // Add the new request to the waiting list.
        let req = LockRequest {
            transaction: tx.clone(),
            sender: s,
            when_requested: chrono::Utc::now(),
        };
        state.waiting_to_acquire.push_back(req);

        //  If the lock is open, give the opportunity for the new transaction to acquire it.
        if state.open {
            self.grant_lock_to_next_waiter(&mut state);
        }
        Ok(r)
    }

    pub async fn release(&self, tx: Arc<TransactionInfo>) {
        let mut state = self.state.write().await;
        if let Some(current_holder) = &state.current_holder {
            if current_holder.transaction.id == tx.id {
                //  If the current-holder is the one releasing the lock, we must mark the lock as open and grant the lock to the next possible waiter.
                state.open = true;
                state.current_holder = None;
                state.previous_holder = None;
                self.grant_lock_to_next_waiter(&mut state);
            } else if let Some(previous_holder) = &state.previous_holder {
                if previous_holder.transaction.id == tx.id {
                    //  If the previous-holder is the one releasing the lock, we just remove it so it's not listed as a dependency.
                    state.previous_holder = None;
                }
            }
            // If the transaction was holding the lock further in the past, its release has no effect.
        }
    }

    pub async fn is_currently_holding(&self, tx: Arc<TransactionInfo>) -> bool {
        let state = self.state.read().await;
        match &state.current_holder {
            None => false,
            Some(current) => current.transaction.id == tx.id,
        }
    }
}
