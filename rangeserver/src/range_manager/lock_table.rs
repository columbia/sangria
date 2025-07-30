use super::labeled_histograms::compute_entropy;
use crate::{error::Error, transaction_abort_reason::TransactionAbortReason};
use chrono::DateTime;
use colored::Colorize;
use common::transaction_info::TransactionInfo;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

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
    current_holder: Option<CurrentLockHolder>,
    waiting_for_release: VecDeque<LockRequest>,
    waiting_to_acquire: VecDeque<LockRequest>,
    soft_released: HashSet<Uuid>,
}

#[derive(Default, Clone)]
pub struct Statistics {
    pub num_waiters: Vec<i64>,
    pub num_pending_commits: Vec<i64>,
    pub request_timestamps: Vec<UtcDateTime>,
    pub previous_request_timestamp: Option<UtcDateTime>,
    pub sum_delta_between_requests: f64,
    pub total_requests: f64,

    pub sum_entropies: f64,
    pub total_entropies: f64,
    pub waiters_and_pending_histogram: HashMap<i64, f64>,
    pub total_waiters_and_pending: i64,
    pub predictions: Vec<i64>,
    pub avg_delta_between_requests: Vec<f64>,
    pub avg_entropies: Vec<f64>,
}

// Implements transaction lock table for the range.
// Currently there is just a single lock for the entire range despite having
// "Table" in the name, but we might partition the lock to allow for more
// concurrency down the line.
pub struct LockTable {
    state: RwLock<State>,
    statistics: RwLock<Statistics>,
}

// const HISTORY_SIZE: usize = 20;
// const REQUESTS_WINDOW_MILLISECONDS: i64 = 10;

impl LockTable {
    pub fn new() -> LockTable {
        LockTable {
            state: RwLock::new(State {
                current_holder: None,
                waiting_for_release: VecDeque::new(),
                waiting_to_acquire: VecDeque::new(),
                soft_released: HashSet::new(),
            }),
            statistics: RwLock::new(Statistics::default()),
        }
    }

    pub async fn get_num_waiters_and_pending(&self) -> f64 {
        let (num_soft_released, num_waiting_to_acquire) = {
            let state = self.state.read().await;
            (state.soft_released.len(), state.waiting_to_acquire.len())
        };
        let sum = num_waiting_to_acquire as f64 + num_soft_released as f64;
        sum
        // num_waiting_to_acquire as f64
        // let mut statistics = self.statistics.write().await;
        // statistics.num_waiters.push((num_waiting_to_acquire) as i64);
        // statistics.num_pending_commits.push(num_soft_released as i64);
        // sum
    }

    pub async fn get_statistics(&self) -> Statistics {
        let mut statistics = self.statistics.write().await;
        let statistics_clone = statistics.clone();
        statistics.num_waiters.clear();
        statistics.num_pending_commits.clear();
        statistics.request_timestamps.clear();
        statistics.waiters_and_pending_histogram.clear();
        statistics.predictions.clear();
        statistics.sum_delta_between_requests = 0.0;
        statistics.total_requests = 0.0;
        statistics.previous_request_timestamp = None;
        statistics.avg_delta_between_requests.clear();
        statistics.avg_entropies.clear();
        statistics_clone
    }

    pub async fn print_state(&self, range_id: Uuid) {
        let state = self.state.read().await;
        info!(
            "{}",
            format!(
                "Range: {:?}, Lock table holder: {:?} and waiting to acquire: {:?}",
                range_id,
                state.current_holder.as_ref().map(|h| h.transaction.id),
                state
                    .waiting_to_acquire
                    .iter()
                    .map(|r| r.transaction.id)
                    .collect::<Vec<_>>()
            )
            .blue()
        );
    }

    pub async fn get_current_holder_id(&self) -> Option<Uuid> {
        let state = self.state.read().await;
        state
            .current_holder
            .as_ref()
            .map(|holder| holder.transaction.id)
    }

    pub async fn maybe_wait_for_current_holder(
        &self,
        tx: Arc<TransactionInfo>,
    ) -> oneshot::Receiver<()> {
        let (s, r) = oneshot::channel();
        let mut state = self.state.write().await;
        match &state.current_holder {
            None => s.send(()).unwrap(),
            Some(_) => {
                let req = LockRequest {
                    transaction: tx.clone(),
                    sender: s,
                    when_requested: chrono::Utc::now(),
                };
                state.waiting_for_release.push_back(req);
            }
        };
        r
    }

    // async fn update_sum_delta_between_requests(&self, when_requested: UtcDateTime) {
    //     let mut statistics = self.statistics.write().await;
    //     statistics.request_timestamps.push(when_requested);
    //     if statistics.previous_request_timestamp.is_none() {
    //         statistics.previous_request_timestamp = Some(when_requested);
    //         return;
    //     }
    //     let delta = when_requested - statistics.previous_request_timestamp.unwrap();
    //     statistics.sum_delta_between_requests += delta.num_milliseconds() as f64;
    //     statistics.previous_request_timestamp = Some(when_requested);
    //     statistics.total_requests += 1.0;
    // }

    pub async fn acquire(&self, tx: Arc<TransactionInfo>) -> Result<oneshot::Receiver<()>, Error> {
        let when_requested = chrono::Utc::now();

        // self.update_sum_delta_between_requests(when_requested).await;

        let (s, r) = oneshot::channel();
        let mut state = self.state.write().await;
        match &state.current_holder {
            None => {
                let holder = CurrentLockHolder {
                    transaction: tx.clone(),
                    when_requested,
                    when_acquired: when_requested,
                };
                state.current_holder = Some(holder);
                s.send(()).unwrap();
                Ok(r)
            }
            Some(current_holder) => {
                if current_holder.transaction.id == tx.id {
                    s.send(()).unwrap();
                    Ok(r)
                } else {
                    // let highest_waiter = state
                    //     .waiting_to_acquire
                    //     .back()
                    //     .map_or(current_holder.transaction.id, |r| r.transaction.id);
                    // if highest_waiter > tx.id {
                    //     // TODO: allow for skipping these checks if locks are ordered!
                    //     Err(Error::TransactionAborted(TransactionAbortReason::WaitDie))
                    // } else {
                    let req = LockRequest {
                        transaction: tx.clone(),
                        sender: s,
                        when_requested: chrono::Utc::now(),
                    };
                    state.waiting_to_acquire.push_back(req);
                    Ok(r)
                    // }
                }
            }
        }
    }

    pub async fn release(&self, mark_as_violatable: Option<bool>) {
        let mut state = self.state.write().await;
        let released_holder = state.current_holder.as_ref().map(|h| h.transaction.id);
        if mark_as_violatable.unwrap_or(false) && released_holder.is_some() {
            state.soft_released.insert(released_holder.unwrap());
        }
        state.current_holder = None;
        while !state.waiting_for_release.is_empty() {
            let req = state.waiting_for_release.pop_front().unwrap();
            req.sender.send(()).unwrap();
        }
        match state.waiting_to_acquire.pop_front() {
            None => (),
            Some(req) => {
                let when_acquired = chrono::Utc::now();
                let new_holder = CurrentLockHolder {
                    transaction: req.transaction.clone(),
                    when_requested: req.when_requested,
                    when_acquired,
                };
                state.current_holder = Some(new_holder);
                req.sender.send(()).unwrap();
            }
        }
    }

    pub async fn soft_release(&self) {
        self.release(Some(true)).await;
    }

    pub async fn clean_soft_released(&self, tx_ids: &Vec<Uuid>) {
        let mut state = self.state.write().await;
        state.soft_released.retain(|tx_id| !tx_ids.contains(tx_id));
    }

    pub async fn is_currently_holding(&self, tx_ids: &Vec<Uuid>) -> bool {
        let state = self.state.read().await;
        match &state.current_holder {
            None => false,
            Some(current) => tx_ids.contains(&current.transaction.id),
        }
    }
}

// pub async fn get_concurrency_prediction(&self) -> Option<i64> {
//     let (num_soft_released, num_waiting_to_acquire) =
//         {
//             let state = self.state.read().await;
//             (state.soft_released.len(), state.waiting_to_acquire.len())
//         };
//     let sum = num_waiting_to_acquire + num_soft_released;

//     let prediction = {
//         // Save them to be able to collect statistics later
//         let mut statistics = self.statistics.write().await;
//         statistics.num_waiters.push((num_waiting_to_acquire) as i64);
//         statistics.num_pending_commits.push(num_soft_released as i64);
//         statistics.total_waiters_and_pending += 1 as i64;
//         statistics.waiters_and_pending_histogram.entry(sum as i64).and_modify(|e| *e += 1.0).or_insert(1.0);

//         let entropy = compute_entropy(&statistics.waiters_and_pending_histogram, statistics.total_waiters_and_pending);
//         statistics.sum_entropies += entropy;
//         statistics.total_entropies += 1.0;
//         let avg_entropy = statistics.sum_entropies / statistics.total_entropies;
//         statistics.avg_entropies.push(avg_entropy);

//         let prediction = if avg_entropy > 100.0 {
//             1
//         } else if avg_entropy > 30.0 {
//             5
//         } else if avg_entropy > 18.0 {
//             25
//         } else if avg_entropy > 12.5 {
//             50
//         } else if avg_entropy > 7.5 {
//             150
//         } else {
//             300
//         };
//         statistics.predictions.push(prediction);
//         prediction
//     };

//     Some(prediction)
// }

// pub async fn get_concurrency_prediction(&self) -> Option<i64> {
//     // TODO change to read lock
//     let mut statistics = self.statistics.write().await;
//     let avg = statistics.sum_delta_between_requests / statistics.total_requests;
//     statistics.avg_delta_between_requests.push(avg);
//     let prediction = if avg > 100.0 {
//         1
//     } else if avg > 30.0 {
//         5
//     } else if avg > 18.0 {
//         25
//     } else if avg > 12.5 {
//         50
//     } else if avg > 7.5 {
//         150
//     } else {
//         300
//     };
//     statistics.predictions.push(prediction);
//     Some(prediction)
// }
