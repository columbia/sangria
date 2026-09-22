use std::{collections::HashMap, fs::File, io::Write, time::Instant};
use uuid::Uuid;

#[derive(Debug)]
pub struct StatisticsTracker {
    request_count: u64,
    last_reset_time: Instant,
    // This bounded history is intentionally retained for the adaptive policy.
    waiting_transactions_samples: Vec<usize>,
    // Unlike the policy history, this high-water mark covers the entire
    // measurement interval. It is also updated directly when a transaction is
    // inserted into the waiting queue, so a short-lived peak cannot be missed
    // by the periodic sampler.
    max_resolver_queue: usize,
    // Full-interval summary of the same rolling signal read by Adaptive.
    resolver_load_signal_sum: f64,
    resolver_load_signal_count: u64,
    resolver_load_signal_min: Option<f64>,
    resolver_load_signal_max: Option<f64>,
    // The latest depth is kept per transaction because a predecessor may be
    // registered after its successor and increase the successor's depth.
    dependency_depths: HashMap<Uuid, usize>,
}

const MAX_SAMPLES: usize = 200;

impl StatisticsTracker {
    pub fn new() -> Self {
        Self {
            request_count: 0,
            last_reset_time: Instant::now(),
            waiting_transactions_samples: Vec::new(),
            max_resolver_queue: 0,
            resolver_load_signal_sum: 0.0,
            resolver_load_signal_count: 0,
            resolver_load_signal_min: None,
            resolver_load_signal_max: None,
            dependency_depths: HashMap::new(),
        }
    }

    pub fn record_request(&mut self) {
        self.request_count += 1;
    }

    pub fn record_waiting_transactions_sample(&mut self, count: usize) {
        self.record_waiting_transactions_count(count);
        self.waiting_transactions_samples.push(count);
        if self.waiting_transactions_samples.len() > MAX_SAMPLES {
            self.waiting_transactions_samples.remove(0);
        }
        let signal = self.get_average_waiting_transactions();
        self.resolver_load_signal_sum += signal;
        self.resolver_load_signal_count += 1;
        self.resolver_load_signal_min = Some(
            self.resolver_load_signal_min
                .map_or(signal, |current| current.min(signal)),
        );
        self.resolver_load_signal_max = Some(
            self.resolver_load_signal_max
                .map_or(signal, |current| current.max(signal)),
        );
    }

    pub fn record_waiting_transactions_count(&mut self, count: usize) {
        self.max_resolver_queue = self.max_resolver_queue.max(count);
    }

    pub fn record_dependency_depth(&mut self, transaction_id: Uuid, depth: usize) {
        self.dependency_depths
            .entry(transaction_id)
            .and_modify(|current| *current = (*current).max(depth))
            .or_insert(depth);
    }

    pub fn get_average_waiting_transactions(&self) -> f64 {
        if self.waiting_transactions_samples.is_empty() {
            return 0.0;
        }
        // self.waiting_transactions_samples.iter().sum::<usize>() as f64
        // / self.waiting_transactions_samples.len() as f64
        let waiting_transactions_max = self
            .waiting_transactions_samples
            .iter()
            .max()
            .copied()
            .unwrap_or(0);
        waiting_transactions_max as f64
    }

    pub fn get_stats(&mut self) -> HashMap<String, f64> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_reset_time);

        // Calculate requests per second
        let requests_per_second = if elapsed.as_secs() > 0 {
            self.request_count as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        // Calculate waiting transactions statistics
        let waiting_transactions_avg = if !self.waiting_transactions_samples.is_empty() {
            self.waiting_transactions_samples.iter().sum::<usize>() as f64
                / self.waiting_transactions_samples.len() as f64
        } else {
            0.0
        };

        let waiting_transactions_min = self
            .waiting_transactions_samples
            .iter()
            .min()
            .copied()
            .unwrap_or(0);

        let waiting_transactions_max = self
            .waiting_transactions_samples
            .iter()
            .max()
            .copied()
            .unwrap_or(0);

        // Calculate standard deviation
        let waiting_transactions_std = if self.waiting_transactions_samples.len() > 1 {
            let variance = self
                .waiting_transactions_samples
                .iter()
                .map(|&x| {
                    let diff = x as f64 - waiting_transactions_avg;
                    diff * diff
                })
                .sum::<f64>()
                / (self.waiting_transactions_samples.len() - 1) as f64;
            variance.sqrt()
        } else {
            0.0
        };

        let mut stats = HashMap::new();
        stats.insert("requests_per_second".to_string(), requests_per_second);
        stats.insert(
            "waiting_transactions_avg".to_string(),
            waiting_transactions_avg,
        );
        stats.insert(
            "waiting_transactions_min".to_string(),
            waiting_transactions_min as f64,
        );
        stats.insert(
            "waiting_transactions_max".to_string(),
            waiting_transactions_max as f64,
        );
        stats.insert(
            "waiting_transactions_std".to_string(),
            waiting_transactions_std,
        );
        stats.insert("num_requests".to_string(), self.request_count as f64);
        stats.insert(
            "max_resolver_queue".to_string(),
            self.max_resolver_queue as f64,
        );
        stats.insert(
            "resolver_load_signal_avg".to_string(),
            if self.resolver_load_signal_count == 0 {
                0.0
            } else {
                self.resolver_load_signal_sum / self.resolver_load_signal_count as f64
            },
        );
        stats.insert(
            "resolver_load_signal_min".to_string(),
            self.resolver_load_signal_min.unwrap_or_default(),
        );
        stats.insert(
            "resolver_load_signal_max".to_string(),
            self.resolver_load_signal_max.unwrap_or_default(),
        );

        let mut dependency_depths = self.dependency_depths.values().copied().collect::<Vec<_>>();
        dependency_depths.sort_unstable();
        stats.insert(
            "dependency_depth_count".to_string(),
            dependency_depths.len() as f64,
        );
        stats.insert(
            "dependency_depth_p50".to_string(),
            percentile_nearest_rank(&dependency_depths, 0.50) as f64,
        );
        stats.insert(
            "dependency_depth_p95".to_string(),
            percentile_nearest_rank(&dependency_depths, 0.95) as f64,
        );
        stats.insert(
            "dependency_depth_max".to_string(),
            dependency_depths.last().copied().unwrap_or(0) as f64,
        );
        stats
    }

    pub fn reset(&mut self) {
        self.request_count = 0;
        self.last_reset_time = Instant::now();
        self.waiting_transactions_samples.clear();
        self.max_resolver_queue = 0;
        self.resolver_load_signal_sum = 0.0;
        self.resolver_load_signal_count = 0;
        self.resolver_load_signal_min = None;
        self.resolver_load_signal_max = None;
        self.dependency_depths.clear();
    }
}

fn percentile_nearest_rank(sorted_values: &[usize], percentile: f64) -> usize {
    if sorted_values.is_empty() {
        return 0;
    }

    let rank = (percentile * sorted_values.len() as f64).ceil() as usize;
    sorted_values[rank.saturating_sub(1).min(sorted_values.len() - 1)]
}
