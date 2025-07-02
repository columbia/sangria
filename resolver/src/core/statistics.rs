use std::{collections::HashMap, time::Instant};

#[derive(Debug)]
pub struct StatisticsTracker {
    request_count: u64,
    last_reset_time: Instant,
    waiting_transactions_samples: Vec<usize>,
}

const MAX_SAMPLES: usize = 100;

impl StatisticsTracker {
    pub fn new() -> Self {
        Self {
            request_count: 0,
            last_reset_time: Instant::now(),
            waiting_transactions_samples: Vec::new(),
        }
    }

    pub fn record_request(&mut self) {
        self.request_count += 1;
    }

    pub fn record_waiting_transactions_sample(&mut self, count: usize) {
        self.waiting_transactions_samples.push(count);
        if self.waiting_transactions_samples.len() > MAX_SAMPLES {
            self.waiting_transactions_samples.remove(0);
        }
    }

    pub fn get_average_waiting_transactions(&self) -> f64 {
        if self.waiting_transactions_samples.is_empty() {
            return 0.0;
        }
        self.waiting_transactions_samples.iter().sum::<usize>() as f64
            / self.waiting_transactions_samples.len() as f64
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
        stats.insert(
            "num_requests".to_string(),
            self.request_count as f64,
        );
        stats
    }

    pub fn reset(&mut self) {
        self.request_count = 0;
        self.last_reset_time = Instant::now();
        self.waiting_transactions_samples.clear();
    }
}
