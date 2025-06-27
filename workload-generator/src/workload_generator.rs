use crate::{transaction::Transaction, workload_config::WorkloadConfig};
use colored::Colorize;
use common::{
    keyspace::Keyspace,
    region::{Region, Zone},
};
use proto::{
    frontend::frontend_client::FrontendClient,
    resolver::resolver_client::ResolverClient,
    resolver::GetStatsRequest,
    universe::{CreateKeyspaceRequest, KeyRangeRequest, Zone as ProtoZone},
};
use rand::{distributions::WeightedIndex, prelude::*, rngs::StdRng, SeedableRng};
use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    runtime::Handle,
    sync::{Mutex, Semaphore},
    task::JoinSet,
};
use tracing::{error, info};

// Add a struct to hold our metrics
#[derive(Default, Debug)]
pub struct Metrics {
    pub total_duration: Duration,
    pub total_transactions: usize,
    pub avg_latency: Duration,
    pub p50_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
    pub throughput: f64,
    pub resolver_stats: HashMap<String, usize>,
}

// Add a struct to hold our metrics
#[derive(Default)]
struct InternalMetrics {
    latencies: Vec<Duration>,
    start_time: Option<Instant>,
    completed_transactions: usize,
}

pub struct WorkloadGenerator {
    workload_config: WorkloadConfig,
    client: FrontendClient<tonic::transport::Channel>,
    resolver_client: ResolverClient<tonic::transport::Channel>,
    metrics: Arc<Mutex<InternalMetrics>>,
    value_per_key: Arc<Mutex<HashMap<usize, u64>>>, // For verification
    rng: Arc<Mutex<StdRng>>,
}

impl WorkloadGenerator {
    pub fn new(
        workload_config: WorkloadConfig,
        client: FrontendClient<tonic::transport::Channel>,
        resolver_client: ResolverClient<tonic::transport::Channel>,
    ) -> Self {
        let seed = workload_config
            .seed
            .unwrap_or_else(|| rand::thread_rng().gen());
        info!("Using seed: {}", seed);
        let rng = Arc::new(Mutex::new(StdRng::seed_from_u64(seed)));
        Self {
            workload_config,
            client,
            resolver_client,
            metrics: Arc::new(Mutex::new(InternalMetrics::default())),
            value_per_key: Arc::new(Mutex::new(HashMap::new())),
            rng,
        }
    }

    fn zipf_sample_without_replacement(&self, sample_count: usize, rng: &mut StdRng) -> Vec<usize> {
        // Step 1: Build weights using Zipf formula: weight ∝ 1 / rank^s
        let mut weights: Vec<f64> = (1..=self.workload_config.num_keys)
            .map(|rank| 1.0 / (rank as f64).powf(self.workload_config.zipf_exponent))
            .collect();

        // Step 2: Normalize weights
        let total_weight: f64 = weights.iter().sum();
        for w in weights.iter_mut() {
            *w /= total_weight;
        }

        // Step 3: Sample without replacement
        let mut chosen_keys = Vec::with_capacity(sample_count);
        let mut available_keys: Vec<usize> = (0..self.workload_config.num_keys as usize).collect();

        for _ in 0..sample_count {
            if available_keys.is_empty() || weights.is_empty() {
                break;
            }
            let dist = WeightedIndex::new(&weights).unwrap();
            let idx = dist.sample(&mut *rng);
            chosen_keys.push(available_keys[idx]);

            // Remove selected item and its weight
            available_keys.remove(idx);
            weights.remove(idx);
        }

        chosen_keys.sort();
        chosen_keys
    }

    pub async fn generate_transaction(&self) -> Transaction {
        //  sample num_keys uniformly from {1, 2}
        let mut rng = self.rng.lock().await;
        let num_keys = rng.gen_range(1..3);
        let keys = self.zipf_sample_without_replacement(num_keys, &mut rng);
        // info!("{}", format!("Generated transaction with keys: {:?}", keys).blue());
        // sample num_keys without replacement from uniform distribution
        // let mut keys: Vec<usize> = (0..self.workload_config.num_keys)
        //     .choose_multiple(&mut thread_rng(), num_keys)
        //     .into_iter()
        //     .map(|k| k as usize)
        //     .collect();
        // keys.sort();

        Transaction::new(
            Keyspace {
                namespace: self.workload_config.namespace.clone(),
                name: self.workload_config.name.clone(),
            },
            keys.clone(),
            Some(keys),
        )
    }

    pub async fn create_keyspace(&self) {
        let zone = Zone {
            region: Region {
                cloud: None,
                name: "test-region".into(),
            },
            name: "a".into(),
        };

        // Create the keyspace for this experiment
        let mut client_clone = self.client.clone();
        let response = client_clone
            .create_keyspace(CreateKeyspaceRequest {
                namespace: self.workload_config.namespace.clone(),
                name: self.workload_config.name.clone(),
                primary_zone: Some(ProtoZone::from(zone)),
                base_key_ranges: (0..self.workload_config.num_keys)
                    .map(|i| KeyRangeRequest {
                        lower_bound_inclusive: i.to_be_bytes().to_vec(),
                        upper_bound_exclusive: (i + 1).to_be_bytes().to_vec(),
                    })
                    .collect(),
            })
            .await
            .unwrap();
        let keyspace_id = response.get_ref().keyspace_id.clone();
        info!("Created keyspace with ID: {:?}", keyspace_id);
    }

    pub async fn run(&self, runtime_handle: Handle) -> Metrics {
        info!("Starting workload");
        let max_concurrency = min(
            self.workload_config.max_concurrency,
            self.workload_config.num_queries.unwrap_or(u64::MAX),
        );
        let semaphore = Arc::new(Semaphore::new(max_concurrency as usize));

        {
            let mut metrics = self.metrics.lock().await;
            metrics.start_time = Some(Instant::now());
        }

        let mut task_counter = 0;
        let mut join_set = JoinSet::new();
        loop {
            tokio::select! {
                Ok(permit) = semaphore.clone().acquire_owned() => {
                    task_counter += 1;
                    if task_counter > self.workload_config.num_queries.unwrap_or(u64::MAX) {
                        drop(permit);
                        break;
                    }
                    let next_task = self.generate_transaction().await;
                    let mut client_clone = self.client.clone();
                    let metrics = self.metrics.clone();
                    let value_per_key = self.value_per_key.clone();

                    let join_handle = runtime_handle.spawn(async move {
                        let start_time = Instant::now();
                        let result = next_task.execute(&mut client_clone, value_per_key).await;
                        let latency = start_time.elapsed();

                        // Record metrics
                        let mut metrics = metrics.lock().await;
                        metrics.latencies.push(latency);
                        metrics.completed_transactions += 1;
                        drop(permit);
                        result
                    });

                    join_set.spawn(async move {
                        let _ = join_handle.await;
                    });
                }
                Some(res) = join_set.join_next() => {
                    if let Err(e) = res {
                        error!("Task panicked: {:?}", e);
                    }
                }
                else => break,
            }
        }

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                error!("Task panicked: {:?}", e);
            }
        }

        // Calculate and return metrics
        let metrics = self.metrics.lock().await;
        let total_duration = metrics.start_time.unwrap().elapsed();
        let total_transactions = metrics.completed_transactions;

        // Calculate statistics
        let avg_latency: Duration =
            metrics.latencies.iter().sum::<Duration>() / metrics.latencies.len() as u32;
        let throughput = total_transactions as f64 / total_duration.as_secs_f64();

        // Sort latencies for percentile calculation
        let mut sorted_latencies = metrics.latencies.clone();
        sorted_latencies.sort();
        let p99_latency = sorted_latencies[(sorted_latencies.len() as f64 * 0.99) as usize];
        let p95_latency = sorted_latencies[(sorted_latencies.len() as f64 * 0.95) as usize];
        let p50_latency = sorted_latencies[(sorted_latencies.len() as f64 * 0.50) as usize];

        info!("Workload Complete - Performance Metrics:");
        info!("Throughput: {:.2} transactions/second", throughput);
        info!("Average Latency: {:?}", avg_latency);
        info!("P50 Latency: {:?}", p50_latency);
        info!("P95 Latency: {:?}", p95_latency);
        info!("P99 Latency: {:?}", p99_latency);
        info!("Total Duration: {:?}", total_duration);
        info!("Total Transactions: {}", total_transactions);

        let mut resolver_client_clone = self.resolver_client.clone();
        let response = resolver_client_clone
            .get_stats(GetStatsRequest {})
            .await
            .unwrap();
        let response = response.into_inner();
        let mut stats_map = HashMap::new();
        for (group_size, count) in response.stats {
            stats_map.insert(group_size, count as usize);
        }
        info!("Resolver stats: {:?}", stats_map);

        Metrics {
            total_duration,
            total_transactions,
            avg_latency,
            p50_latency,
            p95_latency,
            p99_latency,
            throughput,
            resolver_stats: stats_map,
        }
    }
}
