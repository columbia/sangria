use crate::{
    transaction::Transaction, transaction_impl::fake_transaction::FakeTransaction,
    transaction_impl::rw_transaction::RwTransaction, workload_config::WorkloadConfig,
};
use tokio::signal;

use colored::Colorize;
use common::{
    keyspace::Keyspace,
    region::{Region, Zone},
};
use proto::{
    frontend::frontend_client::FrontendClient,
    rangeserver::{range_server_client::RangeServerClient, GetStatisticsRequest},
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
    sync::{Mutex, RwLock, Semaphore},
    task::JoinSet,
};
use tracing::{error, info};
use uuid::Uuid;

use tokio::signal::unix::{signal, SignalKind};

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
    pub resolver_stats: HashMap<String, f64>,
    pub range_server_stats: HashMap<String, HashMap<String, Vec<String>>>,
}

// Add a struct to hold our metrics
#[derive(Default)]
struct InternalMetrics {
    latencies: Vec<Duration>,
    start_time: Option<Instant>,
    completed_transactions: usize,
}

pub struct WorkloadGenerator {
    workload_config: Arc<WorkloadConfig>,
    client: FrontendClient<tonic::transport::Channel>,
    resolver_client: ResolverClient<tonic::transport::Channel>,
    range_server_client: RangeServerClient<tonic::transport::Channel>,
    metrics: Arc<Mutex<InternalMetrics>>,
    value_per_key: Arc<Mutex<HashMap<usize, u64>>>, // For verification
    rng: Arc<Mutex<StdRng>>,
    pending_commit_table: Arc<RwLock<HashMap<usize, Uuid>>>,
    ycsb_transactions: Arc<Mutex<Option<Vec<String>>>>,
}

impl WorkloadGenerator {
    pub async fn new(
        workload_config: Arc<WorkloadConfig>,
        client: FrontendClient<tonic::transport::Channel>,
        resolver_client: ResolverClient<tonic::transport::Channel>,
        range_server_client: RangeServerClient<tonic::transport::Channel>,
    ) -> Arc<Self> {
        let seed = workload_config
            .seed
            .unwrap_or_else(|| rand::thread_rng().gen());
        info!("Using seed: {}", seed);
        let rng = Arc::new(Mutex::new(StdRng::seed_from_u64(seed)));
        let zipf_exponent = workload_config.zipf_exponent;
        let workload_type = workload_config.workload_type.clone();
        let mut ycsb_transactions: Arc<Mutex<Option<Vec<String>>>> = Arc::new(Mutex::new(None));
        if workload_type == "ycsb" {
            let zipf_str = format!("{:.1}", zipf_exponent);
            let filename = format!(
                "{}/ycsb_queries/workload_{}.txt",
                std::env::current_dir().unwrap().display(),
                zipf_str
            );
            match std::fs::read_to_string(&filename) {
                Ok(contents) => {
                    let txs: Vec<String> = contents
                        .lines()
                        .map(|line| line.trim().to_string())
                        .filter(|line| !line.is_empty())
                        .collect();
                    *ycsb_transactions.lock().await = Some(txs);
                    info!(
                        "Loaded {} YCSB transactions from {}",
                        ycsb_transactions.lock().await.as_ref().unwrap().len(),
                        filename
                    );
                }
                Err(e) => {
                    error!("Failed to load YCSB workload file {}: {}", filename, e);
                }
            }
        }
        Arc::new(Self {
            workload_config,
            client,
            resolver_client,
            range_server_client,
            metrics: Arc::new(Mutex::new(InternalMetrics::default())),
            value_per_key: Arc::new(Mutex::new(HashMap::new())),
            rng,
            pending_commit_table: Arc::new(RwLock::new(HashMap::new())),
            ycsb_transactions,
        })
    }

    fn zipf_sample_without_replacement(
        self: &Arc<Self>,
        sample_count: usize,
        rng: &mut StdRng,
    ) -> Vec<usize> {
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

    pub async fn generate_transaction(
        self: &Arc<Self>,
        workload_id: usize,
    ) -> Arc<dyn Transaction + Send + Sync> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let keys: Vec<usize> = match self.workload_config.workload_type.as_str() {
            "custom" => {
                // Sample num_keys uniformly from {1, 2}
                let num_keys = 2;
                let mut rng = self.rng.lock().await;
                let mut keys: Vec<usize> = (0..self.workload_config.num_keys)
                    .choose_multiple(&mut *rng, num_keys)
                    .into_iter()
                    .map(|k| k as usize)
                    .collect();
                drop(rng);
                keys.sort();
                keys
            }
            "ycsb" => {
                // Get the next transaction from the YCSB workload
                let mut txs = self.ycsb_transactions.lock().await;
                let key: usize = txs
                    .as_mut()
                    .unwrap()
                    .pop()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                vec![key]
            }
            _ => {
                panic!(
                    "Invalid workload type: {}",
                    self.workload_config.workload_type
                );
            }
        };
        let name = match workload_id {
            0 => self.workload_config.name.clone(),
            _ => format!("{}_{}", self.workload_config.name.clone(), workload_id),
        };
        let keyspace = Keyspace {
            namespace: self.workload_config.namespace.clone(),
            name,
        };
        let transaction: Arc<dyn Transaction + Send + Sync> = match self
            .workload_config
            .fake_transactions
        {
            true => {
                FakeTransaction::new(
                    self.resolver_client.clone(),
                    keyspace,
                    keys.clone(),
                    self.pending_commit_table.clone(),
                )
                .await
            }
            false => RwTransaction::new(self.client.clone(), keyspace, keys.clone(), Some(keys)),
        };
        transaction
    }

    pub async fn create_keyspace(self: &Arc<Self>, namespace: String, name: String) {
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
                namespace,
                name,
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

    pub async fn run(self: Arc<Self>, runtime_handle: Handle) -> Metrics {
        info!("Starting workload");
        {
            let mut metrics = self.metrics.lock().await;
            metrics.start_time = Some(Instant::now());
        }

        let mut join_set = JoinSet::new();
        let concurrency_strings: Vec<String> = self
            .workload_config
            .max_concurrency
            .split(';')
            .map(|s| s.to_string())
            .collect();

        for (workload_id, mixed_concurrency_config) in concurrency_strings.into_iter().enumerate() {
            info!("Mixed concurrency config: {:?}", mixed_concurrency_config);
            let self_clone = self.clone();
            let runtime_handle_clone = runtime_handle.clone();

            join_set.spawn(async move {
                info!("Starting workload with ID: {}", workload_id);
                let concurrency_configs: Vec<Vec<u64>> = if !mixed_concurrency_config.contains(":")
                {
                    vec![vec![
                        mixed_concurrency_config.parse::<u64>().unwrap(),
                        self_clone.workload_config.num_queries.unwrap_or(u64::MAX),
                    ]]
                } else {
                    mixed_concurrency_config
                        .split(",")
                        .map(|s| {
                            s.split(":")
                                .map(|s| s.parse::<u64>().unwrap())
                                .collect::<Vec<u64>>()
                        })
                        .collect::<Vec<Vec<u64>>>()
                };
                info!("Concurrency configs: {:?}", concurrency_configs);
                for concurrency_config in concurrency_configs {
                    let max_concurrency = concurrency_config[0];
                    let num_queries = concurrency_config[1];

                    let semaphore = Arc::new(Semaphore::new(max_concurrency as usize));
                    let mut sigusr1_stream = signal(SignalKind::user_defined1()).unwrap();

                    {
                        let mut task_counter = 0;
                        let mut join_set = JoinSet::new();
                        let sc = self_clone.clone();
                        let rhc = runtime_handle_clone.clone();
                        loop {
                            tokio::select! {
                                Ok(permit) = semaphore.clone().acquire_owned() => {
                                    task_counter += 1;
                                    if task_counter > num_queries {
                                        drop(permit);
                                        break;
                                    }
                                    let next_task = sc.generate_transaction(workload_id).await;
                                    let metrics = sc.metrics.clone();
                                    let value_per_key = sc.value_per_key.clone();
                                    let join_handle = rhc.spawn(async move {
                                        let start_time = Instant::now();
                                        let result = next_task.execute(value_per_key).await;
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
                                _ = sigusr1_stream.recv() => {
                                    println!("Received SIGUSR1, initiating shutdown");
                                    return;
                                }

                                else => break,
                            }
                        }

                        while let Some(res) = join_set.join_next().await {
                            if let Err(e) = res {
                                error!("Task panicked: {:?}", e);
                            }
                        }
                    }
                }
            });
        }

        // Wait for all spawned tasks to complete
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
        for (key, value) in response.stats {
            stats_map.insert(key, value as f64);
        }
        info!("Resolver stats: {:?}", stats_map);

        // Get range server stats
        let mut range_server_stats = HashMap::new();
        let mut client_clone = self.range_server_client.clone();
        let response = client_clone
            .get_statistics(GetStatisticsRequest {})
            .await
            .unwrap();
        let response = response.into_inner();
        for range_statistic in &response.range_statistics {
            range_server_stats.insert(
                range_statistic.range_id.to_string(),
                HashMap::from([
                    (
                        "num_waiters".to_string(),
                        range_statistic
                            .num_waiters
                            .iter()
                            .map(|t| t.to_string())
                            .collect::<Vec<String>>(),
                    ),
                    (
                        "num_pending_commits".to_string(),
                        range_statistic
                            .num_pending_commits
                            .iter()
                            .map(|t| t.to_string())
                            .collect::<Vec<String>>(),
                    ),
                    // ("request_timestamps".to_string(), range_statistic.request_timestamps.clone()),
                    // ("predictions".to_string(), range_statistic.predictions.clone()),
                    // ("avg_delta_between_requests".to_string(), range_statistic.avg_delta_between_requests.clone()),
                    // ("avg_entropies".to_string(), range_statistic.avg_entropies.clone()),
                ]),
            );
        }
        // info!("Range server stats: {:?}", range_server_stats);
        Metrics {
            total_duration,
            total_transactions,
            avg_latency,
            p50_latency,
            p95_latency,
            p99_latency,
            throughput,
            resolver_stats: stats_map,
            range_server_stats,
        }
    }
}
