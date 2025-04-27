use crate::{transaction::Transaction, workload_config::WorkloadConfig};
use common::region::{Region, Zone};
use coordinator::keyspace::Keyspace;
use proto::{
    frontend::frontend_client::FrontendClient,
    universe::{CreateKeyspaceRequest, KeyRangeRequest, Zone as ProtoZone},
};
use rand::{distributions::WeightedIndex, prelude::*};
use std::{cmp::min, sync::Arc};
use tokio::{
    runtime::Handle,
    sync::Semaphore,
    task::JoinSet,
    time::{sleep, Duration},
};
use tracing::{error, info};

pub struct WorkloadGenerator {
    workload_config: WorkloadConfig,
    client: FrontendClient<tonic::transport::Channel>,
}

impl WorkloadGenerator {
    pub fn new(
        workload_config: WorkloadConfig,
        client: FrontendClient<tonic::transport::Channel>,
    ) -> Self {
        Self {
            workload_config,
            client,
        }
    }

    fn zipf_sample_without_replacement(&self, sample_count: usize) -> Vec<usize> {
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
            let idx = dist.sample(&mut thread_rng());
            chosen_keys.push(available_keys[idx]);

            // Remove selected item and its weight
            available_keys.remove(idx);
            weights.remove(idx);
        }

        chosen_keys
    }

    pub fn generate_transaction(&self) -> Transaction {
        //  sample num_keys uniformly from {1, 2}
        let num_keys = thread_rng().gen_range(1..3);
        let keys = self.zipf_sample_without_replacement(num_keys);
        Transaction::new(
            Keyspace {
                namespace: self.workload_config.namespace.clone(),
                name: self.workload_config.name.clone(),
            },
            keys.clone(),
            if thread_rng().gen_bool(1.0) {
                Some(keys)
            } else {
                None
            },
        )
    }

    pub async fn create_keyspace(&self, client: &mut FrontendClient<tonic::transport::Channel>) {
        let zone = Zone {
            region: Region {
                cloud: None,
                name: "test-region".into(),
            },
            name: "a".into(),
        };

        // Create the keyspace for this experiment
        let response = client
            .create_keyspace(CreateKeyspaceRequest {
                namespace: self.workload_config.namespace.clone(),
                name: self.workload_config.name.clone(),
                primary_zone: Some(ProtoZone::from(zone)),
                base_key_ranges: (0..self.workload_config.num_keys)
                    .map(|i| KeyRangeRequest {
                        lower_bound_inclusive: vec![i.try_into().unwrap()],
                        upper_bound_exclusive: vec![(i + 1).try_into().unwrap()],
                    })
                    .collect(),
            })
            .await
            .unwrap();
        let keyspace_id = response.get_ref().keyspace_id.clone();
        info!("Created keyspace with ID: {:?}", keyspace_id);
        sleep(Duration::from_millis(1000));
    }

    pub async fn run(&self, runtime_handle: Handle) {
        info!("Starting workload");
        let max_concurrency = min(
            self.workload_config.max_concurrency,
            self.workload_config.num_queries,
        );
        let semaphore = Arc::new(Semaphore::new(max_concurrency as usize));

        let mut task_counter = 0;
        let mut join_set = JoinSet::new();
        loop {
            tokio::select! {
                Ok(permit) = semaphore.clone().acquire_owned() => {
                    task_counter += 1;
                    if task_counter > self.workload_config.num_queries {
                        drop(permit);
                        break;
                    }
                    let next_task = self.generate_transaction();
                    let mut client_clone = self.client.clone();
                    let join_handle = runtime_handle.spawn(async move {
                        info!("Executing task");
                        let _ = next_task.execute(&mut client_clone).await;
                        drop(permit);
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
        info!("All transactions processed and completed");
    }
}
