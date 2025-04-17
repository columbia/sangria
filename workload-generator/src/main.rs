use common::config::Config;
use common::region::{Region, Zone};
use std::process::exit;
use coordinator::keyspace::Keyspace;
use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::{
    CommitRequest, GetRequest, Keyspace as ProtoKeyspace, PutRequest, StartTransactionRequest,
};
use proto::universe::{CreateKeyspaceRequest, KeyRangeRequest, Zone as ProtoZone};

use workload_generator::workload_config::WorkloadConfig;

use rand::distributions::WeightedIndex;
use rand::prelude::*;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[derive(Debug)]
pub struct Transaction {
    keyspace: Keyspace,
    read_keys: Vec<usize>,
    write_keys: Option<Vec<usize>>,
}

pub struct WorkloadGenerator {
    keyspace_size: u64,
    zipf_exponent: f64,
    namespace: String,
    name: String,
    num_queries: u64,
}

impl WorkloadGenerator {
    pub fn new(
        keyspace_size: u64,
        zipf_exponent: f64,
        namespace: String,
        name: String,
        num_queries: u64,
    ) -> Self {
        Self {
            keyspace_size,
            zipf_exponent,
            namespace,
            name,
            num_queries,
        }
    }

    fn zipf_sample_without_replacement(&self, sample_count: usize) -> Vec<usize> {
        // Step 1: Build weights using Zipf formula: weight ∝ 1 / rank^s
        let mut weights: Vec<f64> = (1..=self.keyspace_size)
            .map(|rank| 1.0 / (rank as f64).powf(self.zipf_exponent))
            .collect();

        // Step 2: Normalize weights
        let total_weight: f64 = weights.iter().sum();
        for w in weights.iter_mut() {
            *w /= total_weight;
        }

        // Step 3: Sample without replacement
        let mut chosen_keys = Vec::with_capacity(sample_count);
        let mut available_keys: Vec<usize> = (0..self.keyspace_size as usize).collect();

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
        Transaction {
            keyspace: Keyspace {
                namespace: self.namespace.clone(),
                name: self.name.clone(),
            },
            read_keys: keys.clone(),
            write_keys: if thread_rng().gen_bool(0.5) {
                Some(keys)
            } else {
                None
            },
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();

    let workload_config: WorkloadConfig = serde_json::from_str(
        &std::fs::read_to_string("workload-generator/configs/config.json").unwrap(),
    )
    .unwrap();
    info!("Workload config: {:?}", workload_config);

    let generator = Arc::new(WorkloadGenerator::new(
        workload_config.num_keys,
        workload_config.zipf_exponent,
        workload_config.namespace.clone(),
        workload_config.name.clone(),
        workload_config.num_queries,
    ));

    let frontend_addr = config.frontend.proto_server_addr.to_string();
    let mut client = FrontendClient::connect(format!("http://{}", frontend_addr))
        .await
        .unwrap();

    let zone = Zone {
        region: Region {
            cloud: None,
            name: "test-region".into(),
        },
        name: "a".into(),
    };
    // let base_key_ranges: Vec<KeyRangeRequest> = (0..generator.keyspace_size)
    //     .map(|i| KeyRangeRequest {
    //         lower_bound_inclusive: vec![i.try_into().unwrap()],
    //         upper_bound_exclusive: vec![(i + 1).try_into().unwrap()],
    //     })
    //     .collect();
    // info!("Base key ranges: {:?}", base_key_ranges);
    // let transaction = generator.generate_transaction();
    // info!("Generated transaction: {:?}", transaction);
    // exit(0);

    // Create the keyspace for this experiment
    let response = client
        .create_keyspace(CreateKeyspaceRequest {
            namespace: generator.namespace.clone(),
            name: generator.name.clone(),
            primary_zone: Some(ProtoZone::from(zone)),
            base_key_ranges: (0..generator.keyspace_size)
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

    let mut handles = vec![];

    for _ in 0..generator.num_queries {
            let mut client_clone = client.clone();
            let generator_clone = generator.clone();
        
            let handle = tokio::spawn(async move {
                    // Start a transaction
                    let response = client_clone
                        .start_transaction(StartTransactionRequest {})
                        .await
                        .unwrap();
                    let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
                    info!("Started transaction with ID: {:?}", transaction_id);
            
                // Generate a Transaction
                let transaction = generator_clone.generate_transaction();
                info!("Generated transaction: {:?}", transaction);

            // Read the keys in parallel
            let read_handles = transaction.read_keys.iter().map(|key| {
                let mut client_clone = client_clone.clone();
                let transaction_id = transaction_id.clone();
                let key = key.clone();
                let keyspace = transaction.keyspace.clone();
                tokio::spawn(async move {
                    let _response = client_clone
                        .get(GetRequest {
                            transaction_id: transaction_id.to_string(),
                            keyspace: Some(ProtoKeyspace {
                                namespace: keyspace.namespace.clone(),
                                name: keyspace.name.clone(),
                            }),
                            key: vec![key as u8],
                        })
                        .await
                        .unwrap();
                    info!("Read key: {:?}", key);
                })
            });
            for handle in read_handles {
                handle.await.unwrap();
            }

            // Write the keys
            if let Some(write_keys) = &transaction.write_keys {
                for key in write_keys {
                    let _response = client_clone
                        .put(PutRequest {
                            transaction_id: transaction_id.to_string(),
                            keyspace: Some(ProtoKeyspace {
                                namespace: transaction.keyspace.namespace.clone(),
                                name: transaction.keyspace.name.clone(),
                            }),
                            key: vec![*key as u8],
                            value: vec![100],
                        })
                        .await
                        .unwrap();
                    info!("Wrote key: {:?}", key);
                }
            }

            // Commit the transaction
            client_clone
                .commit(CommitRequest {
                    transaction_id: transaction_id.to_string(),
                })
                .await
                .unwrap();
            info!("Committed transaction");
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
}
