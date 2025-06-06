use common::keyspace::Keyspace;
use frontend::error::Error as FrontendError;
use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::Keyspace as ProtoKeyspace;
use proto::frontend::{CommitRequest, GetRequest, PutRequest, StartTransactionRequest};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

const VERIFICATION: bool = false;

#[derive(Debug)]
pub struct Transaction {
    keyspace: Keyspace,
    readset: Vec<usize>,
    writeset: Option<Vec<usize>>,
}

impl Transaction {
    pub fn new(keyspace: Keyspace, readset: Vec<usize>, writeset: Option<Vec<usize>>) -> Self {
        Self {
            keyspace,
            readset,
            writeset,
        }
    }

    pub async fn execute(
        &self,
        client: &mut FrontendClient<tonic::transport::Channel>,
        value_per_key: Arc<Mutex<HashMap<usize, u64>>>,
    ) -> Result<(), FrontendError> {
        let mut results = HashMap::new();

        // Start a transaction
        let response = client
            .start_transaction(StartTransactionRequest {})
            .await
            .unwrap();
        let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
        // let transaction_id_int = transaction_id.as_u128() as u64;
        // info!("Started transaction with ID: {:?}", transaction_id);
        // info!(
        //     "Transaction id: {:?}, Readset: {:?}, Writeset: {:?}",
        //     transaction_id, self.readset, self.writeset
        // );
        for key in &self.readset {
            let response = client
                .get(GetRequest {
                    transaction_id: transaction_id.to_string(),
                    keyspace: Some(ProtoKeyspace {
                        namespace: self.keyspace.namespace.clone(),
                        name: self.keyspace.name.clone(),
                    }),
                    key: vec![*key as u8],
                })
                .await
                .unwrap();
            let value_int = match response.get_ref().value.as_ref() {
                Some(bytes) => String::from_utf8(bytes.clone())
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
                None => 0,
            };
            if VERIFICATION {
                let mut value_per_key = value_per_key.lock().await;
                // info!("key: {:?}, expected value: {:?}, value_int: {:?}", key, value_per_key.get(key), value_int);
                if let Some(expected_value) = value_per_key.get(key) {
                    if value_int != *expected_value {
                        panic!("Read value mismatch");
                    }
                }
            }
            results.insert(key, value_int);
        }

        // Write the keys
        if let Some(writeset) = &self.writeset {
            // info!(
            //     "Writing keys: {:?} tx id: {:?}",
            //     writeset, transaction_id_int
            // );
            for key in writeset {
                let value = results.get(key).unwrap() + 1;
                let _response = client
                    .put(PutRequest {
                        transaction_id: transaction_id.to_string(),
                        keyspace: Some(ProtoKeyspace {
                            namespace: self.keyspace.namespace.clone(),
                            name: self.keyspace.name.clone(),
                        }),
                        key: vec![*key as u8],
                        value: value.to_string().as_bytes().to_vec(),
                    })
                    .await
                    .unwrap();
                if VERIFICATION {
                    let mut value_per_key = value_per_key.lock().await;
                    // info!("Writing key: {:?}, value: {:?}", key, value);
                    value_per_key.insert(*key, value);
                }
            }
        }

        // Commit the transaction
        client
            .commit(CommitRequest {
                transaction_id: transaction_id.to_string(),
            })
            .await
            .unwrap();
        // info!(
        //     "Committed transaction with keys: {:?} tx id: {:?}",
        //     self.writeset, transaction_id_int
        // );

        Ok(())
    }
}
