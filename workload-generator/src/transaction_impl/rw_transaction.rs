use crate::transaction::Transaction;
use async_trait::async_trait;
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
pub struct RwTransaction {
    client: FrontendClient<tonic::transport::Channel>,
    keyspace: Keyspace,
    readset: Vec<usize>,
    writeset: Option<Vec<usize>>,
}

impl RwTransaction {
    pub fn new(
        client: FrontendClient<tonic::transport::Channel>,
        keyspace: Keyspace,
        readset: Vec<usize>,
        writeset: Option<Vec<usize>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            keyspace,
            readset,
            writeset,
        })
    }
}

#[async_trait]
impl Transaction for RwTransaction {
    async fn execute(
        &self,
        value_per_key: Arc<Mutex<HashMap<usize, u64>>>,
    ) -> Result<(), FrontendError> {
        let mut results = HashMap::new();

        // Start a transaction
        let mut client_clone = self.client.clone();
        let response = client_clone
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
            let response = client_clone
                .get(GetRequest {
                    transaction_id: transaction_id.to_string(),
                    keyspace: Some(ProtoKeyspace {
                        namespace: self.keyspace.namespace.clone(),
                        name: self.keyspace.name.clone(),
                    }),
                    key: key.to_be_bytes().to_vec(),
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
                let _response = client_clone
                    .put(PutRequest {
                        transaction_id: transaction_id.to_string(),
                        keyspace: Some(ProtoKeyspace {
                            namespace: self.keyspace.namespace.clone(),
                            name: self.keyspace.name.clone(),
                        }),
                        key: key.to_be_bytes().to_vec(),
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
        let mut client_clone = self.client.clone();
        client_clone
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
