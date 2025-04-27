use coordinator::keyspace::Keyspace;
use frontend::error::Error as FrontendError;
use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::Keyspace as ProtoKeyspace;
use proto::frontend::{CommitRequest, GetRequest, PutRequest, StartTransactionRequest};
use std::collections::HashMap;
use tracing::info;
use uuid::Uuid;

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
    ) -> Result<(), FrontendError> {
        let mut results = HashMap::new();

        // Start a transaction
        let response = client
            .start_transaction(StartTransactionRequest {})
            .await
            .unwrap();
        let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
        info!("Started transaction with ID: {:?}", transaction_id);

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
            info!("Read key: {:?}, value: {:?}", key, response.get_ref().value);
            let value_int = match response.get_ref().value.as_ref() {
                Some(bytes) => String::from_utf8(bytes.clone())
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
                None => 0,
            };
            results.insert(key, value_int);
        }

        // Write the keys
        if let Some(writeset) = &self.writeset {
            for key in writeset {
                let value = results.get(key).unwrap() + 1;
                info!("Writing key: {:?}, value: {:?}", key, value);
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
                info!("Wrote key: {:?}, value: {:?}", key, value);
            }
        }

        // Commit the transaction
        client
            .commit(CommitRequest {
                transaction_id: transaction_id.to_string(),
            })
            .await
            .unwrap();
        info!("Committed transaction");

        Ok(())
    }
}
