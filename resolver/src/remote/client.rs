use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    participant_range_info::ParticipantRangeInfo,
    resolver_client::ResolverClient as ResolverClientTrait,
};
use async_trait::async_trait;
use common::config::Config;
use coordinator_rangeclient::error::Error;
use proto::resolver::resolver_client::ResolverClient as ProtoResolverClient;
use proto::resolver::{
    CommitRequest, ParticipantRangeInfo as ProtoParticipantRangeInfo,
    RegisterCommittedTransactionsRequest,
};

#[derive(Clone)]
pub struct ResolverClient {
    client: ProtoResolverClient<tonic::transport::Channel>,
}

impl ResolverClient {
    pub async fn new(config: Config) -> Arc<Self> {
        Arc::new(ResolverClient {
            client: ProtoResolverClient::connect(format!(
                "http://{}",
                config.resolver.proto_server_addr
            ))
            .await
            .unwrap(),
        })
    }
}

#[async_trait]
impl ResolverClientTrait for ResolverClient {
    async fn commit(
        &self,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges: Vec<ParticipantRangeInfo>,
    ) -> Result<(), Error> {
        let request = CommitRequest {
            transaction_id: transaction_id.to_string(),
            dependencies: dependencies.into_iter().map(|id| id.to_string()).collect(),
            participant_ranges: participant_ranges
                .iter()
                .map(|info| ProtoParticipantRangeInfo::from(info.clone()))
                .collect(),
        };
        let mut client = self.client.clone();
        let response = client.commit(request).await.unwrap();
        assert!(response.into_inner().status == "Commit ok".to_string());
        Ok(())
    }

    async fn register_committed_transactions(
        &self,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        let request = RegisterCommittedTransactionsRequest {
            transaction_ids: transaction_ids.iter().map(|id| id.to_string()).collect(),
        };
        let mut client = self.client.clone();
        let response = client
            .register_committed_transactions(request)
            .await
            .unwrap();
        assert!(response.into_inner().status == "Register committed transactions ok".to_string());
        Ok(())
    }
}
