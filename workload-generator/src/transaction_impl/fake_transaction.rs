use common::{full_range_id::FullRangeId, keyspace::Keyspace, keyspace_id::KeyspaceId};
use frontend::error::Error as FrontendError;
use proto::{
    resolver::resolver_client::ResolverClient, resolver::CommitRequest,
    resolver::ParticipantRangeInfo as ProtoParticipantRangeInfo,
};
use resolver::participant_range_info::ParticipantRangeInfo;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};
use tracing::info;
use uuid::Uuid;

use crate::transaction::Transaction;
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug)]
pub struct FakeTransaction {
    id: Uuid,
    client: ResolverClient<tonic::transport::Channel>,
    keyspace: Keyspace,
    writeset: Vec<usize>,
    dependencies: HashSet<Uuid>,
}

impl FakeTransaction {
    pub async fn new(
        client: ResolverClient<tonic::transport::Channel>,
        keyspace: Keyspace,
        writeset: Vec<usize>,
        pending_commit_table: Arc<RwLock<HashMap<usize, Uuid>>>,
    ) -> Arc<Self> {
        let id = Uuid::new_v4();
        let mut dependencies = HashSet::new();
        let mut pending_commit_table = pending_commit_table.write().await;
        for key in &writeset {
            if let Some(dependency) = pending_commit_table.get(&key) {
                dependencies.insert(*dependency);
            }
            pending_commit_table.insert(*key, id);
        }
        Arc::new(Self {
            id,
            client,
            keyspace,
            writeset,
            dependencies,
        })
    }
}

#[async_trait]
impl Transaction for FakeTransaction {
    async fn execute(
        &self,
        value_per_key: Arc<Mutex<HashMap<usize, u64>>>,
    ) -> Result<(), FrontendError> {
        let mut participants_info = Vec::new();
        for key in &self.writeset {
            let range_id = FullRangeId {
                keyspace_id: KeyspaceId::new(Uuid::from_u128(0)),
                range_id: Uuid::from_u128(*key as u128),
            };
            participants_info.push(ParticipantRangeInfo::new(range_id, true));
        }

        let mut client = self.client.clone();
        client
            .commit(CommitRequest {
                transaction_id: self.id.to_string(),
                dependencies: self.dependencies.iter().map(|d| d.to_string()).collect(),
                participant_ranges: participants_info
                    .iter()
                    .map(|info| ProtoParticipantRangeInfo::from(info.clone()))
                    .collect(),
                fake: true,
            })
            .await
            .unwrap();

        Ok(())
    }
}
