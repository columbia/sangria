use std::{collections::HashSet, sync::Arc};
use uuid::Uuid;

use crate::{
    group_commit::GroupCommit, participant_range_info::ParticipantRangeInfo, resolver::Resolver,
    resolver_trait::Resolver as ResolverTrait,
};
use async_trait::async_trait;
use coordinator_rangeclient::error::Error;

pub struct ResolverLocal {
    resolver: Arc<Resolver>,
}

impl ResolverLocal {
    pub fn new(group_commit: GroupCommit, bg_runtime: tokio::runtime::Handle) -> Self {
        ResolverLocal {
            resolver: Arc::new(Resolver::new(group_commit, bg_runtime)),
        }
    }
}

#[async_trait]
impl ResolverTrait for ResolverLocal {
    async fn commit(
        &self,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges: Vec<ParticipantRangeInfo>,
    ) -> Result<(), Error> {
        Resolver::commit(
            self.resolver.clone(),
            transaction_id,
            dependencies,
            participant_ranges,
        )
        .await
    }

    async fn register_committed_transactions(
        &self,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        Resolver::spawn_register_committed_transactions(self.resolver.clone(), transaction_ids)
    }
}
