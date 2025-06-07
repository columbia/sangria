use async_trait::async_trait;
use coordinator_rangeclient::error::Error;
use std::collections::HashSet;
use uuid::Uuid;

use crate::participant_range_info::ParticipantRangeInfo;

#[async_trait]
pub trait Resolver: Send + Sync + 'static {
    async fn commit(
        &self,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges: Vec<ParticipantRangeInfo>,
    ) -> Result<(), Error>;
    async fn register_committed_transactions(
        &self,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error>;
}
