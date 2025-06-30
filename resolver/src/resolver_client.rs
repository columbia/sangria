use async_trait::async_trait;
use coordinator_rangeclient::error::Error;
use std::collections::HashSet;
use uuid::Uuid;

use crate::{core::group_commit::Stats, participant_range_info::ParticipantRangeInfo};

#[async_trait]
pub trait ResolverClient: Send + Sync + 'static {
    async fn get_stats(&self) -> Stats;
    async fn get_status(&self) -> String;
    async fn get_transaction_info_status(&self) -> String;
    async fn get_resolved_transactions_status(&self) -> String;
    async fn get_waiting_transactions_status(&self) -> String;
    async fn get_group_commit_status(&self) -> String;
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
