use common::config::Config;
use proto::resolver::resolver_server::{
    Resolver as ProtoResolver, ResolverServer as ProtoResolverServer,
};
use proto::resolver::{
    CommitRequest, CommitResponse, GetAverageWaitingTransactionsRequest,
    GetAverageWaitingTransactionsResponse, GetGroupCommitStatusRequest,
    GetGroupCommitStatusResponse, GetNumWaitingTransactionsRequest,
    GetNumWaitingTransactionsResponse, GetResolvedTransactionsStatusRequest,
    GetResolvedTransactionsStatusResponse, GetStatsRequest, GetStatsResponse,
    GetTransactionInfoStatusRequest, GetTransactionInfoStatusResponse,
    GetWaitingTransactionsStatusRequest, GetWaitingTransactionsStatusResponse,
    RegisterCommittedTransactionsRequest, RegisterCommittedTransactionsResponse,
};
use std::{net::ToSocketAddrs, str::FromStr, sync::Arc};
use tonic::{Request, Response, Status as TStatus, transport::Server as TServer};
use tracing::{info, instrument};
use uuid::Uuid;

use crate::{core::resolver::Resolver, participant_range_info::ParticipantRangeInfo};

#[derive(Clone)]
struct ProtoServer {
    resolver_server: Arc<ResolverServer>,
}

#[tonic::async_trait]
impl ProtoResolver for ProtoServer {
    #[instrument(skip(self))]
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let request = request.into_inner();
        let transaction_id = Uuid::from_str(&request.transaction_id).unwrap();
        let dependencies = request
            .dependencies
            .iter()
            .map(|id| Uuid::from_str(id).unwrap())
            .collect();
        let participant_ranges_info = request
            .participant_ranges
            .iter()
            .map(|info| ParticipantRangeInfo::from(info.clone()))
            .collect();

        let resolver_server = self.resolver_server.clone();
        let _ = Resolver::commit(
            resolver_server.resolver.clone(),
            transaction_id,
            dependencies,
            participant_ranges_info,
            request.fake,
        )
        .await;

        Ok(Response::new(CommitResponse {
            status: "Commit ok".to_string(),
        }))
    }

    #[instrument(skip(self))]
    async fn register_committed_transactions(
        &self,
        request: Request<RegisterCommittedTransactionsRequest>,
    ) -> Result<Response<RegisterCommittedTransactionsResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let request = request.into_inner();
        let transaction_ids = request
            .transaction_ids
            .iter()
            .map(|id| Uuid::from_str(id).unwrap())
            .collect();
        let resolver_server = self.resolver_server.clone();
        let _ = Resolver::spawn_register_committed_transactions(
            resolver_server.resolver.clone(),
            transaction_ids,
        );

        Ok(Response::new(RegisterCommittedTransactionsResponse {
            status: "Register committed transactions ok".to_string(),
        }))
    }

    #[instrument(skip(self))]
    async fn get_stats(
        &self,
        request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let resolver_server = self.resolver_server.clone();
        let stats = Resolver::get_stats(resolver_server.resolver.clone()).await;
        Ok(Response::new(GetStatsResponse { stats }))
    }

    #[instrument(skip(self))]
    async fn get_transaction_info_status(
        &self,
        request: Request<GetTransactionInfoStatusRequest>,
    ) -> Result<Response<GetTransactionInfoStatusResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let resolver_server = self.resolver_server.clone();
        let status = resolver_server.resolver.get_transaction_info_status().await;
        Ok(Response::new(GetTransactionInfoStatusResponse { status }))
    }

    #[instrument(skip(self))]
    async fn get_resolved_transactions_status(
        &self,
        request: Request<GetResolvedTransactionsStatusRequest>,
    ) -> Result<Response<GetResolvedTransactionsStatusResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let resolver_server = self.resolver_server.clone();
        let status = resolver_server
            .resolver
            .get_resolved_transactions_status()
            .await;
        Ok(Response::new(GetResolvedTransactionsStatusResponse {
            status,
        }))
    }

    #[instrument(skip(self))]
    async fn get_waiting_transactions_status(
        &self,
        request: Request<GetWaitingTransactionsStatusRequest>,
    ) -> Result<Response<GetWaitingTransactionsStatusResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let resolver_server = self.resolver_server.clone();
        let status = resolver_server
            .resolver
            .get_waiting_transactions_status()
            .await;
        Ok(Response::new(GetWaitingTransactionsStatusResponse {
            status,
        }))
    }

    #[instrument(skip(self))]
    async fn get_group_commit_status(
        &self,
        request: Request<GetGroupCommitStatusRequest>,
    ) -> Result<Response<GetGroupCommitStatusResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let resolver_server = self.resolver_server.clone();
        let status = resolver_server.resolver.get_group_commit_status().await;
        Ok(Response::new(GetGroupCommitStatusResponse { status }))
    }

    #[instrument(skip(self))]
    async fn get_num_waiting_transactions(
        &self,
        request: Request<GetNumWaitingTransactionsRequest>,
    ) -> Result<Response<GetNumWaitingTransactionsResponse>, TStatus> {
        info!("Got a request: {:?}", request);
        let resolver_server = self.resolver_server.clone();
        let num_waiting_transactions = resolver_server
            .resolver
            .get_num_waiting_transactions()
            .await;
        Ok(Response::new(GetNumWaitingTransactionsResponse {
            num_waiting_transactions: num_waiting_transactions as u32,
        }))
    }

    #[instrument(skip(self))]
    async fn get_average_waiting_transactions(
        &self,
        request: Request<GetAverageWaitingTransactionsRequest>,
    ) -> Result<Response<GetAverageWaitingTransactionsResponse>, TStatus> {
        let average_waiting_transactions = self
            .resolver_server
            .resolver
            .get_average_waiting_transactions()
            .await;
        Ok(Response::new(GetAverageWaitingTransactionsResponse {
            average_waiting_transactions,
        }))
    }
}

pub struct ResolverServer {
    config: Config,
    resolver: Arc<Resolver>,
}

impl ResolverServer {
    pub fn new(config: Config, resolver: Arc<Resolver>) -> Arc<Self> {
        Arc::new(ResolverServer { config, resolver })
    }

    pub async fn start(resolver_server: Arc<Self>, bg_runtime: tokio::runtime::Handle) {
        let proto_server = ProtoServer {
            resolver_server: resolver_server.clone(),
        };

        let addr = resolver_server
            .config
            .resolver
            .proto_server_addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        bg_runtime.spawn(async move {
            if let Err(e) = TServer::builder()
                .add_service(ProtoResolverServer::new(proto_server))
                .serve(addr)
                .await
            {
                panic!("Unable to start proto server: {:?}", e);
            }
        });

        // Spawn a task to periodically collect the number of waiting transactions
        let resolver_server_clone = resolver_server.clone();
        bg_runtime.spawn(async move {
            loop {
                resolver_server_clone
                    .resolver
                    .sample_waiting_transactions()
                    .await;
                tokio::time::sleep(resolver_server_clone.config.resolver.stats_sampling_period)
                    .await;
            }
        });
    }
}
