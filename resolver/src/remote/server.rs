use common::config::Config;
use proto::resolver::resolver_server::{
    Resolver as ProtoResolver, ResolverServer as ProtoResolverServer,
};
use proto::resolver::{
    CommitRequest, CommitResponse, GetStatsRequest, GetStatsResponse,
    RegisterCommittedTransactionsRequest, RegisterCommittedTransactionsResponse,
};
use std::{collections::HashMap, net::ToSocketAddrs, str::FromStr, sync::Arc};
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
        let mut response_stats = HashMap::new();
        for (group_size, count) in stats.committed_group_sizes {
            response_stats.insert(group_size, count as i32);
        }
        Ok(Response::new(GetStatsResponse {
            stats: response_stats,
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
    }
}
