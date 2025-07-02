use std::collections::HashMap;

use proto::resolver::resolver_client::ResolverClient as ProtoResolverClient;
use proto::resolver::{
    GetAverageWaitingTransactionsRequest, GetGroupCommitStatusRequest, GetNumWaitingTransactionsRequest,
    GetResolvedTransactionsStatusRequest, GetStatsRequest, GetTransactionInfoStatusRequest,
    GetWaitingTransactionsStatusRequest,
};

pub struct MonitoringClient {
    client: ProtoResolverClient<tonic::transport::Channel>,
}

impl MonitoringClient {
    pub async fn new(addr: String) -> Self {
        let client = ProtoResolverClient::connect(format!("http://{}", addr))
            .await
            .unwrap();
        MonitoringClient { client }
    }

    pub async fn get_stats(&mut self) -> HashMap<String, f64> {
        let request = tonic::Request::new(GetStatsRequest {});
        let response = self.client.get_stats(request).await.unwrap();
        response.into_inner().stats
    }

    // Individual component status functions
    pub async fn get_transaction_info_status(&mut self) -> String {
        let request = tonic::Request::new(GetTransactionInfoStatusRequest {});
        let response = self
            .client
            .get_transaction_info_status(request)
            .await
            .unwrap();
        response.into_inner().status
    }

    pub async fn get_resolved_transactions_status(&mut self) -> String {
        let request = tonic::Request::new(GetResolvedTransactionsStatusRequest {});
        let response = self
            .client
            .get_resolved_transactions_status(request)
            .await
            .unwrap();
        response.into_inner().status
    }

    pub async fn get_waiting_transactions_status(&mut self) -> String {
        let request = tonic::Request::new(GetWaitingTransactionsStatusRequest {});
        let response = self
            .client
            .get_waiting_transactions_status(request)
            .await
            .unwrap();
        response.into_inner().status
    }

    pub async fn get_group_commit_status(&mut self) -> String {
        let request = tonic::Request::new(GetGroupCommitStatusRequest {});
        let response = self.client.get_group_commit_status(request).await.unwrap();
        response.into_inner().status
    }

    pub async fn get_num_waiting_transactions(&mut self) -> usize {
        let request = tonic::Request::new(GetNumWaitingTransactionsRequest {});
        let response = self
            .client
            .get_num_waiting_transactions(request)
            .await
            .unwrap();
        response.into_inner().num_waiting_transactions as usize
    }

    pub async fn get_average_waiting_transactions(&mut self) -> f64 {
        let request = tonic::Request::new(GetAverageWaitingTransactionsRequest {});
        let response = self
            .client
            .get_average_waiting_transactions(request)
            .await
            .unwrap();
        response.into_inner().average_waiting_transactions
    }
}
