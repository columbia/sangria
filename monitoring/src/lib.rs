use proto::resolver::resolver_client::ResolverClient as ProtoResolverClient;
use proto::resolver::{
    GetGroupCommitStatusRequest, GetResolvedTransactionsStatusRequest, GetStatusRequest,
    GetTransactionInfoStatusRequest, GetWaitingTransactionsStatusRequest,
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

    pub async fn get_resolver_status(&mut self) -> String {
        let request = tonic::Request::new(GetStatusRequest {});
        let response = self.client.get_status(request).await.unwrap();
        response.into_inner().status
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
}
