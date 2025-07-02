use clap::Parser;
use monitoring::MonitoringClient;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Which status to retrieve
    #[arg(short, long, value_enum, default_value_t = StatusType::All)]
    status: StatusType,
}

#[derive(clap::ValueEnum, Clone)]
enum StatusType {
    All,
    Resolver,
    TransactionInfo,
    ResolvedTransactions,
    WaitingTransactions,
    GroupCommit,
    NumWaitingTransactions,
    AverageWaitingTransactions,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut client = MonitoringClient::new("127.0.0.1:50059".to_string()).await;

    match args.status {
        StatusType::All => {
            // Get resolver status
            let status = client.get_stats().await;
            println!("Resolver Statistics:\n{:?}", status);

            // Get transaction info status
            let transaction_info_status = client.get_transaction_info_status().await;
            println!("Transaction Info Status:\n{}", transaction_info_status);

            // Get resolved transactions status
            let resolved_transactions_status = client.get_resolved_transactions_status().await;
            println!(
                "Resolved Transactions Status:\n{}",
                resolved_transactions_status
            );

            // Get waiting transactions status
            let waiting_transactions_status = client.get_waiting_transactions_status().await;
            println!(
                "Waiting Transactions Status:\n{}",
                waiting_transactions_status
            );

            // Get group commit status
            let group_commit_status = client.get_group_commit_status().await;
            println!("Group Commit Status:\n{}", group_commit_status);
        }
        StatusType::Resolver => {
            let status = client.get_stats().await;
            println!("Resolver Statistics:\n{:?}", status);
        }
        StatusType::TransactionInfo => {
            let transaction_info_status = client.get_transaction_info_status().await;
            println!("Transaction Info Status:\n{}", transaction_info_status);
        }
        StatusType::ResolvedTransactions => {
            let resolved_transactions_status = client.get_resolved_transactions_status().await;
            println!(
                "Resolved Transactions Status:\n{}",
                resolved_transactions_status
            );
        }
        StatusType::WaitingTransactions => {
            let waiting_transactions_status = client.get_waiting_transactions_status().await;
            println!(
                "Waiting Transactions Status:\n{}",
                waiting_transactions_status
            );
        }
        StatusType::GroupCommit => {
            let group_commit_status = client.get_group_commit_status().await;
            println!("Group Commit Status:\n{}", group_commit_status);
        }

        StatusType::NumWaitingTransactions => {
            let num_waiting_transactions = client.get_num_waiting_transactions().await;
            println!(
                "Number of waiting transactions: {}",
                num_waiting_transactions
            );
        }
        StatusType::AverageWaitingTransactions => {
            let average_waiting_transactions = client.get_average_waiting_transactions().await;
            println!(
                "Average waiting transactions: {}",
                average_waiting_transactions
            );
        }
    }

    Ok(())
}
