use clap::Parser;
use common::config::Config;
use common::util::core_affinity::restrict_to_cores;
use core_affinity;
use proto::frontend::frontend_client::FrontendClient;
use proto::rangeserver::range_server_client::RangeServerClient;
use proto::resolver::resolver_client::ResolverClient;
use serde_json;
use std::fs;
use std::sync::Arc;
use tokio::runtime::{Builder, Handle};
use tokio::time::{sleep, Duration};
use tracing::info;
use workload_generator::{
    workload_config::WorkloadConfig,
    workload_generator::{Metrics, WorkloadGenerator},
};

#[derive(Parser, Debug)]
#[command(name = "workload-generator")]
#[command(about = "Generates workloads based on config files", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,

    #[arg(
        long,
        default_value = "workload-generator/configs/workload-config.json"
    )]
    workload_config: String,

    #[arg(long, default_value = "false")]
    create_keyspace: bool,
}

async fn run_workload(
    runtime_handle: Handle,
    config: Config,
    workload_config: Arc<WorkloadConfig>,
    create_keyspace: bool,
) -> Metrics {
    let frontend_addr = config.frontend.proto_server_addr.to_string();
    let client = FrontendClient::connect(format!("http://{}", frontend_addr))
        .await
        .unwrap();

    let resolver_addr = config.resolver.proto_server_addr.to_string();
    let resolver_client = ResolverClient::connect(format!("http://{}", resolver_addr))
        .await
        .unwrap();

    let range_server_addr = config.range_server.proto_server_addr.to_string();
    let range_server_client = RangeServerClient::connect(format!("http://{}", range_server_addr))
        .await
        .unwrap();

    let workload_generator = WorkloadGenerator::new(
        workload_config.clone(),
        client,
        resolver_client,
        range_server_client,
    )
    .await;
    if create_keyspace {
        workload_generator
            .create_keyspace(
                workload_config.namespace.clone(),
                workload_config.name.clone(),
            )
            .await;
        sleep(Duration::from_millis(2000)).await;
        // If Mixed workload: Create second keyspace
        if workload_config.max_concurrency.contains(";") {
            workload_generator
                .create_keyspace(
                    workload_config.namespace.clone(),
                    format!("{}_1", workload_config.name.clone()),
                )
                .await;
            sleep(Duration::from_millis(2000)).await;
        }
    }

    let runtime_handle_clone = runtime_handle.clone();
    let workload_handle =
        runtime_handle.spawn(async move { workload_generator.run(runtime_handle_clone).await });
    let metrics = workload_handle.await.unwrap();
    info!("Workload generator finished");
    metrics
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = serde_json::from_str(&fs::read_to_string(&args.config).unwrap()).unwrap();
    let workload_config: Arc<WorkloadConfig> = Arc::new(
        serde_json::from_str(&fs::read_to_string(&args.workload_config).unwrap()).unwrap(),
    );

    let mut background_runtime_cores = workload_config.background_runtime_core_ids.clone();
    if background_runtime_cores.is_empty() {
        background_runtime_cores = core_affinity::get_core_ids()
            .unwrap()
            .iter()
            .map(|id| id.id as u32)
            .collect();
    }
    restrict_to_cores(&background_runtime_cores);
    let runtime = Builder::new_multi_thread()
        .worker_threads(background_runtime_cores.len())
        .enable_all()
        .build()
        .unwrap();
    let runtime_handle = runtime.handle().clone();
    let metrics = runtime.block_on(async move {
        run_workload(
            runtime_handle,
            config,
            workload_config,
            args.create_keyspace,
        )
        .await
    });

    // Return metrics in a format that the Python script can parse
    println!(
        "METRICS_START\n\
        Throughput: {:.2} transactions/second\n\
        Average Latency: {:?}\n\
        P50 Latency: {:?}\n\
        P95 Latency: {:?}\n\
        P99 Latency: {:?}\n\
        Total Duration: {:?}\n\
        Total Transactions: {}\n\
        Resolver stats: {:?}\n\
        Range server stats: {:?}\n\
        METRICS_END",
        metrics.throughput,
        metrics.avg_latency,
        metrics.p50_latency,
        metrics.p95_latency,
        metrics.p99_latency,
        metrics.total_duration,
        metrics.total_transactions,
        metrics.resolver_stats,
        metrics.range_server_stats,
    )
}
