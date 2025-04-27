use common::config::Config;
use proto::frontend::frontend_client::FrontendClient;
use std::sync::Arc;
use tokio::runtime::{Builder, Handle};
use tracing::info;
use workload_generator::{workload_config::WorkloadConfig, workload_generator::WorkloadGenerator};

async fn run_workload(runtime_handle: Handle) {
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();
    let workload_config: WorkloadConfig = serde_json::from_str(
        &std::fs::read_to_string("workload-generator/configs/config.json").unwrap(),
    )
    .unwrap();
    info!("Workload config: {:?}", workload_config);
    let frontend_addr = config.frontend.proto_server_addr.to_string();
    let mut client = FrontendClient::connect(format!("http://{}", frontend_addr))
        .await
        .unwrap();

    let workload_generator = Arc::new(WorkloadGenerator::new(workload_config, client));
    // generator.create_keyspace(&mut client).await;

    let workload_generator_clone = workload_generator.clone();
    let runtime_handle_clone = runtime_handle.clone();
    let workload_handle = runtime_handle.spawn(async move {
        workload_generator_clone.run(runtime_handle_clone).await;
    });
    workload_handle.await.unwrap();
    info!("Workload generator finished");
}

fn main() {
    tracing_subscriber::fmt::init();

    let num_threads = num_cpus::get();
    let runtime = Builder::new_multi_thread()
        .worker_threads(num_threads)
        .enable_all()
        .build()
        .unwrap();
    let runtime_handle = runtime.handle().clone();
    runtime.block_on(async move { run_workload(runtime_handle).await });
}
