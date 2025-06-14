use clap::Parser;
use common::network::fast_network::spawn_tokio_polling_thread;
use common::region::Zone;
use common::{config::Config, region::Region, util::core_affinity::restrict_to_cores};
use core_affinity;
use std::{
    fs::read_to_string,
    net::{ToSocketAddrs, UdpSocket},
    sync::Arc,
};
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;

use common::network::for_testing::udp_fast_network::UdpFastNetwork;
use coordinator_rangeclient::{
    range_assignment_oracle::RangeAssignmentOracle, rangeclient::RangeClient,
};
use proto::universe::universe_client::UniverseClient;
use resolver::{
    core::{group_commit::GroupCommit, resolver::Resolver},
    remote::server::ResolverServer,
};
use tx_state_store::client::Client as TxStateStoreClient;
use std::fs::{create_dir_all, write};

#[derive(Parser, Debug)]
#[command(name = "resolver")]
#[command(about = "Resolver", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,

    #[arg(long, default_value = "test-region")]
    region: String,

    #[arg(long, default_value = "a")]
    zone: String,
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config: Config = serde_json::from_str(&read_to_string(&args.config).unwrap()).unwrap();

    let zone = Zone {
        region: Region {
            cloud: None,
            name: args.region.into(),
        },
        name: args.zone.into(),
    };

    // ------------------------------------- Cores Configuration -------------------------------------
    // Pin process to a specific set of cores.
    let mut background_runtime_cores = config.resolver.background_runtime_core_ids.clone();
    if background_runtime_cores.is_empty() {
        background_runtime_cores = core_affinity::get_core_ids()
            .unwrap()
            .iter()
            .map(|id| id.id as u32)
            .collect();
    }
    restrict_to_cores(&background_runtime_cores);

    // Set CPU limit for the process.
    let limited_cgroup_path = "/sys/fs/cgroup/limited_cpu";
    create_dir_all(limited_cgroup_path).unwrap();

    let period = 100_000u32;
    let quota = (config.resolver.cpu_percentage * period as f32).round() as u32;
    let cpu_max_value = format!("{} {}", quota, period);
    info!(
        "Setting CPU limit to {}%",
        config.resolver.cpu_percentage * 100.0
    );
    write(format!("{}/cpu.max", limited_cgroup_path), cpu_max_value).unwrap(); // 10% CPU
    write(
        format!("{}/cgroup.procs", limited_cgroup_path),
        std::process::id().to_string(),
    );
    // ------------------------------------- / Cores Configuration -------------------------------------

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let fast_network_addr = config
        .resolver
        .fast_network_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(fast_network_addr).unwrap(),
    ));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        spawn_tokio_polling_thread(
            "fast-network-poller-resolver",
            fast_network_clone,
            config.resolver.fast_network_polling_core_id as usize,
        )
        .await;
    });

    let cancellation_token = CancellationToken::new();

    let bg_runtime = Builder::new_multi_thread()
        .worker_threads(background_runtime_cores.len())
        .enable_all()
        .build()
        .unwrap();

    let bg_runtime_clone = bg_runtime.handle().clone();
    let runtime_clone = runtime.handle().clone();
    let cancellation_token_clone = cancellation_token.clone();
    runtime.spawn(async move {
        // Initialize the Resolver
        let client =
            UniverseClient::connect(format!("http://{}", config.universe.proto_server_addr))
                .await
                .unwrap();

        let range_client = Arc::new(RangeClient::new(
            Arc::new(RangeAssignmentOracle::new(client)),
            fast_network,
            runtime_clone,
            cancellation_token_clone,
        ));
        let tx_state_store = Arc::new(TxStateStoreClient::new(config.clone(), zone.region).await);
        let group_commit = GroupCommit::new(range_client, tx_state_store);
        let resolver = Arc::new(Resolver::new(group_commit, bg_runtime_clone.clone()));
        let resolver_server = ResolverServer::new(config, resolver);
        ResolverServer::start(resolver_server, bg_runtime_clone).await;
    });
    info!("Hello Resolver...");
    runtime.block_on(async move { cancellation_token.cancelled().await });
}
