use common::{
    config::Config,
    network::{
        fast_network::spawn_tokio_polling_thread, for_testing::udp_fast_network::UdpFastNetwork,
    },
    region::{Region, Zone},
};
use frontend::frontend::Server;
use std::sync::atomic::AtomicU64;
use std::{
    net::{ToSocketAddrs, UdpSocket},
    sync::Arc,
};
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;
use core_affinity;
use frontend::range_assignment_oracle::RangeAssignmentOracle;
use proto::universe::universe_client::UniverseClient;

fn main() {
    tracing_subscriber::fmt::init();

    // TODO(kelly): take the config path as an argument.
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();

    //TODO(kelly): the name, zone etc should be passed in as an argument or environment.
    let zone = Zone {
        region: Region {
            cloud: None,
            name: "test-region".into(),
        },
        name: "a".into(),
    };

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let fast_network_addr = config
        .frontend
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
            "fast-network-poller-frontend",
            fast_network_clone,
            config.frontend.polling_core_id,
        )
        .await;
    });

    let cancellation_token = CancellationToken::new();
    let runtime_handle = runtime.handle().clone();
    let ct_clone = cancellation_token.clone();
    
    let all_cores = core_affinity::get_core_ids().unwrap();
    let allowed_cores = all_cores
        .into_iter()
        .filter(|c| c.id != 38 && c.id != 39)
        .collect::<Vec<_>>();

    let num_cores = allowed_cores.clone().len();

    let core_pool = std::sync::Arc::new(parking_lot::Mutex::new(allowed_cores.into_iter()));

    let bg_runtime = Builder::new_multi_thread()
        .worker_threads(num_cores)
        .on_thread_start({
            let core_pool = core_pool.clone();
            move || {
                if let Some(core) = core_pool.lock().next() {
                    core_affinity::set_for_current(core);
                }
            }
        })
        .enable_all()
        .build()
        .unwrap();
    let bg_runtime_clone = bg_runtime.handle().clone();

    runtime.spawn(async move {
        let proto_server_addr = &config.universe.proto_server_addr;
        let client = UniverseClient::connect(format!("http://{}", proto_server_addr))
            .await
            .unwrap();
        let range_assignment_oracle = Arc::new(RangeAssignmentOracle::new(client));
        let server = Server::new(
            config,
            zone,
            fast_network.clone(),
            range_assignment_oracle,
            runtime_handle,
            bg_runtime_clone,
            ct_clone,
            AtomicU64::new(0),
        )
        .await;

        Server::start(server).await;
    });
    info!("Hello Frontend...");
    runtime.block_on(async move { cancellation_token.cancelled().await });
}
