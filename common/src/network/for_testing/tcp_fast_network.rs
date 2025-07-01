use crate::network::fast_network::FastNetwork as Trait;
use std::{collections::HashMap, net::SocketAddr};

use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, RwLock},
};
use tracing::{info, trace};

enum DefaultHandler {
    NotRegistered,
    Registered(mpsc::UnboundedSender<(oneshot::Sender<Bytes>, Bytes)>),
}

pub struct TcpFastNetwork {
    listener: TcpListener,
    connections: RwLock<HashMap<SocketAddr, mpsc::UnboundedSender<Bytes>>>,
    default_handler: RwLock<DefaultHandler>,
    bind_addr: SocketAddr,
}

impl TcpFastNetwork {
    pub async fn new(bind_addr: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        Ok(Self {
            listener,
            connections: RwLock::new(HashMap::new()),
            default_handler: RwLock::new(DefaultHandler::NotRegistered),
            bind_addr,
        })
    }

    /// Start accepting connections and spawning tasks to read
    pub async fn run(self: Arc<Self>) -> std::io::Result<()> {
        loop {
            let (socket, addr) = self.listener.accept().await?;
            info!("Accepted connection from: {:?}", addr);
            let network = self.clone();
            tokio::spawn(async move {
                if let Err(e) = network.handle_connection(socket, addr).await {
                    tracing::error!("Connection error from {}: {}", addr, e);
                }
            });
        }
    }

    async fn handle_connection(
        &self,
        mut socket: TcpStream,
        addr: SocketAddr,
    ) -> std::io::Result<()> {
        let mut buf = vec![0u8; 65535];
        let (p, s) = oneshot::channel();
        let n = socket.read(&mut buf).await?;
        let message = Bytes::copy_from_slice(&buf[..n]);
        let listeners = self.connections.read().await;
        if let Some(listener) = listeners.get(&addr) {
            let _ = listener.send(message.clone());
        } else {
            let dh = self.default_handler.read().await;
            if let DefaultHandler::Registered(tx) = &*dh {
                let _ = tx.send((p, message));
            } else {
                trace!("No handler registered for message from {}", addr);
            }
        }

        let response = s.await.unwrap();
        socket.write_all(&response).await?;
        Ok(())
    }
}

#[async_trait]
impl Trait for TcpFastNetwork {
    async fn send(&self, to: SocketAddr, payload: Bytes) -> Result<Option<Bytes>, std::io::Error> {
        info!("Sending message to: {:?}", to);
        let mut stream = TcpStream::connect(to).await?;
        stream.write_all(&payload).await?;
        stream.flush().await?;
        let mut response_buf = Vec::new();
        stream.read_to_end(&mut response_buf).await?;
        stream.shutdown().await?;
        Ok(Some(Bytes::from(response_buf)))
    }

    async fn listen_default(&self) -> mpsc::UnboundedReceiver<(oneshot::Sender<Bytes>, Bytes)> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut default_handler = self.default_handler.write().await;
        *default_handler = DefaultHandler::Registered(tx);
        rx
    }

    async fn register(&self, from: SocketAddr) -> mpsc::UnboundedReceiver<Bytes> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut connections = self.connections.write().await;
        connections.insert(from, tx);
        rx
    }

    // Poll isn't meaningful with async TCP, just return false for compatibility
    fn poll(&self) -> bool {
        false
    }
}
