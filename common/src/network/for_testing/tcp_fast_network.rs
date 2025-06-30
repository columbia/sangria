use crate::network::fast_network::FastNetwork as Trait;
use std::{collections::HashMap, net::SocketAddr, sync::RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use std::str::FromStr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tracing::{info, trace};

const ADDR_LEN: usize = 15; // Length of "127.0.0.1:50058", adjust as needed

enum DefaultHandler {
    NotRegistered,
    Registered(mpsc::UnboundedSender<(SocketAddr, Bytes)>),
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
    pub async fn run(self: std::sync::Arc<Self>) -> std::io::Result<()> {
        loop {
            let (socket, addr) = self.listener.accept().await?;
            // info!("Accepted connection from: {:?}", addr);
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
        loop {
            let n = socket.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let bytes = Bytes::copy_from_slice(&buf[..n]);
            if bytes.len() < ADDR_LEN {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Message too short",
                ));
            }
            let (message, addr_bytes) = bytes.split_at(bytes.len() - ADDR_LEN);
            let addr_str = std::str::from_utf8(addr_bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let addr = SocketAddr::from_str(&addr_str).unwrap();
            info!("Received message from: {:?}", addr);
            let message = Bytes::from(message.to_vec());

            let listeners = self.connections.read().unwrap();
            if let Some(listener) = listeners.get(&addr) {
                let _ = listener.send(message.clone());
            } else {
                let dh = self.default_handler.read().unwrap();
                if let DefaultHandler::Registered(tx) = &*dh {
                    let _ = tx.send((addr, message));
                } else {
                    trace!("No handler registered for message from {}", addr);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Trait for TcpFastNetwork {
    async fn send(&self, to: SocketAddr, payload: Bytes) -> Result<(), std::io::Error> {
        // info!("Sending to: {:?}", to);
        let mut p = payload.to_vec();
        p.extend_from_slice(self.bind_addr.to_string().as_bytes());
        let ext_payload = Bytes::from(p);
        let mut stream = TcpStream::connect(to).await?;
        stream.write_all(&ext_payload).await?;
        Ok(())
    }

    fn listen_default(&self) -> mpsc::UnboundedReceiver<(SocketAddr, Bytes)> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut default_handler = self.default_handler.write().unwrap();
        *default_handler = DefaultHandler::Registered(tx);
        rx
    }

    fn register(&self, from: SocketAddr) -> mpsc::UnboundedReceiver<Bytes> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut connections = self.connections.write().unwrap();
        connections.insert(from, tx);
        rx
    }

    // Poll isn't meaningful with async TCP, just return false for compatibility
    fn poll(&self) -> bool {
        false
    }
}
