use crate::config::CephContext;
use crate::error::{Error, Result};
use crate::messenger::{Connection, Message, MessageDispatcher, MessageHandler, ProtocolState};
use crate::types::{ConnectionState, EntityAddr, EntityName, FeatureSet};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

pub struct Messenger {
    local_name: EntityName,
    local_addr: EntityAddr,
    protocol_state: Arc<RwLock<ProtocolState>>,
    connections: Arc<RwLock<HashMap<EntityName, Arc<Connection>>>>,
    dispatcher: Arc<MessageDispatcher>,
    listener: Option<Arc<Mutex<TcpListener>>>,
    shutdown_tx: Option<mpsc::UnboundedSender<()>>,
    context: Option<Arc<CephContext>>,
}

impl Messenger {
    pub fn new(local_name: EntityName, bind_addr: SocketAddr) -> Self {
        let local_addr = EntityAddr::new_with_random_nonce(bind_addr);
        let protocol_state = Arc::new(RwLock::new(ProtocolState::new()));
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let dispatcher = Arc::new(MessageDispatcher::new());

        Self {
            local_name,
            local_addr,
            protocol_state,
            connections,
            dispatcher,
            listener: None,
            shutdown_tx: None,
            context: None,
        }
    }

    pub fn with_context(mut self, context: Arc<CephContext>) -> Self {
        self.context = Some(context);
        self
    }

    pub async fn start(&mut self) -> Result<()> {
        info!(
            "Starting messenger {} on {}",
            self.local_name, self.local_addr
        );

        // Start listening for incoming connections
        let listener = TcpListener::bind(self.local_addr.addr)
            .await
            .map_err(Error::from)?;

        info!("Listening on {}", self.local_addr.addr);

        let listener = Arc::new(Mutex::new(listener));
        self.listener = Some(listener.clone());

        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
        self.shutdown_tx = Some(shutdown_tx);

        // Spawn accept loop
        let connections = self.connections.clone();
        let local_name = self.local_name;
        let local_addr = self.local_addr.clone();
        let protocol_state = self.protocol_state.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = Self::accept_connection(
                        listener.clone(),
                        local_name,
                        local_addr.clone(),
                        protocol_state.clone()
                    ) => {
                        match result {
                            Ok(conn) => {
                                let peer_name = conn.info().await.peer_name;
                                if let Some(peer_name) = peer_name {
                                    let mut conns = connections.write().await;
                                    conns.insert(peer_name, Arc::new(conn));
                                    info!("Accepted connection from {}", peer_name);
                                }
                            }
                            Err(e) => {
                                error!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown signal received, stopping accept loop");
                        break;
                    }
                }
            }
        });

        // Start connection maintenance task
        self.start_maintenance_task().await;

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down messenger");

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Close all connections
        let mut connections = self.connections.write().await;
        for (name, conn) in connections.drain() {
            info!("Closing connection to {}", name);
            if let Err(e) = conn.close().await {
                warn!("Error closing connection to {}: {}", name, e);
            }
        }

        self.listener = None;
        info!("Messenger shut down complete");
        Ok(())
    }

    pub async fn connect_to(
        &self,
        peer_addr: EntityAddr,
        peer_name: Option<EntityName>,
    ) -> Result<()> {
        info!(
            "Connecting to {} ({})",
            peer_addr,
            peer_name
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );

        let features = {
            let state = self.protocol_state.read().await;
            state.features().supported
        };

        let conn = Connection::connect(
            peer_addr,
            self.local_addr.clone(),
            self.local_name,
            features,
            self.context.clone(),
        )
        .await?;

        let peer_info = conn.info().await;
        let effective_peer_name = peer_name
            .or(peer_info.peer_name)
            .ok_or_else(|| Error::Protocol("No peer name available".into()))?;

        // Store connection
        {
            let mut connections = self.connections.write().await;
            connections.insert(effective_peer_name, Arc::new(conn));
        }

        info!("Successfully connected to {}", effective_peer_name);
        Ok(())
    }

    pub async fn send_message(&self, dest: EntityName, msg: Message) -> Result<()> {
        let connections = self.connections.read().await;

        if let Some(conn) = connections.get(&dest) {
            conn.send_message(msg).await
        } else {
            Err(Error::Connection(format!("No connection to {}", dest)))
        }
    }

    pub async fn register_handler(&mut self, msg_type: u16, handler: Box<dyn MessageHandler>) {
        let dispatcher = Arc::get_mut(&mut self.dispatcher)
            .expect("Failed to get mutable reference to dispatcher");
        dispatcher.register_handler(msg_type, handler);
    }

    pub async fn get_connections(&self) -> Vec<EntityName> {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect()
    }

    pub async fn get_connection_info(
        &self,
        peer_name: EntityName,
    ) -> Option<crate::types::ConnectionInfo> {
        let connections = self.connections.read().await;
        if let Some(conn) = connections.get(&peer_name) {
            Some(conn.info().await)
        } else {
            None
        }
    }

    pub fn local_name(&self) -> EntityName {
        self.local_name
    }

    pub fn local_addr(&self) -> EntityAddr {
        self.local_addr.clone()
    }

    async fn accept_connection(
        listener: Arc<Mutex<TcpListener>>,
        local_name: EntityName,
        local_addr: EntityAddr,
        protocol_state: Arc<RwLock<ProtocolState>>,
    ) -> Result<Connection> {
        let (stream, peer_addr) = {
            let listener = listener.lock().await;
            listener.accept().await.map_err(Error::from)?
        };

        debug!("Accepting connection from {}", peer_addr);

        let features = {
            let state = protocol_state.read().await;
            state.features().supported
        };

        Connection::accept(stream, local_addr, local_name, features).await
    }

    async fn start_maintenance_task(&self) {
        let connections = self.connections.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                // Clean up closed connections
                let mut to_remove = Vec::new();
                {
                    let conns = connections.read().await;
                    for (name, conn) in conns.iter() {
                        if conn.state().await == ConnectionState::Closed {
                            to_remove.push(*name);
                        }
                    }
                }

                if !to_remove.is_empty() {
                    let mut conns = connections.write().await;
                    for name in to_remove {
                        conns.remove(&name);
                        debug!("Removed closed connection to {}", name);
                    }
                }
            }
        });
    }
}

impl Drop for Messenger {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessengerConfig {
    pub bind_addr: SocketAddr,
    pub features: FeatureSet,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub keepalive_interval: Duration,
}

impl MessengerConfig {
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            features: FeatureSet::MSGR2,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(30),
        }
    }

    pub fn with_features(mut self, features: FeatureSet) -> Self {
        self.features = features;
        self
    }

    pub fn with_timeouts(mut self, connect: Duration, read: Duration, write: Duration) -> Self {
        self.connect_timeout = connect;
        self.read_timeout = read;
        self.write_timeout = write;
        self
    }

    pub fn with_keepalive_interval(mut self, interval: Duration) -> Self {
        self.keepalive_interval = interval;
        self
    }
}

impl Default for MessengerConfig {
    fn default() -> Self {
        Self::new(([0, 0, 0, 0], 0).into())
    }
}
