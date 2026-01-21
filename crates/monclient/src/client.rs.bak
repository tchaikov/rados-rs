//! Monitor client implementation
//!
//! Main MonClient struct and implementation.

use crate::connection::MonConnection;
use crate::error::{MonClientError, Result};
use crate::messages::*;
use crate::monmap::MonMap;
use crate::subscription::MonSub;
use crate::types::{CommandResult, EntityName};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info};

/// Monitor client configuration
#[derive(Debug, Clone)]
pub struct MonClientConfig {
    /// Entity name (e.g., "client.admin")
    pub entity_name: String,

    /// Initial monitor addresses
    pub mon_addrs: Vec<String>,

    /// Path to keyring file
    pub keyring_path: String,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Command timeout
    pub command_timeout: Duration,

    /// Hunt interval (time between connection attempts)
    pub hunt_interval: Duration,
}

impl Default for MonClientConfig {
    fn default() -> Self {
        Self {
            entity_name: "client.admin".to_string(),
            mon_addrs: Vec::new(),
            keyring_path: "/etc/ceph/ceph.client.admin.keyring".to_string(),
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(60),
            hunt_interval: Duration::from_secs(3),
        }
    }
}

/// Monitor client
pub struct MonClient {
    /// Configuration
    config: MonClientConfig,

    /// Entity name
    entity_name: EntityName,

    /// Client state
    state: Arc<RwLock<MonClientState>>,

    /// Tokio runtime handle
    runtime: tokio::runtime::Handle,

    /// Message receive task handle
    recv_task: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Event broadcaster for map updates
    map_events: broadcast::Sender<MapEvent>,
}

/// Events for map updates
#[derive(Debug, Clone)]
pub enum MapEvent {
    MonMapUpdated { epoch: u32 },
    OsdMapUpdated { epoch: u64 },
    MgrMapUpdated { epoch: u64 },
    MdsMapUpdated { epoch: u64 },
}

struct MonClientState {
    /// Monitor map
    monmap: MonMap,

    /// Active connection
    active_con: Option<Arc<MonConnection>>,

    /// Pending connections (during hunting)
    pending_cons: HashMap<usize, Arc<MonConnection>>,

    /// Monitors we've tried (to avoid retry storms)
    tried: std::collections::HashSet<usize>,

    /// Subscription manager
    subscriptions: MonSub,

    /// Command tracking
    commands: HashMap<u64, CommandTracker>,
    last_command_tid: u64,

    /// Version request tracking
    version_requests: HashMap<u64, VersionTracker>,
    last_version_req_id: u64,

    /// Map version waiters (for async wait_for_map)
    map_waiters: HashMap<String, Vec<MapWaiter>>,

    /// State flags
    authenticated: bool,
    global_id: u64,
    hunting: bool,
    want_monmap: bool,
    initialized: bool,
    stopping: bool,
}

struct MapWaiter {
    what: String,
    version: u64,
    tx: oneshot::Sender<()>,
}

struct CommandTracker {
    tid: u64,
    cmd: Vec<String>,
    result_tx: oneshot::Sender<CommandResult>,
}

struct VersionTracker {
    req_id: u64,
    what: String,
    result_tx: oneshot::Sender<(u64, u64)>,
}

impl MonClient {
    /// Create a new MonClient
    pub async fn new(config: MonClientConfig) -> Result<Self> {
        // Parse entity name
        let entity_name: EntityName = config
            .entity_name
            .parse()
            .map_err(|e| MonClientError::Other(format!("Invalid entity name: {}", e)))?;

        // Build initial monmap - try env var first, then config
        let monmap = if std::env::var("CEPH_MON_ADDR").is_ok() {
            info!("Building monmap from CEPH_MON_ADDR environment variable");
            MonMap::build_from_env()?
        } else if !config.mon_addrs.is_empty() {
            info!("Building monmap from config");
            MonMap::build_initial(&config.mon_addrs)?
        } else {
            return Err(MonClientError::InvalidMonMap(
                "No monitor addresses provided (set CEPH_MON_ADDR or provide mon_addrs)".into(),
            ));
        };

        info!("Initial monmap has {} monitors", monmap.size());

        // Create event broadcaster
        let (map_events, _) = broadcast::channel(100);

        let state = MonClientState {
            monmap,
            active_con: None,
            pending_cons: HashMap::new(),
            tried: std::collections::HashSet::new(),
            subscriptions: MonSub::new(),
            commands: HashMap::new(),
            last_command_tid: 0,
            version_requests: HashMap::new(),
            last_version_req_id: 0,
            map_waiters: HashMap::new(),
            authenticated: false,
            global_id: 0,
            hunting: false,
            want_monmap: true,
            initialized: false,
            stopping: false,
        };

        Ok(Self {
            config,
            entity_name,
            state: Arc::new(RwLock::new(state)),
            runtime: tokio::runtime::Handle::current(),
            recv_task: Arc::new(RwLock::new(None)),
            map_events,
        })
    }

    /// Initialize and connect to monitors
    pub async fn init(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if state.initialized {
            return Err(MonClientError::AlreadyInitialized);
        }

        info!("Initializing MonClient for {}", self.entity_name);

        // Start hunting for monitors
        state.hunting = true;
        state.initialized = true;

        drop(state);

        // Start message receive loop BEFORE connecting
        // This ensures we don't miss any messages the monitor sends immediately after session establishment
        self.start_message_loop();

        // Start hunting process (connects to monitor)
        self.start_hunting().await?;

        // Send initial subscriptions (monmap)
        info!("Subscribing to monmap...");
        self.subscribe("monmap", 0, 0).await?;

        info!("MonClient initialized successfully");
        Ok(())
    }

    /// Shutdown the client
    pub async fn shutdown(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if state.stopping {
            return Ok(());
        }

        info!("Shutting down MonClient");
        state.stopping = true;

        // Close active connection
        if let Some(con) = state.active_con.take() {
            con.close().await?;
        }

        // Close pending connections
        for (_, con) in state.pending_cons.drain() {
            con.close().await?;
        }

        drop(state);

        // Stop receive task
        let mut recv_task = self.recv_task.write().await;
        if let Some(handle) = recv_task.take() {
            handle.abort();
            info!("Aborted message receive task");
        }

        Ok(())
    }

    /// Start hunting for an available monitor
    async fn start_hunting(&self) -> Result<()> {
        info!("Starting monitor hunt");

        let state = self.state.read().await;
        let ranks = state.monmap.get_all_ranks();
        drop(state);

        // Try connecting to monitors
        // TODO: Implement weighted random selection
        // TODO: Try multiple monitors in parallel
        // For now, just try the first one

        if let Some(&rank) = ranks.first() {
            self.connect_to_mon(rank).await?;
        } else {
            return Err(MonClientError::MonitorUnavailable);
        }

        Ok(())
    }

    /// Connect to a specific monitor
    async fn connect_to_mon(&self, rank: usize) -> Result<()> {
        let state = self.state.read().await;
        let mon_info = state
            .monmap
            .get_mon(rank)
            .ok_or(MonClientError::InvalidMonitorRank(rank))?;

        // Get msgr2 address
        let addr = mon_info
            .addrs
            .get_msgr2()
            .ok_or(MonClientError::InvalidMonMap("No msgr2 address".into()))?;

        let socket_addr = addr.addr;
        let addrs = mon_info.addrs.clone();
        drop(state);

        info!("Connecting to mon.{} at {:?}", rank, socket_addr);

        // Create actual msgr2 connection
        let mon_con = Arc::new(MonConnection::connect(socket_addr, rank, addrs).await?);

        info!("✅ MonConnection created, testing if connection is alive...");

        // Test if connection is alive by trying to get a lock
        {
            let conn_arc = mon_con.connection();
            let _conn_test = conn_arc.lock().await;
            info!("✅ Connection lock acquired successfully");
        }

        // Store as active connection
        let mut state = self.state.write().await;
        state.active_con = Some(mon_con);
        state.hunting = false;
        state.authenticated = true; // TODO: Implement proper auth
        drop(state);

        info!("Successfully connected to mon.{}", rank);
        Ok(())
    }

    /// Subscribe to a cluster map
    pub async fn subscribe(&self, what: &str, start: u64, flags: u8) -> Result<()> {
        let mut state = self.state.write().await;

        if !state.initialized {
            return Err(MonClientError::NotInitialized);
        }

        debug!("Subscribing to {} from version {}", what, start);

        if state.subscriptions.want(what, start, flags) {
            // New subscription, send it if connected
            if state.active_con.is_some() {
                drop(state);
                self.send_subscriptions().await?;
            }
        }

        Ok(())
    }

    /// Unsubscribe from a cluster map
    pub async fn unsubscribe(&self, what: &str) -> Result<()> {
        let mut state = self.state.write().await;
        debug!("Unsubscribing from {}", what);
        state.subscriptions.unwant(what);
        Ok(())
    }

    /// Send pending subscriptions
    async fn send_subscriptions(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if !state.subscriptions.have_new() {
            return Ok(());
        }

        let active_con = state
            .active_con
            .as_ref()
            .ok_or(MonClientError::NotConnected)?
            .clone();

        // Build subscription message
        let mut msg = MMonSubscribe::new();
        for (what, item) in state.subscriptions.get_subs() {
            msg.add(what.clone(), *item);
        }

        state.subscriptions.renewed();
        drop(state);

        // Encode and send
        let payload = msg.encode()?;
        let mut message = msgr2::message::Message::new(CEPH_MSG_MON_SUBSCRIBE, payload);

        // Set message version (MMonSubscribe HEAD_VERSION = 3, COMPAT_VERSION = 1)
        message.header.version = 3;
        message.header.compat_version = 1;

        active_con.send_message(message).await?;

        debug!("Sent subscriptions");
        Ok(())
    }

    /// Start background message receive loop
    fn start_message_loop(&self) {
        let state = Arc::clone(&self.state);
        let map_events = self.map_events.clone();

        let handle = tokio::spawn(async move {
            info!("Message receive loop started");
            let mut iteration = 0;
            loop {
                iteration += 1;
                debug!("Message loop iteration {}", iteration);

                // Check if stopping
                {
                    let state_guard = state.read().await;
                    if state_guard.stopping {
                        info!("Stopping flag set, exiting message loop");
                        break;
                    }
                }

                // Get active connection, wait if not available yet
                let active_con = loop {
                    let state_guard = state.read().await;
                    match &state_guard.active_con {
                        Some(con) => {
                            debug!("Got active connection");
                            break Arc::clone(con);
                        }
                        None => {
                            // Check if stopping before waiting
                            if state_guard.stopping {
                                info!("Stopping while waiting for connection");
                                return;
                            }
                            drop(state_guard);
                            // Wait a bit before checking again
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                    }
                };

                // Receive message
                debug!("About to call receive_message()...");
                match active_con.receive_message().await {
                    Ok(msg) => {
                        info!("✓ Received message type: 0x{:04x}", msg.msg_type());
                        if let Err(e) = Self::dispatch_message(&state, &map_events, msg).await {
                            tracing::error!("Error dispatching message: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error receiving message: {}", e);
                        break;
                    }
                }
            }
            info!("Message receive loop terminated");
        });

        // Store task handle
        let recv_task = Arc::clone(&self.recv_task);
        tokio::spawn(async move {
            let mut task_guard = recv_task.write().await;
            *task_guard = Some(handle);
        });

        info!("Started message receive loop");
    }

    /// Dispatch received message to appropriate handler
    async fn dispatch_message(
        state: &Arc<RwLock<MonClientState>>,
        map_events: &broadcast::Sender<MapEvent>,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        match msg.msg_type() {
            msgr2::message::CEPH_MSG_MON_MAP => {
                info!("Received CEPH_MSG_MON_MAP");
                Self::handle_monmap(state, map_events, msg).await?;
            }
            CEPH_MSG_MON_SUBSCRIBE_ACK => {
                info!("Received CEPH_MSG_MON_SUBSCRIBE_ACK");
                Self::handle_subscribe_ack(state, msg).await?;
            }
            CEPH_MSG_MON_GET_VERSION_REPLY => {
                debug!("Received CEPH_MSG_MON_GET_VERSION_REPLY");
                Self::handle_version_reply(state, msg).await?;
            }
            msgr2::message::CEPH_MSG_MON_COMMAND_ACK => {
                debug!("Received CEPH_MSG_MON_COMMAND_ACK");
                Self::handle_command_ack(state, msg).await?;
            }
            _ => {
                debug!("Received unknown message type: {}", msg.msg_type());
            }
        }
        Ok(())
    }

    /// Handle MonMap message
    async fn handle_monmap(
        state: &Arc<RwLock<MonClientState>>,
        map_events: &broadcast::Sender<MapEvent>,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        info!("Handling MonMap message ({} bytes)", msg.front.len());

        // Decode MMonMap
        let mmonmap = MMonMap::decode(&msg.front)?;
        info!("Received monmap blob: {} bytes", mmonmap.monmap_bl.len());

        // Decode the actual MonMap
        let monmap = MonMap::decode(&mmonmap.monmap_bl)?;
        info!(
            "Decoded MonMap: epoch={}, fsid={}, {} monitors",
            monmap.epoch,
            monmap.fsid,
            monmap.size()
        );

        // Update state
        let mut state_guard = state.write().await;
        state_guard.monmap = monmap.clone();
        state_guard.want_monmap = false;

        // Mark subscription as received
        state_guard.subscriptions.got("monmap", monmap.epoch as u64);

        info!("✓ MonMap updated successfully");
        Ok(())
    }

    /// Handle subscription ack
    async fn handle_subscribe_ack(
        state: &Arc<RwLock<MonClientState>>,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        let ack = MMonSubscribeAck::decode(&msg.front)?;
        info!("Subscription acknowledged: interval={}", ack.interval);

        let mut state_guard = state.write().await;
        state_guard.subscriptions.acked(ack.interval);

        Ok(())
    }

    /// Handle version reply
    async fn handle_version_reply(
        _state: &Arc<RwLock<MonClientState>>,
        _msg: msgr2::message::Message,
    ) -> Result<()> {
        // TODO: Implement version reply handling
        Ok(())
    }

    /// Handle command ack
    async fn handle_command_ack(
        _state: &Arc<RwLock<MonClientState>>,
        _msg: msgr2::message::Message,
    ) -> Result<()> {
        // TODO: Implement command ack handling
        Ok(())
    }

    /// Get the latest version of a cluster map
    pub async fn get_version(&self, what: &str) -> Result<(u64, u64)> {
        let (tx, rx) = oneshot::channel();

        let mut state = self.state.write().await;

        if !state.initialized {
            return Err(MonClientError::NotInitialized);
        }

        let active_con = state
            .active_con
            .as_ref()
            .ok_or(MonClientError::NotConnected)?
            .clone();

        // Allocate request ID
        state.last_version_req_id += 1;
        let req_id = state.last_version_req_id;

        // Track request
        state.version_requests.insert(
            req_id,
            VersionTracker {
                req_id,
                what: what.to_string(),
                result_tx: tx,
            },
        );

        drop(state);

        // Send request
        let msg = MMonGetVersion::new(req_id, what.to_string());
        let payload = msg.encode()?;
        let message = msgr2::message::Message::new(CEPH_MSG_MON_GET_VERSION, payload);

        active_con.send_message(message).await?;

        // Wait for response with timeout
        let result = tokio::time::timeout(self.config.command_timeout, rx)
            .await
            .map_err(|_| MonClientError::Timeout)?
            .map_err(|_| MonClientError::Other("Channel closed".into()))?;

        Ok(result)
    }

    /// Send a command to the monitor cluster
    pub async fn send_command(&self, cmd: Vec<String>, inbl: Bytes) -> Result<CommandResult> {
        let (tx, rx) = oneshot::channel();

        let mut state = self.state.write().await;

        if !state.initialized {
            return Err(MonClientError::NotInitialized);
        }

        let active_con = state
            .active_con
            .as_ref()
            .ok_or(MonClientError::NotConnected)?
            .clone();

        // Allocate command ID
        state.last_command_tid += 1;
        let tid = state.last_command_tid;

        // Track command
        state.commands.insert(
            tid,
            CommandTracker {
                tid,
                cmd: cmd.clone(),
                result_tx: tx,
            },
        );

        drop(state);

        // Send command
        let msg = MMonCommand::new(tid, cmd, inbl);
        let payload = msg.encode()?;
        let message = msgr2::message::Message::new(msgr2::message::CEPH_MSG_MON_COMMAND, payload);

        active_con.send_message(message).await?;

        // Wait for response with timeout
        let result = tokio::time::timeout(self.config.command_timeout, rx)
            .await
            .map_err(|_| MonClientError::Timeout)?
            .map_err(|_| MonClientError::Other("Channel closed".into()))?;

        Ok(result)
    }

    /// Check if connected to a monitor
    pub async fn is_connected(&self) -> bool {
        let state = self.state.read().await;
        state.active_con.is_some()
    }

    /// Check if authenticated
    pub async fn is_authenticated(&self) -> bool {
        let state = self.state.read().await;
        state.authenticated
    }

    /// Get the cluster FSID
    pub async fn get_fsid(&self) -> uuid::Uuid {
        let state = self.state.read().await;
        state.monmap.fsid
    }

    /// Get the number of monitors
    pub async fn get_mon_count(&self) -> usize {
        let state = self.state.read().await;
        state.monmap.size()
    }

    /// Get the current monmap epoch
    pub async fn get_monmap_epoch(&self) -> u32 {
        let state = self.state.read().await;
        state.monmap.epoch
    }

    /// Get monitor addresses by rank
    pub async fn get_mon_addrs(&self, rank: usize) -> Result<crate::types::EntityAddrVec> {
        let state = self.state.read().await;
        state
            .monmap
            .get_addrs(rank)
            .cloned()
            .ok_or(MonClientError::InvalidMonitorRank(rank))
    }

    /// Wait for a specific map version (async)
    pub async fn wait_for_map(&self, what: &str, version: u64) -> Result<()> {
        // Check if we already have this version
        {
            let state = self.state.read().await;
            match what {
                "monmap" => {
                    if state.monmap.epoch as u64 >= version {
                        return Ok(());
                    }
                }
                _ => {
                    // For other maps, we'd check their versions here
                }
            }
        }

        // Subscribe if not already subscribed
        self.subscribe(what, version, 0).await?;

        // Create waiter
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.write().await;
            state
                .map_waiters
                .entry(what.to_string())
                .or_insert_with(Vec::new)
                .push(MapWaiter {
                    what: what.to_string(),
                    version,
                    tx,
                });
        }

        // Wait for notification
        rx.await
            .map_err(|_| MonClientError::Other("Waiter channel closed".into()))?;

        Ok(())
    }

    /// Subscribe to map events
    pub fn subscribe_events(&self) -> broadcast::Receiver<MapEvent> {
        self.map_events.subscribe()
    }
}

impl std::fmt::Debug for MonClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonClient")
            .field("entity_name", &self.entity_name)
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_client() {
        let config = MonClientConfig {
            entity_name: "client.test".to_string(),
            mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
            ..Default::default()
        };

        let client = MonClient::new(config).await.unwrap();
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_subscription() {
        let config = MonClientConfig {
            entity_name: "client.test".to_string(),
            mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
            ..Default::default()
        };

        let client = MonClient::new(config).await.unwrap();

        // Should fail before init
        assert!(client.subscribe("osdmap", 0, 0).await.is_err());
    }
}
