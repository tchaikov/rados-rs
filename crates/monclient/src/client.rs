//! Monitor client implementation
//!
//! Main MonClient struct and implementation.

use crate::connection::MonConnection;
use crate::error::{MonClientError, Result};
use crate::messages::*;
use crate::monmap::MonMap;
use crate::paxos_service_message::PaxosServiceMessage;
use crate::subscription::MonSub;
use crate::types::{CommandResult, EntityName};
use bytes::Bytes;
use denc::denc::VersionedEncode;
use denc::UuidD;
use msgr2::ceph_message::{CephMessage, CrcFlags};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

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
            entity_name: String::new(), // Must be provided by caller
            mon_addrs: Vec::new(),
            keyring_path: String::new(), // Must be provided by caller
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
    #[allow(dead_code)]
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

    /// OSD map (latest received)
    osdmap: Option<Arc<denc::osdmap::OSDMap>>,

    /// Active connection
    active_con: Option<Arc<MonConnection>>,

    /// Pending connections (during hunting)
    pending_cons: HashMap<usize, Arc<MonConnection>>,

    /// Monitors we've tried (to avoid retry storms)
    #[allow(dead_code)]
    tried: std::collections::HashSet<usize>,

    /// Subscription manager
    subscriptions: MonSub,

    /// Command tracking
    commands: HashMap<u64, CommandTracker>,
    last_command_tid: u64,

    /// Pool operation tracking
    pool_ops: HashMap<u64, PoolOpTracker>,
    last_poolop_tid: u64,

    /// Version request tracking
    version_requests: HashMap<u64, VersionTracker>,
    last_version_req_id: u64,

    /// Map version waiters (for async wait_for_map)
    map_waiters: HashMap<String, Vec<MapWaiter>>,

    /// State flags
    authenticated: bool,
    #[allow(dead_code)]
    global_id: u64,
    hunting: bool,
    want_monmap: bool,
    initialized: bool,
    stopping: bool,
}

struct MapWaiter {
    #[allow(dead_code)]
    what: String,
    #[allow(dead_code)]
    version: u64,
    #[allow(dead_code)]
    tx: oneshot::Sender<()>,
}

struct CommandTracker {
    #[allow(dead_code)]
    tid: u64,
    #[allow(dead_code)]
    cmd: Vec<String>,
    #[allow(dead_code)]
    result_tx: oneshot::Sender<CommandResult>,
}

/// Pool operation result
#[derive(Debug, Clone)]
pub struct PoolOpResult {
    /// Reply code (0 = success, negative = error)
    pub reply_code: i32,
    /// Epoch
    pub epoch: u32,
    /// Response data
    pub response_data: Bytes,
}

impl PoolOpResult {
    pub fn new(reply_code: i32, epoch: u32, response_data: Bytes) -> Self {
        Self {
            reply_code,
            epoch,
            response_data,
        }
    }

    pub fn is_success(&self) -> bool {
        self.reply_code == 0
    }
}

struct PoolOpTracker {
    #[allow(dead_code)]
    tid: u64,
    #[allow(dead_code)]
    pool_name: String,
    #[allow(dead_code)]
    result_tx: oneshot::Sender<PoolOpResult>,
}

struct VersionTracker {
    #[allow(dead_code)]
    req_id: u64,
    #[allow(dead_code)]
    what: String,
    #[allow(dead_code)]
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
            osdmap: None,
            active_con: None,
            pending_cons: HashMap::new(),
            tried: std::collections::HashSet::new(),
            subscriptions: MonSub::new(),
            commands: HashMap::new(),
            last_command_tid: 0,
            pool_ops: HashMap::new(),
            last_poolop_tid: 0,
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

        // Send initial subscriptions (monmap and osdmap)
        info!("Subscribing to monmap...");
        self.subscribe("monmap", 0, 0).await?;

        info!("Subscribing to osdmap...");
        self.subscribe("osdmap", 0, 0).await?;

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

        // Get keyring path from config
        let keyring_path = if self.config.keyring_path.is_empty() {
            None
        } else {
            Some(self.config.keyring_path.clone())
        };

        // Create actual msgr2 connection
        let mon_con = Arc::new(
            MonConnection::connect(
                socket_addr,
                rank,
                addrs,
                self.config.entity_name.clone(),
                keyring_path,
            )
            .await?,
        );

        // Note: Connection is now managed by a background task, no need to test it
        // The task was already spawned in MonConnection::connect()

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

        // Use unified CephMessage framework
        let ceph_msg = CephMessage::from_payload(&msg, 0, CrcFlags::ALL)?;
        let message = msgr2::message::Message::from_ceph_message(ceph_msg);

        active_con.send_message(message).await?;

        debug!("Sent subscriptions");
        Ok(())
    }

    /// Helper to renew a subscription from within spawned tasks
    /// This doesn't require &self, so it can be called from the message loop
    async fn renew_subscription(
        state_arc: &Arc<RwLock<MonClientState>>,
        what: &str,
        epoch: u64,
    ) -> Result<()> {
        let mut state = state_arc.write().await;

        // Update subscription
        state.subscriptions.want(what, epoch, 0);

        let active_con = match state.active_con.as_ref() {
            Some(con) => con.clone(),
            None => {
                tracing::warn!("Cannot renew subscription: not connected");
                return Ok(()); // Don't fail, just skip
            }
        };

        // Build subscription message
        let mut msg = MMonSubscribe::new();
        for (what, item) in state.subscriptions.get_subs() {
            msg.add(what.clone(), *item);
        }

        state.subscriptions.renewed();
        drop(state);

        // Send subscription message
        let ceph_msg = CephMessage::from_payload(&msg, 0, CrcFlags::ALL)?;
        let message = msgr2::message::Message::from_ceph_message(ceph_msg);

        active_con.send_message(message).await?;

        debug!("Renewed subscription for {} at epoch {}", what, epoch);
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
        let msg_type = msg.msg_type();
        debug!(
            "Dispatching message type: 0x{:04x} ({}), front.len()={}",
            msg_type,
            msg_type,
            msg.front.len()
        );

        match msg_type {
            msgr2::message::CEPH_MSG_MON_MAP => {
                info!("Received CEPH_MSG_MON_MAP");
                Self::handle_monmap(state, map_events, msg).await?;
            }
            CEPH_MSG_OSD_MAP => {
                info!("Received CEPH_MSG_OSD_MAP");
                Self::handle_osdmap(state, map_events, msg).await?;
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
                debug!(
                    "Received CEPH_MSG_MON_COMMAND_ACK (0x{:04x})",
                    msgr2::message::CEPH_MSG_MON_COMMAND_ACK
                );
                Self::handle_command_ack(state, msg).await?;
            }
            msgr2::message::CEPH_MSG_POOLOP_REPLY => {
                debug!(
                    "Received CEPH_MSG_POOLOP_REPLY (0x{:04x})",
                    msgr2::message::CEPH_MSG_POOLOP_REPLY
                );
                Self::handle_poolop_reply(state, msg).await?;
            }
            _ => {
                debug!(
                    "Received unknown message type: 0x{:04x} ({})",
                    msg_type, msg_type
                );
            }
        }
        Ok(())
    }

    /// Handle MonMap message
    async fn handle_monmap(
        state: &Arc<RwLock<MonClientState>>,
        _map_events: &broadcast::Sender<MapEvent>,
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

    /// Handle OSDMap message
    async fn handle_osdmap(
        state: &Arc<RwLock<MonClientState>>,
        map_events: &broadcast::Sender<MapEvent>,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        info!("Handling OSDMap message ({} bytes)", msg.front.len());

        // Dump the raw MOSDMap message for analysis
        let mosdmap_dump_path = "/tmp/mosdmap-raw.bin";
        if let Err(e) = std::fs::write(mosdmap_dump_path, &msg.front) {
            tracing::warn!("Failed to dump MOSDMap to {}: {}", mosdmap_dump_path, e);
        } else {
            info!("✓ Dumped MOSDMap to {}", mosdmap_dump_path);
        }

        // Decode MOSDMap
        let mosdmap = MOSDMap::decode(&msg.front)?;
        info!(
            "Received MOSDMap: {} full maps, {} incremental maps, newest={}",
            mosdmap.maps.len(),
            mosdmap.incremental_maps.len(),
            mosdmap.newest_map
        );

        // Decode the full osdmaps
        for (epoch, osdmap_bl) in &mosdmap.maps {
            info!(
                "Decoding OSDMap epoch {} ({} bytes)",
                epoch,
                osdmap_bl.len()
            );

            // Dump the raw bytes to a file for analysis
            let dump_path = format!("/tmp/osdmap-epoch-{}.bin", epoch);
            if let Err(e) = std::fs::write(&dump_path, osdmap_bl) {
                tracing::warn!("Failed to dump OSDMap to {}: {}", dump_path, e);
            } else {
                info!("✓ Dumped OSDMap epoch {} to {}", epoch, dump_path);
            }

            match denc::osdmap::OSDMap::decode_versioned(&mut osdmap_bl.as_ref(), 0) {
                Ok(osdmap) => {
                    info!(
                        "✓ Decoded OSDMap: epoch={}, fsid={}, {} pools, {} osds",
                        osdmap.epoch,
                        uuid::Uuid::from_bytes(osdmap.fsid.bytes),
                        osdmap.pools.len(),
                        osdmap.osd_state.len()
                    );

                    // Store the OSDMap in state
                    let mut state_guard = state.write().await;
                    state_guard.osdmap = Some(Arc::new(osdmap.clone()));

                    // Mark subscription as received
                    state_guard.subscriptions.got("osdmap", osdmap.epoch as u64);

                    let epoch = osdmap.epoch as u64;
                    drop(state_guard);

                    // Broadcast map event
                    let _ = map_events.send(MapEvent::OsdMapUpdated { epoch });

                    info!("✓ OSDMap stored and broadcast successfully");

                    // IMPORTANT: Renew subscription to tell monitor we're ready for more updates
                    // This is critical - without this, the monitor won't send us further updates!
                    // We subscribe to the NEXT epoch (current + 1) since we've already processed the current one
                    if let Err(e) = Self::renew_subscription(state, "osdmap", epoch + 1).await {
                        tracing::warn!("Failed to renew osdmap subscription: {}", e);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to decode OSDMap epoch {}: {}", epoch, e);
                }
            }
        }

        // Process incremental OSDMaps
        for (epoch, inc_bl) in &mosdmap.incremental_maps {
            info!(
                "Decoding incremental OSDMap epoch {} ({} bytes)",
                epoch,
                inc_bl.len()
            );

            match denc::osdmap::OSDMapIncremental::decode_versioned(&mut inc_bl.as_ref(), 0) {
                Ok(inc_map) => {
                    // Log version info for debugging
                    debug!(
                        "OSDMapIncremental encoding version info: epoch={}",
                        inc_map.epoch
                    );

                    info!(
                        "✓ Decoded incremental OSDMap: epoch={}, fsid={}, {} new pools, {} old pools",
                        inc_map.epoch,
                        uuid::Uuid::from_bytes(inc_map.fsid.bytes),
                        inc_map.new_pools.len(),
                        inc_map.old_pools.len()
                    );

                    // Debug: Log pool changes in detail
                    if !inc_map.new_pools.is_empty() {
                        for pool_id in inc_map.new_pools.keys() {
                            info!("  New pool ID: {}", pool_id);
                        }
                    }
                    if !inc_map.old_pools.is_empty() {
                        info!("  Old pools being removed: {:?}", inc_map.old_pools);
                    }
                    if !inc_map.new_pool_names.is_empty() {
                        for (pool_id, name) in &inc_map.new_pool_names {
                            info!("  New pool name: {} = {}", pool_id, name);
                        }
                    }

                    // Apply incremental to existing map or create new map
                    let mut state_guard = state.write().await;

                    if let Some(current_map) = &state_guard.osdmap {
                        // Apply incremental to current map
                        let mut updated_map = (**current_map).clone();
                        if let Err(e) = inc_map.apply_to(&mut updated_map) {
                            tracing::warn!(
                                "Failed to apply incremental OSDMap epoch {}: {}",
                                epoch,
                                e
                            );
                            drop(state_guard);
                            continue;
                        }

                        state_guard.osdmap = Some(Arc::new(updated_map));
                        state_guard.subscriptions.got("osdmap", *epoch as u64);

                        let epoch_u64 = *epoch as u64;
                        drop(state_guard);

                        // Broadcast map event
                        let _ = map_events.send(MapEvent::OsdMapUpdated { epoch: epoch_u64 });

                        info!("✓ Incremental OSDMap applied and broadcast successfully");

                        // IMPORTANT: Renew subscription to tell monitor we're ready for more updates
                        // We subscribe to the NEXT epoch (current + 1) since we've already processed the current one
                        if let Err(e) =
                            Self::renew_subscription(state, "osdmap", epoch_u64 + 1).await
                        {
                            tracing::warn!("Failed to renew osdmap subscription: {}", e);
                        }
                    } else {
                        // No base map yet, skip this incremental
                        tracing::warn!(
                            "Cannot apply incremental OSDMap epoch {}: no base map available",
                            epoch
                        );
                        drop(state_guard);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to decode incremental OSDMap epoch {}: {}", epoch, e);
                }
            }
        }

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
        state: &Arc<RwLock<MonClientState>>,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        // Decode the version reply message from the front payload
        let reply = MMonGetVersionReply::decode(&msg.front)?;

        // Get the transaction ID from the message payload (not header in this case)
        let tid = reply.tid;

        debug!(
            "Received version reply: tid={}, version={}, oldest_version={}",
            tid, reply.version, reply.oldest_version
        );

        // Find and complete the pending version request
        let mut state_guard = state.write().await;
        if let Some(tracker) = state_guard.version_requests.remove(&tid) {
            drop(state_guard); // Release lock before sending
            let _ = tracker
                .result_tx
                .send((reply.version, reply.oldest_version));
        } else {
            debug!(
                "Received version reply for tid {} but no pending request found",
                tid
            );
        }

        Ok(())
    }

    /// Handle command ack
    async fn handle_command_ack(
        state: &Arc<RwLock<MonClientState>>,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        // Decode the command ack message from the front payload
        let ack = MMonCommandAck::decode(&msg.front)?;

        // Get the transaction ID from the message header
        let tid = msg.header.tid;

        debug!(
            "Received command ack: tid={}, r={}, rs={}",
            tid, ack.r, ack.rs
        );

        // Find and complete the pending command by matching tid
        let mut state_guard = state.write().await;
        if let Some(tracker) = state_guard.commands.remove(&tid) {
            drop(state_guard); // Release lock before sending
                               // The command output is in the data field, not the rs field
                               // Use rs only if data is empty (for error messages)
            let outs = if !msg.data.is_empty() {
                String::from_utf8_lossy(&msg.data).to_string()
            } else {
                ack.rs
            };
            let result = CommandResult::new(ack.r, outs, msg.data);
            let _ = tracker.result_tx.send(result);
        } else {
            debug!(
                "Received command ack for tid {} but no pending command found",
                tid
            );
        }

        Ok(())
    }

    /// Handle pool operation reply
    async fn handle_poolop_reply(
        state: &Arc<RwLock<MonClientState>>,
        msg: msgr2::message::Message,
    ) -> Result<()> {
        // Decode the pool operation reply message from the front payload
        let reply = MPoolOpReply::decode(&msg.front)?;

        // Get the transaction ID from the message header
        let tid = msg.header.tid;

        debug!(
            "Received pool op reply: tid={}, reply_code={}, epoch={}",
            tid, reply.reply_code, reply.epoch
        );

        info!(
            "Pool op reply details: tid={}, reply_code={}, target epoch={}",
            tid, reply.reply_code, reply.epoch
        );

        // Check if we need to wait for OSDMap update
        let reply_epoch = reply.epoch;
        let current_epoch = {
            let state_guard = state.read().await;
            state_guard.osdmap.as_ref().map(|m| m.epoch).unwrap_or(0)
        };

        // If reply epoch is newer than our current OSDMap, wait for update
        if reply_epoch > current_epoch {
            info!(
                "Pool op reply epoch {} is newer than current OSDMap epoch {}, waiting for update...",
                reply_epoch, current_epoch
            );

            // Wait for OSDMap to be updated (with timeout)
            let max_wait = std::time::Duration::from_secs(30);
            let start = std::time::Instant::now();

            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                let state_guard = state.read().await;
                let current = state_guard.osdmap.as_ref().map(|m| m.epoch).unwrap_or(0);
                drop(state_guard);

                if current >= reply_epoch {
                    info!("✓ OSDMap updated to epoch {}", current);
                    break;
                }

                if start.elapsed() > max_wait {
                    warn!(
                        "Timeout waiting for OSDMap epoch {} (current: {})",
                        reply_epoch, current
                    );
                    break;
                }
            }
        }

        // Find and complete the pending pool operation by matching tid
        let mut state_guard = state.write().await;
        if let Some(tracker) = state_guard.pool_ops.remove(&tid) {
            drop(state_guard); // Release lock before sending
            let result =
                PoolOpResult::new(reply.reply_code as i32, reply.epoch, reply.response_data);
            let _ = tracker.result_tx.send(result);
        } else {
            debug!(
                "Received pool op reply for tid {} but no pending pool operation found",
                tid
            );
        }

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

        // Use unified CephMessage framework
        let msg = MMonGetVersion::new(req_id, what.to_string());
        let ceph_msg = CephMessage::from_payload(&msg, 0, CrcFlags::ALL)?;
        let mut message = msgr2::message::Message::from_ceph_message(ceph_msg);

        // Set the transaction ID in the message header to match the payload
        message.header.tid = req_id;

        active_con.send_message(message).await?;

        // Wait for response with timeout
        let result = tokio::time::timeout(self.config.command_timeout, rx)
            .await
            .map_err(|_| MonClientError::Timeout)?
            .map_err(|_| MonClientError::Other("Channel closed".into()))?;

        // If we're getting osdmap version, wait for the actual map to be received
        if what == "osdmap" {
            let target_version = result.0;
            let start = tokio::time::Instant::now();
            let timeout = tokio::time::Duration::from_secs(2);

            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                let state = self.state.read().await;
                if let Some(osdmap) = &state.osdmap {
                    let current_epoch = osdmap.epoch as u64;
                    if current_epoch >= target_version {
                        info!(
                            "OSDMap updated to epoch {} (target: {})",
                            current_epoch, target_version
                        );
                        drop(state);
                        break;
                    }
                    info!(
                        "Waiting for OSDMap: current epoch={}, target={}",
                        current_epoch, target_version
                    );
                }
                drop(state);

                if start.elapsed() > timeout {
                    return Err(MonClientError::Other(format!(
                        "Timeout waiting for OSDMap to reach version {}",
                        target_version
                    )));
                }
            }
        }

        Ok(result)
    }

    /// Send a command to the monitor cluster
    ///
    /// This is the low-level interface for sending commands to monitors.
    /// It accepts a vector of command strings (in JSON format) and an optional input buffer,
    /// and returns a result containing both string output and binary output.
    ///
    /// # Arguments
    /// * `cmd` - Vector of command strings (usually JSON-formatted commands)
    /// * `inbl` - Input buffer (optional binary data to send with the command)
    ///
    /// # Returns
    /// * `Ok(CommandResult)` containing:
    ///   - `retval`: Return code (0 = success, negative = error)
    ///   - `outs`: String output from the command
    ///   - `outbl`: Binary output from the command
    /// * `Err(MonClientError)` if the operation failed
    ///
    /// # Example
    /// ```no_run
    /// # use monclient::MonClient;
    /// # use bytes::Bytes;
    /// # async fn example(client: &MonClient) {
    /// // List pools
    /// let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];
    /// let result = client.invoke(cmd, Bytes::new()).await.unwrap();
    /// println!("Pools: {}", result.outs);
    ///
    /// // Create pool with specific pg_num
    /// let cmd = vec![r#"{"prefix": "osd pool create", "pool": "mypool", "pg_num": 128}"#.to_string()];
    /// let result = client.invoke(cmd, Bytes::new()).await.unwrap();
    /// # }
    /// ```
    pub async fn invoke(&self, cmd: Vec<String>, inbl: Bytes) -> Result<CommandResult> {
        self.send_command(cmd, inbl).await
    }

    /// Send a command to the monitor cluster (internal implementation)
    async fn send_command(&self, cmd: Vec<String>, inbl: Bytes) -> Result<CommandResult> {
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

        // Create MMonCommand with cluster fsid
        let fsid = self.get_fsid().await;
        tracing::info!(
            "send_command: Sending command (tid={}): {:?} with fsid: {}",
            tid,
            cmd,
            fsid
        );
        let msg = MMonCommand::new(tid, cmd, inbl, fsid);

        // Use unified CephMessage framework
        let ceph_msg = CephMessage::from_payload(&msg, 0, CrcFlags::ALL)?;
        let mut message = msgr2::message::Message::from_ceph_message(ceph_msg);

        // Set the transaction ID in the message header
        message.header.tid = tid;

        tracing::info!(
            "send_command: About to send command message with tid={}",
            tid
        );
        active_con.send_message(message).await?;
        tracing::info!("send_command: Command message sent successfully, waiting for response");

        // Wait for response with timeout
        let result = tokio::time::timeout(self.config.command_timeout, rx)
            .await
            .map_err(|_| MonClientError::Timeout)?
            .map_err(|_| MonClientError::Other("Channel closed".into()))?;

        Ok(result)
    }

    /// Send a pool operation to the monitor cluster
    async fn send_poolop(&self, pool_name: String, msg: MPoolOp) -> Result<PoolOpResult> {
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

        // Allocate pool operation ID
        state.last_poolop_tid += 1;
        let tid = state.last_poolop_tid;

        // Track pool operation
        state.pool_ops.insert(
            tid,
            PoolOpTracker {
                tid,
                pool_name: pool_name.clone(),
                result_tx: tx,
            },
        );

        drop(state);

        // Use unified CephMessage framework
        let ceph_msg = CephMessage::from_payload(&msg, 0, CrcFlags::ALL)?;
        let mut message = msgr2::message::Message::from_ceph_message(ceph_msg);

        // Set the transaction ID in the message header
        message.header.tid = tid;

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
    pub async fn get_fsid(&self) -> UuidD {
        let state = self.state.read().await;
        UuidD::from_bytes(*state.monmap.fsid.as_bytes())
    }

    /// Get the current OSDMap
    ///
    /// Returns the latest OSDMap received from the monitors.
    /// Returns an error if no OSDMap has been received yet.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # async fn example(mon_client: Arc<monclient::MonClient>) -> monclient::Result<()> {
    /// // Subscribe to OSDMap updates
    /// mon_client.subscribe("osdmap", 0, 0).await?;
    ///
    /// // Wait a moment for the OSDMap to arrive
    /// tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    ///
    /// // Get the current OSDMap
    /// let osdmap = mon_client.get_osdmap().await?;
    /// println!("OSDMap epoch: {}", osdmap.epoch);
    /// println!("Number of pools: {}", osdmap.pools.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_osdmap(&self) -> Result<Arc<denc::osdmap::OSDMap>> {
        let state = self.state.read().await;
        state.osdmap.clone().ok_or(MonClientError::Other(
            "No OSDMap available - subscribe to 'osdmap' first".to_string(),
        ))
    }

    /// Get a ServiceAuthProvider for connecting to OSDs/MDSs/MGRs
    ///
    /// This creates an authorizer-based auth provider using the service tickets
    /// obtained during monitor authentication. Returns None if the monitor
    /// connection is not established or no authentication was used.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use monclient::MonClient;
    /// # async fn example(mon_client: &MonClient) -> Result<(), Box<dyn std::error::Error>> {
    /// // After connecting to monitor, get service auth provider for OSDs
    /// if let Some(service_auth) = mon_client.get_service_auth_provider().await {
    ///     // Use service_auth to connect to OSDs
    ///     let config = msgr2::ConnectionConfig::with_auth_provider(Box::new(service_auth));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_service_auth_provider(&self) -> Option<auth::ServiceAuthProvider> {
        let state = self.state.read().await;
        state
            .active_con
            .as_ref()
            .and_then(|conn| conn.create_service_auth_provider())
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

    /// Create a new pool
    ///
    /// This is a simplified interface for pool creation using MPoolOp messages.
    /// For advanced pool creation with custom parameters (pg_num, pgp_num, pool_type, etc.),
    /// use the `invoke()` method with a mon_command instead.
    ///
    /// # Arguments
    /// * `pool_name` - Name of the pool to create
    /// * `crush_rule` - Optional CRUSH rule ID (uses cluster default if None)
    ///
    /// # Returns
    /// * `Ok(())` if the pool was created successfully
    /// * `Err(MonClientError)` if the operation failed
    ///
    /// # Example
    /// ```no_run
    /// # use monclient::MonClient;
    /// # async fn example(client: &MonClient) {
    /// // Create pool with default settings
    /// client.create_pool("mypool", None).await.unwrap();
    ///
    /// // Create pool with specific crush rule
    /// client.create_pool("mypool", Some(1)).await.unwrap();
    ///
    /// // For more control (e.g., pg_num), use invoke():
    /// let cmd = vec![r#"{"prefix": "osd pool create", "pool": "mypool", "pg_num": 128}"#.to_string()];
    /// client.invoke(cmd, bytes::Bytes::new()).await.unwrap();
    /// # }
    /// ```
    pub async fn create_pool(&self, pool_name: &str, crush_rule: Option<i16>) -> Result<()> {
        // Use MPoolOp message (official librados approach)
        let fsid = self.get_fsid().await;

        // For pool creation, pool_id is 0 (pool doesn't exist yet)
        let msg = MPoolOp::create_pool(fsid.bytes, pool_name.to_string(), crush_rule);

        let result = self.send_poolop(pool_name.to_string(), msg).await?;

        if result.is_success() {
            Ok(())
        } else {
            Err(MonClientError::CommandFailed {
                code: result.reply_code,
                message: format!("Pool operation failed with code {}", result.reply_code),
            })
        }
    }

    /// Delete a pool using MPoolOp protocol
    ///
    /// This operation uses the MPoolOp binary message protocol to delete a pool,
    /// matching the official librados implementation.
    ///
    /// # Arguments
    /// * `pool_name` - Name of the pool to delete
    /// * `confirm` - Must be set to true to confirm deletion (safety check)
    ///
    /// # Returns
    /// * `Ok(())` if the pool was deleted successfully
    /// * `Err(MonClientError)` if the operation failed
    ///
    /// # Safety
    /// This operation is destructive and will delete all data in the pool.
    /// The `confirm` parameter must be explicitly set to `true`.
    pub async fn delete_pool(&self, pool_name: &str, confirm: bool) -> Result<()> {
        if !confirm {
            return Err(MonClientError::Other(
                "Pool deletion requires explicit confirmation".into(),
            ));
        }

        // Get fsid
        let fsid = self.get_fsid().await;

        // Look up pool ID from OSDMap
        let state = self.state.read().await;
        let osdmap = state
            .osdmap
            .as_ref()
            .ok_or_else(|| MonClientError::Other("OSD map not available yet".into()))?;

        let pool_id = osdmap
            .pool_name
            .iter()
            .find(|(_, name)| name.as_str() == pool_name)
            .map(|(id, _)| *id as u32)
            .ok_or_else(|| MonClientError::Other(format!("Pool '{}' not found", pool_name)))?;

        info!("Deleting pool '{}' with ID {}", pool_name, pool_id);

        drop(state);

        // Create and send MPoolOp delete message
        let msg = MPoolOp::delete_pool(fsid.bytes, pool_id, pool_name.to_string());
        let result = self.send_poolop(pool_name.to_string(), msg).await?;

        if result.is_success() {
            Ok(())
        } else {
            Err(MonClientError::CommandFailed {
                code: result.reply_code,
                message: format!("Pool operation failed with code {}", result.reply_code),
            })
        }
    }

    /// List all pools
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - List of pool names
    /// * `Err(MonClientError)` if the operation failed
    pub async fn list_pools(&self) -> Result<Vec<String>> {
        // Read from local cached OSDMap like C++ does, not from monitor
        let state = self.state.read().await;
        let osdmap = state
            .osdmap
            .as_ref()
            .ok_or_else(|| MonClientError::Other("OSD map not available yet".into()))?;

        // Iterate over pools (the authoritative source) and look up names
        // This matches C++ librados: for (auto p : o.get_pools()) v.push_back(o.get_pool_name(p.first))
        let mut pools: Vec<String> = osdmap
            .pools
            .keys()
            .filter_map(|pool_id| osdmap.pool_name.get(pool_id).cloned())
            .collect();

        // Sort for consistent ordering
        pools.sort();

        Ok(pools)
    }

    /// List all pools by sending a command to the monitor
    ///
    /// This queries the monitor directly rather than using the cached OSDMap.
    /// Prefer using list_pools() which reads from the local cache for consistency.
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - List of pool names
    /// * `Err(MonClientError)` if the operation failed
    pub async fn list_pools_from_monitor(&self) -> Result<Vec<String>> {
        tracing::info!("list_pools: Creating command");
        // Commands must be JSON formatted: {"prefix": "command"}
        let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];

        tracing::info!("list_pools: Calling send_command with cmd={:?}", cmd);
        let result = self.send_command(cmd, Bytes::new()).await?;
        tracing::info!("list_pools: Received result from send_command");

        if result.is_success() {
            // Parse the output - pool names are separated by newlines
            let pools: Vec<String> = result
                .outs
                .lines()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Ok(pools)
        } else {
            Err(MonClientError::CommandFailed {
                code: result.retval,
                message: result.outs,
            })
        }
    }

    /// Get pool statistics
    ///
    /// # Arguments
    /// * `pool_name` - Name of the pool
    ///
    /// # Returns
    /// * `Ok(String)` - JSON-formatted pool statistics
    /// * `Err(MonClientError)` if the operation failed
    pub async fn get_pool_stats(&self, pool_name: &str) -> Result<String> {
        // Commands must be JSON formatted: {"prefix": "command", "pool_name": "name"}
        let cmd = vec![format!(
            r#"{{"prefix": "osd pool stats", "pool_name": "{}"}}"#,
            pool_name
        )];

        let result = self.send_command(cmd, Bytes::new()).await?;

        if result.is_success() {
            Ok(result.outs)
        } else {
            Err(MonClientError::CommandFailed {
                code: result.retval,
                message: result.outs,
            })
        }
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
