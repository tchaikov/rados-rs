//! Monitor client implementation
//!
//! Main MonClient struct and implementation.

use crate::connection::{KeepalivePolicy, MonConnection};
use crate::error::{MonClientError, Result};
use crate::messages::*;
use crate::monmap::MonMap;
use crate::paxos_service_message::PaxosServiceMessage;
use crate::subscription::MonSub;
use crate::types::{CommandResult, EntityName};
use crate::wait_helper::wait_for_condition;
use async_trait::async_trait;
use bytes::Bytes;
use denc::UuidD;
use msgr2::ceph_message::{CephMessage, CrcFlags};
use msgr2::{Dispatcher, MessageBus};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

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

    /// Number of monitors to try connecting to in parallel during hunt
    /// (0 means try all available monitors)
    pub hunt_parallel: usize,

    /// Keepalive interval (how often to send keepalive messages)
    /// Default: 10 seconds
    pub keepalive_interval: Duration,

    /// Keepalive timeout (how long to wait for keepalive ACK before reconnecting)
    /// Set to 0 to disable keepalive timeout checking
    /// Default: 30 seconds
    pub keepalive_timeout: Duration,

    /// Tick interval (how often to run periodic maintenance tasks like auth renewal)
    /// Default: same as hunt_interval
    pub tick_interval: Option<Duration>,

    /// Backoff multiplier applied to hunt_interval after each failed hunt
    /// Default: 1.5
    pub hunt_interval_backoff: f64,

    /// Minimum multiplier for hunt interval backoff
    /// Default: 1.0
    pub hunt_interval_min_multiple: f64,

    /// Maximum multiplier for hunt interval backoff
    /// Default: 10.0
    pub hunt_interval_max_multiple: f64,
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
            hunt_parallel: 3, // Try 3 monitors in parallel by default
            keepalive_interval: Duration::from_secs(10),
            keepalive_timeout: Duration::from_secs(30),
            tick_interval: None, // Defaults to hunt_interval
            hunt_interval_backoff: 1.5,
            hunt_interval_min_multiple: 1.0,
            hunt_interval_max_multiple: 10.0,
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

    /// Tick task handle (for periodic keepalive and auth renewal)
    tick_task: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Keepalive state (separate lock to avoid contention)
    keepalive_state: Arc<Mutex<KeepaliveState>>,

    /// Event broadcaster for map updates
    map_events: broadcast::Sender<MapEvent>,

    /// Global message bus for inter-component routing
    message_bus: Arc<MessageBus>,

    /// Notification for authentication completion
    auth_notify: Arc<tokio::sync::Notify>,

    /// Notification for MonMap arrival
    monmap_notify: Arc<tokio::sync::Notify>,
}

impl Clone for MonClient {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            entity_name: self.entity_name.clone(),
            state: Arc::clone(&self.state),
            runtime: self.runtime.clone(),
            tick_task: Arc::clone(&self.tick_task),
            keepalive_state: Arc::clone(&self.keepalive_state),
            map_events: self.map_events.clone(),
            message_bus: Arc::clone(&self.message_bus),
            auth_notify: Arc::clone(&self.auth_notify),
            monmap_notify: Arc::clone(&self.monmap_notify),
        }
    }
}

/// Events for map updates
#[derive(Debug, Clone)]
pub enum MapEvent {
    MonMapUpdated { epoch: u32 },
    OsdMapUpdated { epoch: u64 },
    MgrMapUpdated { epoch: u64 },
    MdsMapUpdated { epoch: u64 },
}

/// Keepalive state tracking
///
/// Keepalive is implemented at the msgr2 protocol layer via MonConnection.
/// The MonConnection background task:
/// - Sends Keepalive2 frames at the configured interval
/// - Monitors for keepalive ACKs with timeout checking
/// - Notifies the tick loop via timeout_rx channel when keepalive timeout occurs
///
/// The tick loop (lines 919-943) handles keepalive timeouts by:
/// - Checking try_recv_timeout() on the active connection
/// - Triggering hunting for a new monitor if timeout detected
#[derive(Default)]
#[allow(dead_code)]
struct KeepaliveState {
    /// Last time we sent a keepalive (Keepalive2 frame)
    /// Note: Actual keepalive sending is handled by msgr2 protocol layer
    last_sent: Option<std::time::Instant>,
}

struct MonClientState {
    /// Monitor map
    monmap: MonMap,

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

    /// Hunting backoff state
    /// Current backoff multiplier for hunt interval
    reopen_interval_multiplier: f64,
    /// Time of last hunting attempt
    last_hunt_attempt: Option<std::time::Instant>,
    /// Whether we've ever had a successful connection (for backoff logic)
    had_a_connection: bool,
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
    /// Create a new MonClient with default MessageBus
    ///
    /// This is a convenience wrapper that creates a new MessageBus.
    /// For production use with OSDClient integration, use `new_with_bus()`.
    /// Create a new MonClient with a shared MessageBus
    ///
    /// This allows MonClient to forward messages (like OSDMap) to other components
    /// like OSDClient through the shared message bus.
    ///
    /// # Arguments
    ///
    /// * `config` - MonClient configuration
    /// * `message_bus` - Shared MessageBus for inter-component communication
    pub async fn new(
        config: MonClientConfig,
        message_bus: Arc<MessageBus>,
    ) -> std::result::Result<Self, MonClientError> {
        // Parse entity name
        let entity_name: EntityName = config
            .entity_name
            .parse()
            .map_err(|e| MonClientError::Other(format!("Invalid entity name: {}", e)))?;

        // Build initial monmap from config
        let monmap = if !config.mon_addrs.is_empty() {
            info!("Building monmap from config");
            MonMap::build_initial(&config.mon_addrs)?
        } else {
            return Err(MonClientError::InvalidMonMap(
                "No monitor addresses provided in config (mon_addrs)".into(),
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
            reopen_interval_multiplier: config.hunt_interval_min_multiple,
            last_hunt_attempt: None,
            had_a_connection: false,
        };

        Ok(Self {
            config,
            entity_name,
            state: Arc::new(RwLock::new(state)),
            runtime: tokio::runtime::Handle::current(),
            tick_task: Arc::new(RwLock::new(None)),
            keepalive_state: Arc::new(Mutex::new(KeepaliveState::default())),
            map_events,
            message_bus,
            auth_notify: Arc::new(tokio::sync::Notify::new()),
            monmap_notify: Arc::new(tokio::sync::Notify::new()),
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

        // NOTE: register_handlers() must be called separately after init()
        // This ensures MonClient is registered on MessageBus to receive messages

        // Start tick loop for periodic keepalive and auth renewal
        self.start_tick_loop();

        // Start hunting process (connects to monitor)
        self.start_hunting().await?;

        // Send initial subscriptions (monmap and osdmap)
        info!("Subscribing to monmap...");
        self.subscribe("monmap", 0, 0).await?;

        // NOTE: OSDMap subscription removed - OSDClient handles this via MessageBus
        // Applications should explicitly subscribe to osdmap after registering OSDClient
        // on the MessageBus to avoid race conditions with message routing

        info!("MonClient initialized successfully");
        Ok(())
    }

    /// Shutdown the client
    pub async fn shutdown(&self) -> Result<()> {
        let (active_con, pending_cons) = {
            let mut state = self.state.write().await;

            if state.stopping {
                return Ok(());
            }

            info!("Shutting down MonClient");
            state.stopping = true;

            // Take connections to close them after releasing the lock
            let active_con = state.active_con.take();
            let pending_cons: Vec<_> = state.pending_cons.drain().map(|(_, con)| con).collect();

            (active_con, pending_cons)
        };

        // Close connections after releasing the lock
        if let Some(con) = active_con {
            con.close().await?;
        }

        for con in pending_cons {
            con.close().await?;
        }

        // Stop tick task
        let mut tick_task = self.tick_task.write().await;
        if let Some(handle) = tick_task.take() {
            handle.abort();
            info!("Aborted tick task");
        }

        Ok(())
    }

    /// Start hunting for an available monitor
    async fn start_hunting(&self) -> Result<()> {
        info!("Starting monitor hunt");

        // Apply backoff delay if we recently tried hunting
        let delay = {
            let state = self.state.read().await;

            if state.had_a_connection {
                if let Some(last_attempt) = state.last_hunt_attempt {
                    let elapsed = last_attempt.elapsed();
                    let hunt_delay = self
                        .config
                        .hunt_interval
                        .mul_f64(state.reopen_interval_multiplier);

                    if elapsed < hunt_delay {
                        let remaining = hunt_delay - elapsed;
                        debug!(
                            "Applying hunting backoff: waiting {:?} (multiplier: {:.2})",
                            remaining, state.reopen_interval_multiplier
                        );
                        Some(remaining)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Sleep for backoff delay if needed
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        // Record hunt attempt time before trying
        let hunt_start = std::time::Instant::now();

        let state = self.state.read().await;
        let monmap = state.monmap.clone();
        let hunt_parallel = self.config.hunt_parallel;
        drop(state);

        // Get monitors grouped by priority (lowest priority first)
        let priority_groups = monmap.get_monitors_by_priority();

        if priority_groups.is_empty() {
            return Err(MonClientError::MonitorUnavailable);
        }

        // Try the lowest priority group first
        let ranks = &priority_groups[0];

        if ranks.is_empty() {
            return Err(MonClientError::MonitorUnavailable);
        }

        // Shuffle ranks based on weights
        let mut selected_ranks = ranks.clone();
        if selected_ranks.len() > 1 {
            // Check if all weights are zero
            let weights: Vec<u16> = selected_ranks
                .iter()
                .map(|&rank| monmap.get_weight(rank))
                .collect();

            let total_weight: u32 = weights.iter().map(|&w| w as u32).sum();

            if total_weight == 0 {
                // All weights are zero, use uniform random selection
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                selected_ranks.shuffle(&mut rng);
            } else {
                // Use weighted random selection
                use rand::distributions::WeightedIndex;
                use rand::prelude::*;
                let mut rng = rand::thread_rng();

                // Shuffle with weights (Fisher-Yates with weighted selection)
                let mut shuffled = Vec::new();
                let mut remaining_ranks = selected_ranks.clone();
                let mut remaining_weights = weights.clone();

                while !remaining_ranks.is_empty() {
                    let dist = WeightedIndex::new(&remaining_weights).map_err(|e| {
                        MonClientError::InvalidMonMap(format!("Invalid weights: {}", e))
                    })?;
                    let idx = dist.sample(&mut rng);
                    shuffled.push(remaining_ranks.remove(idx));
                    remaining_weights.remove(idx);
                }

                selected_ranks = shuffled;
            }
        }

        // Determine how many monitors to try in parallel
        let n = if hunt_parallel == 0 || hunt_parallel > selected_ranks.len() {
            selected_ranks.len()
        } else {
            hunt_parallel
        };

        // Try connecting to n monitors in parallel
        let hunt_result = if n == 1 {
            // Simple case: try one monitor
            self.connect_to_mon(selected_ranks[0]).await
        } else {
            // Parallel case: try multiple monitors, first one to succeed wins
            use futures::future::select_ok;

            let futures: Vec<_> = selected_ranks
                .iter()
                .take(n)
                .map(|&rank| {
                    let client = self.clone();
                    Box::pin(async move { client.connect_to_mon(rank).await })
                })
                .collect();

            // Wait for the first successful connection
            match select_ok(futures).await {
                Ok((_, _)) => {
                    // First connection succeeded
                    info!("Successfully connected to a monitor");
                    Ok(())
                }
                Err(e) => Err(e),
            }
        };

        // Update backoff state based on hunt result
        {
            let mut state = self.state.write().await;
            state.last_hunt_attempt = Some(hunt_start);

            if hunt_result.is_err() && state.had_a_connection {
                // Hunt failed - increase backoff for next attempt
                state.reopen_interval_multiplier *= self.config.hunt_interval_backoff;
                if state.reopen_interval_multiplier > self.config.hunt_interval_max_multiple {
                    state.reopen_interval_multiplier = self.config.hunt_interval_max_multiple;
                }
                debug!(
                    "Hunt failed, increased backoff multiplier to {:.2}",
                    state.reopen_interval_multiplier
                );
            }
        }

        hunt_result
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

        // Create keepalive policy from config
        let keepalive_policy = if self.config.keepalive_interval.as_secs() > 0 {
            KeepalivePolicy::new(
                self.config.keepalive_interval,
                self.config.keepalive_timeout,
            )
        } else {
            KeepalivePolicy::disabled()
        };

        // Create actual msgr2 connection
        let mon_con = Arc::new(
            MonConnection::connect(
                socket_addr,
                rank,
                addrs,
                self.config.entity_name.clone(),
                keyring_path,
                keepalive_policy,
                Arc::clone(&self.message_bus), // Pass MessageBus to connection
            )
            .await?,
        );

        // Note: Connection is now managed by a background task, no need to test it
        // The task was already spawned in MonConnection::connect()

        // Get global_id before taking lock (avoid holding lock during async call)
        let global_id = mon_con.global_id().await;
        tracing::debug!("Retrieved global_id {} from MonConnection", global_id);

        // Store as active connection (but check if we won the race)
        let mut state = self.state.write().await;

        // If we're no longer hunting, another connection won the race
        if !state.hunting {
            debug!(
                "Connection to mon.{} succeeded but another monitor already won the hunt",
                rank
            );
            drop(state);
            // Close this connection since we don't need it
            mon_con.close().await?;
            return Ok(());
        }

        // We won the race - set this as the active connection
        state.active_con = Some(mon_con);
        state.hunting = false;
        // Authentication was completed during MonConnection::connect() -> establish_session()
        state.authenticated = true;
        state.global_id = global_id; // Store global_id in MonClient

        // Clear any pending connections (from parallel hunt)
        state.pending_cons.clear();

        // Mark that we've had a successful connection
        state.had_a_connection = true;

        // Un-backoff: reduce the backoff multiplier on successful connection
        let old_multiplier = state.reopen_interval_multiplier;
        state.reopen_interval_multiplier = (state.reopen_interval_multiplier
            / self.config.hunt_interval_backoff)
            .max(self.config.hunt_interval_min_multiple);

        if old_multiplier != state.reopen_interval_multiplier {
            debug!(
                "Un-backoff: reduced multiplier from {:.2} to {:.2}",
                old_multiplier, state.reopen_interval_multiplier
            );
        }

        drop(state);

        // Notify waiters that authentication is complete (after releasing lock)
        self.auth_notify.notify_waiters();

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
    /// NOTE: Currently only used by handle_osdmap which will move to OSDClient
    #[allow(dead_code)]
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

    /// Register MonClient as a handler on the MessageBus for messages it handles
    pub async fn register_handlers(self: Arc<Self>) -> Result<()> {
        use msgr2::message::*;
        use msgr2::Dispatcher;

        info!("Registering MonClient handlers on MessageBus");

        // Register for all message types that MonClient handles
        self.message_bus
            .register(CEPH_MSG_MON_MAP, self.clone() as Arc<dyn Dispatcher>)
            .await;
        self.message_bus
            .register(CEPH_MSG_PING, self.clone() as Arc<dyn Dispatcher>)
            .await;
        self.message_bus
            .register(CEPH_MSG_PING_ACK, self.clone() as Arc<dyn Dispatcher>)
            .await;
        self.message_bus
            .register(
                CEPH_MSG_MON_SUBSCRIBE_ACK,
                self.clone() as Arc<dyn Dispatcher>,
            )
            .await;
        self.message_bus
            .register(
                CEPH_MSG_MON_GET_VERSION_REPLY,
                self.clone() as Arc<dyn Dispatcher>,
            )
            .await;
        self.message_bus
            .register(
                CEPH_MSG_MON_COMMAND_ACK,
                self.clone() as Arc<dyn Dispatcher>,
            )
            .await;
        self.message_bus
            .register(CEPH_MSG_POOLOP_REPLY, self.clone() as Arc<dyn Dispatcher>)
            .await;

        info!("✓ MonClient handlers registered");
        Ok(())
    }

    /// Start background tick loop for periodic maintenance
    fn start_tick_loop(&self) {
        let state = Arc::clone(&self.state);
        let keepalive_state = Arc::clone(&self.keepalive_state);
        let config = self.config.clone();
        let self_clone = self.clone(); // Clone Arc<Self> for use in spawned task

        // Use tick_interval if specified, otherwise use hunt_interval
        let tick_interval = config.tick_interval.unwrap_or(config.hunt_interval);

        let handle = tokio::spawn(async move {
            info!("Tick loop started (interval: {:?})", tick_interval);

            loop {
                tokio::time::sleep(tick_interval).await;

                // Check if stopping
                {
                    let state_guard = state.read().await;
                    if state_guard.stopping {
                        info!("Stopping flag set, exiting tick loop");
                        break;
                    }
                }

                // Perform tick operations
                if let Err(e) = self_clone.tick(&state, &keepalive_state).await {
                    tracing::error!("Error in tick: {}", e);
                }
            }

            info!("Tick loop terminated");
        });

        // Store task handle directly (no need to spawn a task just to store a value)
        // Use try_write since we're called from async context (init())
        let tick_task = Arc::clone(&self.tick_task);
        let mut task_guard = tick_task
            .try_write()
            .expect("tick_task lock should not be held during initialization");
        *task_guard = Some(handle);

        info!("Started tick loop");
    }

    /// Periodic maintenance tick
    async fn tick(
        &self,
        state: &Arc<RwLock<MonClientState>>,
        _keepalive_state: &Arc<Mutex<KeepaliveState>>,
    ) -> Result<()> {
        // Get active connection (using read lock, quickly released)
        let active_con = {
            let state_guard = state.read().await;
            match &state_guard.active_con {
                Some(con) => Arc::clone(con),
                None => {
                    debug!("No active connection in tick");
                    return Ok(());
                }
            }
        };

        // Check for keepalive timeout
        // The background task in MonConnection sends a timeout notification
        // when keepalive ACK is not received within the configured timeout
        if let Some(()) = active_con.try_recv_timeout().await {
            warn!(
                "Keepalive timeout detected on mon.{}, hunting for new monitor",
                active_con.rank()
            );

            // Clear the failed connection and trigger hunting
            {
                let mut state_guard = state.write().await;
                state_guard.active_con = None;
                state_guard.hunting = true;
            }

            // Trigger hunting to find a more responsive monitor
            if let Err(e) = self.start_hunting().await {
                error!("Failed to start hunting after keepalive timeout: {}", e);
                return Err(e);
            }

            info!("Successfully started hunting after keepalive timeout");
            return Ok(());
        }

        // Check if auth tickets need renewal
        // This matches the official MonClient::_check_auth_tickets() behavior
        if let Some(auth_provider_arc) = active_con.get_auth_provider() {
            // Lock the auth provider to get the handler reference
            let auth_provider = auth_provider_arc.lock().await;
            let handler_arc = std::sync::Arc::clone(auth_provider.handler());
            drop(auth_provider); // Release the tokio mutex early

            // Check if tickets need renewal and collect needed keys
            let renewal_info = {
                // Lock the handler to check if tickets need renewal
                let handler = handler_arc
                    .lock()
                    .map_err(|e| MonClientError::Other(format!("Failed to lock handler: {}", e)))?;

                if let Some(session) = handler.get_session() {
                    // Check each ticket handler to see if any need renewal
                    let mut needs_renewal = false;
                    let mut needed_keys = 0u32;

                    for (service_id, ticket_handler) in &session.ticket_handlers {
                        if ticket_handler.need_key() {
                            debug!(
                                "Service ticket for {} needs renewal (renew_after reached)",
                                match *service_id {
                                    auth::service_id::MON => "MON",
                                    auth::service_id::OSD => "OSD",
                                    auth::service_id::MDS => "MDS",
                                    auth::service_id::MGR => "MGR",
                                    _ => "UNKNOWN",
                                }
                            );
                            needs_renewal = true;
                            needed_keys |= *service_id;
                        }
                    }

                    if needs_renewal {
                        Some((session.global_id, needed_keys))
                    } else {
                        None
                    }
                } else {
                    None
                }
                // handler guard is dropped here
            };

            // If renewal is needed, build and send the request
            if let Some((global_id, needed_keys)) = renewal_info {
                debug!(
                    "Building ticket renewal request for services: 0x{:x}",
                    needed_keys
                );

                // Lock the handler again (mutably) to build the ticket renewal request
                let auth_payload = {
                    let mut handler_mut = handler_arc.lock().map_err(|e| {
                        MonClientError::Other(format!("Failed to lock handler: {}", e))
                    })?;

                    handler_mut.build_ticket_renewal_request(global_id, needed_keys)
                    // handler_mut guard is dropped here
                };

                match auth_payload {
                    Ok(auth_payload) => {
                        debug!("Built ticket renewal request: {} bytes", auth_payload.len());

                        // Create MAuth message with CEPHX protocol (2)
                        let mauth = crate::messages::MAuth::new(2, auth_payload);

                        // Convert to CephMessage and then to Message
                        let ceph_msg = msgr2::ceph_message::CephMessage::from_payload(
                            &mauth,
                            0, // features
                            msgr2::ceph_message::CrcFlags::ALL,
                        )
                        .map_err(|e| {
                            MonClientError::Other(format!(
                                "Failed to create MAuth message: {:?}",
                                e
                            ))
                        })?;

                        let msg = msgr2::message::Message::from_ceph_message(ceph_msg);

                        // Send the MAuth message to the monitor
                        if let Err(e) = active_con.send_message(msg).await {
                            warn!("Failed to send ticket renewal request: {:?}", e);
                        } else {
                            info!(
                                "Sent ticket renewal request for services: 0x{:x}",
                                needed_keys
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to build ticket renewal request: {:?}", e);
                    }
                }
            }
        }

        trace!("Tick completed");
        Ok(())
    }

    /// Dispatch received message to appropriate handler
    async fn dispatch_message(
        state: &Arc<RwLock<MonClientState>>,
        _keepalive_state: &Arc<Mutex<KeepaliveState>>,
        map_events: &broadcast::Sender<MapEvent>,
        monmap_notify: &Arc<tokio::sync::Notify>,
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
                Self::handle_monmap(state, map_events, monmap_notify, msg).await?;
            }
            msgr2::message::CEPH_MSG_PING => {
                trace!("Received CEPH_MSG_PING, sending PING_ACK");
                // Respond to monitor's ping with PING_ACK
                // Clone connection before async operation to avoid holding lock
                let active_con = {
                    let state_guard = state.read().await;
                    state_guard.active_con.clone()
                };

                if let Some(active_con) = active_con {
                    let ping_ack = msgr2::message::Message::ping_ack();
                    if let Err(e) = active_con.send_message(ping_ack).await {
                        warn!("Failed to send PING_ACK: {}", e);
                    }
                }
            }
            msgr2::message::CEPH_MSG_PING_ACK => {
                // PING/PING_ACK are not used between clients and monitors
                // Only monitors exchange PING messages with each other
                // Clients use Keepalive2 frames at the msgr2 protocol level
                trace!(
                    "Received unexpected CEPH_MSG_PING_ACK (monitors don't send these to clients)"
                );
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
                Self::handle_poolop_reply(state, map_events, msg).await?;
            }
            _ => {
                return Err(MonClientError::Other(format!(
                    "Received unknown message type 0x{:04x} - this is a bug! MonClient should only receive messages it subscribed for",
                    msg_type
                )));
            }
        }
        Ok(())
    }

    /// Handle MonMap message
    async fn handle_monmap(
        state: &Arc<RwLock<MonClientState>>,
        map_events: &broadcast::Sender<MapEvent>,
        monmap_notify: &Arc<tokio::sync::Notify>,
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

        let epoch = monmap.epoch;

        // Update state
        let mut state_guard = state.write().await;
        state_guard.monmap = monmap.clone();
        state_guard.want_monmap = false;

        // Mark subscription as received
        state_guard.subscriptions.got("monmap", monmap.epoch as u64);

        drop(state_guard);

        // Broadcast MonMap update event
        let _ = map_events.send(MapEvent::MonMapUpdated { epoch });

        // Notify waiters that MonMap has arrived
        monmap_notify.notify_waiters();

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
        _map_events: &broadcast::Sender<MapEvent>,
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
        tracing::debug!(
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

        tracing::trace!(
            "send_command: About to send command message with tid={}",
            tid
        );
        active_con.send_message(message).await?;
        tracing::trace!("send_command: Command message sent successfully, waiting for response");

        // Wait for response with timeout
        let result = tokio::time::timeout(self.config.command_timeout, rx)
            .await
            .map_err(|_| MonClientError::Timeout)?
            .map_err(|_| MonClientError::Other("Channel closed".into()))?;

        Ok(result)
    }

    /// Send a pool operation to the monitor cluster
    ///
    /// This is a low-level helper for sending MPoolOp messages.
    /// Most users should use OSDClient's create_pool() and delete_pool() methods instead.
    pub async fn send_poolop(&self, pool_name: String, msg: MPoolOp) -> Result<PoolOpResult> {
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

    /// Wait for authentication to complete
    ///
    /// This waits until the MonClient is authenticated with the monitor cluster
    /// and has received all service tickets (OSD, MDS, MGR, etc.).
    ///
    /// Should be called after init() to ensure authentication is fully complete
    /// before creating service clients like OSDClient.
    pub async fn wait_for_auth(&self, timeout: std::time::Duration) -> Result<()> {
        wait_for_condition(
            || async {
                if self.is_authenticated().await && self.get_service_auth_provider().await.is_some()
                {
                    info!("Authentication complete with service tickets available");
                    Some(())
                } else {
                    None
                }
            },
            &self.auth_notify,
            timeout,
            MonClientError::AuthenticationTimeout,
        )
        .await
    }

    /// Wait for MonMap to be received
    ///
    /// This waits until the MonClient has received a MonMap from the monitor cluster.
    /// The MonMap contains the cluster FSID and monitor addresses.
    ///
    /// Should be called after init() to ensure MonMap is available before
    /// creating service clients that need the FSID.
    pub async fn wait_for_monmap(&self, timeout: std::time::Duration) -> Result<()> {
        wait_for_condition(
            || async {
                let state = self.state.read().await;
                if !state.want_monmap && state.monmap.fsid != uuid::Uuid::nil() {
                    info!("MonMap received via event notification");
                    Some(())
                } else {
                    None
                }
            },
            &self.monmap_notify,
            timeout,
            MonClientError::Timeout,
        )
        .await
    }

    /// Get the cluster FSID
    pub async fn get_fsid(&self) -> UuidD {
        let state = self.state.read().await;
        UuidD::from_bytes(*state.monmap.fsid.as_bytes())
    }

    /// Get the global ID assigned during authentication
    ///
    /// Returns the global ID assigned by the monitor during authentication.
    /// Returns 0 if not authenticated yet.
    pub async fn get_global_id(&self) -> u64 {
        let state = self.state.read().await;
        state.global_id
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
        if let Some(conn) = state.active_con.as_ref() {
            conn.create_service_auth_provider().await
        } else {
            None
        }
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

/// Implement Dispatcher trait for MonClient to handle monitor-specific messages
#[async_trait]
impl Dispatcher for MonClient {
    async fn dispatch(
        &self,
        msg: msgr2::message::Message,
    ) -> std::result::Result<(), denc::RadosError> {
        // Use From trait for error conversion
        Self::dispatch_message(
            &self.state,
            &self.keepalive_state,
            &self.map_events,
            &self.monmap_notify,
            msg,
        )
        .await
        .map_err(Into::into)
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

        let message_bus = Arc::new(msgr2::MessageBus::new());
        let client = MonClient::new(config, message_bus).await.unwrap();
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_subscription() {
        let config = MonClientConfig {
            entity_name: "client.test".to_string(),
            mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
            ..Default::default()
        };

        let message_bus = Arc::new(msgr2::MessageBus::new());
        let client = MonClient::new(config, message_bus).await.unwrap();

        // Should fail before init
        assert!(client.subscribe("osdmap", 0, 0).await.is_err());
    }
}
