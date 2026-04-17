//! Monitor client implementation
//!
//! Main MonClient struct and implementation.

use crate::EntityName;
use crate::UuidD;
use crate::monclient::connection::{KeepalivePolicy, MonConnection, MonConnectionParams};
use crate::monclient::defaults;
use crate::monclient::error::{MonClientError, Result};
use crate::monclient::messages::*;
use crate::monclient::monmap::MonMapState;
use crate::monclient::subscription::{MonService, MonSub};
use crate::monclient::types::CommandResult;
use crate::monclient::wait_helper::wait_for_condition;
use crate::msgr2::MapSender;
use crate::msgr2::ceph_message::{CephMessage, CrcFlags};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

/// Decode a typed message payload from a raw msgr2 message.
///
/// This helper eliminates the repeated pattern of constructing a dummy CephMsgHeader
/// just to call decode_payload on Denc-derived messages.
fn decode_message<T: crate::msgr2::ceph_message::CephMessagePayload>(
    msg: &crate::msgr2::message::Message,
) -> std::result::Result<T, crate::msgr2::Msgr2Error> {
    use crate::msgr2::ceph_message::CephMsgHeader;
    let header = CephMsgHeader::new(T::msg_type(), T::msg_version(0));
    T::decode_payload(&header, &msg.front, &msg.middle, &msg.data)
}

/// Encode a `CephMessagePayload` into a wire `Message`, stamp it with `tid`, and send it.
///
/// This is the common send-only primitive used by `send_and_wait` (for `get_version` and
/// `send_poolop`), by `invoke` (for the initial command send and on-reconnect resend),
/// and by the reconnect resend loop.  It is a free function because it needs no
/// `MonClient` state — only the connection and the message data.
async fn send_msg_with_tid(
    con: &MonConnection,
    msg: &impl crate::msgr2::ceph_message::CephMessagePayload,
    tid: u64,
) -> Result<()> {
    let ceph_msg = CephMessage::from_payload(msg, 0, CrcFlags::ALL)?;
    let mut message = crate::msgr2::message::Message::from_ceph_message(ceph_msg);
    message.header.set_tid(tid);
    con.send_message(message).await
}

/// Broadcast channel capacity for map events (MOSDMap, MConfig, etc.)
const MAP_EVENT_BROADCAST_CAPACITY: usize = 100;
/// Message channel capacity for monitor messages
const MON_MESSAGE_CHANNEL_CAPACITY: usize = 256;

/// Monitor client configuration
#[derive(Debug, Clone)]
pub struct MonClientConfig {
    /// Initial monitor addresses
    pub mon_addrs: Vec<String>,

    /// Authentication configuration
    /// If None, will attempt to auto-detect from /etc/ceph/ceph.conf
    pub auth: Option<crate::monclient::auth_config::AuthConfig>,

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

    /// DNS SRV service name for monitor discovery
    /// When mon_addrs is empty, the client will attempt to discover monitors
    /// via DNS SRV records using this service name.
    /// Default: "ceph-mon" (queries `_ceph-mon._tcp`)
    /// May include a domain suffix separated by `_`,
    /// e.g., `"ceph-mon_example.com"` queries `_ceph-mon._tcp.example.com`.
    pub dns_srv_name: String,
}

impl Default for MonClientConfig {
    fn default() -> Self {
        Self {
            mon_addrs: Vec::new(),
            auth: None, // Will auto-detect from /etc/ceph/ceph.conf
            command_timeout: defaults::COMMAND_TIMEOUT,
            hunt_interval: defaults::HUNT_INTERVAL,
            hunt_parallel: defaults::HUNT_PARALLEL,
            keepalive_interval: defaults::KEEPALIVE_INTERVAL,
            keepalive_timeout: defaults::KEEPALIVE_TIMEOUT,
            tick_interval: None, // Defaults to hunt_interval
            hunt_interval_backoff: defaults::HUNT_INTERVAL_BACKOFF,
            hunt_interval_min_multiple: defaults::HUNT_INTERVAL_MIN_MULTIPLE,
            hunt_interval_max_multiple: defaults::HUNT_INTERVAL_MAX_MULTIPLE,
            dns_srv_name: crate::monclient::dns_srv::DEFAULT_MON_DNS_SRV_NAME.to_string(),
        }
    }
}

/// Monitor client
#[derive(Clone)]
pub struct MonClient {
    config: MonClientConfig,
    entity_name: EntityName,
    connection_state: Arc<RwLock<ConnectionState>>,
    monmap_state: Arc<RwLock<MonMapHolder>>,
    subscription_state: Arc<RwLock<MonSub>>,

    commands: Arc<DashMap<u64, CommandTracker>>,
    last_command_tid: Arc<AtomicU64>,

    pool_ops: Arc<DashMap<u64, PoolOpTracker>>,
    last_poolop_tid: Arc<AtomicU64>,

    version_requests: Arc<DashMap<u64, VersionTracker>>,
    last_version_req_id: Arc<AtomicU64>,

    auth_state: Arc<RwLock<AuthState>>,
    runtime_config: Arc<RwLock<RuntimeMonClientConfig>>,
    latest_osdmap: Arc<RwLock<Option<crate::monclient::MOSDMap>>>,

    /// Tracks background tasks (tick loop + drain loop) for graceful shutdown.
    tracker: TaskTracker,

    /// Cancel to stop all background tasks
    shutdown_token: CancellationToken,

    map_events: broadcast::Sender<MapEvent>,

    /// None if no OSDClient is integrated (MonClient-only usage)
    osdmap_tx: Option<MapSender<MOSDMap>>,

    mon_msg_tx: mpsc::Sender<crate::msgr2::message::Message>,
    auth_notify: Arc<tokio::sync::Notify>,
    monmap_notify: Arc<tokio::sync::Notify>,
}

/// Events for map updates
#[derive(Debug, Clone)]
pub enum MapEvent {
    MonMapUpdated { epoch: u32 },
    OsdMapUpdated { epoch: u64 },
    MgrMapUpdated { epoch: u64 },
    MdsMapUpdated { epoch: u64 },
    ConfigUpdated { keys: Vec<String> },
}

crate::runtime_config_options! {
    #[derive(Debug, Clone, Copy)]
    struct RuntimeMonClientConfig {
    mon_client_hunt_interval: Duration,
    mon_client_ping_interval: Duration,
    mon_client_ping_timeout: Duration,
    rados_mon_op_timeout: Duration,
}
}

impl RuntimeMonClientConfig {
    fn from_config(config: &MonClientConfig) -> Self {
        Self {
            mon_client_hunt_interval: config.hunt_interval,
            mon_client_ping_interval: config.keepalive_interval,
            mon_client_ping_timeout: config.keepalive_timeout,
            rados_mon_op_timeout: config.command_timeout,
        }
    }
}

/// Connection state (active connection and hunting state)
struct ConnectionState {
    active_con: Option<Arc<MonConnection>>,
    hunting: bool,

    /// Current backoff multiplier for hunt interval
    reopen_interval_multiplier: f64,
    last_hunt_attempt: Option<std::time::Instant>,
    /// Whether we've ever had a successful connection (for backoff logic)
    had_a_connection: bool,
}

/// MonMap holder state
struct MonMapHolder {
    monmap: MonMapState,
    want_monmap: bool,
}

/// Authentication state
struct AuthState {
    authenticated: bool,
    global_id: u64,
    initialized: bool,
}

struct CommandTracker {
    result_tx: oneshot::Sender<CommandResult>,
    /// Original command payload, kept so it can be resent after a reconnect.
    cmd: MMonCommand,
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
    result_tx: oneshot::Sender<PoolOpResult>,
}

struct VersionTracker {
    result_tx: oneshot::Sender<(u64, u64)>,
}

impl MonClient {
    /// Create a new MonClient with optional OSDMap routing
    ///
    /// # Arguments
    ///
    /// * `config` - MonClient configuration
    /// * `osdmap_tx` - Optional channel for routing MOSDMap messages to OSDClient.
    ///   Pass `None` for MonClient-only usage (e.g., `ceph` CLI command operations)
    pub async fn new(
        config: MonClientConfig,
        osdmap_tx: Option<MapSender<MOSDMap>>,
    ) -> std::result::Result<Arc<Self>, MonClientError> {
        // Get auth config (use default if not provided, which tries /etc/ceph/ceph.conf)
        let auth_config = config.auth.clone().unwrap_or_default();

        // Parse entity name from auth config
        let entity_name: EntityName = auth_config
            .entity_name()
            .parse()
            .map_err(|e| MonClientError::Other(format!("Invalid entity name: {e}")))?;

        // Build initial monmap from config or DNS SRV discovery
        let monmap = if !config.mon_addrs.is_empty() {
            info!("Building monmap from config");
            MonMapState::build_initial(&config.mon_addrs)?
        } else {
            info!(
                "No monitor addresses configured, trying DNS SRV discovery with service name: {}",
                config.dns_srv_name
            );
            crate::monclient::dns_srv::resolve_mon_addrs_via_dns_srv(&config.dns_srv_name).await?
        };

        info!("Initial monmap has {} monitors", monmap.len());

        // Create event broadcaster
        let (map_events, _) = broadcast::channel(MAP_EVENT_BROADCAST_CAPACITY);

        // Create channel for monitor messages (256 slots — monitors are low-rate senders)
        let (mon_msg_tx, mut mon_msg_rx) = mpsc::channel(MON_MESSAGE_CHANNEL_CAPACITY);

        // Initialize separate state components
        let connection_state = ConnectionState {
            active_con: None,
            hunting: false,
            reopen_interval_multiplier: config.hunt_interval_min_multiple,
            last_hunt_attempt: None,
            had_a_connection: false,
        };

        let monmap_state = MonMapHolder {
            monmap,
            want_monmap: true,
        };

        let auth_state = AuthState {
            authenticated: false,
            global_id: 0,
            initialized: false,
        };

        let runtime_config_init = RuntimeMonClientConfig::from_config(&config);
        let shutdown_token = CancellationToken::new();

        let client = Arc::new(Self {
            config,
            entity_name,
            connection_state: Arc::new(RwLock::new(connection_state)),
            monmap_state: Arc::new(RwLock::new(monmap_state)),
            subscription_state: Arc::new(RwLock::new(MonSub::new())),
            commands: Arc::new(DashMap::new()),
            last_command_tid: Arc::new(AtomicU64::new(0)),
            pool_ops: Arc::new(DashMap::new()),
            last_poolop_tid: Arc::new(AtomicU64::new(0)),
            version_requests: Arc::new(DashMap::new()),
            last_version_req_id: Arc::new(AtomicU64::new(0)),
            auth_state: Arc::new(RwLock::new(auth_state)),
            runtime_config: Arc::new(RwLock::new(runtime_config_init)),
            latest_osdmap: Arc::new(RwLock::new(None)),
            tracker: TaskTracker::new(),
            shutdown_token: shutdown_token.clone(),
            map_events,
            osdmap_tx,
            mon_msg_tx,
            auth_notify: Arc::new(tokio::sync::Notify::new()),
            monmap_notify: Arc::new(tokio::sync::Notify::new()),
        });

        // Spawn drain task for monitor messages.
        // run_until_cancelled wraps recv() in Option<Option<_>>:
        //   None         → token cancelled (shutdown)
        //   Some(None)   → channel closed (MonClient dropped)
        //   Some(Some(m))→ message ready
        let client_weak = Arc::downgrade(&client);
        let drain_token = shutdown_token.clone();
        client.tracker.spawn(async move {
            while let Some(Some(msg)) = drain_token.run_until_cancelled(mon_msg_rx.recv()).await {
                if let Some(client_arc) = client_weak.upgrade() {
                    if let Err(e) = client_arc.dispatch_message(msg).await {
                        error!("Failed to dispatch monitor message: {}", e);
                    }
                } else {
                    info!("MonClient dropped, terminating drain task");
                    break;
                }
                // Yield after each message to avoid starving other tasks.
                tokio::task::yield_now().await;
            }
            info!("MonClient drain task terminated");
        });

        Ok(client)
    }

    /// Initialize and connect to monitors
    pub async fn init(&self) -> Result<()> {
        let mut auth_state = self.auth_state.write().await;

        if auth_state.initialized {
            return Err(MonClientError::AlreadyInitialized);
        }

        info!("Initializing MonClient for {}", self.entity_name);

        // Mark as initialized
        auth_state.initialized = true;
        drop(auth_state);

        // Start hunting for monitors
        {
            let mut conn_state = self.connection_state.write().await;
            conn_state.hunting = true;
        }

        // Start tick loop for periodic keepalive and auth renewal
        self.start_tick_loop().await?;

        // Start hunting process (connects to monitor)
        self.start_hunting().await?;

        // Send initial subscriptions (monmap and config)
        info!("Subscribing to monmap...");
        self.subscribe(MonService::MonMap, 0, 0).await?;
        info!("Subscribing to config...");
        self.subscribe(MonService::Config, 0, 0).await?;

        // OSDMap subscription is handled by the application after OSDClient is ready

        info!("MonClient initialized successfully");
        Ok(())
    }

    /// Shutdown the client
    pub async fn shutdown(&self) -> Result<()> {
        if self.shutdown_token.is_cancelled() {
            return Ok(());
        }

        info!("Shutting down MonClient");

        // Signal shutdown immediately so that concurrent tick() / drain loops
        // notice before we start tearing down connections.
        // Mirrors C++ MonClient::shutdown() setting `stopping = true` first.
        // See: https://github.com/ceph/ceph/pull/64756
        self.shutdown_token.cancel();

        // Cancel all pending operations by clearing their trackers.
        // Callers blocked on rx.await will receive a RecvError ("Channel closed"),
        // which they handle as an error. This matches C++ MonClient::shutdown()
        // which cancels pending version_requests, commands, and pool_ops.
        self.version_requests.clear();
        self.commands.clear();
        self.pool_ops.clear();

        // Take the active connection to close it after releasing the lock
        let active_con = {
            let mut conn_state = self.connection_state.write().await;
            conn_state.active_con.take()
        };

        // Close connection after releasing the lock (close() now awaits task termination)
        if let Some(con) = active_con {
            con.close().await?;
        }

        // Await all background tasks (already signalled by the early cancel above).
        self.tracker.close();
        self.tracker.wait().await;

        info!("MonClient shutdown complete");
        Ok(())
    }

    /// Start hunting for an available monitor
    async fn start_hunting(&self) -> Result<()> {
        info!("Starting monitor hunt");

        // Apply backoff delay if we recently tried hunting
        let delay = {
            let conn_state = self.connection_state.read().await;
            let runtime_config = self.runtime_config.read().await;
            self.compute_hunt_backoff_delay(&conn_state, &runtime_config)
        };

        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        // Record hunt attempt time before trying
        let hunt_start = std::time::Instant::now();

        let monmap = {
            let monmap_state = self.monmap_state.read().await;
            monmap_state.monmap.clone()
        };

        // Select monitors by priority and shuffle them
        let selected_ranks = self.select_monitors_by_priority(&monmap)?;

        // Try connecting to monitors in parallel
        let hunt_result = self.connect_parallel(&selected_ranks).await;

        // Update backoff state based on hunt result
        self.update_hunt_backoff(hunt_start, hunt_result.is_err())
            .await;

        hunt_result
    }

    /// Select monitors by priority and apply weighted shuffling
    ///
    /// Returns a list of monitor ranks to try, ordered by weighted random selection.
    fn select_monitors_by_priority(&self, monmap: &MonMapState) -> Result<Vec<usize>> {
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

        Ok(self.weighted_shuffle(ranks, monmap))
    }

    /// Shuffle monitor ranks using weighted random selection
    ///
    /// If all weights are zero, uses uniform random shuffling.
    /// Otherwise, uses Fisher-Yates with weighted selection.
    fn weighted_shuffle(&self, ranks: &[usize], monmap: &MonMapState) -> Vec<usize> {
        let mut selected_ranks = ranks.to_vec();

        if selected_ranks.len() <= 1 {
            return selected_ranks;
        }

        // Collect weights for all ranks
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
            // Use weighted random selection (Fisher-Yates with weighted selection)
            use rand::distributions::WeightedIndex;
            use rand::prelude::*;
            let mut rng = rand::thread_rng();

            let mut shuffled = Vec::with_capacity(selected_ranks.len());
            let mut remaining_ranks = selected_ranks;
            let mut remaining_weights = weights;

            while !remaining_ranks.is_empty() {
                let idx = match WeightedIndex::new(&remaining_weights) {
                    Ok(dist) => dist.sample(&mut rng),
                    Err(_) => {
                        // Remaining monitors all have zero weight — pick uniformly.
                        rng.gen_range(0..remaining_ranks.len())
                    }
                };
                shuffled.push(remaining_ranks.remove(idx));
                remaining_weights.remove(idx);
            }

            selected_ranks = shuffled;
        }

        selected_ranks
    }

    /// Attempt to connect to monitors in parallel
    ///
    /// Tries up to `hunt_parallel` monitors concurrently.
    /// Returns success if any connection succeeds.
    async fn connect_parallel(&self, selected_ranks: &[usize]) -> Result<()> {
        let hunt_parallel = self.config.hunt_parallel;

        // Determine how many monitors to try in parallel
        let n = if hunt_parallel == 0 || hunt_parallel > selected_ranks.len() {
            selected_ranks.len()
        } else {
            hunt_parallel
        };

        if n == 1 {
            return self.connect_to_mon(selected_ranks[0]).await;
        }

        // Parallel case: JoinSet aborts remaining tasks on drop after a winner.
        let mut set = tokio::task::JoinSet::new();
        for &rank in selected_ranks.iter().take(n) {
            let client = self.clone();
            set.spawn(async move { client.connect_to_mon(rank).await });
        }

        let mut last_err = MonClientError::NotConnected;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(())) => {
                    info!("Successfully connected to a monitor");
                    // JoinSet::drop aborts remaining tasks.
                    return Ok(());
                }
                Ok(Err(e)) => last_err = e,
                Err(e) => last_err = MonClientError::Other(e.to_string()),
            }
        }
        Err(last_err)
    }

    /// Update hunt backoff state after a hunt attempt
    async fn update_hunt_backoff(&self, hunt_start: std::time::Instant, hunt_failed: bool) {
        let mut conn_state = self.connection_state.write().await;
        conn_state.last_hunt_attempt = Some(hunt_start);

        if hunt_failed && conn_state.had_a_connection {
            // Hunt failed - increase backoff for next attempt
            conn_state.reopen_interval_multiplier *= self.config.hunt_interval_backoff;
            if conn_state.reopen_interval_multiplier > self.config.hunt_interval_max_multiple {
                conn_state.reopen_interval_multiplier = self.config.hunt_interval_max_multiple;
            }
            debug!(
                "Hunt failed, increased backoff multiplier to {:.2}",
                conn_state.reopen_interval_multiplier
            );
        }
    }

    /// Compute the backoff delay before the next hunt attempt.
    ///
    /// Returns `Some(duration)` if we should wait before hunting again,
    /// or `None` if we can hunt immediately.
    fn compute_hunt_backoff_delay(
        &self,
        conn_state: &ConnectionState,
        runtime_config: &RuntimeMonClientConfig,
    ) -> Option<Duration> {
        if !conn_state.had_a_connection {
            return None;
        }
        let last_attempt = conn_state.last_hunt_attempt?;
        let elapsed = last_attempt.elapsed();
        let hunt_delay = runtime_config
            .mon_client_hunt_interval
            .mul_f64(conn_state.reopen_interval_multiplier);

        if elapsed >= hunt_delay {
            return None;
        }

        let remaining = hunt_delay - elapsed;
        debug!(
            "Applying hunting backoff: waiting {:?} (multiplier: {:.2})",
            remaining, conn_state.reopen_interval_multiplier
        );
        Some(remaining)
    }

    /// Connect to a specific monitor
    async fn connect_to_mon(&self, rank: usize) -> Result<()> {
        let (mon_info, runtime_config) = {
            let monmap_state = self.monmap_state.read().await;
            let mon_info = monmap_state
                .monmap
                .get_mon(rank)
                .ok_or(MonClientError::InvalidMonitorRank(rank))?;
            let runtime_config = self.runtime_config.read().await;
            (mon_info.clone(), *runtime_config)
        };

        // Get msgr2 address
        let addr = mon_info
            .addrs
            .get_msgr2()
            .ok_or(MonClientError::InvalidMonMap("No msgr2 address".into()))?;

        let socket_addr = addr.to_socket_addr().ok_or(MonClientError::InvalidMonMap(
            "No socket addr for msgr2 address".into(),
        ))?;

        info!("Connecting to mon.{} at {:?}", rank, socket_addr);

        let auth_config = self.config.auth.clone().unwrap_or_default();
        let auth_provider = auth_config.clone_provider();

        // Create keepalive policy from config
        let keepalive_policy = if runtime_config.mon_client_ping_interval > Duration::ZERO {
            KeepalivePolicy::new(
                runtime_config.mon_client_ping_interval,
                runtime_config.mon_client_ping_timeout,
            )
        } else {
            KeepalivePolicy::disabled()
        };

        // Create actual msgr2 connection
        let mon_con = Arc::new(
            MonConnection::connect(MonConnectionParams {
                addr: socket_addr,
                rank,
                auth_provider,
                keepalive_policy,
                osdmap_tx: self.osdmap_tx.clone(),
                mon_msg_tx: self.mon_msg_tx.clone(),
            })
            .await?,
        );

        let global_id = mon_con.global_id();
        debug!("Retrieved global_id {} from MonConnection", global_id);

        // Store as active connection (but check if we won the race)
        let won_race = {
            let mut conn_state = self.connection_state.write().await;

            // If we're no longer hunting, another connection won the race
            if !conn_state.hunting {
                debug!(
                    "Connection to mon.{} succeeded but another monitor already won the hunt",
                    rank
                );
                false
            } else {
                // We won the race - set this as the active connection
                conn_state.active_con = Some(Arc::clone(&mon_con));
                conn_state.hunting = false;
                conn_state.had_a_connection = true;

                // Un-backoff: reduce the backoff multiplier on successful connection
                let old_multiplier = conn_state.reopen_interval_multiplier;
                conn_state.reopen_interval_multiplier = (conn_state.reopen_interval_multiplier
                    / self.config.hunt_interval_backoff)
                    .max(self.config.hunt_interval_min_multiple);

                if old_multiplier != conn_state.reopen_interval_multiplier {
                    debug!(
                        "Un-backoff: reduced multiplier from {:.2} to {:.2}",
                        old_multiplier, conn_state.reopen_interval_multiplier
                    );
                }

                true
            }
        };

        if !won_race {
            mon_con.close().await?;
            return Ok(());
        }

        // Update auth state (authentication completed during establish_session)
        {
            let mut auth_state = self.auth_state.write().await;
            auth_state.authenticated = true;
            auth_state.global_id = global_id;
        }

        // Move previously-sent subscriptions back to pending so they are resent on reconnect.
        // MON_STATEFUL_SUB suppresses periodic renewal churn, but a fresh monitor session still
        // needs the current subscription set replayed.
        let should_send_subscriptions = {
            let mut sub_state = self.subscription_state.write().await;
            sub_state.reload()
        };

        // Notify waiters that authentication is complete (after releasing lock)
        self.auth_notify.notify_waiters();

        if should_send_subscriptions {
            self.send_subscriptions().await?;
        }

        // Resend any commands that were in-flight when the previous connection dropped.
        // Mirrors C++ MonClient::_resend_mon_commands(): iterate pending commands and
        // retransmit each on the new connection.  The callers' oneshot receivers remain
        // open, so they transparently receive the reply without knowing a reconnect
        // occurred.  If a resend itself fails we log and skip — the caller will time out.
        let to_resend: Vec<(u64, MMonCommand)> = self
            .commands
            .iter()
            .map(|e| (*e.key(), e.value().cmd.clone()))
            .collect();
        if !to_resend.is_empty() {
            info!(
                "Resending {} pending command(s) after reconnect",
                to_resend.len()
            );
            for (tid, cmd) in to_resend {
                if let Err(e) = send_msg_with_tid(&mon_con, &cmd, tid).await {
                    warn!("Failed to resend command tid={}: {}", tid, e);
                }
            }
        }

        info!("Successfully connected to mon.{}", rank);
        Ok(())
    }

    /// Subscribe to a cluster map
    pub async fn subscribe(&self, what: MonService, start: u64, flags: u8) -> Result<()> {
        {
            let auth_state = self.auth_state.read().await;
            if !auth_state.initialized {
                return Err(MonClientError::NotInitialized);
            }
        }

        debug!("Subscribing to {} from version {}", what, start);

        let is_new = {
            let mut sub_state = self.subscription_state.write().await;
            sub_state.want(what, start, flags)
        };

        if is_new {
            let has_connection = {
                let conn_state = self.connection_state.read().await;
                conn_state.active_con.is_some()
            };
            if has_connection {
                self.send_subscriptions().await?;
            }
        }

        Ok(())
    }

    /// Unsubscribe from a cluster map
    pub async fn unsubscribe(&self, what: MonService) -> Result<()> {
        let mut sub_state = self.subscription_state.write().await;
        debug!("Unsubscribing from {}", what);
        sub_state.unwant(what);
        Ok(())
    }

    /// Notify that a map has been received
    ///
    /// Called by clients (e.g., OSDClient) after successfully processing a map update.
    /// This updates subscription tracking and triggers renewal if needed.
    ///
    /// # Arguments
    ///
    /// * `what` - Monitor service to update
    /// * `epoch` - Epoch of the received map
    pub async fn notify_map_received(&self, what: MonService, epoch: u64) -> Result<()> {
        debug!("Map received: {} epoch {}", what, epoch);

        let supports_stateful_subscriptions = {
            let conn_state = self.connection_state.read().await;
            conn_state
                .active_con
                .as_ref()
                .is_some_and(|con| con.supports_stateful_subscriptions())
        };

        // Update subscription tracking - this increments the start epoch.
        // Modern monitors with MON_STATEFUL_SUB keep the subscription state server-side and
        // do not require legacy renewal traffic after each map update.
        let need_renew = {
            let mut sub_state = self.subscription_state.write().await;
            sub_state.got(what, epoch);
            let legacy_renew_due = sub_state.need_renew();

            if supports_stateful_subscriptions && legacy_renew_due {
                debug!(
                    "Skipping legacy subscription renewal after {} epoch {} because active monitor negotiated MON_STATEFUL_SUB",
                    what, epoch
                );
            }

            !supports_stateful_subscriptions && legacy_renew_due
        };

        // Check if subscriptions need renewal
        if need_renew {
            debug!(
                "Subscriptions need renewal after receiving {} epoch {}",
                what, epoch
            );
            self.send_subscriptions().await?;
        }

        Ok(())
    }

    /// Send pending subscriptions
    async fn send_subscriptions(&self) -> Result<()> {
        let (active_con, msg) = {
            let mut sub_state = self.subscription_state.write().await;

            if !sub_state.have_new() {
                return Ok(());
            }

            let conn_state = self.connection_state.read().await;
            let active_con = conn_state
                .active_con
                .as_ref()
                .ok_or(MonClientError::NotConnected)?
                .clone();
            drop(conn_state);

            // Build subscription message
            let mut msg = MMonSubscribe::new();
            for (&what, item) in sub_state.get_subs() {
                msg.add(what, *item);
            }

            sub_state.renewed();

            (active_con, msg)
        };

        send_msg_with_tid(&active_con, &msg, 0).await?;

        debug!("Sent subscriptions");
        Ok(())
    }

    async fn command_timeout(&self) -> Duration {
        let runtime_config = self.runtime_config.read().await;
        runtime_config.rados_mon_op_timeout
    }

    /// Send a CephMessage with a tid and wait for a response on the given oneshot receiver.
    ///
    /// This helper encapsulates the common send-and-wait-with-timeout pattern
    /// shared by get_version, send_command, and send_poolop.
    async fn send_and_wait<T>(
        &self,
        active_con: &MonConnection,
        msg: &impl crate::msgr2::ceph_message::CephMessagePayload,
        tid: u64,
        rx: oneshot::Receiver<T>,
    ) -> Result<T> {
        send_msg_with_tid(active_con, msg, tid).await?;
        tokio::time::timeout(self.command_timeout().await, rx)
            .await
            .map_err(|_| MonClientError::Timeout)?
            .map_err(|_| MonClientError::Other("Channel closed".into()))
    }

    /// Check that the client is initialized and connected, returning the active connection.
    ///
    /// This is the common preamble for request methods that need to send a message.
    async fn require_active_con(&self) -> Result<Arc<MonConnection>> {
        let auth_state = self.auth_state.read().await;
        if !auth_state.initialized {
            return Err(MonClientError::NotInitialized);
        }
        drop(auth_state);

        let conn_state = self.connection_state.read().await;
        conn_state
            .active_con
            .as_ref()
            .cloned()
            .ok_or(MonClientError::NotConnected)
    }

    /// Start background tick loop for periodic maintenance
    async fn start_tick_loop(&self) -> Result<()> {
        let connection_state = Arc::clone(&self.connection_state);
        let runtime_config = Arc::clone(&self.runtime_config);
        let self_clone = self.clone();
        let tick_token = self.shutdown_token.clone();

        // Explicit tick_interval overrides adaptive scheduling when set
        let explicit_tick_interval = self.config.tick_interval;

        // Spawn into TaskTracker so shutdown() can await all tasks together.
        self.tracker.spawn(async move {
            info!("Tick loop started");

            // run_until_cancelled returns None when the token fires, Some(()) after
            // sleep elapses.  This collapses the select! + is_cancelled guard into a
            // single expression with unambiguous semantics.
            while tick_token
                .run_until_cancelled(async {
                    // Adaptive tick interval: hunt faster when hunting, slower when
                    // connected.  Matches C++ MonClient::schedule_tick() logic.
                    let interval = match explicit_tick_interval {
                        Some(explicit) => explicit,
                        None => {
                            let conn_state = connection_state.read().await;
                            let runtime_cfg = runtime_config.read().await;
                            if conn_state.hunting {
                                runtime_cfg
                                    .mon_client_hunt_interval
                                    .mul_f64(conn_state.reopen_interval_multiplier)
                            } else {
                                runtime_cfg.mon_client_ping_interval
                            }
                        }
                    };
                    tokio::time::sleep(interval).await;
                })
                .await
                .is_some()
            {
                if let Err(e) = self_clone.tick().await {
                    error!("Error in tick: {}", e);
                }
                tokio::task::yield_now().await;
            }

            info!("Tick loop terminated");
        });
        info!("Started tick loop");
        Ok(())
    }

    /// Periodic maintenance tick
    async fn tick(&self) -> Result<()> {
        if self.shutdown_token.is_cancelled() {
            debug!("Skipping tick during shutdown");
            return Ok(());
        }

        let (is_hunting, active_con_opt) = {
            let conn_state = self.connection_state.read().await;
            (
                conn_state.hunting,
                conn_state.active_con.as_ref().map(Arc::clone),
            )
        };

        if is_hunting {
            info!("Continuing hunt in tick");
            return self.start_hunting().await;
        }

        let Some(active_con) = active_con_opt else {
            debug!("No active connection in tick (not hunting)");
            return Ok(());
        };

        // Detect unexpected I/O task death (connection died without explicit shutdown).
        // The background task exits on keepalive timeout, send errors, or recv errors.
        if !self.shutdown_token.is_cancelled() && active_con.is_task_finished().await {
            warn!(
                "Connection to mon.{} lost (I/O task exited), hunting for new monitor",
                active_con.rank()
            );

            {
                let mut conn_state = self.connection_state.write().await;
                conn_state.active_con = None;
                conn_state.hunting = true;
            }
            {
                let mut auth_state = self.auth_state.write().await;
                auth_state.authenticated = false;
            }

            self.start_hunting().await?;
            return Ok(());
        }

        // Check if auth tickets need renewal
        if let Err(e) = self.check_auth_tickets(&active_con).await {
            warn!("Auth ticket renewal check failed: {:?}", e);
        }

        trace!("Tick completed");
        Ok(())
    }

    /// Check if auth tickets need renewal and send renewal requests if so.
    async fn check_auth_tickets(&self, active_con: &MonConnection) -> Result<()> {
        let Some(handler_arc) = active_con.get_auth_handler() else {
            return Ok(());
        };

        let lock_err = |e| MonClientError::Other(format!("Failed to lock auth handler: {e}"));

        // Check which tickets need renewal and build the request in a single lock.
        let auth_payload = {
            let mut handler = handler_arc.lock().map_err(lock_err)?;
            let Some(session) = handler.get_session() else {
                return Ok(());
            };

            let mut needed_keys = crate::auth::EntityType::empty();
            for (service_type, ticket_handler) in &session.ticket_handlers {
                if ticket_handler.need_key() {
                    debug!(
                        "Service ticket for {:?} needs renewal (renew_after reached)",
                        *service_type
                    );
                    needed_keys |= *service_type;
                }
            }

            if needed_keys.is_empty() {
                return Ok(());
            }

            let global_id = session.global_id;
            debug!(
                "Building ticket renewal request for services: {:?}",
                needed_keys
            );

            match handler.build_ticket_renewal_request(global_id, needed_keys) {
                Ok(payload) => payload,
                Err(e) => {
                    warn!("Failed to build ticket renewal request: {:?}", e);
                    return Ok(());
                }
            }
        };

        debug!("Built ticket renewal request: {} bytes", auth_payload.len());

        let mauth = crate::monclient::messages::MAuth::new(
            crate::auth::protocol::CEPH_AUTH_CEPHX,
            auth_payload,
        );
        if let Err(e) = send_msg_with_tid(active_con, &mauth, 0).await {
            warn!("Failed to send ticket renewal request: {:?}", e);
        } else {
            info!("Sent ticket renewal request");
        }

        Ok(())
    }

    /// Dispatch received message to appropriate handler
    async fn dispatch_message(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        use crate::msgr2::message::{
            CEPH_MSG_CONFIG, CEPH_MSG_MON_COMMAND_ACK, CEPH_MSG_MON_MAP, CEPH_MSG_OSD_MAP,
            CEPH_MSG_PING, CEPH_MSG_PING_ACK, CEPH_MSG_POOLOP_REPLY,
        };

        let msg_type = msg.msg_type();
        debug!(
            "Dispatching message type: 0x{:04x} ({}), front.len()={}",
            msg_type,
            msg_type,
            msg.front.len()
        );

        match msg_type {
            CEPH_MSG_MON_MAP => {
                info!("Received CEPH_MSG_MON_MAP");
                self.handle_monmap(msg).await?;
            }
            CEPH_MSG_PING => {
                trace!("Received CEPH_MSG_PING, sending PING_ACK");
                let active_con = {
                    let conn_state = self.connection_state.read().await;
                    conn_state.active_con.clone()
                };

                if let Some(active_con) = active_con {
                    let ping_ack = crate::msgr2::message::Message::ping_ack();
                    if let Err(e) = active_con.send_message(ping_ack).await {
                        warn!("Failed to send PING_ACK: {}", e);
                    }
                }
            }
            CEPH_MSG_PING_ACK => {
                // Clients use Keepalive2 frames at the msgr2 protocol level;
                // monitors don't send PING_ACK to clients.
                trace!(
                    "Received unexpected CEPH_MSG_PING_ACK (monitors don't send these to clients)"
                );
            }
            CEPH_MSG_MON_SUBSCRIBE_ACK => {
                info!("Received CEPH_MSG_MON_SUBSCRIBE_ACK");
                self.handle_subscribe_ack(msg).await?;
            }
            CEPH_MSG_MON_GET_VERSION_REPLY => {
                debug!("Received CEPH_MSG_MON_GET_VERSION_REPLY");
                self.handle_version_reply(msg).await?;
            }
            CEPH_MSG_MON_COMMAND_ACK => {
                debug!("Received CEPH_MSG_MON_COMMAND_ACK");
                self.handle_command_ack(msg).await?;
            }
            CEPH_MSG_POOLOP_REPLY => {
                debug!("Received CEPH_MSG_POOLOP_REPLY");
                self.handle_poolop_reply(msg).await?;
            }
            CEPH_MSG_CONFIG => {
                debug!("Received CEPH_MSG_CONFIG");
                self.handle_config(msg).await?;
            }
            CEPH_MSG_OSD_MAP => {
                debug!("Received CEPH_MSG_OSD_MAP");
                self.handle_osdmap(msg).await?;
            }
            _ => {
                return Err(MonClientError::Other(format!(
                    "Received unknown message type 0x{msg_type:04x} - this is a bug! MonClient should only receive messages it subscribed for"
                )));
            }
        }
        Ok(())
    }

    /// Handle MonMap message
    async fn handle_monmap(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        let mmonmap: MMonMap = decode_message(&msg)?;
        debug!("Received monmap blob: {} bytes", mmonmap.monmap_bl.len());

        // Decode the actual MonMap
        let monmap = MonMapState::decode(&mmonmap.monmap_bl)?;
        info!(
            "Decoded MonMapState: epoch={}, fsid={}, {} monitors",
            monmap.epoch,
            monmap.fsid,
            monmap.len()
        );

        let epoch = monmap.epoch;

        // Mark subscription as received
        {
            let mut sub_state = self.subscription_state.write().await;
            sub_state.got(MonService::MonMap, epoch as u64);
        }

        // Update monmap state (moves monmap, so must be after epoch/subscription extraction)
        {
            let mut monmap_state = self.monmap_state.write().await;
            monmap_state.monmap = monmap;
            monmap_state.want_monmap = false;
        }

        // Broadcast MonMap update event
        let _ = self.map_events.send(MapEvent::MonMapUpdated { epoch });

        // Notify waiters that MonMap has arrived
        self.monmap_notify.notify_waiters();

        info!("MonMap updated successfully");
        Ok(())
    }

    /// Handle subscription ack
    async fn handle_subscribe_ack(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        let ack: MMonSubscribeAck = decode_message(&msg)?;
        info!(
            "Subscription acknowledged by legacy monitor: interval={}",
            ack.interval
        );

        let mut sub_state = self.subscription_state.write().await;
        sub_state.acked(ack.interval);

        Ok(())
    }

    /// Handle config update message
    async fn handle_config(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        let mconfig: MConfig = decode_message(&msg)?;

        {
            let mut runtime_config = self.runtime_config.write().await;
            runtime_config.update_from_map(&mconfig.config);
        }

        let keys: Vec<String> = mconfig.config.into_keys().collect();
        let _ = self.map_events.send(MapEvent::ConfigUpdated { keys });
        Ok(())
    }

    /// Handle OSDMap message
    async fn handle_osdmap(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        let osdmap: MOSDMap = decode_message(&msg)?;
        let epoch = osdmap.get_last();
        debug!("Received OSDMap: epoch={}", epoch);

        // Cache the latest OSDMap
        {
            let mut latest_osdmap = self.latest_osdmap.write().await;
            *latest_osdmap = Some(osdmap);
        }

        // Mark subscription as received
        {
            let mut sub_state = self.subscription_state.write().await;
            sub_state.got(MonService::OsdMap, epoch as u64);
        }

        // Broadcast OSDMap update event
        let _ = self.map_events.send(MapEvent::OsdMapUpdated {
            epoch: epoch as u64,
        });

        Ok(())
    }

    /// Handle version reply
    async fn handle_version_reply(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        let reply: MMonGetVersionReply = decode_message(&msg)?;

        // tid comes from the payload, not the message header
        let tid = reply.tid;

        debug!(
            "Received version reply: tid={}, version={}, oldest_version={}",
            tid, reply.version, reply.oldest_version
        );

        // Find and complete the pending version request
        if let Some((_, tracker)) = self.version_requests.remove(&tid) {
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
    async fn handle_command_ack(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        let ack: MMonCommandAck = decode_message(&msg)?;
        let tid = msg.header.get_tid();

        debug!(
            "Received command ack: tid={}, r={}, rs={}",
            tid, ack.r, ack.rs
        );

        if let Some((_, tracker)) = self.commands.remove(&tid) {
            // C++ MonClient passes rs (string status) and data (binary output)
            // as separate fields. Don't conflate them.
            let result = CommandResult::new(ack.r, ack.rs, msg.data);
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
    async fn handle_poolop_reply(&self, msg: crate::msgr2::message::Message) -> Result<()> {
        let reply: MPoolOpReply = decode_message(&msg)?;
        let tid = msg.tid();

        debug!(
            "Received pool op reply: tid={}, reply_code={}, epoch={}",
            tid, reply.reply_code, reply.epoch
        );

        if let Some((_, tracker)) = self.pool_ops.remove(&tid) {
            let result = PoolOpResult::new(reply.reply_code, reply.epoch, reply.response_data);
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
    pub async fn get_version(&self, what: MonService) -> Result<(u64, u64)> {
        let (tx, rx) = oneshot::channel();

        let active_con = self.require_active_con().await?;
        let tid = self.last_version_req_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.version_requests
            .insert(tid, VersionTracker { result_tx: tx });

        let msg = MMonGetVersion::new(tid, what);
        self.send_and_wait(&active_con, &msg, tid, rx)
            .await
            .inspect_err(|_| {
                self.version_requests.remove(&tid);
            })
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
    /// # use crate::monclient::MonClient;
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
        let (tx, rx) = oneshot::channel();

        let active_con = self.require_active_con().await?;
        let tid = self.last_command_tid.fetch_add(1, Ordering::SeqCst) + 1;

        let fsid = self.get_fsid().await;
        debug!("send_command: tid={}, cmd={:?}, fsid={}", tid, cmd, fsid);
        let msg = MMonCommand::new(cmd, inbl, fsid);

        self.commands.insert(
            tid,
            CommandTracker {
                result_tx: tx,
                cmd: msg.clone(),
            },
        );
        self.send_and_wait(&active_con, &msg, tid, rx)
            .await
            .inspect_err(|_| {
                self.commands.remove(&tid);
            })
    }

    /// Send a pool operation to the monitor cluster
    ///
    /// This is a low-level helper for sending MPoolOp messages.
    /// Most users should use OSDClient's create_pool() and delete_pool() methods instead.
    pub async fn send_poolop(&self, msg: MPoolOp) -> Result<PoolOpResult> {
        let (tx, rx) = oneshot::channel();

        let active_con = self.require_active_con().await?;
        let tid = self.last_poolop_tid.fetch_add(1, Ordering::SeqCst) + 1;
        self.pool_ops.insert(tid, PoolOpTracker { result_tx: tx });

        self.send_and_wait(&active_con, &msg, tid, rx)
            .await
            .inspect_err(|_| {
                self.pool_ops.remove(&tid);
            })
    }

    /// Send a pool operation and return an error if the reply indicates failure.
    async fn send_poolop_checked(&self, msg: MPoolOp, operation: &str) -> Result<()> {
        let result = self.send_poolop(msg).await?;
        if result.is_success() {
            Ok(())
        } else {
            Err(MonClientError::Other(format!(
                "{} failed with code {}",
                operation, result.reply_code
            )))
        }
    }

    /// Create a pool-level snapshot.
    ///
    /// # Arguments
    /// * `pool_id` - Pool ID to snapshot
    /// * `name` - Snapshot name
    ///
    /// # Returns
    /// `Ok(())` on success, error otherwise.
    pub async fn snap_create(&self, pool_id: u32, name: &str) -> Result<()> {
        let fsid = self.get_fsid().await;
        let msg = MPoolOp::create_snap(fsid.bytes, pool_id, name.to_string(), 0);
        self.send_poolop_checked(msg, "snap_create").await
    }

    /// Remove a pool-level snapshot by name.
    ///
    /// # Arguments
    /// * `pool_id` - Pool ID
    /// * `name` - Snapshot name to remove
    pub async fn snap_remove(&self, pool_id: u32, name: &str) -> Result<()> {
        let fsid = self.get_fsid().await;
        // snap_id = 0 here; the monitor resolves it from the name
        let msg = MPoolOp::delete_snap(fsid.bytes, pool_id, 0, name.to_string(), 0);
        self.send_poolop_checked(msg, "snap_remove").await
    }

    /// Check if connected to a monitor
    pub async fn is_connected(&self) -> bool {
        let conn_state = self.connection_state.read().await;
        conn_state.active_con.is_some()
    }

    /// Check if authenticated
    pub async fn is_authenticated(&self) -> bool {
        let auth_state = self.auth_state.read().await;
        auth_state.authenticated
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
    /// This waits until the MonClient has received a MonMapState from the monitor cluster.
    /// The MonMapState contains the cluster FSID and monitor addresses.
    ///
    /// Should be called after init() to ensure MonMapState is available before
    /// creating service clients that need the FSID.
    pub async fn wait_for_monmap(&self, timeout: std::time::Duration) -> Result<()> {
        wait_for_condition(
            || async {
                let monmap_state = self.monmap_state.read().await;
                if !monmap_state.want_monmap && monmap_state.monmap.fsid != uuid::Uuid::nil() {
                    info!("MonMapState received via event notification");
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
        let monmap_state = self.monmap_state.read().await;
        UuidD::from_bytes(*monmap_state.monmap.fsid.as_bytes())
    }

    /// Get the global ID assigned during authentication
    ///
    /// Returns the global ID assigned by the monitor during authentication.
    /// Returns 0 if not authenticated yet.
    pub async fn get_global_id(&self) -> u64 {
        let auth_state = self.auth_state.read().await;
        auth_state.global_id
    }

    /// Get the entity name string (e.g., "client.admin")
    ///
    /// Returns the entity name configured for this MonClient.
    /// This is useful for service clients that need to use the same entity name.
    pub fn get_entity_name_string(&self) -> String {
        self.entity_name.to_string()
    }

    /// Get our own entity address from the active monitor connection.
    ///
    /// This is the address we sent in CLIENT_IDENT and what the cluster uses for
    /// blocklist entries when fencing this client.  Returns `None` if no monitor
    /// connection is currently active.
    pub async fn get_client_addr(&self) -> Option<crate::EntityAddr> {
        let conn_state = self.connection_state.read().await;
        conn_state.active_con.as_ref().map(|c| *c.client_addr())
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
    /// # use crate::monclient::MonClient;
    /// # async fn example(mon_client: &MonClient) -> Result<(), Box<dyn std::error::Error>> {
    /// // After connecting to monitor, get service auth provider for OSDs
    /// if let Some(service_auth) = mon_client.get_service_auth_provider().await {
    ///     // Use service_auth to connect to OSDs
    ///     let config = crate::msgr2::ConnectionConfig::with_auth_provider(Box::new(service_auth));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_service_auth_provider(&self) -> Option<crate::auth::ServiceAuthProvider> {
        let conn_state = self.connection_state.read().await;
        if let Some(conn) = conn_state.active_con.as_ref() {
            conn.create_service_auth_provider()
        } else {
            None
        }
    }

    /// Get the number of monitors
    pub async fn get_mon_count(&self) -> usize {
        let monmap_state = self.monmap_state.read().await;
        monmap_state.monmap.len()
    }

    /// Get the current monmap epoch
    pub async fn get_monmap_epoch(&self) -> u32 {
        let monmap_state = self.monmap_state.read().await;
        monmap_state.monmap.epoch
    }

    /// Get monitor addresses by rank
    pub async fn get_mon_addrs(&self, rank: usize) -> Result<crate::EntityAddrvec> {
        let monmap_state = self.monmap_state.read().await;
        monmap_state
            .monmap
            .get_addrs(rank)
            .cloned()
            .ok_or(MonClientError::InvalidMonitorRank(rank))
    }

    /// Wait for a specific map version (async)
    pub async fn wait_for_map(&self, what: MonService, version: u64) -> Result<()> {
        // Check if we already have this version
        match what {
            MonService::MonMap => {
                let monmap_state = self.monmap_state.read().await;
                if monmap_state.monmap.epoch as u64 >= version {
                    return Ok(());
                }
            }
            MonService::OsdMap => {
                let latest_osdmap = self.latest_osdmap.read().await;
                if latest_osdmap
                    .as_ref()
                    .is_some_and(|osdmap| u64::from(osdmap.get_last()) >= version)
                {
                    return Ok(());
                }
            }
            MonService::Config => {
                return Err(MonClientError::Other(
                    "wait_for_map does not support config updates".into(),
                ));
            }
            MonService::MgrMap | MonService::MdsMap => {
                return Err(MonClientError::Other(format!(
                    "wait_for_map does not yet support {what}"
                )));
            }
        }

        // Subscribe if not already subscribed
        self.subscribe(what, version, 0).await?;

        let mut events = self.subscribe_events();
        let deadline = tokio::time::Instant::now() + self.command_timeout().await;

        match what {
            MonService::OsdMap => {
                self.wait_for_osdmap_epoch(&mut events, version, deadline)
                    .await
            }
            MonService::MonMap => loop {
                tokio::select! {
                    event = events.recv() => match event {
                        Ok(MapEvent::MonMapUpdated { epoch }) if u64::from(epoch) >= version => {
                            return Ok(());
                        }
                        Ok(_) => {}
                        Err(e) => return Err(MonClientError::Other(format!("Event channel error: {e}"))),
                    },
                    _ = tokio::time::sleep_until(deadline) => return Err(MonClientError::Timeout),
                }
            },
            _ => unreachable!(), // guarded above
        }
    }

    /// Wait until the cached OSDMap has epoch >= `min_epoch` or the deadline passes.
    async fn wait_for_osdmap_epoch(
        &self,
        events: &mut broadcast::Receiver<MapEvent>,
        min_epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    Ok(MapEvent::OsdMapUpdated { epoch }) if epoch >= min_epoch => return Ok(()),
                    Ok(_) => {}
                    Err(e) => return Err(MonClientError::Other(format!("Event channel error: {e}"))),
                },
                _ = tokio::time::sleep_until(deadline) => return Err(MonClientError::Timeout),
            }
        }
    }

    /// Subscribe to map events
    pub fn subscribe_events(&self) -> broadcast::Receiver<MapEvent> {
        self.map_events.subscribe()
    }

    /// Get the current OSDMap from the monitor
    ///
    /// This is a convenience wrapper that subscribes to osdmap and waits for the latest version.
    /// For more control, use the low-level subscribe/get_version APIs directly.
    ///
    /// # Returns
    /// The latest OSDMap received from the monitor
    ///
    /// # Errors
    /// Returns an error if not connected, subscription fails, or timeout occurs
    pub async fn get_osdmap(&self) -> Result<crate::monclient::MOSDMap> {
        // Subscribe to osdmap if not already subscribed
        self.subscribe(MonService::OsdMap, 0, 0).await?;

        // Get current version
        let (epoch, _) = self.get_version(MonService::OsdMap).await?;

        // Wait for that version via event channel.
        // Anchor the deadline to now so it doesn't reset on each loop iteration.
        let mut events = self.subscribe_events();
        let deadline = tokio::time::Instant::now() + self.command_timeout().await;

        // Check if the map is already cached (the event may have fired before subscribe_events).
        {
            let guard = self.latest_osdmap.read().await;
            if let Some(ref m) = *guard
                && u64::from(m.get_last()) >= epoch
            {
                return Ok(m.clone());
            }
        }

        self.wait_for_osdmap_epoch(&mut events, epoch, deadline)
            .await?;
        let guard = self.latest_osdmap.read().await;
        guard
            .clone()
            .ok_or_else(|| MonClientError::Other("OSDMap event received but map not cached".into()))
    }

    /// Get the current MonMapState
    ///
    /// Returns a cloned copy of the cached MonMapState.
    pub async fn get_monmap(&self) -> MonMapState {
        let monmap_state = self.monmap_state.read().await;
        monmap_state.monmap.clone()
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
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_create_client() {
        let auth = crate::monclient::auth_config::AuthConfig::no_auth("client.test".to_string());
        let config = MonClientConfig {
            mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
            auth: Some(auth),
            ..Default::default()
        };

        let client = MonClient::new(config, None).await.unwrap();
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_subscription() {
        let auth = crate::monclient::auth_config::AuthConfig::no_auth("client.test".to_string());
        let config = MonClientConfig {
            mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
            auth: Some(auth),
            ..Default::default()
        };

        let client = MonClient::new(config, None).await.unwrap();

        // Should fail before init
        assert!(client.subscribe(MonService::OsdMap, 0, 0).await.is_err());
    }

    #[test]
    fn test_runtime_config_update_from_map() {
        let config = MonClientConfig::default();
        let mut runtime_config = RuntimeMonClientConfig::from_config(&config);
        let mut updates = HashMap::new();
        updates.insert("mon_client_hunt_interval".to_string(), "5".to_string());
        updates.insert("mon_client_ping_interval".to_string(), "11".to_string());
        updates.insert("mon_client_ping_timeout".to_string(), "22".to_string());
        updates.insert("rados_mon_op_timeout".to_string(), "33".to_string());

        runtime_config.update_from_map(&updates);

        assert_eq!(
            runtime_config.mon_client_hunt_interval,
            Duration::from_secs(5)
        );
        assert_eq!(
            runtime_config.mon_client_ping_interval,
            Duration::from_secs(11)
        );
        assert_eq!(
            runtime_config.mon_client_ping_timeout,
            Duration::from_secs(22)
        );
        assert_eq!(runtime_config.rados_mon_op_timeout, Duration::from_secs(33));
    }

    #[test]
    fn test_parse_duration_option() {
        assert_eq!(
            RuntimeMonClientConfig::parse_option::<Duration>("1.5s"),
            Some(Duration::from_secs_f64(1.5))
        );
        assert_eq!(
            RuntimeMonClientConfig::parse_option::<Duration>("2.25"),
            Some(Duration::from_secs_f64(2.25))
        );
        assert_eq!(RuntimeMonClientConfig::parse_option::<Duration>("-1"), None);
        assert_eq!(
            RuntimeMonClientConfig::parse_option::<Duration>("inf"),
            None
        );
        assert_eq!(
            RuntimeMonClientConfig::parse_option::<Duration>("NaN"),
            None
        );
        assert_eq!(
            RuntimeMonClientConfig::parse_option::<Duration>("not-a-duration"),
            None
        );
    }

    #[test]
    fn test_parse_option_generic() {
        assert_eq!(RuntimeMonClientConfig::parse_option::<u64>("42"), Some(42));
        assert_eq!(RuntimeMonClientConfig::parse_option::<u64>("abc"), None);
    }
}
